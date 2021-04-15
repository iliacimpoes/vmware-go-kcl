/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package worker

import (
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	kcl "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

// ShardConsumer is responsible for consuming data records of a (specified) shard.
// Note: ShardConsumer only deal with one shard.
type ShardConsumer struct {
	commonShardConsumer
	streamName string
	stop       *chan struct{}
	consumerID string
	mService   metrics.MonitoringService
}

func (sc *ShardConsumer) getShardIterator() (*string, error) {
	startPosition, err := sc.getStartingPosition()
	if err != nil {
		return nil, err
	}
	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:                &sc.shard.ID,
		ShardIteratorType:      startPosition.Type,
		StartingSequenceNumber: startPosition.SequenceNumber,
		Timestamp:              startPosition.Timestamp,
		StreamName:             &sc.streamName,
	}
	iterResp, err := sc.kc.GetShardIterator(shardIterArgs)
	if err != nil {
		return nil, err
	}
	return iterResp.ShardIterator, nil
}

// getRecords continously poll one shard for data record
// Precondition: it currently has the lease on the shard.
func (sc *ShardConsumer) getRecords() error {
	defer sc.releaseLease()

	log := sc.kclConfig.Logger

	// If the shard is child shard, need to wait until the parent finished.
	if err := sc.waitOnParentShard(); err != nil {
		// If parent shard has been deleted by Kinesis system already, just ignore the error.
		if err != chk.ErrSequenceIDNotFound {
			log.Errorf("Error in waiting for parent shard: %v to finish. Error: %+v", sc.shard.ParentShardId, err)
			return err
		}
	}

	shardIterator, err := sc.getShardIterator()
	if err != nil {
		log.Errorf("Unable to get shard iterator for %s: %v", sc.shard.ID, err)
		return err
	}

	// Start processing events and notify record processor on shard and starting checkpoint
	input := &kcl.InitializationInput{
		ShardId:                sc.shard.ID,
		ExtendedSequenceNumber: &kcl.ExtendedSequenceNumber{SequenceNumber: aws.String(sc.shard.GetCheckpoint())},
	}
	sc.recordProcessor.Initialize(input)

	recordCheckpointer := NewRecordProcessorCheckpoint(sc.shard, sc.checkpointer)
	retriedErrors := 0

	for {
		if time.Now().UTC().After(sc.shard.LeaseTimeout.Add(-time.Duration(sc.kclConfig.LeaseRefreshPeriodMillis) * time.Millisecond)) {
			log.Debugf("Refreshing lease on shard: %s for worker: %s", sc.shard.ID, sc.consumerID)
			err = sc.checkpointer.GetLease(sc.shard, sc.consumerID)
			if err != nil {
				if err.Error() == chk.ErrLeaseNotAquired {
					log.Warnf("Failed in acquiring lease on shard: %s for worker: %s", sc.shard.ID, sc.consumerID)
					return nil
				}
				// log and return error
				log.Errorf("Error in refreshing lease on shard: %s for worker: %s. Error: %+v",
					sc.shard.ID, sc.consumerID, err)
				return err
			}
		}

		getRecordsStartTime := time.Now()

		log.Debugf("Trying to read %d record from iterator: %v", sc.kclConfig.MaxRecords, aws.StringValue(shardIterator))
		getRecordsArgs := &kinesis.GetRecordsInput{
			Limit:         aws.Int64(int64(sc.kclConfig.MaxRecords)),
			ShardIterator: shardIterator,
		}
		// Get records from stream and retry as needed
		getResp, err := sc.kc.GetRecords(getRecordsArgs)
		if err != nil {
			if awsErrCode(err) == kinesis.ErrCodeProvisionedThroughputExceededException || awsErrCode(err) == kinesis.ErrCodeKMSThrottlingException {
				log.Errorf("Error getting records from shard %v: %+v", sc.shard.ID, err)
				retriedErrors++
				// exponential backoff
				// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff
				time.Sleep(time.Duration(math.Exp2(float64(retriedErrors))*100) * time.Millisecond)
				continue
			}
			log.Errorf("Error getting records from Kinesis that cannot be retried: %+v Request: %s", err, getRecordsArgs)
			return err
		}

		// Convert from nanoseconds to milliseconds
		getRecordsTime := time.Since(getRecordsStartTime) / 1000000
		sc.mService.RecordGetRecordsTime(sc.shard.ID, float64(getRecordsTime))

		// reset the retry count after success
		retriedErrors = 0

		// IRecordProcessorCheckpointer
		processRecordsInput := &kcl.ProcessRecordsInput{
			Records:            getResp.Records,
			MillisBehindLatest: aws.Int64Value(getResp.MillisBehindLatest),
			Checkpointer:       recordCheckpointer,
		}
		sc.processRecords(processRecordsInput)

		// The shard has been closed, so no new records can be read from it
		if getResp.NextShardIterator == nil {
			log.Infof("Shard %s closed", sc.shard.ID)
			shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.TERMINATE, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		}
		shardIterator = getResp.NextShardIterator

		// Idle between each read, the user is responsible for checkpoint the progress
		// This value is only used when no records are returned; if records are returned, it should immediately
		// retrieve the next set of records.
		if len(getResp.Records) == 0 && processRecordsInput.MillisBehindLatest < int64(sc.kclConfig.IdleTimeBetweenReadsInMillis) {
			time.Sleep(time.Duration(sc.kclConfig.IdleTimeBetweenReadsInMillis) * time.Millisecond)
		}

		select {
		case <-*sc.stop:
			shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.REQUESTED, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		default:
		}
	}
}

// commonShardConsumer implements common functionality for regular and enhanced fan-out consumers
type commonShardConsumer struct {
	shard           *par.ShardStatus
	kc              kinesisiface.KinesisAPI
	checkpointer    chk.Checkpointer
	recordProcessor kcl.IRecordProcessor
	kclConfig       *config.KinesisClientLibConfiguration
	mService        metrics.MonitoringService
}

// Cleanup the internal lease cache
func (sc *commonShardConsumer) releaseLease() {
	log := sc.kclConfig.Logger
	log.Infof("Release lease for shard %s", sc.shard.ID)
	sc.shard.SetLeaseOwner("")

	// Release the lease by wiping out the lease owner for the shard
	// Note: we don't need to do anything in case of error here and shard lease will eventually be expired.
	if err := sc.checkpointer.RemoveLeaseOwner(sc.shard.ID); err != nil {
		log.Errorf("Failed to release shard lease or shard: %s Error: %+v", sc.shard.ID, err)
	}

	// reporting lease lose metrics
	sc.mService.LeaseLost(sc.shard.ID)
}

// getStartingPosition gets kinesis stating position.
// First try to fetch checkpoint. If checkpoint is not found use InitialPositionInStream
func (sc *commonShardConsumer) getStartingPosition() (*kinesis.StartingPosition, error) {
	err := sc.checkpointer.FetchCheckpoint(sc.shard)
	if err != nil && err != chk.ErrSequenceIDNotFound {
		return nil, err
	}

	checkpoint := sc.shard.GetCheckpoint()
	if checkpoint != "" {
		sc.kclConfig.Logger.Debugf("Start shard: %v at checkpoint: %v", sc.shard.ID, checkpoint)
		return &kinesis.StartingPosition{
			Type:           aws.String("AFTER_SEQUENCE_NUMBER"),
			SequenceNumber: &checkpoint,
		}, nil
	}

	shardIteratorType := config.InitalPositionInStreamToShardIteratorType(sc.kclConfig.InitialPositionInStream)
	sc.kclConfig.Logger.Debugf("No checkpoint recorded for shard: %v, starting with: %v", sc.shard.ID, aws.StringValue(shardIteratorType))

	if sc.kclConfig.InitialPositionInStream == config.AT_TIMESTAMP {
		return &kinesis.StartingPosition{
			Type:      shardIteratorType,
			Timestamp: sc.kclConfig.InitialPositionInStreamExtended.Timestamp,
		}, nil
	}

	return &kinesis.StartingPosition{
		Type: shardIteratorType,
	}, nil
}

// Need to wait until the parent shard finished
func (sc *commonShardConsumer) waitOnParentShard() error {
	if len(sc.shard.ParentShardId) == 0 {
		return nil
	}

	pshard := &par.ShardStatus{
		ID:  sc.shard.ParentShardId,
		Mux: &sync.RWMutex{},
	}

	for {
		if err := sc.checkpointer.FetchCheckpoint(pshard); err != nil {
			return err
		}

		// Parent shard is finished.
		if pshard.GetCheckpoint() == chk.SHARD_END {
			return nil
		}

		time.Sleep(time.Duration(sc.kclConfig.ParentShardPollIntervalMillis) * time.Millisecond)
	}
}

func (sc *commonShardConsumer) processRecords(input *kcl.ProcessRecordsInput) {
	recordLength := len(input.Records)
	recordBytes := int64(0)
	sc.kclConfig.Logger.Debugf("Received %d records, MillisBehindLatest: %v", recordLength, input.MillisBehindLatest)

	for _, r := range input.Records {
		recordBytes += int64(len(r.Data))
	}

	if recordLength > 0 || sc.kclConfig.CallProcessRecordsEvenForEmptyRecordList {
		processRecordsStartTime := time.Now()

		// Delivery the events to the record processor
		sc.recordProcessor.ProcessRecords(input)

		// Convert from nanoseconds to milliseconds
		processedRecordsTiming := time.Since(processRecordsStartTime) / 1000000
		sc.mService.RecordProcessRecordsTime(sc.shard.ID, float64(processedRecordsTiming))
	}

	sc.mService.IncrRecordsProcessed(sc.shard.ID, recordLength)
	sc.mService.IncrBytesProcessed(sc.shard.ID, recordBytes)
	sc.mService.MillisBehindLatest(sc.shard.ID, float64(input.MillisBehindLatest))
}
