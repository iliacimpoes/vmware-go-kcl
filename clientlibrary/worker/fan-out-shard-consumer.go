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
package worker

import (
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

// FanOutShardConsumer is responsible for consuming data records of a (specified) shard.
// Note: FanOutShardConsumer only deal with one shard.
type FanOutShardConsumer struct {
	streamName      string
	consumerARN     string
	shard           *par.ShardStatus
	kc              kinesisiface.KinesisAPI
	checkpointer    chk.Checkpointer
	recordProcessor kcl.IRecordProcessor
	kclConfig       *config.KinesisClientLibConfiguration
	stop            *chan struct{}
	consumerID      string
	mService        metrics.MonitoringService
	state           ShardConsumerState
}

func (sc *FanOutShardConsumer) subscribeToShard(shard *par.ShardStatus) (*kinesis.SubscribeToShardOutput, error) {
	log := sc.kclConfig.Logger

	// Get checkpoint of the shard from dynamoDB
	err := sc.checkpointer.FetchCheckpoint(shard)
	if err != nil && err != chk.ErrSequenceIDNotFound {
		return nil, err
	}

	startPosition := &kinesis.StartingPosition{
		Type:           aws.String("AFTER_SEQUENCE_NUMBER"),
		SequenceNumber: &shard.Checkpoint,
	}

	// If there isn't any checkpoint for the shard, use the configuration value.
	if shard.Checkpoint == "" {
		initPos := sc.kclConfig.InitialPositionInStream
		shardIteratorType := config.InitalPositionInStreamToShardIteratorType(initPos)
		log.Debugf("No checkpoint recorded for shard: %v, starting with: %v", shard.ID,
			aws.StringValue(shardIteratorType))

		if initPos == config.AT_TIMESTAMP {
			startPosition = &kinesis.StartingPosition{
				Type:      shardIteratorType,
				Timestamp: sc.kclConfig.InitialPositionInStreamExtended.Timestamp,
			}
		} else {
			startPosition = &kinesis.StartingPosition{
				Type: shardIteratorType,
			}
		}
	}

	return sc.kc.SubscribeToShard(&kinesis.SubscribeToShardInput{
		ConsumerARN:      &sc.consumerARN,
		ShardId:          &shard.ID,
		StartingPosition: startPosition,
	})
}

// getRecords continously poll one shard for data record
// Precondition: it currently has the lease on the shard.
func (sc *FanOutShardConsumer) getRecords(shard *par.ShardStatus) error {
	defer sc.releaseLease(shard)

	log := sc.kclConfig.Logger

	// If the shard is child shard, need to wait until the parent finished.
	if err := sc.waitOnParentShard(shard); err != nil {
		// If parent shard has been deleted by Kinesis system already, just ignore the error.
		if err != chk.ErrSequenceIDNotFound {
			log.Errorf("Error in waiting for parent shard: %v to finish. Error: %+v", shard.ParentShardId, err)
			return err
		}
	}

	// Start processing events and notify record processor on shard and starting checkpoint
	input := &kcl.InitializationInput{
		ShardId:                shard.ID,
		ExtendedSequenceNumber: &kcl.ExtendedSequenceNumber{SequenceNumber: aws.String(shard.Checkpoint)},
	}
	sc.recordProcessor.Initialize(input)

	recordCheckpointer := NewRecordProcessorCheckpoint(shard, sc.checkpointer)

	shardSub, err := sc.subscribeToShard(shard)
	if err != nil {
		log.Errorf("Unable to get shard iterator for %s: %v", shard.ID, err)
		return err
	}
	defer func() {
		err = shardSub.EventStream.Close()
		if err != nil {
			log.Errorf("Unable to close event stream for %s: %v", shard.ID, err)
		}
	}()
	for {
		var continuationSequenceNumber string
		for event := range shardSub.EventStream.Events() {
			if time.Now().UTC().After(shard.LeaseTimeout.Add(-time.Duration(sc.kclConfig.LeaseRefreshPeriodMillis) * time.Millisecond)) {
				log.Debugf("Refreshing lease on shard: %s for worker: %s", shard.ID, sc.consumerID)
				err = sc.checkpointer.GetLease(shard, sc.consumerID)
				if err != nil {
					if err.Error() == chk.ErrLeaseNotAquired {
						log.Warnf("Failed in acquiring lease on shard: %s for worker: %s", shard.ID, sc.consumerID)
						return nil
					}
					// log and return error
					log.Errorf("Error in refreshing lease on shard: %s for worker: %s. Error: %+v",
						shard.ID, sc.consumerID, err)
					return err
				}
			}
			subEvent, ok := event.(*kinesis.SubscribeToShardEvent)
			if !ok {
				log.Errorf("Received unexpected event type: %T", event)
				continue
			}

			getRecordsStartTime := time.Now()
			log.Debugf("Received events from shard")

			// Convert from nanoseconds to milliseconds
			getRecordsTime := time.Since(getRecordsStartTime) / 1000000
			sc.mService.RecordGetRecordsTime(shard.ID, float64(getRecordsTime))

			input := &kcl.ProcessRecordsInput{
				Records:            subEvent.Records,
				MillisBehindLatest: aws.Int64Value(subEvent.MillisBehindLatest),
				Checkpointer:       recordCheckpointer,
			}

			recordLength := len(input.Records)
			log.Debugf("Received %d records, MillisBehindLatest: %v", recordLength, input.MillisBehindLatest)

			recordBytes := int64(0)
			for _, r := range subEvent.Records {
				recordBytes += int64(len(r.Data))
			}

			if recordLength > 0 || sc.kclConfig.CallProcessRecordsEvenForEmptyRecordList {
				processRecordsStartTime := time.Now()

				// Delivery the events to the record processor
				sc.recordProcessor.ProcessRecords(input)

				// Convert from nanoseconds to milliseconds
				processedRecordsTiming := time.Since(processRecordsStartTime) / 1000000
				sc.mService.RecordProcessRecordsTime(shard.ID, float64(processedRecordsTiming))
			}

			sc.mService.IncrRecordsProcessed(shard.ID, recordLength)
			sc.mService.IncrBytesProcessed(shard.ID, recordBytes)
			sc.mService.MillisBehindLatest(shard.ID, float64(*subEvent.MillisBehindLatest))

			// The shard has been closed, so no new records can be read from it
			if subEvent.ContinuationSequenceNumber == nil {
				log.Infof("Shard %s closed", shard.ID)
				shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.TERMINATE, Checkpointer: recordCheckpointer}
				sc.recordProcessor.Shutdown(shutdownInput)
				return nil
			}
			continuationSequenceNumber = *subEvent.ContinuationSequenceNumber

			select {
			case <-*sc.stop:
				shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.REQUESTED, Checkpointer: recordCheckpointer}
				sc.recordProcessor.Shutdown(shutdownInput)
				return nil
			default:
			}
		}
		// need to resubscribe to shard
		err = shardSub.EventStream.Close()
		if err != nil {
			log.Errorf("Unable to close event stream for %s: %v", shard.ID, err)
		}
		startPosition := &kinesis.StartingPosition{
			Type:           aws.String("AFTER_SEQUENCE_NUMBER"),
			SequenceNumber: &continuationSequenceNumber,
		}
		shardSub, err = sc.kc.SubscribeToShard(&kinesis.SubscribeToShardInput{
			ConsumerARN:      &sc.consumerARN,
			ShardId:          &shard.ID,
			StartingPosition: startPosition,
		})
		if err != nil {
			log.Errorf("Unable to get shard iterator for %s: %v", shard.ID, err)
			return err
		}
	}
}

// Need to wait until the parent shard finished
func (sc *FanOutShardConsumer) waitOnParentShard(shard *par.ShardStatus) error {
	if len(shard.ParentShardId) == 0 {
		return nil
	}

	pshard := &par.ShardStatus{
		ID:  shard.ParentShardId,
		Mux: &sync.Mutex{},
	}

	for {
		if err := sc.checkpointer.FetchCheckpoint(pshard); err != nil {
			return err
		}

		// Parent shard is finished.
		if pshard.Checkpoint == chk.SHARD_END {
			return nil
		}

		time.Sleep(time.Duration(sc.kclConfig.ParentShardPollIntervalMillis) * time.Millisecond)
	}
}

// Cleanup the internal lease cache
func (sc *FanOutShardConsumer) releaseLease(shard *par.ShardStatus) {
	log := sc.kclConfig.Logger
	log.Infof("Release lease for shard %s", shard.ID)
	shard.SetLeaseOwner("")

	// Release the lease by wiping out the lease owner for the shard
	// Note: we don't need to do anything in case of error here and shard lease will eventuall be expired.
	if err := sc.checkpointer.RemoveLeaseOwner(shard.ID); err != nil {
		log.Errorf("Failed to release shard lease or shard: %s Error: %+v", shard.ID, err)
	}

	// reporting lease lose metrics
	sc.mService.LeaseLost(shard.ID)
}
