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
