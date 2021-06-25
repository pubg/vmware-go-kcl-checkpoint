package test

import (
	kc "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/logger"
)

type RecordProcessor struct {
	ShardID string
	log     logger.Logger
	count   uint64
}

func (r *RecordProcessor) Initialize(input *kc.InitializationInput) {
	r.log.Infof("Processing SharId: %v at checkpoint: %v", input.ShardId, input.ExtendedSequenceNumber.SequenceNumber)
	r.ShardID = input.ShardId
	r.count = 0
}

func (r *RecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	r.log.Infof("Processing Records...")

	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	for _, v := range input.Records {
		r.log.Infof("Record = %s", v.Data)
		r.count++
	}

	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber
	diff := input.CacheExitTime.Sub(*input.CacheEntryTime)

	r.log.Infof("Checkpoint progress at: %v,  MillisBehindLatest = %v, KCLProcessTime = %v", lastRecordSequenceNumber, input.MillisBehindLatest, diff)
	input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
}

func (r *RecordProcessor) Shutdown(input *kc.ShutdownInput) {
	r.log.Warnf("Processed Record Count = %d", r.count)
	r.log.Warnf("Shutdown Reason: %v", kc.ShutdownReasonMessage(input.ShutdownReason))

	if input.ShutdownReason == kc.TERMINATE {
		input.Checkpointer.Checkpoint(nil)
	}
}
