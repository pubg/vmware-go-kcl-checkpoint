package test

import (
	kc "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/logger"
)

type RecordProcessorFactory struct {
	log logger.Logger
}

func NewRecordProcessorFactory(log logger.Logger) kc.IRecordProcessorFactory {
	return &RecordProcessorFactory{
		log: log,
	}
}

func (f *RecordProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &RecordProcessor{
		log: f.log,
	}
}
