package test

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/joho/godotenv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	wk "github.com/vmware/vmware-go-kcl/clientlibrary/worker"
	logger "github.com/vmware/vmware-go-kcl/logger"

	redis "github.com/bitofsky/vmware-go-kcl-checkpoint/redis"
)

const ()

type Profiles struct {
	kinesis  string
	dynamodb string
}

var (
	appName    = ""
	streamName = ""
	regionName = ""
	workerID   = ""
	profiles   = Profiles{
		kinesis:  "",
		dynamodb: "",
	}
	config = logger.Configuration{
		EnableConsole: true,
		ConsoleLevel:  logger.Info,
	}
)

func init() {
	godotenv.Load()

	appName = os.Getenv("APP_NAME")
	workerID = os.Getenv("WORKER_ID")
	streamName = os.Getenv("STREAM_NAME")
	regionName = os.Getenv("REGION_NAME")

	config = logger.Configuration{
		EnableConsole: true,
		ConsoleLevel:  logger.Info,
	}

	profiles = Profiles{
		kinesis:  os.Getenv("PROFILES_KINESIS"),
		dynamodb: os.Getenv("PROFILES_DYNAMODB"),
	}
}

func createWorker(workerID string, log logger.Logger) *wk.Worker {

	sessKinesis := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           profiles.kinesis,
	}))

	// sessDynamo := session.Must(session.NewSessionWithOptions(session.Options{
	// 	SharedConfigState: session.SharedConfigEnable,
	// 	Profile:           profiles.dynamodb,
	// }))

	kclConfig := cfg.NewKinesisClientLibConfig(appName, streamName, regionName, workerID).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(1000 * 5).
		WithFailoverTimeMillis(1000 * 300).
		WithLogger(log)

	kc := kinesis.New(sessKinesis, aws.NewConfig().WithRegion(regionName))
	checkpointer := redis.NewRedisCheckpoint(kclConfig, &redis.RedisCheckpointOptions{Prefix: "kinesis:consumer:" + kclConfig.StreamName + ":" + kclConfig.ApplicationName + ":"})

	factory := NewRecordProcessorFactory(kclConfig.Logger)

	worker := wk.NewWorker(factory, kclConfig).
		WithCheckpointer(checkpointer).
		WithKinesis(kc)

	err := worker.Start()

	if err != nil {
		panic(err)
	}

	return worker

}

func publishData(t *testing.T) {
	sessKinesis := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           profiles.kinesis,
	}))

	kc := kinesis.New(sessKinesis, aws.NewConfig().WithRegion(regionName))

	// publishSomeData(t, kc)
	publishAggregateRecord(t, kc)
	publishAggregateRecord(t, kc)
	publishAggregateRecord(t, kc)
	publishAggregateRecord(t, kc)
}

func TestRedis(t *testing.T) {

	ctx, kill := context.WithCancel(context.Background())

	log := logger.NewLogrusLoggerWithConfig(config)

	var workers []*wk.Worker

	workers = append(workers, createWorker(workerID+"-0", log))
	workers = append(workers, createWorker(workerID+"-1", log))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Signal processing.
	go func() {
		sig := <-sigs
		log.Warnf("Received signal %s. Exiting", sig)

		for _, worker := range workers {
			worker.Shutdown()
		}

		kill()
	}()

	<-time.After(10 * time.Second)

	go publishData(t)

	<-ctx.Done()

}
