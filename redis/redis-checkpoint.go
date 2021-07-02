package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	rsGoredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"

	chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

type ErrLeaseNotAcquired struct {
	cause string
}

// RedisCheckpoint implements the Checkpoint interface using Redis as a backend
type RedisCheckpoint struct {
	prefix        string
	redisEndpoint string

	LeaseDuration int
	svc           *goredis.Client
	kclConfig     *cfg.KinesisClientLibConfiguration
	lastLeaseSync time.Time
	rs            *redsync.Redsync
	mutex         *redsync.Mutex
}

type RedisCheckpointOptions struct {
	Prefix        string
	RedisEndpoint string
}

type ShardCheckpoint struct {
	ShardID       string
	LeaseTimeout  string
	AssignedTo    string
	Checkpoint    string
	ParentShardId string
	ClaimRequest  string
}

var (
	ctx = context.Background()
)

func (e ErrLeaseNotAcquired) Error() string {
	return fmt.Sprintf("lease not acquired: %s", e.cause)
}

func NewRedisCheckpoint(kclConfig *cfg.KinesisClientLibConfiguration, options *RedisCheckpointOptions) *RedisCheckpoint {

	prefix := options.Prefix
	redisEndpoint := options.RedisEndpoint

	if prefix == "" {
		prefix = "kinesis:consumer:" + kclConfig.StreamName + ":" + kclConfig.ApplicationName + ":"
	}

	if redisEndpoint == "" {
		redisEndpoint = "localhost:6379"
	}

	checkpointer := &RedisCheckpoint{
		prefix:        prefix,
		redisEndpoint: redisEndpoint,
		LeaseDuration: kclConfig.FailoverTimeMillis,
		kclConfig:     kclConfig,
	}

	return checkpointer
}

// WithRedis is used to provide Redis service
func (checkpointer *RedisCheckpoint) WithRedis(svc *goredis.Client) *RedisCheckpoint {
	checkpointer.svc = svc
	return checkpointer
}

// Init initialises the Redis Checkpoint
func (checkpointer *RedisCheckpoint) Init() error {

	checkpointer.kclConfig.Logger.Debugf("Creating Redis session")

	if checkpointer.svc == nil {
		s, err := NewRedisClient(ctx, checkpointer.redisEndpoint)

		if err != nil {
			checkpointer.kclConfig.Logger.Errorf("Redis Init Error - %s", err)
		}

		checkpointer.svc = s
	}

	pool := rsGoredis.NewPool(checkpointer.svc)
	checkpointer.rs = redsync.New(pool)

	return nil
}

// GetLease attempts to gain a lock on the given shard
func (checkpointer *RedisCheckpoint) GetLease(shard *par.ShardStatus, newAssignTo string) error {

	if mutex, err := checkpointer.lock(shard.ID); err != nil {
		return err
	} else {
		defer checkpointer.unlock(shard.ID, mutex)
	}

	isClaimRequestExpired := shard.IsClaimRequestExpired(checkpointer.kclConfig)

	newLeaseTimeout := time.Now().Add(time.Duration(checkpointer.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339)
	newCheckpoint := &ShardCheckpoint{shard.ID, newLeaseTimeoutString, newAssignTo, "", "", ""}
	currentCheckpoint, err := checkpointer.getItem(shard.ID)

	if err != nil {
		return err
	}

	var claimRequest string
	if checkpointer.kclConfig.EnableLeaseStealing {
		currentCheckpointClaimRequest := currentCheckpoint.ClaimRequest
		if currentCheckpointClaimRequest != "" {
			claimRequest = currentCheckpointClaimRequest
			if newAssignTo != claimRequest && !isClaimRequestExpired {
				checkpointer.kclConfig.Logger.Debugf("another worker: %s has a claim on this shard. Not going to renew the lease", claimRequest)
				return errors.New(chk.ErrShardClaimed)
			}
		}
	}

	assignedTo := currentCheckpoint.AssignedTo
	leaseTimeout := currentCheckpoint.LeaseTimeout

	if assignedTo != "" && leaseTimeout != "" {
		if currentLeaseTimeout, err := time.Parse(time.RFC3339, leaseTimeout); err != nil {
			return err
		} else if checkpointer.kclConfig.EnableLeaseStealing {
			if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo && !isClaimRequestExpired {
				return ErrLeaseNotAcquired{fmt.Sprintf("current lease timeout not yet expired shard: %s, leaseTimeout: %s, assignedTo: %s, newAssignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo, newAssignTo)}
			}
		} else {
			if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo {
				return ErrLeaseNotAcquired{fmt.Sprintf("current lease timeout not yet expired shard: %s, leaseTimeout: %s, assignedTo: %s, newAssignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo, newAssignTo)}
			}
			checkpointer.kclConfig.Logger.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s, newAssignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo, newAssignTo)
		}
	}

	if len(shard.ParentShardId) > 0 {
		newCheckpoint.ParentShardId = shard.ParentShardId
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint != "" {
		newCheckpoint.Checkpoint = checkpoint
	}

	err = checkpointer.putItem(newCheckpoint)

	if err != nil {
		return ErrLeaseNotAcquired{err.Error()}
	}

	shard.Mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Mux.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
func (checkpointer *RedisCheckpoint) CheckpointSequence(shard *par.ShardStatus) error {

	if mutex, err := checkpointer.lock(shard.ID); err != nil {
		return err
	} else {
		defer checkpointer.unlock(shard.ID, mutex)
	}

	leaseTimeout := shard.GetLeaseTimeout().UTC().Format(time.RFC3339)
	newCheckpoint := &ShardCheckpoint{
		ShardID:      shard.ID,
		Checkpoint:   shard.GetCheckpoint(),
		AssignedTo:   shard.GetLeaseOwner(),
		LeaseTimeout: leaseTimeout,
	}

	if len(shard.ParentShardId) > 0 {
		newCheckpoint.ParentShardId = shard.ParentShardId
	}

	return checkpointer.putItem(newCheckpoint)
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (checkpointer *RedisCheckpoint) FetchCheckpoint(shard *par.ShardStatus) error {

	if mutex, err := checkpointer.lock(shard.ID); err != nil {
		return err
	} else {
		defer checkpointer.unlock(shard.ID, mutex)
	}

	checkpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	if checkpoint.Checkpoint == "" {
		return chk.ErrSequenceIDNotFound
	}

	sequenceID := checkpoint.Checkpoint

	checkpointer.kclConfig.Logger.Debugf("Retrieved Shard Iterator %s", sequenceID)
	shard.SetCheckpoint(sequenceID)
	shard.SetLeaseOwner(checkpoint.AssignedTo)

	// Use up-to-date leaseTimeout to avoid ConditionalCheckFailedException when claiming
	if checkpoint.LeaseTimeout != "" {
		if currentLeaseTimeout, err := time.Parse(time.RFC3339, checkpoint.LeaseTimeout); err != nil {
			return err
		} else {
			shard.LeaseTimeout = currentLeaseTimeout
		}
	}

	return nil
}

// RemoveLeaseInfo to remove lease info for shard entry in dynamoDB because the shard no longer exists in Kinesis
func (checkpointer *RedisCheckpoint) RemoveLeaseInfo(shardID string) error {

	if mutex, err := checkpointer.lock(shardID); err != nil {
		return err
	} else {
		defer checkpointer.unlock(shardID, mutex)
	}

	err := checkpointer.removeItem(shardID)

	if err != nil {
		checkpointer.kclConfig.Logger.Errorf("Error in removing lease info for shard: %s, Error: %+v", shardID, err)
	} else {
		checkpointer.kclConfig.Logger.Debugf("Lease info for shard: %s has been removed.", shardID)
	}

	return err
}

// RemoveLeaseOwner to remove lease owner for the shard entry
func (checkpointer *RedisCheckpoint) RemoveLeaseOwner(shardID string) error {

	if mutex, err := checkpointer.lock(shardID); err != nil {
		return err
	} else {
		defer checkpointer.unlock(shardID, mutex)
	}

	if cp, err := checkpointer.getItem(shardID); err != nil {
		return err
	} else if cp.AssignedTo != checkpointer.kclConfig.WorkerID {
		return fmt.Errorf("RemoveLeaseOwner invalid AssignedTo")
	} else {
		return checkpointer.removeItem(shardID)
	}
}

// ListActiveWorkers returns a map of workers and their shards
func (checkpointer *RedisCheckpoint) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	checkpointer.kclConfig.Logger.Debugf("ListActiveWorkers WorkerID: %s", checkpointer.kclConfig.WorkerID)
	err := checkpointer.syncLeases(shardStatus)
	if err != nil {
		return nil, err
	}

	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == chk.ShardEnd {
			continue
		}

		if leaseOwner := shard.GetLeaseOwner(); leaseOwner == "" {
			checkpointer.kclConfig.Logger.Debugf("Shard Not Assigned Error. ShardID: %s, WorkerID: %s", shard.ID, checkpointer.kclConfig.WorkerID)
			return nil, chk.ErrShardNotAssigned
		} else if w, ok := workers[leaseOwner]; ok {
			workers[leaseOwner] = append(w, shard)
		} else {
			workers[leaseOwner] = []*par.ShardStatus{shard}
		}
	}
	return workers, nil
}

// ClaimShard places a claim request on a shard to signal a steal attempt
func (checkpointer *RedisCheckpoint) ClaimShard(shard *par.ShardStatus, claimID string) error {

	if err := checkpointer.FetchCheckpoint(shard); err != nil && err != chk.ErrSequenceIDNotFound {
		return err
	}

	if mutex, err := checkpointer.lock(shard.ID); err != nil {
		return err
	} else {
		defer checkpointer.unlock(shard.ID, mutex)
	}

	leaseTimeoutString := shard.GetLeaseTimeout().Format(time.RFC3339)
	newCheckpoint := &ShardCheckpoint{
		ShardID:      shard.ID,
		LeaseTimeout: leaseTimeoutString,
		Checkpoint:   shard.Checkpoint,
		ClaimRequest: claimID,
	}
	currentCheckpoint, err := checkpointer.getItem(shard.ID)

	if err != nil {
		return err
	}

	if leaseOwner := shard.GetLeaseOwner(); leaseOwner == currentCheckpoint.AssignedTo {
		newCheckpoint.AssignedTo = leaseOwner
	} else {
		return fmt.Errorf("ClaimShard invalid leaseOwner")
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint == chk.ShardEnd && currentCheckpoint.Checkpoint != chk.ShardEnd {
		newCheckpoint.Checkpoint = chk.ShardEnd
	} else if checkpoint == currentCheckpoint.Checkpoint {
		newCheckpoint.Checkpoint = checkpoint
	} else {
		return fmt.Errorf("ClaimShard invalid checkpoint")
	}

	if shard.ParentShardId == currentCheckpoint.ParentShardId {
		newCheckpoint.ParentShardId = shard.ParentShardId
	} else {
		return fmt.Errorf("ClaimShard invalid ParentShardId")
	}

	return checkpointer.putItem(newCheckpoint)
}

func (checkpointer *RedisCheckpoint) syncLeases(shardStatus map[string]*par.ShardStatus) error {
	log := checkpointer.kclConfig.Logger

	if (checkpointer.lastLeaseSync.Add(time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis) * time.Millisecond)).After(time.Now()) {
		return nil
	}

	checkpointer.lastLeaseSync = time.Now()
	var cursor uint64

	iter := checkpointer.svc.Scan(ctx, cursor, checkpointer.prefix+"*", 1).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()
		j, err := checkpointer.svc.Get(ctx, key).Result()

		if err != nil { // just logging
			log.Errorf("syncLeases Get Error: %s, %s", err.Error(), key)
			continue
		}

		cp, err := jsonToCheckpoint(j)

		if err != nil { // just logging
			log.Errorf("syncLeases jsonToCheckpoint Error: %s, %s", err.Error(), j)
			continue
		}

		if cp.ShardID == "" || cp.AssignedTo == "" || cp.Checkpoint == "" {
			continue
		}

		if shard, ok := shardStatus[cp.ShardID]; ok {
			shard.SetLeaseOwner(cp.AssignedTo)
			shard.SetCheckpoint(cp.Checkpoint)
		}

	}

	log.Debugf("Lease sync completed. Next lease sync will occur in %s", time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis)*time.Millisecond)

	return nil
}

func (checkpointer *RedisCheckpoint) putItem(newCheckpoint *ShardCheckpoint) error {

	var j []byte
	var err error
	var _ string

	if j, err = json.Marshal(newCheckpoint); err == nil {
		if err = checkpointer.svc.Set(ctx, checkpointer.prefix+newCheckpoint.ShardID, string(j), 0).Err(); err == nil {
			checkpointer.kclConfig.Logger.Infof("putItem : %s", j)
			return nil
		}
	}

	return err
}

func jsonToCheckpoint(j string) (*ShardCheckpoint, error) {
	cp := &ShardCheckpoint{}
	if err := json.Unmarshal([]byte(j), &cp); err != nil {
		return nil, err
	}
	return cp, nil
}

func (checkpointer *RedisCheckpoint) getItem(shardID string) (*ShardCheckpoint, error) {
	if r, err := checkpointer.svc.Get(ctx, checkpointer.prefix+shardID).Result(); err != nil {
		if err == Nil {
			return &ShardCheckpoint{ShardID: shardID}, nil
		} else {
			return nil, err
		}
	} else {
		checkpointer.kclConfig.Logger.Infof("getItem : %s", r)
		return jsonToCheckpoint(r)
	}
}

func (checkpointer *RedisCheckpoint) removeItem(shardID string) error {
	err := checkpointer.svc.Del(ctx, checkpointer.prefix+shardID).Err()

	if err != nil {
		checkpointer.kclConfig.Logger.Infof("removeItem : %s", shardID)
	}

	return err
}

func (checkpointer *RedisCheckpoint) lock(shardID string) (*redsync.Mutex, error) {
	mutexname := checkpointer.prefix + ":locks:" + shardID
	mutex := checkpointer.rs.NewMutex(mutexname)

	if err := mutex.Lock(); err != nil {
		checkpointer.kclConfig.Logger.Errorf("lock error shardID=%s, err=%s", shardID, err)
		return nil, err
	} else {
		checkpointer.kclConfig.Logger.Infof("locked shardID=%s", shardID)
		return mutex, nil
	}
}

func (checkpointer *RedisCheckpoint) unlock(shardID string, mutex *redsync.Mutex) (bool, error) {
	if ok, err := mutex.Unlock(); !ok || err != nil {
		checkpointer.kclConfig.Logger.Errorf("unlock error shardID=%s, ok=%v, err=%s", shardID, ok, err)
		return ok, err
	} else {
		checkpointer.kclConfig.Logger.Infof("unlocked shardID=%s, ok=%v", shardID, ok)
		return ok, err
	}
}
