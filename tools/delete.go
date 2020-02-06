package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/mods"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

const (
	WATER_LOW               = 120
	TASKQ_MAX_LENGTH        = 200
	SCAN_HBASE_LIMIT        = 50
	DEFAULT_DELETE_LOG_PATH = "/var/log/yig/delete.log"
)

var (
	logger      *log.Logger
	RootContext = context.Background()
	yigs        []*storage.YigStorage
	gcTaskQ     chan types.GarbageCollection
	gcWaitgroup sync.WaitGroup
	gcStop      bool
)

func deleteFromCeph(index int) {
	for {
		if gcStop {
			helper.Logger.Print(5, ".")
			return
		}
		var (
			p   *types.Part
			err error
		)
		garbage := <-gcTaskQ
		gcWaitgroup.Add(1)
		if garbage.StorageClass == types.ObjectStorageClassStandard {
			// Ceph
			if len(garbage.Parts) == 0 {
				err = yigs[index].DataStorage[garbage.Location].
					Remove(garbage.Pool, garbage.ObjectId)
				if err != nil {
					if strings.Contains(err.Error(), "ret=-2") {
						helper.Logger.Println(5, "failed delete", garbage.BucketName, ":", garbage.ObjectName, ":",
							garbage.Location, ":", garbage.Pool, ":", garbage.ObjectId, " error:", err)
						goto release
					}
					helper.Logger.Printf(2, "failed to delete obj %s in bucket %s with objid: %s from pool %s, err: %v", garbage.ObjectName, garbage.BucketName, garbage.ObjectId, garbage.Pool, err)
					gcWaitgroup.Done()
					continue
				} else {
					helper.Logger.Println(5, "success delete", garbage.BucketName, ":", garbage.ObjectName, ":",
						garbage.Location, ":", garbage.Pool, ":", garbage.ObjectId)
				}
			} else {
				for _, p = range garbage.Parts {
					err = yigs[index].DataStorage[garbage.Location].
						Remove(garbage.Pool, p.ObjectId)
					if err != nil {
						if strings.Contains(err.Error(), "ret=-2") {
							helper.Logger.Println(5, "failed delete part", garbage.Location, ":", garbage.Pool, ":", p.ObjectId, " error:", err)
							goto release
						}
						helper.Logger.Printf(2, "failed to delete part %s with objid: %s from pool %s, err: %v", garbage.Location, garbage.ObjectId, garbage.Pool, err)
						gcWaitgroup.Done()
						continue
					} else {
						helper.Logger.Println(5, "success delete part", garbage.Location, ":", garbage.Pool, ":", p.ObjectId)
					}
				}
			}
		} else if helper.CONFIG.Glacier.EnableGlacier && garbage.StorageClass == types.ObjectStorageClassGlacier {
			// Remove Archive in Glacier. Big file should be the same as small file.
			err = yigs[index].DeleteObjectFromGlacier(nil, garbage.BucketName, garbage.ObjectName, garbage.ObjectId, garbage.OwnerId)
			// TODO, how to handle delete fail in glacier?
			if err != nil {
				helper.Logger.Println(5, "failed delete from Glacier", err, ":", garbage.BucketName, ":", garbage.ObjectName, ":",
					garbage.ObjectId, ":", garbage.OwnerId, ":")
				//continue
			} else {
				helper.Logger.Println(5, "success delete from Glacier", garbage.BucketName, ":", garbage.ObjectName, ":",
					garbage.ObjectId, ":", garbage.OwnerId, ":")
			}
		}
	release:
		yigs[index].MetaStorage.RemoveGarbageCollection(garbage)
		gcWaitgroup.Done()
	}
}

func removeDeleted() {
	time.Sleep(time.Duration(1000) * time.Millisecond)
	var startRowKey string
	var garbages []types.GarbageCollection
	var err error
	for {
		if gcStop {
			helper.Logger.Print(5, ".")
			return
		}
	wait:
		if len(gcTaskQ) >= WATER_LOW {
			time.Sleep(time.Duration(1) * time.Millisecond)
			goto wait
		}

		if len(gcTaskQ) < WATER_LOW {
			garbages = garbages[:0]
			garbages, err = yigs[0].MetaStorage.ScanGarbageCollection(SCAN_HBASE_LIMIT, startRowKey)
			if err != nil {
				continue
			}
		}

		if len(garbages) == 0 {
			time.Sleep(time.Duration(10000) * time.Millisecond)
			startRowKey = ""
			continue
		} else if len(garbages) == 1 {
			for _, garbage := range garbages {
				gcTaskQ <- garbage
			}
			startRowKey = ""
			time.Sleep(time.Duration(5000) * time.Millisecond)
			continue
		} else {
			startRowKey = garbages[len(garbages)-1].Rowkey
			garbages = garbages[:len(garbages)-1]
			for _, garbage := range garbages {
				gcTaskQ <- garbage
			}
		}
	}
}

func main() {
	helper.SetupConfig()

	f, err := os.OpenFile(DEFAULT_DELETE_LOG_PATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file in current dir")
	}
	defer f.Close()
	gcStop = false
	logger = log.New(f, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger
	gcTaskQ = make(chan types.GarbageCollection, TASKQ_MAX_LENGTH)
	signal.Ignore()
	signalQueue := make(chan os.Signal)

	numOfWorkers := helper.CONFIG.GcThread
	yigs = make([]*storage.YigStorage, helper.CONFIG.GcThread+1)

	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.Close()
	}
	yigs[0] = storage.New(logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)

	// Glacier requires ak/sk, use this to initialize iam plugin in delete process.
	if helper.CONFIG.Glacier.EnableGlacier {
		allPluginMap := mods.InitialPlugins()
		iam.InitializeIamClient(allPluginMap)
	}

	helper.Logger.Println(5, "start gc thread:", numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		yigs[i+1] = storage.New(logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)
		go deleteFromCeph(i + 1)
	}
	go removeDeleted()
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		default:
			// gcStop YIG server, order matters
			gcStop = true
			gcWaitgroup.Wait()
			return
		}
	}

}
