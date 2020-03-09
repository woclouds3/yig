package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
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
			helper.Logger.Info(nil, ".")
			return
		}
		var (
			p   *types.Part
			err error
		)
		garbage := <-gcTaskQ
		gcWaitgroup.Add(1)
		if len(garbage.Parts) == 0 {
			err = yigs[index].DataStorage[garbage.Location].
				Remove(garbage.Pool, garbage.ObjectId)
			if err != nil {
				if strings.Contains(err.Error(), "ret=-2") {
					helper.Logger.Error(nil, "failed delete", garbage.BucketName, ":", garbage.ObjectName, ":",
						garbage.Location, ":", garbage.Pool, ":", garbage.ObjectId, " error:", err)
					goto release
				}
				helper.Logger.Error(nil, fmt.Sprintf("failed to delete obj %s in bucket %s with objid: %s from pool %s, err: %v", garbage.ObjectName, garbage.BucketName, garbage.ObjectId, garbage.Pool, err))
				gcWaitgroup.Done()
				continue
			} else {
				helper.Logger.Info(nil, "success delete", garbage.BucketName, ":", garbage.ObjectName, ":",
					garbage.Location, ":", garbage.Pool, ":", garbage.ObjectId)
			}
		} else {
			for _, p = range garbage.Parts {
				err = yigs[index].DataStorage[garbage.Location].
					Remove(garbage.Pool, p.ObjectId)
				if err != nil {
					if strings.Contains(err.Error(), "ret=-2") {
						helper.Logger.Error(nil, "failed delete part", garbage.Location, ":", garbage.Pool, ":", p.ObjectId, " error:", err)
						goto release
					}
					helper.Logger.Error(nil, "failed to delete part %s with objid: %s from pool %s, err: %v", garbage.Location, garbage.ObjectId, garbage.Pool, err)
					gcWaitgroup.Done()
					continue
				} else {
					helper.Logger.Info(nil, "success delete part", garbage.Location, ":", garbage.Pool, ":", p.ObjectId)
				}
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
			helper.Logger.Info(nil, ".")
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
	gcStop = false

	helper.SetupConfig()
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)

	helper.Logger = log.NewFileLogger(DEFAULT_DELETE_LOG_PATH, logLevel)
	defer helper.Logger.Close()

	gcTaskQ = make(chan types.GarbageCollection, TASKQ_MAX_LENGTH)
	signal.Ignore()
	signalQueue := make(chan os.Signal)

	numOfWorkers := helper.CONFIG.GcThread
	yigs = make([]*storage.YigStorage, helper.CONFIG.GcThread+1)
	yigs[0] = storage.New(helper.Logger, int(meta.NoCache), false, helper.CONFIG.CephConfigPattern)
	helper.Logger.Info(nil, "start gc thread:", numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		yigs[i+1] = storage.New(helper.Logger, int(meta.NoCache), false, helper.CONFIG.CephConfigPattern)
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
