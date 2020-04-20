package storage

import (
	"time"

	"github.com/journeymidnight/yig/helper"
)

// Remove
// 1. deleted objects
// 2. objects that already stored to Ceph but failed to update metadata
// asynchronously

const (
	RECYCLE_QUEUE_SIZE = 100
	MAX_TRY_TIMES      = 3
)

type objectToRecycle struct {
	location   string
	pool       string
	objectId   string
	triedTimes int
}

var RecycleQueue chan objectToRecycle

func initializeRecycler(yig *YigStorage) {
	if RecycleQueue == nil {
		RecycleQueue = make(chan objectToRecycle, RECYCLE_QUEUE_SIZE)
	}
	// TODO: move this part of code to an isolated daemon
	go removeFailed(yig)
}

func removeFailed(yig *YigStorage) {
	yig.WaitGroup.Add(1)
	defer yig.WaitGroup.Done()
	for {
		select {
		case object := <-RecycleQueue:
			err := yig.DataStorage[object.location].Remove(object.pool, object.objectId)
			if err != nil {
				object.triedTimes += 1
				if object.triedTimes > MAX_TRY_TIMES {
					helper.Logger.Warn(nil, "Failed to remove object in Ceph:",
						object.location, object.pool, object.objectId,
						"with error", err)
					continue
				}
				RecycleQueue <- object
				time.Sleep(1 * time.Second)
			}
		default:
			if yig.Stopping {
				helper.Logger.Info(nil, ".")
				if len(RecycleQueue) == 0 {
					return
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}

//func removeDeleted(yig *YigStorage) {
//	yig.WaitGroup.Add(1)
//	defer yig.WaitGroup.Done()
//	for {
//		// randomize garbageCollection table access
//		time.Sleep(time.Duration(rand.Intn(10000)) * time.Millisecond)
//
//		if yig.Stopping {
//			helper.Logger.Print(5, ".")
//			return
//		}
//
//		garbages, err := yig.MetaStorage.ScanGarbageCollection(10)
//		if err != nil {
//			continue
//		}
//		for _, garbage := range garbages {
//			garbage.Status = "Deleting"
//			values, err := garbage.GetValues()
//			if err != nil {
//				continue
//			}
//			put, err := hrpc.NewPutStr(RootContext, meta.GARBAGE_COLLECTION_TABLE,
//				garbage.Rowkey, values)
//			if err != nil {
//				continue
//			}
//			processed, err := yig.MetaStorage.Hbase.CheckAndPut(put,
//				meta.GARBAGE_COLLECTION_COLUMN_FAMILY, "status", []byte("Pending"))
//			if !processed || err != nil {
//				continue
//			}
//			success := true
//			if len(garbage.Parts) == 0 {
//				err = yig.DataStorage[garbage.Location].
//					Remove(garbage.Pool, garbage.ObjectId)
//				if err != nil {
//					success = false
//				}
//			} else {
//				for _, p := range garbage.Parts {
//					err = yig.DataStorage[garbage.Location].
//						Remove(garbage.Pool, p.ObjectId)
//					if err != nil {
//						success = false
//					}
//				}
//			}
//			if success {
//				yig.MetaStorage.RemoveGarbageCollection(garbage)
//			} else {
//				garbage.TriedTimes += 1
//				if garbage.TriedTimes > MAX_TRY_TIMES {
//					helper.Logger.Println(5, "Failed to remove object in Ceph:",
//						garbage)
//					yig.MetaStorage.RemoveGarbageCollection(garbage)
//					continue
//				}
//				garbage.Status = "Pending"
//				values, err := garbage.GetValues()
//				if err != nil {
//					continue
//				}
//				put, err := hrpc.NewPutStr(RootContext,
//					meta.GARBAGE_COLLECTION_TABLE, garbage.Rowkey, values)
//				if err != nil {
//					continue
//				}
//				_, err = yig.MetaStorage.Hbase.Put(put)
//				if err != nil {
//					helper.Logger.Println(5, "Inconsistent data:",
//						"garbage collection", garbage.Rowkey,
//						"should have status `Pending`")
//				}
//			}
//		}
//	}
//}
