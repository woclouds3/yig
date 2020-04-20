package storage

import (
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
)

// Remove
// 1. deleted objects
// 2. objects that already stored to Ceph but failed to update metadata
// asynchronously

const (
	META_SYNC_QUEUE_SIZE    = 100
	META_SYNC_MAX_TRY_TIMES = 3
)

func initializeMetaSyncWorker(yig *YigStorage) {
	/*if meta.MetaSyncQueue == nil {
		meta.MetaSyncQueue = make(chan meta.SyncEvent, META_SYNC_QUEUE_SIZE)
	}*/
	go metaSync(yig)
}

func metaSync(yig *YigStorage) {
	yig.WaitGroup.Add(1)
	defer yig.WaitGroup.Done()
	event := meta.SyncEvent{
		Type: meta.SYNC_EVENT_TYPE_BUCKET_USAGE,
	}
	for {
		if yig.Stopping {
			helper.Logger.Info(nil, ".")
			// check whether all changed bucket usages are synced.
			err := yig.MetaStorage.Sync(event)
			if err != nil {
				helper.Logger.Error(nil, "failed to perform bucket usage sync, err:", err)
			}
			helper.Logger.Info(nil, "meta sync job stopped")
			break
		}

		err := yig.MetaStorage.Sync(event)
		if err != nil {
			helper.Logger.Error(nil, "failed to perform bucket usage sync, err:", err)
		}

		time.Sleep(5 * time.Second)
	}
}
