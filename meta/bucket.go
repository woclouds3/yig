package meta

import (
	"context"
	"fmt"
	"strings"
	"time"

	errs "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

const (
	BUCKET_CACHE_PREFIX = "bucket:"
	USER_CACHE_PREFIX   = "user:"
)

// Note the usage info got from this method is possibly not accurate because we don't
// invalid cache when updating usage. For accurate usage info, use `GetUsage()`
func (m *Meta) GetBucket(ctx context.Context, bucketName string, willNeed bool) (bucket *types.Bucket, err error) {
	getBucket := func() (b helper.Serializable, err error) {
		b, err = m.Client.GetBucket(bucketName)
		helper.Logger.Info(ctx, "GetBucket CacheMiss. bucket:", bucketName)
		return b, err
	}
	toBucket := func(fields map[string]string) (interface{}, error) {
		b := &types.Bucket{}
		return b.Deserialize(fields)
	}

	b, err := m.Cache.Get(ctx, redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName, getBucket, toBucket, willNeed)
	if err != nil {
		return
	}
	bucket, ok := b.(*types.Bucket)
	if !ok {
		helper.Logger.Error(ctx, "Cast b failed:", b)
		err = errs.ErrInternalError
		return
	}
	return bucket, nil
}

func (m *Meta) GetBuckets() (buckets []*types.Bucket, err error) {
	// try to get buckets from cache first, if fails, get them from db.
	// try to get all bucket usage keys from cache.
	pattern := fmt.Sprintf("%s*", BUCKET_CACHE_PREFIX)
	bucketsInCache, err := m.Cache.Keys(redis.BucketTable, pattern)
	if err != nil {
		// failed to get buckets from cache, try to get them from db.
		helper.Logger.Error(nil, fmt.Sprintf("failed to get bucket usage from cache, err: %v", err))
		buckets, err = m.Client.GetBuckets()
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to get bucket usage from db, err: %v", err))
			return nil, err
		}
		return buckets, nil
	}
	// deserialize the buckets info saved in cache.
	for _, bic := range bucketsInCache {
		elems := strings.Split(bic, ":")
		name := bic
		if len(elems) > 0 {
			name = elems[1]
		}
		cacheElems, err := m.Cache.HGetAll(redis.BucketTable, BUCKET_CACHE_PREFIX, name)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to get buckets info for bucket: %s, err: %v", name, err))
			continue
		}
		bu := &types.Bucket{}
		_, err = bu.Deserialize(cacheElems)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to deserialize the bucket info for bucket: %s, err: %v", name, err))
			continue
		}
		buckets = append(buckets, bu)
	}
	return buckets, nil
}

func (m *Meta) UpdateBucketInfo(ctx context.Context, bucketName string, field string, size int64) error {
	tstart := time.Now()

	newSize, err := m.Cache.HIncrBy(redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName, field, size)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to update bucket[%s] %s by %d, err: %v",
			bucketName, field, size, err))
		return err
	}
	tinc := time.Now()
	dur := tinc.Sub(tstart)
	if dur/1000000 >= 100 {
		helper.Logger.Warn(ctx, fmt.Sprintf("slow log: RedisIncrBy: bucket: %s, size: %d, takes: %d",
			bucketName, size, dur))
	}

	err = m.addBucketInfoSyncEvent(bucketName)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to add bucket %s sync event for bucket: %s, err: %v",
			field, bucketName, err))
		return err
	}
	helper.Logger.Info(ctx, fmt.Sprintf("incr %s for bucket %s, now: %d", field, bucketName, newSize))
	tend := time.Now()
	dur = tend.Sub(tinc)
	if dur/1000000 >= 100 {
		helper.Logger.Error(ctx, fmt.Sprintf("slow log: AddBucketInfoSyncEvent: bucket: %s, size: %d, takes: %d",
			bucketName, size, dur))
	}
	dur = tend.Sub(tstart)
	if dur/1000000 >= 100 {
		helper.Logger.Error(ctx, fmt.Sprintf("slow log: cache update, bucket: %s, size: %d, takes: %d",
			bucketName, size, dur))
	}
	return nil
}

func (m *Meta) GetUsage(ctx context.Context, bucketName string) (int64, error) {
	usage, err := m.Cache.HGetInt64(redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName, types.FIELD_NAME_USAGE)
	if err != nil {
		helper.Logger.Info(ctx, "failed to get usage for bucket: ", bucketName, ", err: ", err)
		return 0, err
	}
	return usage, nil
}

func (m *Meta) GetBucketInfo(ctx context.Context, bucketName string) (*types.Bucket, error) {
	m.Cache.Remove(redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName)
	bucket, err := m.GetBucket(ctx, bucketName, true)
	if err != nil {
		return bucket, err
	}
	return bucket, nil
}

func (m *Meta) GetUserInfo(ctx context.Context, uid string) ([]string, error) {
	m.Cache.Remove(redis.UserTable, USER_CACHE_PREFIX, uid)
	buckets, err := m.GetUserBuckets(ctx, uid, true)
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

/*
* init bucket usage cache when meta is newed.
*
 */
func (m *Meta) InitBucketUsageCache() error {
	// the map contains the bucket usage which are not in cache.
	bucketUsageMap := make(map[string]*types.Bucket)
	// the map contains the bucket usage which are in cache and will be synced into database.
	bucketUsageCacheMap := make(map[string]*types.BucketInfo)
	// the usage in buckets table is accurate now.
	buckets, err := m.Client.GetBuckets()
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("failed to get buckets from db. err: ", err))
		return err
	}

	// init the bucket usage key in cache.
	for _, bucket := range buckets {
		bucketUsageMap[bucket.Name] = bucket
	}

	// try to get all bucket usage keys from cache.
	pattern := fmt.Sprintf("%s*", BUCKET_CACHE_PREFIX)
	bucketsInCache, err := m.Cache.Keys(redis.BucketTable, pattern)
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("failed to get bucket usage from cache, err: ", err))
		return err
	}

	if len(bucketsInCache) > 0 {
		// query all usages from cache.
		for _, bic := range bucketsInCache {
			elems := strings.Split(bic, ":")
			name := bic
			if len(elems) > 0 {
				name = elems[1]
			}
			usage, err := m.Cache.HGetInt64(redis.BucketTable, BUCKET_CACHE_PREFIX, name, types.FIELD_NAME_USAGE)
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("failed to get usage for bucket: ", name, " with err: ", err))
				continue
			}
			fileNum, err := m.Cache.HGetInt64(redis.BucketTable, BUCKET_CACHE_PREFIX, name, types.FIELD_NAME_FILE_NUM)
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("failed to get fileNum for bucket: %s, err: %v", name, err))
				continue
			}
			// add the to be synced usage.
			bucketUsageCacheMap[name] = &types.BucketInfo{
				Usage:   usage,
				FileNum: fileNum,
			}
			if _, ok := bucketUsageMap[name]; ok {
				// if the key already exists in cache, then delete it from map
				delete(bucketUsageMap, name)
			}
		}

	}

	// init the bucket usage in cache.
	if len(bucketUsageMap) > 0 {
		for _, bk := range bucketUsageMap {
			fields, err := bk.Serialize()
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("failed to serialize for bucket: ", bk.Name, " with err: ", err))
				return err
			}
			_, err = m.Cache.HMSet(redis.BucketTable, BUCKET_CACHE_PREFIX, bk.Name, fields)
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("failed to set bucket to cache: ", bk.Name, " with err: ", err))
				return err
			}
		}

	}
	// sync the buckets usage in cache into database.
	if len(bucketUsageCacheMap) > 0 {
		err = m.Client.UpdateBucketInfo(bucketUsageCacheMap, nil)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to sync usages to database, err: ", err))
			return err
		}
	}
	return nil
}

func (m *Meta) bucketUsageSync() error {
	buckets, err := m.Cache.HGetAll(redis.BucketTable, types.SYNC_EVENT_BUCKET_USAGE_PREFIX, "trigger")
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("failed to get buckets whose usage are changed, err: %v", err))
		return err
	}
	if len(buckets) <= 0 {
		return nil
	}
	var cacheRemove []string
	bucketInfos := make(map[string]*types.BucketInfo)
	for k, _ := range buckets {
		usage, err := m.Cache.HGetInt64(redis.BucketTable, BUCKET_CACHE_PREFIX, k, types.FIELD_NAME_USAGE)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to get usage for bucket: %s, err: %v", k, err))
			continue
		}
		fileNum, err := m.Cache.HGetInt64(redis.BucketTable, BUCKET_CACHE_PREFIX, k, types.FIELD_NAME_FILE_NUM)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to get fileNum for bucket: %s, err: %v", k, err))
			continue
		}
		bucketInfos[k] = &types.BucketInfo{
			Usage:   usage,
			FileNum: fileNum,
		}

		cacheRemove = append(cacheRemove, k)

		helper.Logger.Info(nil, fmt.Sprintf("add bucket info[%s, %d, %d] for update", k, usage, fileNum))
	}

	if len(bucketInfos) > 0 {
		err = m.Client.UpdateBucketInfo(bucketInfos, nil)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to update bucket info, err: %v", err))
			return err
		}
	}
	// remove the bucket usage sync event.
	if len(cacheRemove) > 0 {
		_, err = m.Cache.HDel(redis.BucketTable, types.SYNC_EVENT_BUCKET_USAGE_PREFIX, "trigger", cacheRemove)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to unset the bucket usage change event for %v, err: %v", cacheRemove, err))
			return err
		}
		helper.Logger.Info(nil, fmt.Sprintf("succeed to remove bucket usage trigger for %v", cacheRemove))
	}
	return nil
}

func (m *Meta) addBucketInfoSyncEvent(bucketName string) error {
	_, err := m.Cache.HSet(redis.BucketTable, types.SYNC_EVENT_BUCKET_USAGE_PREFIX, "trigger", bucketName, 1)
	if err != nil {
		return err
	}
	return nil
}
