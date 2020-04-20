package tidbclient

import (
	"context"
	"database/sql"
	"time"
	"fmt"

	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

const DEFAULT_TRANSITION_INTERVAL = 3600 * 24 // 24 hours in seconds.
const DEFAULT_TRANSITION_INTERVAL_HOURS	= 24		// 1 day.

func (t *TidbClient) PutBucketToLifeCycle(ctx context.Context, lifeCycle LifeCycle) error {
	sqltext := "insert ignore into lifecycle(bucketname,status) values (?,?);"
	_, err := t.Client.Exec(sqltext, lifeCycle.BucketName, lifeCycle.Status)
	if err != nil {
		helper.Logger.Error(ctx, "Failed in PutBucketToLifeCycle: ", sqltext)
		return nil
	}
	return nil
}

func (t *TidbClient) PutBucketToTransition(ctx context.Context, lifeCycle LifeCycle) error {
	sqltext := "insert ignore into transition(bucketname, lastvisitedtime) values (?,?);"
	_, err := t.Client.Exec(sqltext, lifeCycle.BucketName, time.Now().Format(TIME_LAYOUT_TIDB))
	if err != nil {
		helper.Logger.Error(ctx, "Failed in PutBucketToTransition:", sqltext, err)
		
		return nil
	}
	return nil
}

func (t *TidbClient) RemoveBucketFromLifeCycle(ctx context.Context, bucket *Bucket) error {
	sqltext := "delete from lifecycle where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		helper.Logger.Error(ctx, "Failed in RemoveBucketFromLifeCycle:", sqltext)
		return nil
	}
	return nil
}

func (t *TidbClient) RemoveBucketFromTransition(ctx context.Context, bucket *Bucket) error {
	sqltext := "delete from transition where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		helper.Logger.Error(ctx, "Failed in RemoveBucketFromTransition: %s\n", sqltext)
		return nil
	}
	return nil
}

func (t *TidbClient) ScanLifeCycle(ctx context.Context, limit int, marker string) (result ScanLifeCycleResult, err error) {
	result.Truncated = false
	sqltext := "select * from lifecycle where bucketname > ? limit ?;"
	rows, err := t.Client.Query(sqltext, marker, limit)
	if err == sql.ErrNoRows {
		helper.Logger.Error(ctx, "Failed in sql.ErrNoRows:", sqltext)
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()
	result.Lcs = make([]LifeCycle, 0, limit)
	var lc LifeCycle
	for rows.Next() {
		err = rows.Scan(
			&lc.BucketName,
			&lc.Status)
		if err != nil {
			helper.Logger.Error(ctx, "Failed in ScanLifeCycle:", result.Lcs, result.NextMarker)
			return
		}
		result.Lcs = append(result.Lcs, lc)
	}
	result.NextMarker = lc.BucketName
	if len(result.Lcs) == limit {
		result.Truncated = true
	}
	return result, nil
}

func (t *TidbClient) ScanHiddenBuckets(ctx context.Context, limit int, marker string) (buckets []string, truncated bool, err error) {
	err = nil
	truncated = false
	buckets = nil

	sqltext := "select bucketname from users where bucketname like ? and bucketname > ? order by bucketname limit ?;"
	rows, err := t.Client.Query(sqltext, HIDDEN_BUCKET_PREFIX+"%", marker, limit)
	if err == sql.ErrNoRows {
		return
	} else if err != nil {
		helper.Logger.Info(ctx, fmt.Sprintf("Failed in ScanHiddenBuckets: err %v patten %s marker %s limit %d sql %s",
			err, "'"+HIDDEN_BUCKET_PREFIX+"%"+"'", marker, limit, sqltext))
		return
	}

	defer rows.Close()

	buckets = make([]string, 0, limit)
	for rows.Next() {
		var bucketName string
		if err = rows.Scan(&bucketName); err != nil {
			return
		}

		buckets = append(buckets, bucketName)
	}

	if len(buckets) == limit {
		truncated = true
	}

	return
}

func getExpectedTime() string {
	if helper.CONFIG.LcDebug == false {
		return time.Now().Add(-1 * DEFAULT_TRANSITION_INTERVAL_HOURS * time.Hour).Format(TIME_LAYOUT_TIDB)
	} else {
		return time.Now().Add(-1 * time.Second).Format(TIME_LAYOUT_TIDB)
	}
}

func (t *TidbClient) ScanTransitionBuckets(ctx context.Context, limit int, marker string) (result ScanLifeCycleResult, err error) {
	result.Truncated = false
	// Each process handles some buckets.
	sqltext := "select * from transition where bucketname > ? and lastvisitedtime < ? limit ?;"
	rows, err := t.Client.Query(sqltext, marker, getExpectedTime(), limit)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		helper.Logger.Error(ctx, err, sqltext, "marker:", marker, time.Now().Add(-1 * DEFAULT_TRANSITION_INTERVAL_HOURS * time.Hour).Format(TIME_LAYOUT_TIDB))
		return
	}
	defer rows.Close()
	result.Lcs = make([]LifeCycle, 0, limit)
	var lc LifeCycle
	for rows.Next() {
		err = rows.Scan(
			&lc.BucketName,
			&lc.Status)
		if err != nil {
			helper.Logger.Error(ctx, "Failed in ScanTransitionBuckets: ", rows, result.NextMarker)
			continue
		}

		sqltext = "update transition set lastvisitedtime=? where bucketname=?"
		if _, err = t.Client.Exec(sqltext, time.Now().Format(TIME_LAYOUT_TIDB), lc.BucketName); err != nil {
			helper.Logger.Error(ctx, "Failed in ScanTransitionBuckets: ", lc.BucketName)
			continue
		}

		result.Lcs = append(result.Lcs, lc)
	}
	result.NextMarker = lc.BucketName
	if len(result.Lcs) == limit {
		result.Truncated = true
	}
	return result, nil
}
