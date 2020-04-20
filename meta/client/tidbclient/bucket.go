package tidbclient

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

const SECONDS_PER_DAY = 3600 * 24 // Seconds per day.
const HOURS_PER_DAY	= 24

func (t *TidbClient) GetBucket(bucketName string) (bucket *Bucket, err error) {
	var acl, cors, lc, policy, createTime string
	var updateTime sql.NullString
	sqltext := "select bucketname,acl,cors,lc,uid,policy,createtime,usages,versioning,update_time from buckets where bucketname=?;"
	tmp := &Bucket{}
	err = t.Client.QueryRow(sqltext, bucketName).Scan(
		&tmp.Name,
		&acl,
		&cors,
		&lc,
		&tmp.OwnerId,
		&policy,
		&createTime,
		&tmp.Usage,
		&tmp.Versioning,
		&updateTime,
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchBucket
		return
	} else if err != nil {
		return
	}
	tmp.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(acl), &tmp.ACL)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(cors), &tmp.CORS)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(lc), &tmp.LC)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(policy), &tmp.Policy)
	if err != nil {
		return
	}
	if updateTime.Valid {
		tmp.UpdateTime, err = time.Parse(TIME_LAYOUT_TIDB, updateTime.String)
		if err != nil {
			return
		}
	}
	bucket = tmp
	return
}

func (t *TidbClient) GetBuckets() (buckets []*Bucket, err error) {
	sqltext := "select bucketname,acl,cors,lc,uid,policy,createtime,usages,versioning,update_time from buckets;"
	rows, err := t.Client.Query(sqltext)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tmp Bucket
		var acl, cors, lc, policy, createTime string
		var updateTime sql.NullString
		err = rows.Scan(
			&tmp.Name,
			&acl,
			&cors,
			&lc,
			&tmp.OwnerId,
			&policy,
			&createTime,
			&tmp.Usage,
			&tmp.Versioning,
			&updateTime)
		if err != nil {
			return
		}
		tmp.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(acl), &tmp.ACL)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(cors), &tmp.CORS)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(lc), &tmp.LC)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(policy), &tmp.Policy)
		if err != nil {
			return
		}
		if updateTime.Valid {
			tmp.UpdateTime, err = time.Parse(TIME_LAYOUT_TIDB, updateTime.String)
			if err != nil {
				return
			}
		}
		buckets = append(buckets, &tmp)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return
}

//Actually this method is used to update bucket
func (t *TidbClient) PutBucket(bucket *Bucket) error {
	sql, args := bucket.GetUpdateSql()
	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) CheckAndPutBucket(bucket *Bucket) (bool, error) {
	var processed bool
	_, err := t.GetBucket(bucket.Name)
	if err == nil {
		processed = false
		return processed, err
	} else if err != nil && err != ErrNoSuchBucket {
		processed = false
		return processed, err
	} else {
		processed = true
	}
	sql, args := bucket.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	return processed, err
}

// ListObjcts is called by both list objects and list object versions, controlled by versioned.
func (t *TidbClient) ListObjects(ctx context.Context, bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool, maxKeys int, withDeleteMarker bool) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	const MaxObjectList = 10000
	var count int
	var exit bool
	commonPrefixes := make(map[string]struct{})
	omarker := marker
	var lastListedVersion uint64

	rawVersionIdMarker := ""
	if versioned && verIdMarker != "" {
		if verIdMarker == "null" {
			var o *Object
			if o, err = t.GetObject(bucketName, marker, "null"); err != nil {
				return
			}
			verIdMarker = o.VersionId
		}
		if rawVersionIdMarker, err = ConvertS3VersionToRawVersion(verIdMarker); err != nil {
			return
		}
	}

	helper.Logger.Info(ctx, bucketName, marker, verIdMarker, prefix, delimiter, versioned, maxKeys, withDeleteMarker)

	for {
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)

		if !versioned {
			// list objects, order by bucketname, name, version. So the latest will be returned.
			sqltext = "select distinct bucketname,name from objects where bucketName=?"
		} else {
			// list object versions.
			sqltext = "select bucketname,name,version from objects where bucketName=?"
		}
		args = append(args, bucketName)
		if prefix != "" {
			sqltext += " and name like ?"
			args = append(args, prefix+"%")
			helper.Logger.Info(ctx, "query prefix:", prefix)
		}
		if marker != "" {
			if !versioned {
				// list objects.
				sqltext += " and name >= ?"
				args = append(args, marker)
				helper.Logger.Info(ctx, "query marker:", marker)
			} else {
				// list object versions.
				if rawVersionIdMarker == "" {
					// First time to list the object after marker versions, excluding marker because it's listed before.
					sqltext += " and name > ?"
					args = append(args, marker)
				} else {
					// Not first time to list marker. Just start from marker, excluding verIdMarker.
					sqltext += " and name = ? and version > ?"
					args = append(args, marker)
					args = append(args, rawVersionIdMarker)
				}
				helper.Logger.Info(ctx, "query marker for versioned:", marker, rawVersionIdMarker)
			}
		}
		if delimiter == "" {
			sqltext += " order by bucketname,name,version limit ?"
			args = append(args, MaxObjectList)
		} else {
			num := len(strings.Split(prefix, delimiter))
			args = append(args, delimiter, num, MaxObjectList)
			sqltext += " group by SUBSTRING_INDEX(name, ?, ?) order by bucketname, name,version limit ?"
		}
		tstart := time.Now()
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			return
		}
		tqueryend := time.Now()
		tdur := tqueryend.Sub(tstart).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Info(ctx, fmt.Sprintf("slow list objects query: %s,args: %v, takes %d", sqltext, args, tdur))
		}

		helper.Logger.Info(ctx, "query sql:", sqltext, "args:", args)

		defer rows.Close()
		for rows.Next() {
			loopcount += 1
			//fetch related date
			var bucketname, name string
			var version uint64 // Internal version, the same as in DB.
			var s3VersionId string
			if !versioned {
				err = rows.Scan(
					&bucketname,
					&name,
				)
				s3VersionId = "" // Get default object later.
			} else {
				err = rows.Scan(
					&bucketname,
					&name,
					&version,
				)
				s3VersionId = ConvertRawVersionToS3Version(version)
			}
			if err != nil {
				helper.Logger.Error(ctx, "rows.Scan() err:", err)
				return
			}
			helper.Logger.Info(ctx, bucketname, name, version)

			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			marker = name

			if !versioned && name == omarker {
				continue
			}

			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					marker = prefixKey[0:(len(prefixKey)-1)] + string(delimiter[len(delimiter)-1]+1)
					if prefixKey == omarker {
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxKeys {
							truncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						nextMarker = prefixKey
						count += 1
					}
					continue
				}
			}

			var o *Object
			o, err = t.GetObject(bucketname, name, s3VersionId)
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("ListObjects: failed to GetObject(%s, %s, %s), err: %v", bucketname, name, ConvertRawVersionToS3Version(version), err))
				return
			}
			if o.DeleteMarker && !withDeleteMarker {
				// list objects, skip DeleteMarker.
				continue
			}

			count += 1
			if count == maxKeys {
				nextMarker = name
				lastListedVersion = version
			}

			if count > maxKeys {
				truncated = true
				exit = true
				break
			}

			retObjects = append(retObjects, o)
		}
		tfor := time.Now()
		tdur = tfor.Sub(tqueryend).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Info(nil, "slow list get objects, takes", tdur)
		}

		if versioned {
			// Looped all the versions in the marker.
			// Start from next object name.
			helper.Logger.Info(ctx, "Looped all the versions for", bucketName, marker, rawVersionIdMarker)

			if !exit && rawVersionIdMarker != "" {
				rawVersionIdMarker = ""
				continue
			}
		}

		if loopcount < MaxObjectList {
			exit = true
		}
		if exit {
			break
		}
	}
	prefixes = helper.Keys(commonPrefixes)
	if versioned && lastListedVersion != 0 {
		nextVerIdMarker = ConvertRawVersionToS3Version(lastListedVersion)
	}
	return
}

func (t *TidbClient) DeleteBucket(bucket *Bucket) error {
	sqltext := "delete from buckets where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) UpdateUsage(bucketName string, size int64, tx interface{}) (err error) {
	var sqlTx *sql.Tx
	if tx == nil {
		tx, err = t.Client.Begin()

		defer func() {
			if err == nil {
				err = sqlTx.Commit()
			}
			if err != nil {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)

	sql := "update buckets set usages=? where bucketname=?;"
	_, err = sqlTx.Exec(sql, size, bucketName)
	return
}

func (t *TidbClient) UpdateBucketInfo(usages map[string]*BucketInfo, tx interface{}) error {
	var sqlTx *sql.Tx
	var err error
	if nil == tx {
		tx, err = t.Client.Begin()
		defer func() {
			if nil == err {
				err = sqlTx.Commit()
			} else {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)
	sqlStr := "update buckets set usages = ?, fileNum = ? where bucketname = ?;"
	st, err := sqlTx.Prepare(sqlStr)
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("UpdateBucketInfo: failed to prepare statement: %s, err: %v",
			sqlStr, err))
		return err
	}
	defer st.Close()

	for bucket, info := range usages {
		_, err = st.Exec(info.Usage, info.FileNum, bucket)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("UpdateBucketInfo: failed to update bucket info for bucket %s, with usage: %d, fileNum: %d, err: %v",
				bucket, info.Usage, info.FileNum, err))
			return err
		}
	}
	return nil
}

func getExpireDuration(expireDays int64) (duration time.Duration, err error) {
	if helper.CONFIG.LcDebug == false {
		return time.ParseDuration(strconv.FormatInt((-1) * expireDays * HOURS_PER_DAY, 10) + "h")
	} else {
		return time.ParseDuration(strconv.FormatInt((-1) * expireDays, 10) + "s")
	}
}

func (t *TidbClient) ListTransitionObjects(bucketName, marker, verIdMarker, prefix string, versioned bool, maxKeys int, expireDays int64) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	const MaxObjectList = 1000
	if versioned {
		return // TODO should versioned in the future
	}
	expireDuration, err := getExpireDuration(expireDays)
	if err != nil {
		return
	}
	helper.Logger.Info(nil, bucketName, marker, verIdMarker, prefix, expireDays, expireDuration)
	var count int
	var exit bool
	objectMap := make(map[string]struct{})
	objectNum := make(map[string]int)
	commonPrefixes := make(map[string]struct{})
	omarker := marker
	for {
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)
		sqltext = "select bucketname,name,version,deletemarker from objects where bucketName=?"
		args = append(args, bucketName)
		if prefix != "" {
			sqltext += " and name like ?"
			args = append(args, prefix+"%")
			helper.Logger.Info(nil, "query prefix:", prefix)
		}
		if marker != "" {
			sqltext += " and name >= ?"
			args = append(args, marker)
			helper.Logger.Info(nil, "query marker:", marker)
		}
		sqltext += " and storageclass=0 and lastmodifiedtime<? order by bucketname,name,version limit ?"
		args = append(args, time.Now().Add(expireDuration).Format(TIME_LAYOUT_TIDB))
		args = append(args, MaxObjectList)
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			return
		}
		helper.Logger.Info(nil, "[ Glacier ] query sql:", sqltext)
		defer rows.Close()
		for rows.Next() {
			loopcount += 1
			//fetch related date
			var bucketname, name string
			var version uint64
			var deletemarker bool
			err = rows.Scan(
				&bucketname,
				&name,
				&version,
				&deletemarker,
			)
			if err != nil {
				helper.Logger.Println(10, "1", err)
				return
			}
			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			if _, ok := objectNum[name]; !ok {
				objectNum[name] = 0
			}
			objectNum[name] += 1
			marker = name

			if _, ok := objectMap[name]; !ok {
				objectMap[name] = struct{}{}
			} else {
				continue
			}
			//filte by deletemarker
			if deletemarker {
				continue
			}
			if name == omarker {
				continue
			}

			var o *Object
			Strver := strconv.FormatUint(version, 10)
			o, err = t.GetObject(bucketname, name, Strver)
			if err != nil {
				helper.Logger.Println(10, "2", err)
				return
			}
			count += 1
			if count == maxKeys {
				nextMarker = name
			}

			if count > maxKeys {
				truncated = true
				exit = true
				break
			}
			retObjects = append(retObjects, o)
		}
		if loopcount < MaxObjectList {
			exit = true
		}
		if exit {
			break
		}
	}
	prefixes = helper.Keys(commonPrefixes)
	return
}

func (t *TidbClient) GetAllBucketInfo() (map[string]*BucketInfo, error) {
	query := "select bucketname, count(objectid) as fileNum, sum(size) as usages from objects group by bucketname;"
	rows, err := t.Client.Query(query)
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("failed to query(%s), err: %v", query, err))
		return nil, err
	}

	infos := make(map[string]*BucketInfo)
	defer rows.Close()
	for rows.Next() {
		bi := &BucketInfo{}
		err = rows.Scan(
			&bi.BucketName,
			&bi.FileNum,
			&bi.Usage,
		)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to scan for query(%s), err: %v", query, err))
			return nil, err
		}
		infos[bi.BucketName] = bi
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("failed to iterator rows for query(%s), err: %v", query, err))
		return nil, err
	}
	return infos, nil
}
