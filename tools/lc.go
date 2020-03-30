package main

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

const (
	SCAN_HBASE_LIMIT    = 50
	DEFAULT_LC_LOG_PATH = "/var/log/yig/lc.log"
	DEFAULT_LC_SLEEP_INTERVAL_HOUR     = 1 //TODO 24
)

var (
	logger      log.Logger
	yig         *storage.YigStorage
	taskQ       chan types.LifeCycle
	signalQueue chan os.Signal
	waitgroup   sync.WaitGroup
	stop        bool
)

func getLifeCycles() {
	var marker string
	helper.Logger.Info(nil, "all bucket lifecycle handle start")
	waitgroup.Add(1)
	defer waitgroup.Done()
	for {
		if stop {
			helper.Logger.Info(nil, ".")
			return
		}

		helper.Logger.Info(nil, "ScanLifeCycle: marker: ", marker)

		result, err := yig.MetaStorage.ScanLifeCycle(nil, SCAN_HBASE_LIMIT, marker)
		if err != nil {
			helper.Logger.Error(nil, "ScanLifeCycle failed, err: ", err, "Sleep and retry.")
			time.Sleep(DEFAULT_LC_SLEEP_INTERVAL_HOUR * time.Hour)
			continue
		}

		for _, entry := range result.Lcs {
			taskQ <- entry
			marker = entry.BucketName
		}

		if result.Truncated == false {
			marker = ""
			helper.Logger.Info(nil, "Sleep after ScanLifeCycle returned len:", len(result.Lcs), "Truncated:", result.Truncated)
			time.Sleep(DEFAULT_LC_SLEEP_INTERVAL_HOUR * time.Hour)
		}
	}

}

func checkIfExpiration(updateTime time.Time, days int) bool {
	if helper.CONFIG.LcDebug == false {
		return int(time.Since(updateTime).Seconds()) >= days*24*3600
	} else {
		return int(time.Since(updateTime).Seconds()) >= days
	}
}

// If a rule has an empty prifex ,the days in it will be consider as a default days for all objects that not specified in
// other rules. For this reason, we have two conditions to check if a object has expired and should be deleted
//  if defaultConfig == true
//                    for each object           check if object name has a prifix
//  list all objects --------------->loop rules---------------------------------->
//                                                                      |     NO
//                                                                      |--------> days = default days ---
//                                                                      |     YES                         |->delete object if expired
//                                                                      |--------> days = specify days ---
//
//  if defaultConfig == false
//                 for each rule get objects by prefix
//  iterator rules ----------------------------------> loop objects-------->delete object if expired
func retrieveBucket(lc types.LifeCycle) error {
	defaultConfig := false
	defaultDays := 0
	bucket, err := yig.MetaStorage.GetBucket(nil, lc.BucketName, false)
	if err != nil {
		return err
	}
	rules := bucket.LC.Rule
	for _, rule := range rules {
		if rule.Prefix == "" {
			defaultConfig = true
			defaultDays, err = strconv.Atoi(rule.Expiration)
			if err != nil {
				return err
			}
		}
	}
	var request datatype.ListObjectsRequest
	request.Versioned = false
	request.MaxKeys = 1000
	if defaultConfig == true {
		for {
			retObjects, _, truncated, nextMarker, nextVerIdMarker, err := yig.ListObjectsInternal(nil, bucket.Name, request)
			if err != nil {
				return err
			}

			for _, object := range retObjects {
				prefixMatch := false
				matchDays := 0
				for _, rule := range rules {
					if rule.Prefix == "" {
						continue
					}
					if strings.HasPrefix(object.Name, rule.Prefix) == false {
						continue
					}
					prefixMatch = true
					matchDays, err = strconv.Atoi(rule.Expiration)
					if err != nil {
						return err
					}
				}
				days := 0
				if prefixMatch == true {
					days = matchDays
				} else {
					days = defaultDays
				}
				helper.Logger.Info(nil, "inteval:", time.Since(object.LastModifiedTime).Seconds())
				if checkIfExpiration(object.LastModifiedTime, days) {
					helper.Logger.Info(nil, "come here")
					if object.NullVersion {
						object.VersionId = ""
					}
					_, err = yig.DeleteObject(nil, object.BucketName, object.Name, object.VersionId, common.Credential{})
					if err != nil {
						helper.Logger.Error(nil, "[FAILED]", object.BucketName, object.Name, object.VersionId, err)
						continue
					}
					helper.Logger.Info(nil, "[DELETED]", object.BucketName, object.Name, object.VersionId)
				}
			}
			if truncated == true {
				request.KeyMarker = nextMarker
				request.VersionIdMarker = nextVerIdMarker
			} else {
				break
			}
		}
	} else {
		for _, rule := range rules {
			if rule.Prefix == "" {
				continue
			}
			days, _ := strconv.Atoi(rule.Expiration)
			if err != nil {
				return err
			}
			request.Prefix = rule.Prefix
			for {

				retObjects, _, truncated, nextMarker, nextVerIdMarker, err := yig.ListObjectsInternal(nil, bucket.Name, request)
				if err != nil {
					return err
				}
				for _, object := range retObjects {
					if checkIfExpiration(object.LastModifiedTime, days) {
						_, err = yig.DeleteObject(nil, object.BucketName, object.Name, object.VersionId, common.Credential{})
						if err != nil {
							helper.Logger.Error(nil, "failed to delete object:", object.Name, object.BucketName)
							helper.Logger.Error(nil, "[FAILED]", object.BucketName, object.Name, object.VersionId, err)
							continue
						}
						helper.Logger.Info(nil, "[DELETED]", object.BucketName, object.Name, object.VersionId)
					}
				}
				if truncated == true {
					request.KeyMarker = nextMarker
					request.VersionIdMarker = nextVerIdMarker
				} else {
					break
				}

			}
		}

	}
	return nil
}

func processLifecycle() {
	time.Sleep(time.Second * 1)
	for {
		if stop {
			helper.Logger.Info(nil, ".")
			return
		}

		item := <-taskQ
		waitgroup.Add(1)
		err := retrieveBucket(item)
		if err != nil {
			helper.Logger.Error(nil, "[ERR] Bucket: ", item.BucketName, err)
		}
		waitgroup.Done()
	}
}

func main() {
	stop = false

	helper.SetupConfig()
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)

	helper.Logger = log.NewFileLogger(DEFAULT_LC_LOG_PATH, logLevel)
	defer helper.Logger.Close()
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.Close()
	}
	yig = storage.New(helper.Logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)
	taskQ = make(chan types.LifeCycle, SCAN_HBASE_LIMIT)
	signal.Ignore()
	signalQueue = make(chan os.Signal)

	numOfWorkers := helper.CONFIG.LcThread
	helper.Logger.Info(nil, "start lc thread:", numOfWorkers)

	for i := 0; i < numOfWorkers; i++ {
		go processLifecycle()
	}
	go getLifeCycles()
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		default:
			// stop YIG server, order matters
			stop = true
			waitgroup.Wait()
			return
		}
	}

}
