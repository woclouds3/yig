package main

import (
	"fmt"
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
	SCAN_HBASE_LIMIT                   = 50
	DEFAULT_LC_LOG_PATH                = "/var/log/yig/lc.log"
	DEFAULT_LC_SLEEP_INTERVAL_HOUR     = 1 //TODO 24
	DEFAULT_LC_WAIT_FOR_GLACIER_MINUTE = 30
	DEFAULT_LC_QUEUE_LENGTH            = 100

	MAX_GLACIER_TRANSITION_SIZE = 4 * storage.TB
)

var (
	logger      *log.Logger
	yig         *storage.YigStorage
	taskQ       chan types.LifeCycle
	signalQueue chan os.Signal
	waitgroup   sync.WaitGroup
	empty       bool
	stop        bool

	// For Glacier only.
	taskHiddenBucketQ       chan string
	hiddenBucketLcWaitGroup sync.WaitGroup
)

func getLifeCycles() {
	var marker string
	logger.Println(5, 5, "all bucket lifecycle handle start")
	waitgroup.Add(1)
	defer waitgroup.Done()
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}

		// Hualu i/o latency may be 3 min at worst. So wait 30 min here.
		if len(taskQ) > DEFAULT_LC_QUEUE_LENGTH {
			logger.Println(5, 5, "taskQ", len(taskQ), "too long, sleep")
			time.Sleep(DEFAULT_LC_WAIT_FOR_GLACIER_MINUTE * time.Minute)
			continue
		}

		logger.Println(5, 5, "ScanLifeCycle start")

		result, err := yig.MetaStorage.ScanLifeCycle(nil, SCAN_HBASE_LIMIT, marker)
		if err != nil {
			logger.Println(5, "ScanLifeCycle failed", err)
			signalQueue <- syscall.SIGQUIT
			return
		}

		if len(result.Lcs) == 0 {
			marker = ""
			time.Sleep(DEFAULT_LC_SLEEP_INTERVAL_HOUR * time.Hour)
			continue
		}

		for _, entry := range result.Lcs {
			taskQ <- entry
			marker = entry.BucketName
		}

		if result.Truncated == false {
			empty = true
			return
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

// If a rule has an empty prefix ,the days in it will be consider as a default days for all objects that not specified in
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
	defaultDays := -1
	bucket, err := yig.MetaStorage.GetBucket(nil, lc.BucketName, false)
	if err != nil {
		return err
	}
	rules := bucket.LC.Rule
	for _, rule := range rules {
		if rule.Prefix == "" {
			defaultConfig = true
			if rule.Expiration != "" {
				defaultDays, err = strconv.Atoi(rule.Expiration)
				if err != nil {
					return err
				}
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
				logger.Println(5, "[Failed] retrieveBucket ListObjectsInternal failed", bucket.Name)
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
				helper.Debugln("interval:", time.Since(object.LastModifiedTime).Seconds())

				/* Check expiration first. */
				if days != -1 && checkIfExpiration(object.LastModifiedTime, days) {
					helper.Debugln("come here")
					if object.NullVersion {
						object.VersionId = ""
					}
					_, err = yig.DeleteObject(nil, object.BucketName, object.Name, object.VersionId, common.Credential{})
					if err != nil {
						helper.Logger.Println(5, "[FAILED]", object.BucketName, object.Name, object.VersionId, err)
						fmt.Println("[FAILED]", object.BucketName, object.Name, object.VersionId, err)
						continue
					}
					helper.Logger.Println(5, "[DELETED]", object.BucketName, object.Name, object.VersionId)
					fmt.Println("[DELETED]", object.BucketName, object.Name, object.VersionId)

					continue
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
							logger.Println(5, "failed to delete object:", object.Name, object.BucketName)
							helper.Logger.Println(5, "[FAILED]", object.BucketName, object.Name, object.VersionId, err)
							fmt.Println("[FAILED]", object.BucketName, object.Name, object.VersionId, err)
							continue
						}
						helper.Logger.Println(5, "[DELETED]", object.BucketName, object.Name, object.VersionId)
						fmt.Println("[DELETED]", object.BucketName, object.Name, object.VersionId)

						continue
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
			helper.Logger.Print(5, ".")
			return
		}
		//		waitgroup.Add(1)
		//		select {
		//		case item := <-taskQ:
		item := <-taskQ
		waitgroup.Add(1)
		err := retrieveBucket(item)
		if err != nil {
			logger.Println(5, "[ERR] Bucket: ", item.BucketName, err)
			fmt.Printf("[ERR] Bucket:%v, %v", item.BucketName, err)
			waitgroup.Done()
			continue
		}
		fmt.Printf("[DONE] Bucket:%s\n", item.BucketName)
		/*
			default:
				if empty == true {
					logger.Println(5, "all bucket lifecycle handle complete. QUIT")
					signalQueue <- syscall.SIGQUIT
					waitgroup.Done()
					return
				}
			}
		*/
		waitgroup.Done()
	}
}

func main() {
	helper.SetupConfig()

	f, err := os.OpenFile(DEFAULT_LC_LOG_PATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file in current dir")
	}
	defer f.Close()
	stop = false
	logger = log.New(f, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.Close()
	}
	yig = storage.New(logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)
	taskQ = make(chan types.LifeCycle, SCAN_HBASE_LIMIT)
	taskHiddenBucketQ = make(chan string, SCAN_HBASE_LIMIT)
	signal.Ignore()
	signalQueue = make(chan os.Signal)

	numOfWorkers := helper.CONFIG.LcThread
	helper.Logger.Println(5, "start lc thread:", numOfWorkers)
	empty = false
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
