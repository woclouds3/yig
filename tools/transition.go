package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/mods"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

const (
	SCAN_TRANSITION_LIMIT                      = 20
	MAX_OBJECT_PER_LOOP						   = 50
	DEFAULT_TRANSITION_LOG_PATH                = "/var/log/yig/transition.log"
	DEFAULT_TRANSITION_SLEEP_INTERVAL_HOUR     = 1 //TODO 24
	DEFAULT_TRANSITION_WAIT_FOR_GLACIER_MINUTE = 30
	DEFAULT_TRANSITION_QUEUE_LENGTH            = 10

	MAX_GLACIER_TRANSITION_SIZE = 4 * storage.TB // ehualu limitation.
)

var (
	logger      *log.Logger
	yig         *storage.YigStorage
	taskQ       chan types.LifeCycle
	signalQueue chan os.Signal
	waitgroup   sync.WaitGroup
	stop        bool

	// For Glacier only. Delete expiring hidden objects timely.
	taskHiddenBucketQ       chan string
	hiddenBucketLcWaitGroup sync.WaitGroup
)

func getLifeCycles() {
	var marker string
	logger.Println(5, "[ Glacier ] all bucket transition handle start")
	waitgroup.Add(1)
	defer waitgroup.Done()
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}

		// Hualu i/o latency may be 3 min at worst. So wait 30 min here.
		if len(taskQ) > DEFAULT_TRANSITION_QUEUE_LENGTH {
			logger.Println(5, "[ Glacier ] taskQ", len(taskQ), "too long, sleep")
			time.Sleep(DEFAULT_TRANSITION_WAIT_FOR_GLACIER_MINUTE * time.Minute)
			continue
		}

		logger.Println(5, "[ Glacier ] ScanTransitionBuckets start")

		// Scan only transition rules.
		result, err := yig.MetaStorage.ScanTransitionBuckets(nil, SCAN_TRANSITION_LIMIT, marker)
		if err != nil {
			logger.Println(5, "[ Glacier ] ScanTransitionBuckets failed", err)
			time.Sleep(DEFAULT_TRANSITION_WAIT_FOR_GLACIER_MINUTE * time.Minute)
			continue
		}

		for _, entry := range result.Lcs {
			logger.Println(20, "[ Glacier ] ScanTransitionBuckets: bucket:", entry)
			taskQ <- entry
			marker = entry.BucketName
		}

		if result.Truncated == false {
			marker = ""
			time.Sleep(DEFAULT_TRANSITION_SLEEP_INTERVAL_HOUR * time.Hour)
			continue
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
	bucket, err := yig.MetaStorage.GetBucket(nil, lc.BucketName, false)
	if err != nil {
		logger.Println(5, "[ Glacier ] retrieveBucket GetBucket failed, ", err)
		return err
	}
	var glacierRules []datatype.LcRule
	for _, rule := range bucket.LC.Rule {
		if rule.TransitionStorageClass == "GLACIER" && rule.Status == "Enabled" {
			glacierRules = append(glacierRules, rule)
		}
	}

	// TODO: should sort glacierRules to match the longest prefix for objects?

	var defaultConfig *datatype.LcRule
	for _, rule := range glacierRules {
		if rule.Prefix == "" {
			defaultConfig = &rule
			continue
		}

		transitionObjectsForRule(bucket, &rule)
	}

	if defaultConfig != nil {
		transitionObjectsForRule(bucket, defaultConfig)
	}

	return nil
}

func transitionObjectsForRule(bucket *types.Bucket, rule *datatype.LcRule) {
	if rule == nil {
		return
	}

	logger.Println(20, "[ Glacier ] transitionObjectsForRule: ", bucket.Name, rule)

	for {
		var nextMarker string
		var nextVerIdMarker string

		retObjects, _, truncated, nextMarker, nextVerIdMarker, err :=
			yig.MetaStorage.Client.ListTransitionObjects(bucket.Name, nextMarker, nextVerIdMarker, rule.Prefix, false, MAX_OBJECT_PER_LOOP, rule.TransitionDays)
		if err != nil {
			helper.Logger.Println(20, "[ Glacier ] ListTransitionObjects failed", err)
			return
		}

		for _, object := range retObjects {
			logger.Println(20, "[ Glacier ] Start transitioning object:", object.BucketName, object.Name, object.VersionId, object.StorageClass, object.LastModifiedTime)
			// Check transition expire, add object to glacier and add the object to gc table. 
			//			if object.StorageClass == types.ObjectStorageClassStandard && checkIfExpiration(object.LastModifiedTime, transitionDays) {
			if err = yig.MetaStorage.MarkObjectTransitioning(object); err != nil {
				helper.Logger.Println(20, "[ Glacier ] [Transition FAILED] for MarkObjectTransitioning", object.BucketName, object.Name, object.VersionId, err)
				continue
			}

			err = yig.TransitObjectToGlacier(nil, bucket, object)
			if err != nil {
				helper.Logger.Println(5, "[ Glacier ] [Transition FAILED]", object.BucketName, object.Name, object.VersionId, err)
				continue
			}
		}

		if truncated == false {
			break
		}
	}
}

func processLifecycle() {
	time.Sleep(time.Second * 1)
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}

		item := <-taskQ
		waitgroup.Add(1)
		err := retrieveBucket(item)
		if err != nil {
			logger.Println(5, "[ Glacier ] [ERR] Bucket: ", item.BucketName, err)
			waitgroup.Done()
			continue
		}
		logger.Printf(20, "[ Glacier ] [DONE] Bucket:%s\n", item.BucketName)

		waitgroup.Done()
	}
}

func getHiddenBucket() {
	var marker string
	helper.Logger.Println(5, "[ HiddenBucket ] hidden bucket lifecycle handle start")
	hiddenBucketLcWaitGroup.Add(1)
	defer hiddenBucketLcWaitGroup.Done()
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}

		if len(taskHiddenBucketQ) > DEFAULT_TRANSITION_QUEUE_LENGTH {
			helper.Logger.Println(5, "[ HiddenBucket ] taskHiddenBucketQ", len(taskHiddenBucketQ), "too long, sleep")
			time.Sleep(DEFAULT_TRANSITION_WAIT_FOR_GLACIER_MINUTE * time.Minute)
			continue
		}

		buckets, truncated, err := yig.MetaStorage.ScanHiddenBuckets(nil, SCAN_TRANSITION_LIMIT, marker)
		if err != nil {
			helper.Logger.Println(5, "[ HiddenBucket ] ScanHiddenBucketLifeCycle failed", err)
			signalQueue <- syscall.SIGQUIT
			return
		}

		helper.Logger.Println(20, "[ HiddenBucket ] Scanned hidden buckets: ", buckets, truncated, "len:", len(buckets))

		if len(buckets) == 0 {
			marker = ""
			time.Sleep(DEFAULT_TRANSITION_SLEEP_INTERVAL_HOUR * time.Hour)
			continue
		}

		for _, entry := range buckets {
			taskHiddenBucketQ <- entry
			helper.Logger.Println(20, "[ HiddenBucket ] getHiddenBucket sent a bucket name:", entry)
			marker = entry
		}
	}

}

func processHiddenBucket() {
	time.Sleep(time.Second * 1)
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}

		bucketName := <-taskHiddenBucketQ
		helper.Logger.Println(20, "[ HiddenBucket ] processHiddenBucket receive a bucket:", bucketName)
		waitgroup.Add(1)
		err := retrieveHiddenBucket(bucketName)
		if err != nil {
			logger.Println(5, "[ERR] Bucket: ", bucketName, err)
		} else {
			fmt.Printf("[DONE] Bucket:%s\n", bucketName)
		}
		waitgroup.Done()
	}
}

func retrieveHiddenBucket(bucketName string) error {
	if !strings.HasPrefix(bucketName, types.HIDDEN_BUCKET_PREFIX) {
		return fmt.Errorf("bucket %s is not a hidden bucket %s !", bucketName, types.HIDDEN_BUCKET_PREFIX)
	}

	var request datatype.ListObjectsRequest
	request.Versioned = false
	request.MaxKeys = 1000

	for {
		retObjects, _, truncated, nextMarker, nextVerIdMarker, err := yig.ListObjectsInternal(nil, bucketName, request)
		if err != nil {
			helper.Logger.Println(20, "[ HiddenBucket ] failed for bucket %s request %v", bucketName, request)
			return err
		}

		helper.Logger.Printf(20, "[ HiddenBucket ] bucket %s objects %v", bucketName, retObjects)

		for _, object := range retObjects {
			days, err := yig.MetaStorage.GetExpireDays(object)
			if err != nil {
				helper.Logger.Printf(10, "[ HiddenBucket ] GetExpireDays failed for bucket %s object %v", bucketName, object)
				return err
			}
			/* Check expiration only. */
			if days > 0 && checkIfExpiration(object.LastModifiedTime, int(days)) {
				_, err = yig.DeleteObject(nil, object.BucketName, object.Name, "", common.Credential{})
				if err != nil {
					helper.Logger.Println(10, "[Hidden Bucket Delete FAILED]", object.BucketName, object.Name, days, err)
				} else {
					helper.Logger.Println(20, "[Hidden Bucket Object DELETED]", object.BucketName, object.Name, days)
				}
			}
		}

		if len(retObjects) != 0 || truncated == true {
			request.KeyMarker = nextMarker
			request.VersionIdMarker = nextVerIdMarker
		} else {
			break
		}
	}

	return nil
}

func main() {
	helper.SetupConfig()

	f, err := os.OpenFile(DEFAULT_TRANSITION_LOG_PATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
	taskQ = make(chan types.LifeCycle, SCAN_TRANSITION_LIMIT)
	taskHiddenBucketQ = make(chan string, SCAN_TRANSITION_LIMIT)
	signal.Ignore()
	signalQueue = make(chan os.Signal)

	// Glacier requires ak/sk, use this to initialize iam plugin in lc process.
	if helper.CONFIG.Glacier.EnableGlacier {
		allPluginMap := mods.InitialPlugins()
		iam.InitializeIamClient(allPluginMap)
	}

	numOfWorkers := helper.CONFIG.LcThread
	helper.Logger.Println(5, "start lc thread:", numOfWorkers)
	for i := 0; i < numOfWorkers; i++ {
		go processLifecycle()
	}
	go getLifeCycles()

	if helper.CONFIG.Glacier.EnableGlacier {
		helper.Logger.Println(5, "start hidden bucket thread:", helper.CONFIG.Glacier.HiddenBucketLcThread)
		for i := 0; i < helper.CONFIG.Glacier.HiddenBucketLcThread; i++ {
			go processHiddenBucket()
		}
		go getHiddenBucket()
	}

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
