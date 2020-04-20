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
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/mods"
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
	transitionTaskQ       chan types.LifeCycle
	transitionWaitGroup   sync.WaitGroup

	// For Glacier only. Delete expiring hidden objects timely.
	taskHiddenBucketQ       chan string
	hiddenBucketLcWaitGroup sync.WaitGroup
)

func getLifeCycleTransition() {
	var marker string
	logger.Info(nil, "[ Glacier ] all bucket transition handle start")
	transitionWaitGroup.Add(1)
	defer transitionWaitGroup.Done()
	for {
		if stop {
			helper.Logger.Info(nil, "stop getLifeCycleTransition.")
			return
		}

		// Hualu i/o latency may be 3 min at worst. So wait 30 min here.
		if len(taskQ) > DEFAULT_TRANSITION_QUEUE_LENGTH {
			logger.Info(nil, "[ Glacier ] taskQ", len(taskQ), "too long, sleep")
			time.Sleep(DEFAULT_TRANSITION_WAIT_FOR_GLACIER_MINUTE * time.Minute)
			continue
		}

		logger.Info(nil, "[ Glacier ] ScanTransitionBuckets start")

		// Scan only transition rules.
		result, err := yig.MetaStorage.ScanTransitionBuckets(nil, SCAN_TRANSITION_LIMIT, marker)
		if err != nil {
			logger.Info(nil, "[ Glacier ] ScanTransitionBuckets failed", err)
			time.Sleep(DEFAULT_TRANSITION_WAIT_FOR_GLACIER_MINUTE * time.Minute)
			continue
		}

		for _, entry := range result.Lcs {
			logger.Info(nil, "[ Glacier ] ScanTransitionBuckets: bucket:", entry)
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
func transitionRetrieveBucket(lc types.LifeCycle) error {
	bucket, err := yig.MetaStorage.GetBucket(nil, lc.BucketName, false)
	if err != nil {
		logger.Println(5, "[ Glacier ] transitionRetrieveBucket GetBucket failed, ", err)
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

func processTransition() {
	time.Sleep(time.Second * 1)
	for {
		if stop {
			helper.Logger.Warn(nil, "stop.")
			return
		}

		item := <-taskQ
		transitionWaitGroup.Add(1)
		err := transitionRetrieveBucket(item)
		if err != nil {
			logger.Error(nil, "[ Glacier ] [ERR] Bucket: ", item.BucketName, err)
			transitionWaitGroup.Done()
			continue
		}
		logger.Info(nil, "[ Glacier ] [DONE] Bucket:%s\n", item.BucketName)

		transitionWaitGroup.Done()
	}
}

func getHiddenBucket() {
	var marker string
	helper.Logger.Info(nil, "[ HiddenBucket ] hidden bucket lifecycle handle start")
	hiddenBucketLcWaitGroup.Add(1)
	defer hiddenBucketLcWaitGroup.Done()
	for {
		if stop {
			helper.Logger.Info(nil, "stop getHiddenBucket.")
			return
		}

		if len(taskHiddenBucketQ) > DEFAULT_TRANSITION_QUEUE_LENGTH {
			helper.Logger.Info(nil, "[ HiddenBucket ] taskHiddenBucketQ", len(taskHiddenBucketQ), "too long, sleep")
			time.Sleep(DEFAULT_TRANSITION_WAIT_FOR_GLACIER_MINUTE * time.Minute)
			continue
		}

		buckets, truncated, err := yig.MetaStorage.ScanHiddenBuckets(nil, SCAN_TRANSITION_LIMIT, marker)
		if err != nil {
			helper.Logger.Error(nil, "[ HiddenBucket ] ScanHiddenBucketLifeCycle failed", err)
			signalQueue <- syscall.SIGQUIT
			return
		}

		helper.Logger.Info(nil, "[ HiddenBucket ] Scanned hidden buckets: ", buckets, truncated, "len:", len(buckets))

		if len(buckets) == 0 {
			marker = ""
			time.Sleep(DEFAULT_TRANSITION_SLEEP_INTERVAL_HOUR * time.Hour)
			continue
		}

		for _, entry := range buckets {
			taskHiddenBucketQ <- entry
			helper.Logger.Info(nil, "[ HiddenBucket ] getHiddenBucket sent a bucket name:", entry)
			marker = entry
		}
	}

}

func processHiddenBucket() {
	time.Sleep(time.Second * 1)
	for {
		if stop {
			helper.Logger.Info(nil, "stop processHiddenBucket .")
			return
		}

		bucketName := <-taskHiddenBucketQ
		helper.Logger.Info(nil, "[ HiddenBucket ] processHiddenBucket receive a bucket:", bucketName)
		transitionWaitGroup.Add(1)
		err := retrieveHiddenBucket(bucketName)
		if err != nil {
			logger.Error(nil, "[ERR] Bucket: ", bucketName, err)
		} else {
			logger.Info(nil, "[DONE] Bucket:%s\n", bucketName)
		}
		transitionWaitGroup.Done()
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
			helper.Logger.Error(nil, "[ HiddenBucket ] failed for bucket %s request %v", bucketName, request)
			return err
		}

		helper.Logger.Info(nil, "[ HiddenBucket ] bucket %s objects %v", bucketName, retObjects)

		for _, object := range retObjects {
			days, err := yig.MetaStorage.GetExpireDays(object)
			if err != nil {
				helper.Logger.Info(nil, "[ HiddenBucket ] GetExpireDays failed for bucket %s object %v", bucketName, object)
				return err
			}
			/* Check expiration only. */
			if days > 0 && checkIfExpiration(object.LastModifiedTime, int(days)) {
				_, err = yig.DeleteObject(nil, object.BucketName, object.Name, "", common.Credential{})
				if err != nil {
					helper.Logger.Error(nil, "[Hidden Bucket Delete FAILED]", object.BucketName, object.Name, days, err)
				} else {
					helper.Logger.Info(nil, "[Hidden Bucket Object DELETED]", object.BucketName, object.Name, days)
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

func StartTransition() {
	if !helper.CONFIG.Glacier.EnableGlacier {
		helper.Logger.Info(nil, "Glacier not enabled.")
		return
	}
	
	taskQ = make(chan types.LifeCycle, SCAN_TRANSITION_LIMIT)
	taskHiddenBucketQ = make(chan string, SCAN_TRANSITION_LIMIT)
	signal.Ignore()
	signalQueue = make(chan os.Signal)

	// Glacier requires ak/sk, use this to initialize iam plugin in lc process.
	allPluginMap := mods.InitialPlugins()
	iam.InitializeIamClient(allPluginMap)

	helper.Logger.Info(nil, "start transition thread:", helper.CONFIG.Glacier.TransitionThread)
	for i := 0; i < helper.CONFIG.Glacier.TransitionThread; i++ {
		go processTransition()
	}
	go getLifeCycleTransition()

	helper.Logger.Info(nil, "start hidden bucket thread:", helper.CONFIG.Glacier.HiddenBucketLcThread)
	for i := 0; i < helper.CONFIG.Glacier.HiddenBucketLcThread; i++ {
		go processHiddenBucket()
	}
	go getHiddenBucket()
	
	helper.Logger.Info(nil, "Glacier transition started.")
}
