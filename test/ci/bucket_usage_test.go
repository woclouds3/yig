package ci

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
	. "gopkg.in/check.v1"
)

func (cs *CISuite) TestBasicBucketUsage(c *C) {
	ctx := context.Background()
	bn := "buckettest"
	key := "objt1"
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage == 0, Equals, true)
	randUtil := &RandUtil{}
	val := randUtil.RandString(128 << 10)
	err = sc.PutObject(bn, key, val)
	c.Assert(err, Equals, nil)
	defer sc.DeleteObject(bn, key)
	// check the bucket usage.
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, int64(128<<10))
}

func (cs *CISuite) TestManyObjectsForBucketUsage(c *C) {
	ctx := context.Background()
	var wg sync.WaitGroup
	bn := "buckettest"
	count := 100
	size := (128 << 10)
	totalObjSize := int64(0)
	var objNames []string
	sc := NewS3()
	sc.DeleteBucket(bn)
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, int64(0))
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int, size int, c *C) {
			defer wg.Done()
			randUtil := &RandUtil{}
			val := randUtil.RandString(size)
			key := fmt.Sprintf("objt%d", idx+1)
			err = sc.PutObject(bn, key, val)
			c.Assert(err, Equals, nil)
			atomic.AddInt64(&totalObjSize, int64(size))
			objNames = append(objNames, key)
		}(i, size, c)
	}
	wg.Wait()

	defer func() {
		for _, obj := range objNames {
			wg.Add(1)
			go func(obj string, c *C) {
				defer wg.Done()
				err = sc.DeleteObject(bn, obj)
				c.Assert(err, Equals, nil)
				atomic.AddInt64(&totalObjSize, int64(-size))
			}(obj, c)
		}
		wg.Wait()
		c.Assert(totalObjSize, Equals, int64(0))
		b, err := cs.storage.GetBucket(ctx, bn)
		c.Assert(err, Equals, nil)
		c.Assert(b, Not(Equals), nil)
		c.Assert(b.Usage, Equals, int64(0))
		sc.DeleteBucket(bn)
	}()
	// check the bucket usage.
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, totalObjSize)
}

func (cs *CISuite) TestBucketUsageForAppendObject(c *C) {
	ctx := context.Background()
	bn := "buckettest"
	key := "objtappend"
	totalSize := int64(0)
	nextSize := int64(0)
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, int64(0))
	ru := &RandUtil{}
	size := (128 << 10)
	body := ru.RandString(size)
	nextSize, err = sc.AppendObject(bn, key, body, 0)
	c.Assert(err, Equals, nil)
	defer sc.DeleteObject(bn, key)
	totalSize = totalSize + int64(size)
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, totalSize)
	appendBody := ru.RandString(size)
	nextSize, err = sc.AppendObject(bn, key, appendBody, int64(size))
	c.Assert(err, Equals, nil)
	totalSize = totalSize + int64(size)
	c.Assert(nextSize, Equals, totalSize)
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, totalSize)
}

func (cs *CISuite) TestBucketUsageForMultiPartAbort(c *C) {
	ctx := context.Background()
	bn := "buckettest"
	key := "objtappend"
	totalSize := int64(0)
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, int64(0))
	uploadId, err := sc.CreateMultiPartUpload(bn, key, s3.ObjectStorageClassStandard)
	c.Assert(err, Equals, nil)
	c.Assert(uploadId, Not(Equals), "")
	defer sc.DeleteObject(bn, key)
	ru := &RandUtil{}
	size := (128 << 10)
	for i := 0; i < 5; i++ {
		partNum := int64(i + 1)
		etag, err := sc.UploadPart(bn, key, ru.RandBytes(size), uploadId, partNum)
		c.Assert(err, Equals, nil)
		c.Assert(etag, Not(Equals), "")
		totalSize = totalSize + int64(size)
		b, err = cs.storage.GetBucket(ctx, bn)
		c.Assert(err, Equals, nil)
		c.Assert(b, Not(Equals), nil)
		c.Assert(b.Usage, Equals, totalSize)
	}

	// abort the multipart.
	err = sc.AbortMultiPartUpload(bn, key, uploadId)
	c.Assert(err, Equals, nil)
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, int64(0))
}

func (cs *CISuite) TestBucketUsageForMultiPartComplete(c *C) {
	ctx := context.Background()
	bn := "buckettest"
	key := "objtappend"
	totalSize := int64(0)
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, int64(0))
	uploadId, err := sc.CreateMultiPartUpload(bn, key, s3.ObjectStorageClassStandard)
	c.Assert(err, Equals, nil)
	c.Assert(uploadId, Not(Equals), "")
	defer sc.DeleteObject(bn, key)
	ru := &RandUtil{}
	size := (128 << 10)
	loopNum := 5
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, loopNum),
	}

	for i := 0; i < loopNum; i++ {
		partNum := int64(i + 1)
		etag, err := sc.UploadPart(bn, key, ru.RandBytes(size), uploadId, partNum)
		c.Assert(err, Equals, nil)
		c.Assert(etag, Not(Equals), "")
		totalSize = totalSize + int64(size)
		b, err = cs.storage.GetBucket(ctx, bn)
		c.Assert(err, Equals, nil)
		c.Assert(b, Not(Equals), nil)
		c.Assert(b.Usage, Equals, totalSize)
		completedUpload.Parts[i] = &s3.CompletedPart{
			PartNumber: aws.Int64(partNum),
			ETag:       aws.String(etag),
		}
	}

	// abort the multipart.
	err = sc.CompleteMultiPartUpload(bn, key, uploadId, completedUpload)
	c.Assert(err, Equals, nil)
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, totalSize)
}

func (cs *CISuite) TestBasicBucketFileNum(c *C) {
	ctx := context.Background()
	bn := "buckettest"
	key := "objfilenum1"
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum == 0, Equals, true)
	randUtil := &RandUtil{}
	val := randUtil.RandString(128 << 10)
	err = sc.PutObject(bn, key, val)
	c.Assert(err, Equals, nil)
	defer sc.DeleteObject(bn, key)
	// check the bucket usage.
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(1))
}

func (cs *CISuite) TestManyObjectsForBucketFileNum(c *C) {
	ctx := context.Background()
	var wg sync.WaitGroup
	bn := "buckettest"
	count := 100
	size := (128 << 10)
	var objNames []string
	sc := NewS3()
	sc.DeleteBucket(bn)
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(0))
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int, size int, c *C) {
			defer wg.Done()
			randUtil := &RandUtil{}
			val := randUtil.RandString(size)
			key := fmt.Sprintf("objt%d", idx+1)
			err = sc.PutObject(bn, key, val)
			c.Assert(err, Equals, nil)
			objNames = append(objNames, key)
		}(i, size, c)
	}
	wg.Wait()

	defer func() {
		for _, obj := range objNames {
			wg.Add(1)
			go func(obj string, c *C) {
				defer wg.Done()
				err = sc.DeleteObject(bn, obj)
				c.Assert(err, Equals, nil)
			}(obj, c)
		}
		wg.Wait()
		b, err := cs.storage.GetBucket(ctx, bn)
		c.Assert(err, Equals, nil)
		c.Assert(b, Not(Equals), nil)
		c.Assert(b.FileNum, Equals, int64(0))
		sc.DeleteBucket(bn)
	}()
	// check the bucket usage.
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(count))
}

func (cs *CISuite) TestBucketFileNumForAppendObject(c *C) {
	ctx := context.Background()
	bn := "buckettest"
	key := "objtappend"
	totalSize := int64(0)
	nextSize := int64(0)
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(0))
	ru := &RandUtil{}
	size := (128 << 10)
	body := ru.RandString(size)
	nextSize, err = sc.AppendObject(bn, key, body, 0)
	c.Assert(err, Equals, nil)
	defer sc.DeleteObject(bn, key)
	totalSize = totalSize + int64(size)
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(1))
	appendBody := ru.RandString(size)
	nextSize, err = sc.AppendObject(bn, key, appendBody, int64(size))
	c.Assert(err, Equals, nil)
	totalSize = totalSize + int64(size)
	c.Assert(nextSize, Equals, totalSize)
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(1))
}

func (cs *CISuite) TestBucketFileNumForMultiPartAbort(c *C) {
	ctx := context.Background()
	bn := "buckettest"
	key := "objtappend"
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(0))
	uploadId, err := sc.CreateMultiPartUpload(bn, key, s3.ObjectStorageClassStandard)
	c.Assert(err, Equals, nil)
	c.Assert(uploadId, Not(Equals), "")
	defer sc.DeleteObject(bn, key)
	ru := &RandUtil{}
	size := (128 << 10)
	for i := 0; i < 5; i++ {
		partNum := int64(i + 1)
		etag, err := sc.UploadPart(bn, key, ru.RandBytes(size), uploadId, partNum)
		c.Assert(err, Equals, nil)
		c.Assert(etag, Not(Equals), "")
		b, err = cs.storage.GetBucket(ctx, bn)
		c.Assert(err, Equals, nil)
		c.Assert(b, Not(Equals), nil)
		c.Assert(b.FileNum, Equals, int64(0))
	}

	// abort the multipart.
	err = sc.AbortMultiPartUpload(bn, key, uploadId)
	c.Assert(err, Equals, nil)
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(0))
}

func (cs *CISuite) TestBucketFileNumForMultiPartComplete(c *C) {
	ctx := context.Background()
	bn := "buckettest"
	key := "objtappend"
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(0))
	uploadId, err := sc.CreateMultiPartUpload(bn, key, s3.ObjectStorageClassStandard)
	c.Assert(err, Equals, nil)
	c.Assert(uploadId, Not(Equals), "")
	defer sc.DeleteObject(bn, key)
	ru := &RandUtil{}
	size := (128 << 10)
	loopNum := 5
	completedUpload := &s3.CompletedMultipartUpload{
		Parts: make([]*s3.CompletedPart, loopNum),
	}

	for i := 0; i < loopNum; i++ {
		partNum := int64(i + 1)
		etag, err := sc.UploadPart(bn, key, ru.RandBytes(size), uploadId, partNum)
		c.Assert(err, Equals, nil)
		c.Assert(etag, Not(Equals), "")
		b, err = cs.storage.GetBucket(ctx, bn)
		c.Assert(err, Equals, nil)
		c.Assert(b, Not(Equals), nil)
		c.Assert(b.FileNum, Equals, int64(0))
		completedUpload.Parts[i] = &s3.CompletedPart{
			PartNumber: aws.Int64(partNum),
			ETag:       aws.String(etag),
		}
	}

	// abort the multipart.
	err = sc.CompleteMultiPartUpload(bn, key, uploadId, completedUpload)
	c.Assert(err, Equals, nil)
	b, err = cs.storage.GetBucket(ctx, bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.FileNum, Equals, int64(1))
}
