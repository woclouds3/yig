package _go

import (
	"strconv"
	"testing"

	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_MakeBucket(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")
}

func Test_HeadBucket(t *testing.T) {
	sc := NewS3()
	err := sc.HeadBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("HeadBucket err:", err)
	}
	t.Log("HeadBucket Success.")
}

func Test_DeleteBucket(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
	err = sc.HeadBucket(TEST_BUCKET)
	if err == nil {
		t.Fatal("DeleteBucket Failed")
		panic(err)
	}
	t.Log("DeleteBucket Success.")
}

func Test_ListObjectVersionsWithMaxKey(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer sc.DeleteBucket(TEST_BUCKET)

	if err = sc.PutBucketVersioning(TEST_BUCKET, "Enabled"); err != nil {
		t.Fatal("PutBucketVersioning err :", err)
		panic(err)
	}

	// TEST_KEY1 and TEST_KEY2, 3 versions for each.
	keyCount := 2
	versionCount := 3
	expectedList := make([][]string, keyCount*versionCount)
	for key := 0; key < keyCount; key++ {
		for version := 0; version < versionCount; version++ {
			versionId, err := sc.PutObjectVersioning(TEST_BUCKET, TEST_KEY+strconv.Itoa(key), TEST_VALUE)
			if err != nil {
				t.Fatal("PutObject err:", err)
				panic(err)
			}

			expectedList[key*versionCount+(versionCount-version-1)] = []string{TEST_KEY + strconv.Itoa(key), versionId}
			defer sc.DeleteObjectVersion(TEST_BUCKET, TEST_KEY+strconv.Itoa(key), versionId)
		}
	}

	keyMarker := ""
	versionIdMarker := ""
	maxKeys := int64(2)
	resultList := make([][]string, 0)
	for {
		var result *[][]string
		var isTruncated bool

		result, isTruncated, keyMarker, versionIdMarker, err = sc.ListObjectVersions(TEST_BUCKET, keyMarker, versionIdMarker, maxKeys)
		if err != nil {
			t.Fatal("ListObjectVersions err:", err)
			panic(err)
		}

		resultList = append(resultList, (*result)...)

		if !isTruncated {
			break
		}
	}

	if len(expectedList) != len(resultList) {
		t.Fatal("resultList:", resultList)
		panic("")
	}

	for i, _ := range expectedList {
		if expectedList[i][0] != resultList[i][0] || expectedList[i][1] != resultList[i][1] {
			t.Fatal("unexpected list result:", resultList)
			panic("")
		}
	}

	t.Log("ListObjectVersions Success.")
}

/*
func Test_ListObjectVersions10000(t *testing.T) {

	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	defer sc.DeleteBucket(TEST_BUCKET)

	if err = sc.PutBucketVersioning(TEST_BUCKET, "Enabled"); err != nil {
		t.Fatal("PutBucketVersioning err :", err)
		panic(err)
	}

	t.Log("Before objects created:", time.Now())

	for i := 0; i < 10000; i++ {
		_, err := sc.PutObjectVersioning(TEST_BUCKET, TEST_KEY, TEST_VALUE)
		if err != nil {
			t.Fatal("PutObjectVersioning failed for", i, err)
		}
	}

	t.Log("All the 10000 objects created:", time.Now())

	keyMarker := ""
	versionIdMarker := ""
	maxKeys := int64(-1)
	var versionCount int
	for {
		var result *[][]string
		var isTruncated bool

		result, isTruncated, keyMarker, versionIdMarker, err = sc.ListObjectVersions(TEST_BUCKET, keyMarker, versionIdMarker, maxKeys)
		if err != nil {
			t.Fatal("ListObjectVersions err:", err)
			panic(err)
		}

		if len(*result) != 1000 {
			t.Fatal("ListObjectVersions unexpected result len:", len(*result))
			panic("")
		}

		versionCount += len(*result)
		t.Log("ListObjectVersions: ", versionCount, time.Now())

		if !isTruncated {
			break
		}
	}

	t.Log("After ListObjectVersions:", time.Now())

	if versionCount != 10000 {
		t.Fatal("ListObjectVersions10000 unexpected count:", versionCount)
		panic("")
	}

	if _, err = sc.ListObjects(TEST_BUCKET); err != nil {
		t.Fatal("ListObjects err:", err)
		panic(err)
	}

	t.Log("After ListObjects:", time.Now())

	for {
		var result *[][]string
		var isTruncated bool

		var err error

		result, isTruncated, _, _, err = sc.ListObjectVersions(TEST_BUCKET, "", "", int64(-1))
		if err != nil {
			t.Fatal("ListObjectVersions err:", err)
			panic(err)
		}

		for _, keyVersion := range *result {
			_ = sc.DeleteObjectVersion(TEST_BUCKET, TEST_KEY, keyVersion[1])
		}

		if !isTruncated {
			break
		}
	}

	t.Log("After DeleteObjects:", time.Now())

	t.Log("ListObjectVersions 10000 Success")
}
*/
