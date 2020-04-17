package _go

import (
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

func Test_MakeInvalidBucket(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_INVALID_BUCKET)
	if err == nil {
		t.Fatal("MakeInvalidBucket err:", err)
		panic(err)
	}
	t.Log("MakeInvalidBucket Success.")
}
