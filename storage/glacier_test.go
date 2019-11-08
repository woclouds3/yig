package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/credentials"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

const (
	GLACIER_TEST_BUCKET             = "test-glacier-bucket2"
	GLACIER_TEST_OBJECT             = "test-smallfile.txt"
	GLACIER_TEST_SMALL_FILE_CONTENT = "Glacier test small file content"
	GLACIER_ACCOUNT_ID              = ""
	TEST_S3_HOST                    = "http://s3.test.com:80"
	TEST_S3_REGION					= "cn-bj-1"
	GLACIER_TEST_AK                 = "U5S0O3O15DDPCGVF9MB6"
	GLACIER_TEST_SK                 = "W59APTYZ1MG6S5SDS9KPFCCY6R3A2IF3T705A5ZT"
	GLACIER_TEST_BIG_OBJECT         = "test-bigfile-"
	GLACIER_TEST_BIG_OBJECT_UNALIGNED	= "test-bigfile-unaligned-"

	DEFAULT_RESTORE_TIME_PER_UNIT	= 60
	DEFAULT_RESTORE_TIME_UNIT		= 256 // MB

	DEFAULT_READER_TYPE				= iota 
	UNALIGNED_READER_TYPE
)

var files [][]int = [][]int{
	[]int {5, 5, 5},
	[]int {60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60},
	// [] int {50, 50, 50, 50, 56, 50, 50, 50, 50, 56},
	// []int{100, 100, 56, 100, 100, 56},
	// []int{256, 256, 256, 256},
	// []int{100, 100, 100, 100, 100, 100, 100, 10},
}

var randomFiles [][]int = [][]int{
	[] int {5 * MB + 10, 5 * MB - 10},
	[] int {50 * MB, 50 * MB, 50 * MB, 50 * MB, 56 * MB - 10, 
			50 * MB, 50 * MB, 50 * MB, 50 * MB, 56 * MB - 10,
			50 * MB, 50 * MB, 50 * MB, 50 * MB, 56 * MB,
			50 * MB, 50 * MB, 50 * MB, 50 * MB, 80 * MB},
}

func handleErr(t *testing.T, err error, funcName string) {
	if err != nil {
		t.Fatal(funcName+" err:", err)
		t.FailNow()
	}
	t.Log(funcName + " Success.")
}

type bigFilePartsReader struct {
	size   int64
	offset int64
	readerType	int
	partOffset	int64
}

func (r *bigFilePartsReader) Read(p []byte) (n int, err error) {
	if r.offset >= r.size {
		//fmt.Println("Read exit 1 ", len(p), r.size, r.offset)
		return 0, io.EOF
	}
	remaining := r.size - r.offset
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	//fmt.Println("Read", len(p), r.size, r.offset)

	if r.readerType == DEFAULT_READER_TYPE {
		for i := 0; i < len(p); i++ {
			p[i] = byte(r.offset / MB)
			r.offset++
		}
	} else {
		for i := 0; i < len(p); i++ {
			p[i] = byte(r.partOffset + r.offset)
			r.offset++
		}
	}

	return len(p), nil
}

func (r *bigFilePartsReader) Len() int {
	// fmt.Println("Len", r.size, r.offset)
	if r.offset >= r.size {
		return 0
	}

	return int(r.size - r.offset)
}

func (r *bigFilePartsReader) Seek(offset int64, whence int) (int64, error) {
	// fmt.Println("Seek", r.size, r.offset, offset, whence)
	switch whence {
	case 0:
		r.offset = offset
	case 1:
		if r.offset+offset >= r.size {
			return 0, io.EOF
		}
		r.offset += offset
	default:
		return 0, io.EOF
	}

	return r.offset, nil
}

func (r *bigFilePartsReader) Close() error {
	return nil
}

func TransitionBigObjectUploadHelper(t *testing.T, fileName string, fileSize []int, readerType int) {
	creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
	sc := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(TEST_S3_HOST),
			Region:      aws.String("r"),
			S3ForcePathStyle:	aws.Bool(true),
		},
	)))

	// Create a big object in parts.
	result, err := sc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(fileName),
	})
	handleErr(t, err, "CreateMultipartUpload " + fileName)

	defer sc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(GLACIER_TEST_BUCKET),
		Key:      aws.String(fileName),
		UploadId: aws.String(*result.UploadId),
	})

	// For example,
	// S3 part              |==256M==|=100M=|==256M==|==256M==|===256M===|
	// Glacier part         |======512M========|=======512M=======|=100M=|
	// Content / MB			|=0-255==|=0-99=|==0-255=|=0-255==|==0-255===|

	var parts = make([]*s3.CompletedPart, len(fileSize))
	totalSize := int64(0)
	for partNumber := 1; partNumber <= len(fileSize); partNumber++ {
		var body io.ReadSeeker
		if readerType == DEFAULT_READER_TYPE {
			body = aws.ReadSeekCloser(&bigFilePartsReader{
				size:   int64(fileSize[partNumber-1] * MB),
				offset: 0,
				readerType:	readerType,
				partOffset:	totalSize,
			})

			totalSize += int64(fileSize[partNumber-1]) * MB
		} else if readerType == UNALIGNED_READER_TYPE {
			body = aws.ReadSeekCloser(&bigFilePartsReader{
				size:   int64(fileSize[partNumber-1]),
				offset: 0,
				readerType:	readerType,
				partOffset:	totalSize,
			})

			totalSize += int64(fileSize[partNumber-1])
		}

		partResult, err := sc.UploadPart(&s3.UploadPartInput{
			Body: 		body,
			Bucket:     aws.String(GLACIER_TEST_BUCKET),
			Key:        aws.String(fileName),
			PartNumber: aws.Int64(int64(partNumber)),
			UploadId:   result.UploadId,
		})

		handleErr(t, err, "Upload part "+strconv.Itoa(partNumber))
		fmt.Println("UploadPart ", partNumber, " Completed.")

		parts[partNumber-1] = &s3.CompletedPart{
			ETag:       aws.String(*partResult.ETag),
			PartNumber: aws.Int64(int64(partNumber)),
		}

		time.Sleep(10 * time.Second)
	}

	_, err = sc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(fileName),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
		UploadId: result.UploadId,
	})
	handleErr(t, err, "CompleteUploadMultipart")

	t.Log("Glacier big file test upload finished for " + fileName)
}

func TransitionBigObjectVerifyHelper(t *testing.T, fileName string, fileSize []int, readerType int) {
	creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
	sc := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(TEST_S3_HOST),
			Region:      aws.String("r"),
			S3ForcePathStyle:	aws.Bool(true),
		},
	)))

	// Check the object, should be "StorageClass" Glacier.
	headInfo, err := sc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(fileName),
	})
	handleErr(t, err, "HeadObject")
	if (*headInfo.StorageClass) != "GLACIER" {
		t.Fatal("Invalid StorageClass after Transition:", *headInfo.StorageClass, *headInfo)
		t.FailNow()
	}

	// Get the object in Glacier without restore. Should fail.
	_, err = sc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(fileName),
	})
	if err == nil {
		t.Log("Get Object shouldn't success without restore")
		t.FailNow()
	}

	// Restore object.
	_, err = sc.RestoreObject(&s3.RestoreObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(fileName),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(1),
			GlacierJobParameters: &s3.GlacierJobParameters{
				Tier: aws.String("Expedited"),
			},
		},
	})
//	handleErr(t, err, "RestoreObject")

	size := 0
	for _, partSize := range(fileSize) {
		size += partSize
	}
	if readerType == UNALIGNED_READER_TYPE {
		size /= MB
	}
	size = size / DEFAULT_RESTORE_TIME_UNIT + 1

	fmt.Println("Restoring...Wait for", size, "min")
	for i := 0; i < size; i++ {
		time.Sleep(DEFAULT_RESTORE_TIME_PER_UNIT * time.Second)
	}
	fmt.Println("Verify file", fileName)

	// Get the object in Glacier.
	getObjectOutput, err := sc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(fileName),
	})
	handleErr(t, err, "GetObject")
	defer getObjectOutput.Body.Close()

	// Verify the content MB by MB.
	buf := make([]byte, MB)
	if readerType == DEFAULT_READER_TYPE {
		for i, size := range fileSize {
			for offset := 0; offset < size; offset++ {
				// Read a MB
				outputLen, err := io.ReadFull(getObjectOutput.Body, buf)
				if err != nil {
					handleErr(t, err, "Read failed for "+fileName+" part "+strconv.Itoa(i)+" offset "+strconv.Itoa(offset)+" outputLen "+strconv.Itoa(outputLen))
					t.FailNow()
				}
	
				for j := 0; j < outputLen; j++ {
					if buf[j] != byte(offset) {
						fmt.Println("Verify failed for", fileName, "part", i, "outputLen", outputLen,  
									"offset", offset, byte(offset), "j", j, "content", buf[j])
						
						//fmt.Println(buf)
						t.FailNow()
					}
				}
	
				// fmt.Println(i, size, offset, outputLen)
			}
		}
	} else if readerType == UNALIGNED_READER_TYPE {
		totalOffset := 0
		for outputLen, err := io.ReadFull(getObjectOutput.Body, buf); err == nil || err == io.EOF; outputLen, err = io.ReadFull(getObjectOutput.Body, buf) {
			// fmt.Println("Verify after io.ReadFull err", err, outputLen)
			for i, content := range buf {
				if i >= outputLen {
					break
				}
				if content != byte(totalOffset + i) {
					fmt.Println("totalOffset", totalOffset, "i", i, "content", content)
					fmt.Println(buf)
					t.FailNow()
				}
			}

			totalOffset += outputLen

			if err != nil {
				break
			}
		}
	}

	handleErr(t, nil, "VerifyObject "+fileName)

	t.Log("Glacier big file test Succeeded. " + fileName)
}

func TransitionBigObjectDeleteHelper(t *testing.T, fileName string) {
	creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
	sc := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(TEST_S3_HOST),
			Region:      aws.String("r"),
			S3ForcePathStyle:	aws.Bool(true),
		},
	)))

	_, err := sc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(fileName),
	})

	handleErr(t, err, "DeleteObject " + fileName)
}

func Test_TransitionMakeBucket(t *testing.T) {
	creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
	sc := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(TEST_S3_HOST),
			Region:      aws.String("r"),
			S3ForcePathStyle:	aws.Bool(true),
		},
	),
	),
	)

	// Create a new bucket.
	_, err := sc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(TEST_S3_REGION),
		},
	})
	handleErr(t, err, "Create Bucket")

	// Add Lifecycle to the bucket. Transit to Glacier after 0 days.
	_, err = sc.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					/*
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(365),
					},
					*/
					ID:     aws.String("TestOnly"),
					Status: aws.String("Enabled"),
					Transitions: []*s3.Transition{
						{
							Days:         aws.Int64(2),
							StorageClass: aws.String("GLACIER"),
						},
					},
				},
			},
		},
	})
	handleErr(t, err, "PutBucketLifecycle")
}

func Test_TransitionSmallObject(t *testing.T) {
	creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
	sc := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(TEST_S3_HOST),
			Region:      aws.String("r"),
			S3ForcePathStyle:	aws.Bool(true),
		},
	),
	),
	)

	// Create a small object.
	_, err := sc.PutObject(&s3.PutObjectInput{
		ACL:    aws.String("public-read-write"),
		Body:   aws.ReadSeekCloser(strings.NewReader(GLACIER_TEST_SMALL_FILE_CONTENT)),
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(GLACIER_TEST_OBJECT),
	})
	handleErr(t, err, "PutObject")
	defer sc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(GLACIER_TEST_OBJECT),
	})

	fmt.Println("Put object finished, waiting to start Lc...")
	time.Sleep(10 * time.Second)

	// Start Lc.
	cmd := exec.Command("bash", "-c", "/work/lc&")
	err = cmd.Run()
	handleErr(t, err, "Run lc")

	fmt.Println("Lc Started, waiting for transition...")
	time.Sleep(60 * time.Second)

	// Check the object, should be "StorageClass" Glacier.
	result, err := sc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(GLACIER_TEST_OBJECT),
	})
	handleErr(t, err, "HeadObject")
	if (*result.StorageClass) != "GLACIER" {
		t.Fatal("Invalid StorageClass after Transition:", *result.StorageClass, *result)
		t.FailNow()
	}

	// Get the object in Glacier. Should fail.
	_, err = sc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(GLACIER_TEST_OBJECT),
	})
	if err == nil {
		t.Log("Get Object shouldn't success without restore")
		t.FailNow()
	}

	// Restore object.
	_, err = sc.RestoreObject(&s3.RestoreObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(GLACIER_TEST_OBJECT),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(1),
			GlacierJobParameters: &s3.GlacierJobParameters{
				Tier: aws.String("Expedited"),
			},
		},
	})
	handleErr(t, err, "RestoreObject")

	fmt.Println("Restoring...")
	time.Sleep(20 * time.Second)

	// Get the object in Glacier.
	getObjectOutput, err := sc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
		Key:    aws.String(GLACIER_TEST_OBJECT),
	})
	defer getObjectOutput.Body.Close()
	handleErr(t, err, "GetObject")
	if buf, err := ioutil.ReadAll(getObjectOutput.Body); err == nil {
		var restoreStringBuilder strings.Builder
		restoreStringBuilder.Write(buf)
		if strings.Compare(GLACIER_TEST_SMALL_FILE_CONTENT, restoreStringBuilder.String()) != 0 {
			t.Log("GetObject output different from src")
			t.Log(GLACIER_TEST_SMALL_FILE_CONTENT)
			t.Log(restoreStringBuilder.String())
			t.FailNow()
		}
	} else {
		t.Log("ioutil.ReadAll failed")
		t.FailNow()
	}
	handleErr(t, nil, "GetObject after restore")

	t.Log("Glacier small file test Succeeded.")
}

func Test_TransitionBigObjectUpload(t *testing.T) {
	for i, fileParts := range files {
		fmt.Println("Start upload file ", i, fileParts)
		TransitionBigObjectUploadHelper(t, GLACIER_TEST_BIG_OBJECT+strconv.Itoa(i), fileParts, DEFAULT_READER_TYPE)
		fmt.Println("Completed upload file ", i)
	}

	for i, fileParts := range randomFiles {
		fmt.Println("Start upload file ", i, fileParts)
		TransitionBigObjectUploadHelper(t, GLACIER_TEST_BIG_OBJECT_UNALIGNED+strconv.Itoa(i), fileParts, UNALIGNED_READER_TYPE)
		fmt.Println("Completed upload file ", i)
	}
}

func Test_TransitionStartLc(t *testing.T) {
	// Start Lc.
	cmd := exec.Command("bash", "-c", "/work/lc&")
	err := cmd.Run()
	handleErr(t, err, "Run lc")
	t.Log("Lc started.")
}

func Test_TransitionBigObjectVerify(t *testing.T) {
	for i, fileParts := range files {
		fmt.Println("Start verify Glacier for file ", i, fileParts)
		TransitionBigObjectVerifyHelper(t, GLACIER_TEST_BIG_OBJECT+strconv.Itoa(i), fileParts, DEFAULT_READER_TYPE)
		fmt.Println("Completed verify file ", i)
	}

	for i, fileParts := range randomFiles {
		fmt.Println("Start verify Glacier for file ", i, fileParts)
		TransitionBigObjectVerifyHelper(t, GLACIER_TEST_BIG_OBJECT_UNALIGNED+strconv.Itoa(i), fileParts, UNALIGNED_READER_TYPE)
		fmt.Println("Completed verify file ", i)
	}
}

func Test_TransitionBigObjectDelete(t *testing.T) {
	for i, fileParts := range files {
		fmt.Println("Start delete Glacier file ", i, fileParts)
		TransitionBigObjectDeleteHelper(t, GLACIER_TEST_BIG_OBJECT+strconv.Itoa(i))
		fmt.Println("Completed delete Glacier file ", i)
	}

	for i, fileParts := range randomFiles {
		fmt.Println("Start delete Glacier file ", i, fileParts)
		TransitionBigObjectDeleteHelper(t, GLACIER_TEST_BIG_OBJECT_UNALIGNED+strconv.Itoa(i))
		fmt.Println("Completed delete Glacier file ", i)
	}

	// Start delete daemon.
	cmd := exec.Command("bash", "-c", "/work/delete&")
	err := cmd.Run()
	handleErr(t, err, "Run delete")
}

func Test_TransitionEnd(t *testing.T) {
	creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
	sc := s3.New(session.Must(session.NewSession(
		&aws.Config{
			Credentials: creds,
			DisableSSL:  aws.Bool(true),
			Endpoint:    aws.String(TEST_S3_HOST),
			Region:      aws.String("r"),
			S3ForcePathStyle:	aws.Bool(true),
		},
	),
	),
	)

	// Delete bucket.
	_, err := sc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(GLACIER_TEST_BUCKET),
	})

	handleErr(t, err, "Run delete bucket")
}
