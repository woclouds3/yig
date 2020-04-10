package lib

import (
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
)

func (s3client *S3Client) MakeBucket(bucketName string) (err error) {
	params := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	if _, err = s3client.Client.CreateBucket(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) DeleteBucket(bucketName string) (err error) {
	params := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	}
	if _, err = s3client.Client.DeleteBucket(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) HeadBucket(bucketName string) (err error) {
	params := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}
	if _, err = s3client.Client.HeadBucket(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) PutBucketVersioning(bucketName, status string) (err error) {
	params := &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String(status),
		},
	}

	_, err = s3client.Client.PutBucketVersioning(params)

	return
}

func (s3client *S3Client) ListObjectVersions(bucketName, keyMarker, versionIdMarker string, maxKeys int64) (*[][]string, bool, string, string, error) {
	var params *s3.ListObjectVersionsInput
	if maxKeys > 0 {
		params = &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucketName),
			KeyMarker:       aws.String(keyMarker),
			VersionIdMarker: aws.String(versionIdMarker),
			MaxKeys:         aws.Int64(maxKeys),
		}
	} else {
		params = &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucketName),
			KeyMarker:       aws.String(keyMarker),
			VersionIdMarker: aws.String(versionIdMarker),
		}
	}

	result, err := s3client.Client.ListObjectVersions(params)
	if err != nil {
		return nil, false, "", "", err
	}

	// fmt.Println(result)

	keyVersionList := make([][]string, len(result.Versions))
	for i, _ := range result.Versions {
		keyVersionList[i] = []string{
			aws.StringValue(result.Versions[i].Key),
			aws.StringValue(result.Versions[i].VersionId),
		}
	}

	return &keyVersionList, aws.BoolValue(result.IsTruncated), aws.StringValue(result.NextKeyMarker), aws.StringValue(result.NextVersionIdMarker), nil
}

func (s3client *S3Client) ListObjects(bucketName string) (*[]string, error) {
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}

	result, err := s3client.Client.ListObjects(params)
	if err != nil {
		return nil, err
	}

	// fmt.Println(result)

	keyList := make([]string, len(result.Contents))
	for i, _ := range result.Contents {
		keyList[i] = aws.StringValue(result.Contents[i].Key)
	}

	return &keyList, nil
}
