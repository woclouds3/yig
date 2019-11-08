package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	DEFAULT_ACCOUNT_ID          = "-"
	DEFAULT_VAULT_NAME          = "-"
	DEFAULT_SNS_TOPIC           = ""
	DEFAULT_LIST_ARCHIVE_OUTPUT = "list-archive.txt"
	DEFAULT_REGION              = "us-east-1"
	DEFAULT_HOST                = "10.3.221.31:9100"
	GLACIER_TEST_AK             = "CSU67IXC8S0CRHGFFA9B" // "U5S0O3O15DDPCGVF9MB6"                     // 
	GLACIER_TEST_SK             = "DC3gsT1CiX19yvWwTS51d0mmng3ogetoSX1BNaQO" // "W59APTYZ1MG6S5SDS9KPFCCY6R3A2IF3T705A5ZT" // 
	TEST_S3_HOST                = "http://s3.test.com:8080"
)

func main() {

	//fmt.Println(os.Args)
	//host := os.Args[1]
	var command string
	if len(os.Args) <= 1 {
		command = ""
	} else {
		command = os.Args[1]
	}

	svc := glacier.New(session.New(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, ""),
		Endpoint:         aws.String(DEFAULT_HOST),
		Region:           aws.String(DEFAULT_REGION),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}))

	var err error
	i := 1

	switch command {
	case "list-vaults":
		fallthrough
	case "lv":
		input := &glacier.ListVaultsInput{
			AccountId: aws.String(DEFAULT_ACCOUNT_ID),
			Limit:     aws.String(""),
			Marker:    aws.String(""),
		}

		if result, err := svc.ListVaults(input); err == nil {
			fmt.Println(command, ": ")
			fmt.Println(result)
		}

	case "create-vaults":
		fallthrough
	case "cv":
		input := &glacier.CreateVaultInput{
			AccountId: aws.String(DEFAULT_ACCOUNT_ID),
			VaultName: aws.String(DEFAULT_VAULT_NAME),
		}

		if result, err := svc.CreateVault(input); err == nil {
			fmt.Println(command, ":")
			fmt.Println(result)
		}

	case "delete-vaults":
		fallthrough
	case "dv":
		input := &glacier.DeleteVaultInput{
			AccountId: aws.String(DEFAULT_ACCOUNT_ID),
			VaultName: aws.String(DEFAULT_VAULT_NAME),
		}

		if result, err := svc.DeleteVault(input); err == nil {
			fmt.Println(command, ":")
			fmt.Println(result)
		}

	case "list-jobs":
		fallthrough
	case "lj":
		input := &glacier.ListJobsInput{
			AccountId: aws.String(DEFAULT_ACCOUNT_ID),
			VaultName: aws.String(DEFAULT_VAULT_NAME),
		}

		if result, err := svc.ListJobs(input); err == nil {
			fmt.Println(command, ": ")
			fmt.Println(result)
		}

	case "list-archives":
		fallthrough
	case "la":
		input := &glacier.InitiateJobInput{
			AccountId: aws.String(DEFAULT_ACCOUNT_ID),
			JobParameters: &glacier.JobParameters{
				Description: aws.String("list-archives"),
				Format:      aws.String("JSON"),
				SNSTopic:    aws.String(DEFAULT_SNS_TOPIC),
				Type:        aws.String("inventory-retrieval"),
			},
			VaultName: aws.String(DEFAULT_VAULT_NAME),
		}

		if result, err := svc.InitiateJob(input); err == nil {
			fmt.Println(command, ": ")
			fmt.Println("Initiated Job Id: ", *result.JobId, "Waiting for Job to complete ...")

			time.Sleep(time.Second * 10)

			getJobOutputInput := &glacier.GetJobOutputInput{
				AccountId: aws.String(DEFAULT_ACCOUNT_ID),
				JobId:     result.JobId,
				Range:     aws.String(""),
				VaultName: aws.String(DEFAULT_VAULT_NAME),
			}

			if jobOutput, err := svc.GetJobOutput(getJobOutputInput); err == nil {
				fmt.Println("[", *getJobOutputInput.JobId, "]", "Job completed: ")
				fmt.Println(jobOutput)
				if buf, err := ioutil.ReadAll(jobOutput.Body); err == nil {
					ioutil.WriteFile(DEFAULT_LIST_ARCHIVE_OUTPUT, buf, os.ModeAppend)
				}
				jobOutput.Body.Close()
			} else {
				fmt.Println(err)
			}
		}

	case "download-archive":
		fallthrough
	case "da":
		if len(os.Args) <= (i + 2) {
			printUsage()
			break
		}

		archiveId := os.Args[i+1]
		outputPath := os.Args[i+2]
		i += 2
		fmt.Println("ArchiveId:", archiveId, "Output file:", outputPath)

		input := &glacier.InitiateJobInput{
			AccountId: aws.String(DEFAULT_ACCOUNT_ID),
			JobParameters: &glacier.JobParameters{
				Description: aws.String("download-archives"),
				Format:      aws.String("JSON"),
				SNSTopic:    aws.String(DEFAULT_SNS_TOPIC),
				Type:        aws.String("archive-retrieval"),
				ArchiveId:   aws.String(archiveId),
			},
			VaultName: aws.String(DEFAULT_VAULT_NAME),
		}

		if result, err := svc.InitiateJob(input); err == nil {
			fmt.Println(command, ": ")
			fmt.Println("Initiated Job Id: ", *result.JobId, "Waiting for Job to complete ...")

			time.Sleep(time.Second * 5)

			getJobOutputInput := &glacier.GetJobOutputInput{
				AccountId: aws.String(DEFAULT_ACCOUNT_ID),
				JobId:     result.JobId,
				Range:     aws.String(""),
				VaultName: aws.String(DEFAULT_VAULT_NAME),
			}

			if jobOutput, err := svc.GetJobOutput(getJobOutputInput); err == nil {
				fmt.Println("[", *getJobOutputInput.JobId, "]", "Job completed: ")
				fmt.Println(jobOutput)
				if buf, err := ioutil.ReadAll(jobOutput.Body); err == nil {
					ioutil.WriteFile(outputPath, buf, os.ModeExclusive)
				}
				jobOutput.Body.Close()
			} else {
				fmt.Println(err)
			}
		}

	case "remove-archive":
		fallthrough
	case "ra":
		if len(os.Args) <= (i + 1) {
			printUsage()
			break
		}

		archiveId := os.Args[i+1]
		input := &glacier.DeleteArchiveInput{
			AccountId: aws.String("-"),
			ArchiveId: aws.String(archiveId),
			VaultName: aws.String(DEFAULT_VAULT_NAME),
		}

		if _, err := svc.DeleteArchive(input); err == nil {
			fmt.Println("Archive Deleted", archiveId)
		}

	case "list-versions":
		if len(os.Args) <= (i + 2) {
			printUsage()
			break
		}

		inputBucket := os.Args[i+1]
		inputKey := os.Args[i+2]

		creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
		sc := s3.New(session.Must(session.NewSession(
			&aws.Config{
				Credentials: creds,
				DisableSSL:  aws.Bool(true),
				Endpoint:    aws.String(TEST_S3_HOST),
				Region:      aws.String("r"),
			},
		),
		),
		)

		input := &s3.ListObjectVersionsInput{
			Bucket: aws.String(inputBucket),
			Prefix: aws.String(inputKey),
		}

		fmt.Println("input ", inputBucket, inputKey)
		if result, err := sc.ListObjectVersions(input); err == nil {
			fmt.Println("Object Versions:")
			fmt.Println("output:", result)
			fmt.Println("Versions: ", result.Versions, len(result.Versions))
			for i, v := range result.Versions {
				fmt.Println(i, v)
			}
		}

	case "download-object":
		fallthrough
	case "do":
		if len(os.Args) <= (i + 3) {
			printUsage()
			break
		}

		bucketName := os.Args[i+1]
		key := os.Args[i+2]
		outputPath := os.Args[i+3]
		var rangeString string
		if len(os.Args) > (i + 4) {
			rangeString = os.Args[i+4]
		} else {
			rangeString = ""
		}

		i += 2
		fmt.Println("bucket:", bucketName, "key:", key, "Output file:", outputPath, "Range: ", rangeString)

		creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
		sc := s3.New(session.Must(session.NewSession(
			&aws.Config{
				Credentials: creds,
				DisableSSL:  aws.Bool(true),
				Endpoint:    aws.String(TEST_S3_HOST),
				Region:      aws.String("r"),
			},
		),
		),
		)

		var getObjectOutput *s3.GetObjectOutput
		if rangeString != "" {
			getObjectOutput, err = sc.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
				Range:  aws.String(rangeString),
			})
		} else {
			getObjectOutput, err = sc.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
			})
		}
		if err == nil {
			defer getObjectOutput.Body.Close()
			buf := make([]byte, 1024*1024)
			outputLen := 0
			err = nil
			f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
			defer f.Close()

			for err == nil {
				outputLen, err = io.ReadFull(getObjectOutput.Body, buf)
				if outputLen == 0 {
					fmt.Println("getObjectOutput Read err: ", outputLen, err)
					break
				}
				fmt.Println("outputLen", outputLen, err, "Body[:10]", buf[:10])

				if _, err := f.Write(buf[:outputLen]); err != nil {
					fmt.Println("Write err")
				}
			}
		}

	case "retrieve-archive":
		fallthrough
	case "rea":
		if len(os.Args) <= (i + 2) {
			printUsage()
			break
		}

		targetBucketName := os.Args[i+1]
		archiveId := os.Args[i+2]
		i += 2
		fmt.Println("BucketName", targetBucketName, "ArchiveId:", archiveId)

		input := &glacier.InitiateJobInput{
			AccountId: aws.String(DEFAULT_ACCOUNT_ID),
			JobParameters: &glacier.JobParameters{
				Description: aws.String("download-archives"),
				Format:      aws.String("JSON"),
				SNSTopic:    aws.String(DEFAULT_SNS_TOPIC),
				Type:        aws.String("archive-retrieval"),
				ArchiveId:   aws.String(archiveId),
				OutputLocation: &glacier.OutputLocation{
					S3: &glacier.S3Location{
						BucketName: aws.String(targetBucketName),
					},
				},
			},
			VaultName: aws.String(DEFAULT_VAULT_NAME),
		}

		if result, err := svc.InitiateJob(input); err == nil {
			fmt.Println(result)
		}

	case "upload-small-archive":
		fallthrough
	case "usa":
		fmt.Println("Upload small file to Glacier.")

		input := &glacier.UploadArchiveInput{
			AccountId:          aws.String(DEFAULT_ACCOUNT_ID),
			ArchiveDescription: aws.String("-"),
			Body:               aws.ReadSeekCloser(strings.NewReader("ehualu test for s3 backend")),
			Checksum:           aws.String(""),
			VaultName:          aws.String(DEFAULT_VAULT_NAME),
		}

		result, err := svc.UploadArchive(input)
		fmt.Println("ArchiveId: ", result, err) //aws.StringValue(result.ArchiveId))

	case "head-object":
		fallthrough
	case "ho":
		if len(os.Args) <= (i + 2) {
			printUsage()
			break
		}

		bucketName := os.Args[i+1]
		key := os.Args[i+2]

		i += 2
		fmt.Println("bucket:", bucketName, "key:", key)

		creds := credentials.NewStaticCredentials(GLACIER_TEST_AK, GLACIER_TEST_SK, "")
		sc := s3.New(session.Must(session.NewSession(
			&aws.Config{
				Credentials: creds,
				DisableSSL:  aws.Bool(true),
				Endpoint:    aws.String(TEST_S3_HOST),
				Region:      aws.String("r"),
			},
		),
		),
		)

		result, err := sc.HeadObject(&s3.HeadObjectInput{
										Bucket: aws.String(bucketName),
										Key:    aws.String(key),
									})
		if err == nil {
			fmt.Println("Head", key)
			fmt.Println(result)
			fmt.Printf("Restore: %s\n", aws.StringValue(result.Restore))
		}

	case "-h":
		fallthrough
	default:
		printUsage()
	}

	fmt.Println("err: ", err)
}

func printUsage() {
	fmt.Println("Options:")
	fmt.Println("list-vaults")
	fmt.Println("delete-vaults")
	fmt.Println("list-jobs")
	fmt.Println("list-archives")
	fmt.Println("download-archive archiveId outputFilePath")
	fmt.Println("remove-archive archiveId")
	fmt.Println("list-versions")
	fmt.Println("download-object bucket key outputFilePath {range like \"bytes=0-9\"}")
	fmt.Println("retrieve-archive bucketName uploadId")
	fmt.Println("upload-small-archive")
	fmt.Println("head-object bucket key")
	fmt.Println("-h")
}
