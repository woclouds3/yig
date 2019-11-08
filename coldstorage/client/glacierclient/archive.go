package glacierclient

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/error"
)

// To upload an archive to a vault.
func (c GlacierClient) PutArchive(accountid, vaultname string, ioReader io.Reader) (string, error) {
	input := &glacier.UploadArchiveInput{
		AccountId:          aws.String(accountid),
		ArchiveDescription: aws.String("-"),
		Body:               aws.ReadSeekCloser(ioReader),
		Checksum:           aws.String(""),
		VaultName:          aws.String(vaultname),
	}
	result, err := c.Client.UploadArchive(input)
	var archiveId string
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeRequestTimeoutException:
				err = ErrRequestTimeout
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
		archiveId = ""
	} else {
		archiveId = aws.StringValue(result.ArchiveId)
	}
	return archiveId, err
}

//To delete an archive from a vault.
func (c GlacierClient) DeleteArchive(accountid string, archiveid string, vaultname string) error {
	input := &glacier.DeleteArchiveInput{
		AccountId: aws.String(accountid),
		ArchiveId: aws.String(archiveid),
		VaultName: aws.String(vaultname),
	}
	_, err := c.Client.DeleteArchive(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
	}
	return err
}

//To initiate a multipart upload.
func (c GlacierClient) CreateMultipart(accountid, partsize, vaultname string) (string, error) {
	input := &glacier.InitiateMultipartUploadInput{
		AccountId: aws.String(accountid),
		PartSize:  aws.String(partsize),
		VaultName: aws.String(vaultname),
	}
	result, err := c.Client.InitiateMultipartUpload(input)
	var uploadId string
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
		uploadId = ""
	} else {
		uploadId = aws.StringValue(result.UploadId)
	}

	return uploadId, err
}

//To upload a part of an archive.
func (c GlacierClient) PutArchivePart(accountid, uploadid, vaultname string, partrange string, ioReader io.Reader) error {
	input := &glacier.UploadMultipartPartInput{
		AccountId: aws.String(accountid),
		Body:      aws.ReadSeekCloser(ioReader),
		Range:     aws.String(partrange),
		UploadId:  aws.String(uploadid),
		VaultName: aws.String(vaultname),
	}
	_, err := c.Client.UploadMultipartPart(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeRequestTimeoutException:
				err = ErrRequestTimeout
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
	}
	return err
}

//All the archive parts have been uploaded and assemble the archive from the uploaded parts.
func (c GlacierClient) CompleteMultipartUpload(accountid, uploadid, vaultname string) (string, error) {
	input := &glacier.CompleteMultipartUploadInput{
		AccountId: aws.String(accountid),
		UploadId:  aws.String(uploadid),
		VaultName: aws.String(vaultname),
	}
	result, err := c.Client.CompleteMultipartUpload(input)
	var archiveId string
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
		archiveId = ""
	} else {
		archiveId = aws.StringValue(result.ArchiveId)
	}

	return archiveId, err
}

//To list in-progress multipart uploads for the specified vault.
func (c GlacierClient) GetMultipartFromVault(accountid, vaultname string) ([]*glacier.UploadListElement, error) {
	input := &glacier.ListMultipartUploadsInput{
		AccountId: aws.String(accountid),
		VaultName: aws.String(vaultname),
	}
	result, err := c.Client.ListMultipartUploads(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
	}
	uploadlist := result.UploadsList
	return uploadlist, err
}

//To list the parts of an archive that have been uploaded in a specific multipart upload.
func (c GlacierClient) GetMultipartFromArchive(accountid, uploadid, vaultname string) ([]*glacier.PartListElement, error) {
	input := &glacier.ListPartsInput{
		AccountId: aws.String(accountid),
		UploadId:  aws.String(uploadid),
		VaultName: aws.String(vaultname),
	}
	result, err := c.Client.ListParts(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
	}
	parts := result.Parts
	return parts, err
}

//To abort a multipart upload identified by the upload ID.
func (c GlacierClient) DeleteMultipart(accountid, uploadid, vaultname string) error {
	input := &glacier.AbortMultipartUploadInput{
		AccountId: aws.String(accountid),
		VaultName: aws.String(vaultname),
	}
	_, err := c.Client.AbortMultipartUpload(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
	}
	return err
}
