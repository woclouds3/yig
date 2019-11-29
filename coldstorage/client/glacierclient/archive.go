package glacierclient

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/client"
	. "github.com/journeymidnight/yig/error"
)

// To upload an archive to a vault.
func (c GlacierClient) PutArchive(accountid, vaultname string, ioreadseeker io.ReadSeeker) (archiveid *string, err error) {
	input := &glacier.UploadArchiveInput{
		AccountId:          aws.String(accountid),
		ArchiveDescription: aws.String(""),
		Body:               ioreadseeker,
		Checksum:           aws.String(""),
		VaultName:          aws.String(vaultname),
	}
	result, err := c.Client.UploadArchive(input)
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
		return
	}
	archiveid = result.ArchiveId
	return archiveid, err
}

//To delete an archive from a vault.
func (c GlacierClient) DeleteArchive(accountid, archiveid, vaultname string) error {
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
func (c GlacierClient) CreateMultipart(accountid, partsize, vaultname string) (uploadid *string, err error) {
	input := &glacier.InitiateMultipartUploadInput{
		AccountId: aws.String(accountid),
		PartSize:  aws.String(partsize),
		VaultName: aws.String(vaultname),
	}
	result, err := c.Client.InitiateMultipartUpload(input)
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
		return
	}
	uploadid = result.UploadId
	return uploadid, err
}

//To upload a part of an archive.
func (c GlacierClient) PutArchivePart(accountid, uploadid, vaultname, partrange string, ioreadseeker io.ReadSeeker) error {
	input := &glacier.UploadMultipartPartInput{
		AccountId: aws.String(accountid),
		Body:      aws.ReadSeekCloser(ioreadseeker),
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
				fmt.Println(aerr.Error())
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
	}
	return err
}

//All the archive parts have been uploaded and assemble the archive from the uploaded parts.
func (c GlacierClient) CompleteMultipartUpload(accountid, uploadid, vaultname string) (archiveid *string, err error) {
	input := &glacier.CompleteMultipartUploadInput{
		AccountId: aws.String(accountid),
		UploadId:  aws.String(uploadid),
		VaultName: aws.String(vaultname),
	}
	result, err := c.Client.CompleteMultipartUpload(input)
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
		return
	}
	archiveid = result.ArchiveId
	return archiveid, err
}

//To list in-progress multipart uploads for the specified vault.
func (c GlacierClient) GetMultipartFromVault(accountid, vaultname string) (uploadlist []*glacier.UploadListElement, err error) {
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
		return
	}
	uploadlist = result.UploadsList
	return uploadlist, err
}

//To list the parts of an archive that have been uploaded in a specific multipart upload.
func (c GlacierClient) GetMultipartFromArchive(accountid, uploadid, vaultname string) (parts []*glacier.PartListElement, err error) {
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
		return
	}
	parts = result.Parts
	return parts, err
}

//To abort a multipart upload identified by the upload ID.
func (c GlacierClient) AbortMultipart(accountid, uploadid, vaultname string) error {
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
