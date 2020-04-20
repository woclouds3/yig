package storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
	"fmt"

	glacier "github.com/journeymidnight/yig/coldstorage/client"
	"github.com/journeymidnight/yig/coldstorage/types/glaciertype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
)

const (
	DEFAULT_VAULT_NAME  = "-" //  one vault "-" for each bucket.
	DEFAULT_KAFKA_TOPIC = ""
)

// A Vault named "-" for each uid. No meta data saved. Buckets under the same uid share the same Vault "-".
func (yig *YigStorage) CreateVault(ctx context.Context, credential common.Credential) (err error) {
	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Error(ctx, "Glacier not enabled")
		return ErrInternalError
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	if _, err = glacierClient.GlacierAPI.GetVaultInfo(credential.UserId, DEFAULT_VAULT_NAME); err != nil {
		helper.Logger.Error(ctx, "No Vault, create Vault for", credential.UserId, err)

		/* It should work even if a vault exists. */
		err = glacierClient.GlacierAPI.CreateVault(credential.UserId, DEFAULT_VAULT_NAME)
		if err != nil {
			helper.Logger.Error(ctx, "CreateBucketVaultInGlacier failed",
				credential.UserId, helper.CONFIG.Glacier.GlacierHost, helper.CONFIG.Glacier.GlacierRegion, err)
			return
		}
	}

	helper.Logger.Info(ctx, "Vault already created for uid", credential.UserId)

	return nil
}

// TODO. Now we don't delete a Vault.
func (yig *YigStorage) DeleteVault(ctx context.Context, credential common.Credential) (err error) {
	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Error(ctx, "Glacier not enabled")
		return ErrInternalError
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	vault, err := glacierClient.GlacierAPI.GetVaultInfo(credential.UserId, DEFAULT_VAULT_NAME)
	if err != nil {
		helper.Logger.Error(ctx, "DescribeVault failed for uid", credential.UserId)
		return
	}

	if vault.NumberOfArchives != 0 {
		helper.Logger.Error(ctx, "vault.NumberOfArchives != 0 for uid", vault.NumberOfArchives, credential.UserId)
		return ErrInvalidRequestBody
	}

	err = glacierClient.GlacierAPI.DeleteVault(credential.UserId, DEFAULT_VAULT_NAME)
	if err != nil {
		helper.Logger.Error(ctx, "DeleteVault failed for uid %s", credential.UserId)
		return
	}

	helper.Logger.Info(ctx, "Vault deleted for uid %s", credential.UserId)

	return nil
}

// Transit object to glacier and delete from ceph. After that, the object StorageClass will be glacier in db.
func (yig *YigStorage) TransitObjectToGlacier(ctx context.Context, bucket *meta.Bucket, object *meta.Object) (err error) {
	helper.Logger.Info(ctx, "TransitObjectToGlacier", bucket.Name, object.Name)

	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Error(ctx, "Glacier not enabled")
		return ErrInternalError
	}
	if object.SseType != "" {
		helper.Logger.Error(ctx, "SSE not supported for Glacier", bucket.Name, object.Name)
		return nil
	}

	credentials, err := iam.GetKeysByUid(bucket.OwnerId)
	if err != nil || len(credentials) == 0 {
		helper.Logger.Error(ctx, "GetKeysByUid failed", bucket.Name, err, credentials)
		return err
	}
	credential := credentials[0]

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	cephCluster, ok := yig.DataStorage[object.Location]
	if !ok {
		helper.Logger.Error(ctx, "Cannot find specified ceph cluster:", object.Location)
		return errors.New("Cannot find specified ceph cluster: " + object.Location)
	}

	var archiveId string

	if len(object.Parts) == 0 {
		// small file
		ioReadCloser, err := cephCluster.getReader(object.Pool, object.ObjectId, 0, object.Size)
		if err != nil {
			helper.Logger.Error(ctx, "get reader failed for", bucket.Name, object.Name)
			return err
		}
		defer ioReadCloser.Close()

		archiveId, err = glacierClient.GlacierAPI.PutArchive(credential.UserId,
			DEFAULT_VAULT_NAME,
			ioReadCloser)
		if err != nil {
			helper.Logger.Error(ctx, "PostArchive failed", bucket.Name, object.Name, err)
			return err
		}
	} else {
		helper.Logger.Info(ctx, fmt.Sprintf("TransitObjectToGlacier multipart bucket: %s object: %s size: %d partNum %d",
			bucket.Name, object.Name, object.Size, len(object.Parts)))

		// https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-archive-mpu.html
		// The following table provides multipart upload core specifications.
		// Item										Specification
		// Maximum archive size						10,000 x 4 GB
		// Maximum number of parts per upload		10,000
		// Part size								1 MB to 4 GB, last part can be < 1 MB.
		// 											You specify the size value in bytes.
		// The part size must be a megabyte (1024 KB) multiplied by a power of 2. For example, 1048576 (1 MB), 2097152 (2 MB), 4194304 (4 MB), 8388608 (8 MB).
		// Maximum number of parts returned for a list parts request	1,000
		// Maximum number of multipart uploads returned in a list multipart uploads request	1,000

		partBuilder := getNewPartReaderBuilder(object.Size, &object.Parts, cephCluster)
		if partBuilder == nil {
			return ErrInternalError
		}

		// A big object will be saved as a single archive. Initiate a job for multipart upload.
		uploadId, err := glacierClient.GlacierAPI.CreateMultipart(credential.UserId,
			strconv.FormatInt(DEFAULT_PART_SIZE, 10),
			DEFAULT_VAULT_NAME)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("CreateMultipart failed for user %s bucket %s object %s size %d partSize %d",
				credential.UserId, object.BucketName, object.Name, object.Size, partBuilder.glacierPartSize))
			return err
		}

		for partIndex := 1; partIndex <= partBuilder.glacierPartNum; partIndex++ {
			if err = partBuilder.buildNextPart(); err != nil {
				helper.Logger.Error(ctx, "partReaderBuilder.Build failed", partBuilder.glacierPartSize, partIndex)
				glacierClient.GlacierAPI.DeleteMultipart(credential.UserId, uploadId, DEFAULT_VAULT_NAME)
				return ErrInternalError
			}

			contentRange, err := getContentRange(partIndex, partBuilder.glacierPartSize, object.Size)
			if err != nil {
				helper.Logger.Error(ctx, "getContentRange failed", partIndex, partBuilder.glacierPartSize)
				glacierClient.GlacierAPI.DeleteMultipart(credential.UserId, uploadId, DEFAULT_VAULT_NAME)
				return ErrInternalError
			}

			partReader := partBuilder.getReader()
			err = glacierClient.GlacierAPI.PutArchivePart(credential.UserId,
				uploadId,
				DEFAULT_VAULT_NAME,
				contentRange,
				partReader)
			partReader.Close()
			if err != nil {
				helper.Logger.Error(ctx, "PutArchivePart failed", partBuilder.glacierPartSize, partIndex)
				glacierClient.GlacierAPI.DeleteMultipart(credential.UserId, uploadId, DEFAULT_VAULT_NAME)
				return err
			}
		}

		if archiveId, err = glacierClient.GlacierAPI.CompleteMultipartUpload(credential.UserId, uploadId, DEFAULT_VAULT_NAME); err != nil {
			helper.Logger.Error(ctx, "CompleteMultipartUpload failed uploadId:", uploadId)
			_ = glacierClient.GlacierAPI.DeleteMultipart(credential.UserId, uploadId, DEFAULT_VAULT_NAME)
			return err
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("TransitObjectToGlacier Succeeded bucket: %s object: %s id %s / %s archiveId %s",
		bucket.Name, object.Name, object.ObjectId, object.VersionId, archiveId))

	// TODO, check checksum for object.

	// Add bucket name, object id, archiveId into DB.
	err = yig.MetaStorage.PutArchive(object, archiveId)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("PutArchive to DB failed archiveId %s bucket %s object %s id %s / %s !",
			archiveId, bucket.Name, object.Name, object.ObjectId, object.VersionId))
		// TODO should we delete archive here?
		return err
	}

	// Update storage class from "STANDARD" to "GLACIER" in DB.
	// After that the object must be restored before get.
	err = yig.MetaStorage.UpdateObjectStorageClass(object)
	if err != nil {
		helper.Logger.Error(ctx, "Update StorageClass in DB failed! err", err)
		return err
	}

	yig.removeCache(bucket.Name, object.Name, object.VersionId)

	// Remove object in Ceph.
	if len(object.Parts) == 0 {
		// Small file.
		// TODO. How to handle delete failure in Ceph?
		err = yig.DataStorage[object.Location].Remove(object.Pool, object.ObjectId)
		if err != nil {
			helper.Logger.Error(ctx, "[FAILED] to Remove from ceph", object.BucketName, object.Name, object.VersionId, object.ObjectId, err)
			return err
		}
	} else {
		// Delete all the parts for big file. Index order is not important, so just iterate all.
		for _, part := range object.Parts {
			if err = yig.DataStorage[object.Location].Remove(object.Pool, part.ObjectId); err != nil {
				helper.Logger.Error(ctx, "[FAILED] to Remove from ceph", object.BucketName, object.Name, object.VersionId, part.ObjectId, err)
				// continue to delete other parts.
				continue
			}

			helper.Logger.Info(ctx, "Parts deleted in ceph ", object.BucketName, object.Name, object.VersionId, part.ObjectId)

			err = yig.MetaStorage.DeleteParts(object, part)
			helper.Logger.Info(ctx, "Delete Parts in DB", object.BucketName, object.Name, object.VersionId, part.ObjectId, err)
		}
	}

	return nil
}

func (yig *YigStorage) RestoreObjectFromGlacier(ctx context.Context, bucketName, objectName, version string, restoreDays int64, tier string, credential common.Credential) (statusCode int, err error) {
	helper.Logger.Info(ctx, fmt.Sprintf("RestoreObjectFromGlacier bucket: %s object: %s version %s days %d", bucketName, objectName, version, restoreDays))

	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Error(ctx, "Glacier not enabled")
		return 0, ErrInternalError
	}

	object, err := yig.GetObjectInfo(ctx, bucketName, objectName, version, credential)
	if err != nil {
		helper.Logger.Error(ctx, "GetObject failed:", bucketName, objectName, version)
		return 0, ErrInternalError
	}

	// If Archive already restored, expand expire days and return 200.
	restoredObject, err := yig.MetaStorage.GetObject(ctx, meta.HIDDEN_BUCKET_PREFIX+credential.UserId, yig.GetRestoredObjectName(ctx, object), true)
	if err == nil {
		err = yig.MetaStorage.UpdateArchiveJobIdAndExpire(object, "", int64(time.Since(restoredObject.LastModifiedTime).Hours()/24)+restoreDays)
		if err != nil {
			helper.Logger.Error(ctx, "Update days in DB failed for bucket %s object %s %s restoreDays %d", bucketName, objectName, object.ObjectId, restoreDays)
			return 0, ErrInternalError
		}

		helper.Logger.Info(ctx, fmt.Sprintf("RestoreObjectFromGlacier only update restore days %d for bucket: %s object: %s version %s",
			int64(time.Since(restoredObject.LastModifiedTime).Hours()/24)+restoreDays, bucketName, objectName, version))

		return http.StatusOK, nil
	}

	archiveId, err := yig.MetaStorage.GetArchiveId(object)
	if err != nil {
		helper.Logger.Info(ctx, "GetArchiveId failed for", bucketName, object.ObjectId)
		return 0, ErrInternalError
	}

	helper.Logger.Info(ctx, "RestoreObjectFromGlacier Initiate a new job for archiveId", archiveId)

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)
	jobId, err := glacierClient.GlacierAPI.PostJob(credential.UserId, DEFAULT_VAULT_NAME,
		archiveId, DEFAULT_KAFKA_TOPIC, helper.CONFIG.Glacier.GlacierTier, meta.HIDDEN_BUCKET_PREFIX+credential.UserId)
	if err != nil {
		helper.Logger.Error(ctx, "PostJob failed for", bucketName, object.Name)
		return 0, err
	}

	// TODO: is KAFKA necessary?

	// Job in process.
	err = yig.MetaStorage.UpdateArchiveJobIdAndExpire(object, jobId, restoreDays)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Update JobId in DB failed for bucket %s object %s %s jobId %s restoreDays %d", bucketName, object.Name, object.ObjectId, jobId, restoreDays))
		return 0, ErrInternalError
	}

	return http.StatusAccepted, nil
}

func (yig *YigStorage) GetArchiveStatus(ctx context.Context, bucketName, objectName, version string, credential common.Credential) (archiveStatus string, expireDate string, err error) {
	archiveStatus = meta.ArchiveStatusCodeNoJob
	expireDate = ""
	helper.Logger.Info(ctx, "GetArchiveStatus", bucketName, objectName, version)

	object, err := yig.GetObjectInfo(ctx, bucketName, objectName, version, credential)
	if err != nil {
		helper.Logger.Error(ctx, "GetObject failed for", bucketName, objectName, version)
		return "", "", ErrInternalError
	}

	jobId, err := yig.MetaStorage.GetJobId(object)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("GetArchiveStatus GetJobId failed for bucket: %s object: %s id %s", 
			object.BucketName, object.Name, object.ObjectId))
		return "", "", ErrInternalError
	}

	if jobId == "" {
		helper.Logger.Info(ctx, fmt.Sprintf("GetArchiveStatus no jobId for bucket: %s object: %s objectId: %s", object.BucketName, object.Name, object.ObjectId))
		return meta.ArchiveStatusCodeNoJob, "", nil
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	job, err := glacierClient.GlacierAPI.GetJobStatus(credential.UserId, jobId, DEFAULT_VAULT_NAME)
	if err == nil {
		helper.Logger.Info(ctx, "GetArchiveStatus job id %s Completed %v StatusCode %v for bucket: %s object: %s %s",
			jobId, job.Completed, job.StatusCode, object.BucketName, object.Name, object.ObjectId)

		if job.Completed && job.StatusCode == glaciertype.StatusCodeFailed {
			return meta.ArchiveStatusCodeFailed, "", nil
		}

		if job.Completed == false && job.StatusCode == glaciertype.StatusCodeInProgress {
			return meta.ArchiveStatusCodeInProgress, "", nil
		}

		if job.Completed && job.StatusCode == glaciertype.StatusCodeSucceeded {
			archiveStatus = meta.ArchiveStatusCodeRestored
			// Continue to find expireDate .
		}
	}

	// A job may expire in 1 day. Check the restored object state then.
	expireDate = ""
	restoredObject, err := yig.MetaStorage.GetObject(ctx, meta.HIDDEN_BUCKET_PREFIX+credential.UserId, yig.GetRestoredObjectName(ctx, object), true)
	if err == nil {
		expireDays, err := yig.MetaStorage.GetExpireDays(restoredObject)
		if err == nil {
			expireDate = restoredObject.LastModifiedTime.AddDate(0, 0, int(expireDays)).Format(http.TimeFormat)
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("GetArchiveStatus return archiveStatus %s expireDate %s for bucket: %s object: %s version %s",
		archiveStatus, expireDate, bucketName, objectName, version))

	return archiveStatus, expireDate, nil
}

func (yig *YigStorage) GetObjectFromGlacier(ctx context.Context, object *meta.Object, version string, startOffset, length int64, writer io.Writer, credential common.Credential) error {
	helper.Logger.Info(ctx, "GetObjectFromGlacier", object.BucketName, object.Name, object.ObjectId, version)

	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Error(ctx, "Glacier not enabled")
		return ErrInternalError
	}

	archiveStatus, _, err := yig.GetArchiveStatus(ctx, object.BucketName, object.Name, version, credential)
	if err != nil {
		helper.Logger.Error(ctx, "GetArchiveStatus failed", object.BucketName, object.Name, object.ObjectId, version)
		return err
	}

	if archiveStatus != meta.ArchiveStatusCodeRestored {
		helper.Logger.Error(ctx, fmt.Sprintf("GetObjectFromGlacier failed as archive is in state %v for bucket: %s object: %s %s version %s",
			archiveStatus, object.BucketName, object.Name, object.ObjectId, version))
		return ErrInvalidObjectState
	}

	// TODO. Should get object directly from Ceph instead from Glacier.
	jobId, err := yig.MetaStorage.GetJobId(object)
	if err != nil || jobId == "" {
		helper.Logger.Error(ctx, "GetObjectFromGlacier GetJobId failed for", object.BucketName, object.Name, object.ObjectId)
		return ErrInternalError
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	// TODO add handle for startOffset and length.
	body, err := glacierClient.GlacierAPI.GetJobOutput(credential.UserId, jobId, DEFAULT_VAULT_NAME)
	if err != nil {
		helper.Logger.Error(ctx, "GetObjectFromGlacier GetJobOutput failed for", object.BucketName, object.Name, object.ObjectId)
		return err
	}
	defer body.Close()

	// TODO add buffer. Handle big file for offset.
	if _, err := io.Copy(writer, body); err != nil {
		helper.Logger.Error(ctx, "GetObjectFromGlacier io.Copy() failed for", object.BucketName, object.Name, object.ObjectId)
		return ErrInternalError
	}

	return nil
}

func (yig *YigStorage) DeleteObjectFromGlacier(ctx context.Context, bucketName, objectName, objectId, ownerId string) (err error) {
	helper.Logger.Info(ctx, fmt.Sprintf("DeleteObjectFromGlacier bucket: %s object: %s id %s ownerId %s", bucketName, objectName, objectId, ownerId))

	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Error(ctx, "DeleteObjectFromGlacier but helper.CONFIG.Glacier.EnableGlacier", helper.CONFIG.Glacier.EnableGlacier)
		return ErrInternalError
	}

	// TODO, it's possible that the account was deleted from iam.
	credentials, err := iam.GetKeysByUid(ownerId)
	if err != nil || len(credentials) == 0 {
		helper.Logger.Error(ctx, "GetKeysByUid failed", bucketName, objectName, objectId, ownerId)
		return err
	}
	credential := credentials[0]

	archiveId, err := yig.MetaStorage.GetArchiveId(&meta.Object{
		Name:       objectName,
		BucketName: bucketName,
		OwnerId:    credential.UserId,
		ObjectId:   objectId,
	})
	if err != nil {
		helper.Logger.Error(ctx, "GetArchiveId failed", bucketName, objectName, objectId)
		return ErrInternalError
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	err = glacierClient.GlacierAPI.DeleteArchive(credential.UserId, archiveId, DEFAULT_VAULT_NAME)
	if err != nil {
		helper.Logger.Error(ctx, "DeleteArchive failed", bucketName, objectName, objectId)
		return err
	}

	// This function should be called by delete, and meta data in table "object" should be deleted by yig.
	// So we just remove the item in table "archives". In fact, only bucketName and ObjectId are necessary.
	yig.MetaStorage.DeleteArchive(&meta.Object{
		Name:       objectName,
		BucketName: bucketName,
		ObjectId:   objectId,
	})
	if err != nil {
		helper.Logger.Error(ctx, "DeleteArchive failed", bucketName, objectName, objectId)
		return ErrInternalError
	}

	return nil
}

// Now ehualu will upload restored archive into yig with archiveId as object name.
func (yig *YigStorage) GetRestoredObjectName(ctx context.Context, object *meta.Object) string {
	archiveId, err := yig.MetaStorage.GetArchiveId(object)
	if err != nil {
		helper.Logger.Error(ctx, "GetArchiveId failed", err)
		return ""
	}

	return archiveId
}
