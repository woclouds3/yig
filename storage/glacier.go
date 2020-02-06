package storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	glacier "github.com/journeymidnight/yig/coldstorage/client"
	"github.com/journeymidnight/yig/coldstorage/types/glaciertype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
	obj "github.com/journeymidnight/yig/meta"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

const (
	DEFAULT_VAULT_NAME  = "-" //  one vault "-" for each bucket.
	DEFAULT_KAFKA_TOPIC = ""
)

// A Vault named "-" for each uid. No meta data saved. Buckets under the same uid share the same Vault "-".
func (yig *YigStorage) CreateVault(ctx context.Context, credential common.Credential) (err error) {
	reqId := helper.RequestIdFromContext(ctx)
	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Printf(10, "[ %s ] Glacier not enabled", reqId)
		return ErrInternalError
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	if _, err = glacierClient.GlacierAPI.GetVaultInfo(credential.UserId, DEFAULT_VAULT_NAME); err != nil {
		helper.Logger.Printf(10, "[ %s ] No Vault for uid %s err %v, create Vault", reqId, credential.UserId, err)

		/* It should work even if a vault exists. */
		err = glacierClient.GlacierAPI.CreateVault(credential.UserId, DEFAULT_VAULT_NAME)
		if err != nil {
			helper.Logger.Printf(5, "[ %s ] CreateBucketVaultInGlacier failed uid %s Glacier %s %s err %v",
				reqId, credential.UserId, helper.CONFIG.Glacier.GlacierHost, helper.CONFIG.Glacier.GlacierRegion, err)
			return
		}
	}

	helper.Logger.Printf(5, "[ %s ] Vault already created for uid %s", reqId, credential.UserId)

	return nil
}

// TODO. Now we don't delete a Vault.
func (yig *YigStorage) DeleteVault(ctx context.Context, credential common.Credential) (err error) {
	reqId := helper.RequestIdFromContext(ctx)
	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Printf(10, "[ %s ] Glacier not enabled", reqId)
		return ErrInternalError
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	vault, err := glacierClient.GlacierAPI.GetVaultInfo(credential.UserId, DEFAULT_VAULT_NAME)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] DescribeVault failed for uid %s", reqId, credential.UserId)
		return
	}

	if vault.NumberOfArchives != 0 {
		helper.Logger.Printf(5, "[ %s ] vault.NumberOfArchives %d != 0 for uid %s", reqId, vault.NumberOfArchives, credential.UserId)
		return ErrInvalidRequestBody
	}

	err = glacierClient.GlacierAPI.DeleteVault(credential.UserId, DEFAULT_VAULT_NAME)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] DeleteVault failed for uid %s", reqId, credential.UserId)
		return
	}

	helper.Logger.Printf(5, "[ %s ] Vault deleted for uid %s", reqId, credential.UserId)

	return nil
}

// Transit object to glacier and delete from ceph. After that, the object StorageClass will be glacier in db.
func (yig *YigStorage) TransitObjectToGlacier(ctx context.Context, bucket *meta.Bucket, object *meta.Object) (err error) {
	reqId := helper.RequestIdFromContext(ctx)
	helper.Logger.Printf(10, "[ %s ] TransitObjectToGlacier bucket: %s object: %s",
		reqId, bucket.Name, object.Name)

	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Printf(10, "[ %s ] Glacier not enabled", reqId)
		return ErrInternalError
	}
	if object.SseType != "" {
		helper.Logger.Printf(10, "[ %s ] SSE not supported for Glacier bucket: %s object: %s",
			reqId, bucket.Name, object.Name)
		return nil
	}

	credentials, err := iam.GetKeysByUid(bucket.OwnerId)
	if err != nil || len(credentials) == 0 {
		helper.Logger.Printf(5, "[ %s ] GetKeysByUid failed bucket %s err %v credentials %v", reqId, bucket.Name, err, credentials)
		return err
	}
	credential := credentials[0]

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	cephCluster, ok := yig.DataStorage[object.Location]
	if !ok {
		helper.Logger.Println(5, "Cannot find specified ceph cluster: "+object.Location)
		return errors.New("Cannot find specified ceph cluster: " + object.Location)
	}

	var archiveId string

	if len(object.Parts) == 0 {
		// small file
		ioReadCloser, err := cephCluster.getReader(object.Pool, object.ObjectId, 0, object.Size)
		if err != nil {
			helper.Logger.Printf(5, "[ %s ] get reader failed for bucket %s object %s", reqId, bucket.Name, object.Name)
			return err
		}
		defer ioReadCloser.Close()

		archiveId, err = glacierClient.GlacierAPI.PutArchive(credential.UserId,
			DEFAULT_VAULT_NAME,
			ioReadCloser)
		if err != nil {
			helper.Logger.Printf(2, "[ %s ] PostArchive failed bucket %s object %s err %v", reqId, bucket.Name, object.Name, err)
			return err
		}
	} else {
		helper.Logger.Printf(20, "[ %s ] TransitObjectToGlacier multipart bucket: %s object: %s size: %d partNum %d",
			reqId, bucket.Name, object.Name, object.Size, len(object.Parts))

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
			helper.Logger.Printf(5, "[ %s ] CreateMultipart failed for user %s bucket %s object %s size %d partSize %d",
				reqId, credential.UserId, object.BucketName, object.Name, object.Size, partBuilder.glacierPartSize)
			return err
		}

		for partIndex := 1; partIndex <= partBuilder.glacierPartNum; partIndex++ {
			if err = partBuilder.buildNextPart(); err != nil {
				helper.Logger.Printf(5, "[ %s ] partReaderBuilder.Build failed %d %d", reqId, partBuilder.glacierPartSize, partIndex)
				glacierClient.GlacierAPI.DeleteMultipart(credential.UserId, uploadId, DEFAULT_VAULT_NAME)
				return ErrInternalError
			}

			contentRange, err := getContentRange(partIndex, partBuilder.glacierPartSize, object.Size)
			if err != nil {
				helper.Logger.Printf(5, "[ %s ] getContentRange failed %d %d", reqId, partIndex, partBuilder.glacierPartSize)
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
				helper.Logger.Printf(5, "[ %s ] PutArchivePart failed %d %d", reqId, partBuilder.glacierPartSize, partIndex)
				glacierClient.GlacierAPI.DeleteMultipart(credential.UserId, uploadId, DEFAULT_VAULT_NAME)
				return err
			}
		}

		if archiveId, err = glacierClient.GlacierAPI.CompleteMultipartUpload(credential.UserId, uploadId, DEFAULT_VAULT_NAME); err != nil {
			helper.Logger.Printf(5, "[ %s ] CompleteMultipartUpload failed uploadId %s ", reqId, uploadId)
			_ = glacierClient.GlacierAPI.DeleteMultipart(credential.UserId, uploadId, DEFAULT_VAULT_NAME)
			return err
		}
	}

	helper.Logger.Printf(10, "[ %s ] TransitObjectToGlacier Succeeded bucket: %s object: %s id %s / %s archiveId %s",
		reqId, bucket.Name, object.Name, object.ObjectId, object.VersionId, archiveId)

	// TODO, check checksum for object.

	// Add bucket name, object id, archiveId into DB.
	err = yig.MetaStorage.PutArchive(object, archiveId)
	if err != nil {
		helper.Logger.Printf(2, "[ %s ] PutArchive to DB failed archiveId %s bucket %s object %s id %s / %s !",
			reqId, archiveId, bucket.Name, object.Name, object.ObjectId, object.VersionId)
		return err
	}

	// Update storage class from "STANDARD" to "GLACIER" in DB.
	// After that the object must be restored before get.
	err = yig.MetaStorage.UpdateObjectStorageClass(object)
	if err != nil {
		helper.Logger.Printf(2, "[ %s ] Update StorageClass in DB failed! err %v", reqId, err)
		return err
	}

	yig.MetaStorage.Cache.Remove(redis.ObjectTable, obj.OBJECT_CACHE_PREFIX, bucket.Name+":"+object.Name+":")
	yig.DataCache.Remove(bucket.Name + ":" + object.Name + ":" + object.GetVersionId())

	// Remove object in Ceph.
	if len(object.Parts) == 0 {
		// Small file.
		err = yig.DataStorage[object.Location].Remove(object.Pool, object.ObjectId)
		if err != nil {
			helper.Logger.Println(5, "[FAILED] to Remove from ceph", object.BucketName, object.Name, object.ObjectId, err)
			return err
		}
	} else {
		// Delete all the parts for big file. Index order is not important, so just iterate all.
		for _, part := range object.Parts {
			if err = yig.DataStorage[object.Location].Remove(object.Pool, part.ObjectId); err != nil {
				helper.Logger.Println(5, "[FAILED] to Remove from ceph", object.BucketName, object.Name, part.ObjectId, err)
				// continue to delete other parts.
				continue
			}

			helper.Logger.Println(20, "[  ] Parts deleted in ceph ", object.BucketName, object.Name, part.ObjectId)

			err = yig.MetaStorage.DeleteParts(object, part)
			helper.Logger.Println(20, "[  ] Delete Parts in DB", object.BucketName, object.Name, part.ObjectId, err)
		}
	}

	return nil
}

func (yig *YigStorage) RestoreObjectFromGlacier(ctx context.Context, bucketName, objectName, version string, restoreDays int64, tier string, credential common.Credential) (statusCode int, err error) {
	reqId := helper.RequestIdFromContext(ctx)
	helper.Logger.Printf(20, "[ %s ] RestoreObjectFromGlacier bucket: %s object: %s version %s days %d", reqId, bucketName, objectName, version, restoreDays)

	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Printf(10, "[ %s ] Glacier not enabled", reqId)
		return 0, ErrInternalError
	}

	object, err := yig.GetObjectInfo(ctx, bucketName, objectName, version, credential)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] GetObject failed bucket %s object %s version %s", reqId, bucketName, objectName, version)
		return 0, ErrInternalError
	}

	// If Archive already restored, expand expire days and return 200.
	restoredObject, err := yig.MetaStorage.GetObject(ctx, meta.HIDDEN_BUCKET_PREFIX+credential.UserId, yig.GetRestoredObjectName(ctx, object), true)
	if err == nil {
		err = yig.MetaStorage.UpdateArchiveJobIdAndExpire(object, "", int64(time.Since(restoredObject.LastModifiedTime).Hours()/24)+restoreDays)
		if err != nil {
			helper.Logger.Printf(5, "[ %s ] Update days in DB failed for bucket %s object %s %s restoreDays %d", reqId, bucketName, objectName, object.ObjectId, restoreDays)
			return 0, ErrInternalError
		}

		helper.Logger.Printf(20, "[ %s ] RestoreObjectFromGlacier only update restore days %d for bucket: %s object: %s version %s",
			reqId, int64(time.Since(restoredObject.LastModifiedTime).Hours()/24)+restoreDays, bucketName, objectName, version)

		return http.StatusOK, nil
	}

	archiveId, err := yig.MetaStorage.GetArchiveId(object)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] GetArchiveId failed bucket %s object %s", reqId, bucketName, object.ObjectId)
		return 0, ErrInternalError
	}

	helper.Logger.Printf(20, "[ %s ] RestoreObjectFromGlacier Initiate a new job for archiveId %s", reqId, archiveId)

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)
	jobId, err := glacierClient.GlacierAPI.PostJob(credential.UserId, DEFAULT_VAULT_NAME,
		archiveId, DEFAULT_KAFKA_TOPIC, helper.CONFIG.Glacier.GlacierTier, meta.HIDDEN_BUCKET_PREFIX+credential.UserId)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] PostJob failed bucket %s object %s", reqId, bucketName, object.Name)
		return 0, err
	}

	// TODO: is KAFKA necessary?

	// Job in process.
	err = yig.MetaStorage.UpdateArchiveJobIdAndExpire(object, jobId, restoreDays)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] Update JobId in DB failed for bucket %s object %s %s jobId %s restoreDays %d", reqId, bucketName, object.Name, object.ObjectId, jobId, restoreDays)
		return 0, ErrInternalError
	}

	return http.StatusAccepted, nil
}

func (yig *YigStorage) GetArchiveStatus(ctx context.Context, bucketName, objectName, version string, credential common.Credential) (archiveStatus string, expireDate string, err error) {
	reqId := helper.RequestIdFromContext(ctx)
	archiveStatus = meta.ArchiveStatusCodeNoJob
	expireDate = ""
	helper.Logger.Printf(20, "[ %s ] GetArchiveStatus bucket: %s object: %s version %s", reqId, bucketName, objectName, version)

	object, err := yig.GetObjectInfo(ctx, bucketName, objectName, version, credential)
	if err != nil {
		helper.Logger.Printf(10, "[ %s ] GetObject failed bucket %s object %s version %s", reqId, bucketName, objectName, version)
		return "", "", ErrInternalError
	}

	jobId, err := yig.MetaStorage.GetJobId(object)
	if err != nil {
		helper.Logger.Printf(10, "[ %s ] GetArchiveStatus GetJobId failed for bucket: %s object: %s id %s", reqId,
			object.BucketName,
			object.Name,
			object.ObjectId)
		return "", "", ErrInternalError
	}

	if jobId == "" {
		helper.Logger.Printf(20, "[ %s ] GetArchiveStatus no jobId for bucket: %s object: %s %s", reqId, object.BucketName, object.Name, object.ObjectId)
		return meta.ArchiveStatusCodeNoJob, "", nil
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	job, err := glacierClient.GlacierAPI.GetJobStatus(credential.UserId, jobId, DEFAULT_VAULT_NAME)
	if err == nil {
		helper.Logger.Printf(20, "[ %s ] GetArchiveStatus job id %s Completed %v StatusCode %v for bucket: %s object: %s %s",
			reqId, jobId, job.Completed, job.StatusCode, object.BucketName, object.Name, object.ObjectId)

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

	helper.Logger.Printf(20, "[ %s ] GetArchiveStatus return archiveStatus %s expireDate %s for bucket: %s object: %s version %s",
		reqId, archiveStatus, expireDate, bucketName, objectName, version)

	return archiveStatus, expireDate, nil
}

func (yig *YigStorage) GetObjectFromGlacier(ctx context.Context, object *meta.Object, version string, startOffset, length int64, writer io.Writer, credential common.Credential) error {
	reqId := helper.RequestIdFromContext(ctx)
	helper.Logger.Printf(20, "[ %s ] GetObjectFromGlacier bucket: %s object: %s %s version %s", reqId, object.BucketName, object.Name, object.ObjectId, version)

	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Printf(10, "[ %s ] Glacier not enabled", reqId)
		return ErrInternalError
	}

	archiveStatus, _, err := yig.GetArchiveStatus(ctx, object.BucketName, object.Name, version, credential)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] GetArchiveStatus failed bucket: %s object: %s %s version %s", reqId, object.BucketName, object.Name, object.ObjectId, version)
		return err
	}

	if archiveStatus != meta.ArchiveStatusCodeRestored {
		helper.Logger.Printf(5, "[ %s ] GetObjectFromGlacier failed as archive is in state %v for bucket: %s object: %s %s version %s",
			reqId, archiveStatus, object.BucketName, object.Name, object.ObjectId, version)
		return ErrInvalidObjectState
	}

	// TODO. Should get object directly from Ceph instead from Glacier.
	jobId, err := yig.MetaStorage.GetJobId(object)
	if err != nil || jobId == "" {
		helper.Logger.Printf(5, "[ %s ] GetObjectFromGlacier GetJobId failed for bucket: %s object: %s %s", reqId, object.BucketName, object.Name, object.ObjectId)
		return ErrInternalError
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	// TODO add handle for startOffset and length.
	body, err := glacierClient.GlacierAPI.GetJobOutput(credential.UserId, jobId, DEFAULT_VAULT_NAME)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] GetObjectFromGlacier GetJobOutput failed for bucket: %s object: %s %s", reqId, object.BucketName, object.Name, object.ObjectId)
		return err
	}
	defer body.Close()

	// TODO add buffer. Handle big file for offset.
	if _, err := io.Copy(writer, body); err != nil {
		helper.Logger.Printf(5, "[ %s ] GetObjectFromGlacier io.Copy() failed for bucket: %s object: %s %s", reqId, object.BucketName, object.Name, object.ObjectId)
		return ErrInternalError
	}

	return nil
}

func (yig *YigStorage) DeleteObjectFromGlacier(ctx context.Context, bucketName, objectName, objectId, ownerId string) (err error) {
	reqId := helper.RequestIdFromContext(ctx)
	helper.Logger.Printf(20, "[ %s ] DeleteObjectFromGlacier bucket: %s object: %s id %s ownerId %s", reqId, bucketName, objectName, objectId, ownerId)

	if helper.CONFIG.Glacier.EnableGlacier == false {
		helper.Logger.Printf(10, "[ %s ] DeleteObjectFromGlacier but helper.CONFIG.Glacier.EnableGlacier %t", reqId, helper.CONFIG.Glacier.EnableGlacier)
	}

	// TODO, it's possible that the account was deleted from iam.
	credentials, err := iam.GetKeysByUid(ownerId)
	if err != nil || len(credentials) == 0 {
		helper.Logger.Printf(5, "[ %s ] GetKeysByUid failed bucket %s", reqId, bucketName)
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
		helper.Logger.Printf(5, "[ %s ] GetArchiveId failed bucket %s object %s id %s", reqId, bucketName, objectName, objectId)
		return ErrInternalError
	}

	glacierClient := glacier.NewClient(helper.CONFIG.Glacier.GlacierHost,
		helper.CONFIG.Glacier.GlacierRegion,
		credential.AccessKeyID,
		credential.SecretAccessKey)

	err = glacierClient.GlacierAPI.DeleteArchive(credential.UserId, archiveId, DEFAULT_VAULT_NAME)
	if err != nil {
		helper.Logger.Printf(5, "[ %s ] DeleteArchive failed bucket %s object %s id %s", reqId, bucketName, objectName, objectId)
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
		helper.Logger.Printf(5, "[ %s ] DeleteArchive failed bucket %s object %s id %s", reqId, bucketName, objectName, objectId)
		return ErrInternalError
	}

	return nil
}

// Now ehualu will upload restored archive into yig with archiveId as object name.
func (yig *YigStorage) GetRestoredObjectName(ctx context.Context, object *meta.Object) string {
	archiveId, err := yig.MetaStorage.GetArchiveId(object)
	if err != nil {
		helper.Logger.Printf(10, "[ %s ] GetArchiveId failed %v", helper.RequestIdFromContext(ctx), err)
		return ""
	}

	return archiveId
}
