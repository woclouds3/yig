package meta

import (
	"context"
	"fmt"
	"time"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

const (
	OBJECT_CACHE_PREFIX = "object:"
)

func (m *Meta) GetObject(ctx context.Context, bucketName string, objectName string, willNeed bool) (object *Object, err error) {
	getObject := func() (o helper.Serializable, err error) {
		helper.Logger.Info(ctx, "GetObject CacheMiss. bucket:", bucketName, "object:", objectName)
		object, err := m.Client.GetObject(bucketName, objectName, "")
		if err != nil {
			return
		}
		helper.Logger.Info(ctx, "GetObject object.Name:", object.Name)
		if object.Name != objectName {
			err = ErrNoSuchKey
			return
		}
		return object, nil
	}

	toObject := func(fields map[string]string) (interface{}, error) {
		o := &Object{}
		return o.Deserialize(fields)
	}

	o, err := m.Cache.Get(ctx, redis.ObjectTable, OBJECT_CACHE_PREFIX, bucketName+":"+objectName+":",
		getObject, toObject, willNeed)
	if err != nil {
		return
	}
	object, ok := o.(*Object)
	if !ok {
		err = ErrInternalError
		return
	}
	return object, nil
}

func (m *Meta) GetAllObject(bucketName string, objectName string) (object []*Object, err error) {
	return m.Client.GetAllObject(bucketName, objectName, "")
}

func (m *Meta) GetObjectVersion(ctx context.Context, bucketName, objectName, version string, willNeed bool) (object *Object, err error) {
	getObjectVersion := func() (o helper.Serializable, err error) {
		object, err := m.Client.GetObject(bucketName, objectName, version)
		if err != nil {
			return
		}
		helper.Logger.Info(ctx, "GetObjectVersion object.Name:", object.Name, version, object.VersionId)
		if object.Name != objectName {
			err = ErrNoSuchKey
			return
		}
		return object, nil
	}

	toObject := func(fields map[string]string) (interface{}, error) {
		o := &Object{}
		return o.Deserialize(fields)
	}

	o, err := m.Cache.Get(ctx, redis.ObjectTable, OBJECT_CACHE_PREFIX, bucketName+":"+objectName+":"+version,
		getObjectVersion, toObject, willNeed)
	if err != nil {
		return
	}
	object, ok := o.(*Object)
	if !ok {
		err = ErrInternalError
		return
	}
	return object, nil
}

func (m *Meta) PutObject(ctx context.Context, object *Object, multipart *Multipart, updateUsage bool) error {
	tstart := time.Now()
	tx, err := m.Client.NewTrans()
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()

	err = m.Client.PutObject(object, tx)
	if err != nil {
		return err
	}

	if multipart != nil {
		err = m.Client.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}
	err = m.Client.CommitTrans(tx)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to put object meta for bucket: %s, obj: %s, err: %v",
			object.BucketName, object.Name, err))
		return err
	}

	err = m.UpdateBucketInfo(ctx, object.BucketName, FIELD_NAME_FILE_NUM, 1)
	if err != nil {
		return err
	}
	if updateUsage {
		ustart := time.Now()
		err = m.UpdateBucketInfo(ctx, object.BucketName, FIELD_NAME_USAGE, object.Size)
		if err != nil {
			return err
		}
		uend := time.Now()
		dur := uend.Sub(ustart)
		if dur/1000000 >= 100 {
			helper.Logger.Info(ctx, fmt.Sprintf("slow log: UpdateUsage, bucket %s, obj: %s, size: %d, takes %d",
				object.BucketName, object.Name, object.Size, dur))
		}
	}
	tend := time.Now()
	dur := tend.Sub(tstart)
	if dur/1000000 >= 100 {
		helper.Logger.Info(ctx, fmt.Sprintf("slow log: MetaPutObject: bucket: %s, obj: %s, size: %d, takes %d",
			object.BucketName, object.Name, object.Size, dur))
	}
	return nil
}

func (m *Meta) PutObjectEntry(object *Object) error {
	err := m.Client.PutObject(object, nil)
	return err
}

func (m *Meta) UpdateObjectAcl(object *Object) error {
	err := m.Client.UpdateObjectAcl(object)
	return err
}

func (m *Meta) UpdateObjectAttrs(object *Object) error {
	err := m.Client.UpdateObjectAttrs(object)
	return err
}

func (m *Meta) DeleteObject(ctx context.Context, object *Object, DeleteMarker bool) error {
	tx, err := m.Client.NewTrans()
	defer func() {
		if err == nil {
			err = m.Client.CommitTrans(tx)
		}
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()

	err = m.Client.DeleteObject(object, tx)
	if err != nil {
		return err
	}

	if DeleteMarker {
		return nil
	}

	err = m.Client.PutObjectToGarbageCollection(object, tx)
	if err != nil {
		return err
	}

	err = m.UpdateBucketInfo(ctx, object.BucketName, FIELD_NAME_USAGE, -object.Size)
	if err != nil {
		return err
	}

	err = m.UpdateBucketInfo(ctx, object.BucketName, FIELD_NAME_FILE_NUM, -1)

	return err
}

func (m *Meta) AppendObject(object *Object, isExist bool, versionId string) error {
	if !isExist {
		return m.Client.PutObject(object, nil)
	}
	return m.Client.UpdateAppendObject(object, versionId)
}

func (m *Meta) UpdateObjectStorageClass(object *Object) error {
	return m.Client.UpdateObjectStorageClass(object)
}
func (m *Meta) PutArchive(object *Object, archiveId string) error {
	return m.Client.PutArchive(object, archiveId)
}
func (m *Meta) GetArchiveId(object *Object) (archiveId string, err error) {
	return m.Client.GetArchiveId(object)
}
func (m *Meta) UpdateArchiveJobIdAndExpire(object *Object, jobId string, days int64) error {
	return m.Client.UpdateArchiveJobIdAndExpire(object, jobId, days)
}

func (m *Meta) GetJobId(object *Object) (jobId string, err error) {
	return m.Client.GetJobId(object)
}

func (m *Meta) DeleteArchive(object *Object) (err error) {
	return m.Client.DeleteArchive(object)
}

func (m *Meta) DeleteParts(object *Object, part *Part) error {
	return m.Client.DeleteParts(object, part)
}

func (m *Meta) GetExpireDays(object *Object) (days int64, err error) {
	return m.Client.GetExpireDays(object)
}
//func (m *Meta) DeleteObjectEntry(object *Object) error {
//	err := m.Client.DeleteObject(object, nil)
//	return err
//}

//func (m *Meta) DeleteObjMapEntry(objMap *ObjMap) error {
//	err := m.Client.DeleteObjectMap(objMap, nil)
//	return err
//}

func (m *Meta) MarkObjectTransitioning(object *Object) error {
	return m.Client.MarkObjectTransitioning(object)
}
