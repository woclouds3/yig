package meta

import (
	"context"
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
		helper.Logger.Println(10, "[", helper.RequestIdFromContext(ctx), "]", "GetObject CacheMiss. bucket:", bucketName, "object:", objectName)
		object, err := m.Client.GetObject(bucketName, objectName, "")
		if err != nil {
			return
		}
		helper.Debugln("[", helper.RequestIdFromContext(ctx), "]", "GetObject object.Name:", object.Name)
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

func (m *Meta) GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error) {
	m.Client.GetObjectMap(bucketName, objectName)
	return
}

func (m *Meta) GetObjectVersion(ctx context.Context, bucketName, objectName, version string, willNeed bool) (object *Object, err error) {
	getObjectVersion := func() (o helper.Serializable, err error) {
		object, err := m.Client.GetObject(bucketName, objectName, version)
		if err != nil {
			return
		}
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

func (m *Meta) PutObject(ctx context.Context, object *Object, multipart *Multipart, objMap *ObjMap, updateUsage bool) error {
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

	if objMap != nil {
		err = m.Client.PutObjectMap(objMap, tx)
		if err != nil {
			return err
		}
	}

	if multipart != nil {
		err = m.Client.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}

	requestId := helper.RequestIdFromContext(ctx)
	if updateUsage {
		ustart := time.Now()
		err = m.UpdateUsage(ctx, object.BucketName, object.Size)
		if err != nil {
			return err
		}
		uend := time.Now()
		dur := uend.Sub(ustart)
		if dur/1000000 >= 100 {
			helper.Logger.Printf(5, "[ %s ] slow log: UpdateUsage, bucket %s, obj: %s, size: %d, takes %d",
				requestId, object.BucketName, object.Name, object.Size, dur)
		}
	}
	err = m.Client.CommitTrans(tx)
	tend := time.Now()
	dur := tend.Sub(tstart)
	if dur/1000000 >= 100 {
		helper.Logger.Printf(5, "[ %s ] slow log: MetaPutObject: bucket: %s, obj: %s, size: %d, takes %d",
			requestId, object.BucketName, object.Name, object.Size, dur)
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

func (m *Meta) PutObjMapEntry(objMap *ObjMap) error {
	err := m.Client.PutObjectMap(objMap, nil)
	return err
}

func (m *Meta) DeleteObject(ctx context.Context, object *Object, DeleteMarker bool, objMap *ObjMap) error {
	tx, err := m.Client.NewTrans()
	defer func() {
		if err != nil {
			m.Client.AbortTrans(tx)
		}
	}()

	err = m.Client.DeleteObject(object, tx)
	if err != nil {
		return err
	}

	if objMap != nil {
		err = m.Client.DeleteObjectMap(objMap, tx)
		if err != nil {
			return err
		}
	}

	if DeleteMarker {
		return nil
	}

	err = m.Client.PutObjectToGarbageCollection(object, tx)
	if err != nil {
		return err
	}

	err = m.UpdateUsage(ctx, object.BucketName, -object.Size)
	if err != nil {
		return err
	}
	err = m.Client.CommitTrans(tx)

	return err
}

func (m *Meta) AppendObject(object *Object, isExist bool) error {
	if !isExist {
		return m.Client.PutObject(object, nil)
	}
	return m.Client.UpdateAppendObject(object)
}

//func (m *Meta) DeleteObjectEntry(object *Object) error {
//	err := m.Client.DeleteObject(object, nil)
//	return err
//}

//func (m *Meta) DeleteObjMapEntry(objMap *ObjMap) error {
//	err := m.Client.DeleteObjectMap(objMap, nil)
//	return err
//}
