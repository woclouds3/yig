package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/helper"
)

const (
	FIELD_NAME_BODY     = "body"
	FIELD_NAME_USAGE    = "usage"
	FIELD_NAME_POLICY   = "policy"
	FIELD_NAME_FILE_NUM = "fileNum"
)

const (
	VersionEnabled   = "Enabled"
	VersionDisabled  = "Disabled"
	VersionSuspended = "Suspended"
)

const HIDDEN_BUCKET_PREFIX = "archive-restore-"

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// should be serialized into format "2006-01-02T15:04:05.000Z"
	CreateTime time.Time
	OwnerId    string
	CORS       datatype.Cors
	ACL        datatype.Acl
	LC         datatype.Lc
	Policy     policy.Policy
	Versioning string // actually enum: Disabled/Enabled/Suspended
	Usage      int64
	FileNum    int64
	UpdateTime time.Time
}

// implements the Serializable interface
func (b *Bucket) Serialize() (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	pjson, err := b.Policy.MarshalJSON()
	if err != nil {
		return nil, err
	}
	oriPolicy := b.Policy
	defer func() {
		b.Policy = oriPolicy
	}()
	b.Policy = policy.Policy{}
	bytes, err := helper.MsgPackMarshal(b)
	if err != nil {
		return nil, err
	}
	fields[FIELD_NAME_BODY] = string(bytes)
	fields[FIELD_NAME_USAGE] = b.Usage
	fields[FIELD_NAME_POLICY] = string(pjson)
	fields[FIELD_NAME_FILE_NUM] = b.FileNum
	return fields, nil
}

func (b *Bucket) Deserialize(fields map[string]string) (interface{}, error) {
	body, ok := fields[FIELD_NAME_BODY]
	if !ok {
		return nil, errors.New(fmt.Sprintf("no field %s found", FIELD_NAME_BODY))
	}

	err := helper.MsgPackUnMarshal([]byte(body), b)
	if err != nil {
		return nil, err
	}
	if usageStr, ok := fields[FIELD_NAME_USAGE]; ok {
		b.Usage, err = strconv.ParseInt(usageStr, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	if pjson, ok := fields[FIELD_NAME_POLICY]; ok {
		err = b.Policy.UnmarshalJSON([]byte(pjson))
		if err != nil {
			return nil, err
		}
	}

	if num, ok := fields[FIELD_NAME_FILE_NUM]; ok {
		b.FileNum, err = strconv.ParseInt(num, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (b *Bucket) String() (s string) {
	s += "Name: " + b.Name + "\n"
	s += "CreateTime: " + b.CreateTime.Format(CREATE_TIME_LAYOUT) + "\n"
	s += "OwnerId: " + b.OwnerId + "\n"
	s += "CORS: " + fmt.Sprintf("%+v", b.CORS) + "\n"
	s += "ACL: " + fmt.Sprintf("%+v", b.ACL) + "\n"
	s += "LifeCycle: " + fmt.Sprintf("%+v", b.LC) + "\n"
	s += "Policy: " + fmt.Sprintf("%+v", b.Policy) + "\n"
	s += "Version: " + b.Versioning + "\n"
	s += "Usage: " + humanize.Bytes(uint64(b.Usage)) + "\n"
	s += "FileNum: " + humanize.Bytes(uint64(b.FileNum)) + "\n"
	s += "UpdateTime: " + b.UpdateTime.Format(CREATE_TIME_LAYOUT) + "\n"
	return
}

/* Learn from this, http://stackoverflow.com/questions/33587227/golang-method-sets-pointer-vs-value-receiver */
/* If you have a T and it is addressable you can call methods that have a receiver type of *T as well as methods that have a receiver type of T */
func (b *Bucket) GetValues() (values map[string]map[string][]byte, err error) {
	cors, err := json.Marshal(b.CORS)
	if err != nil {
		return
	}
	lc, err := json.Marshal(b.LC)
	if err != nil {
		return
	}
	var usage bytes.Buffer
	err = binary.Write(&usage, binary.BigEndian, b.Usage)
	if err != nil {
		return
	}

	var fileNum bytes.Buffer
	err = binary.Write(&fileNum, binary.BigEndian, b.FileNum)
	if err != nil {
		return
	}

	values = map[string]map[string][]byte{
		BUCKET_COLUMN_FAMILY: map[string][]byte{
			"UID":        []byte(b.OwnerId),
			"ACL":        []byte(b.ACL.CannedAcl),
			"CORS":       cors,
			"LC":         lc,
			"createTime": []byte(b.CreateTime.Format(CREATE_TIME_LAYOUT)),
			"versioning": []byte(b.Versioning),
			"usage":      usage.Bytes(),
			"fileNum":    fileNum.Bytes(),
			"UpdateTime": []byte(b.UpdateTime.Format(CREATE_TIME_LAYOUT)),
		},
		// TODO fancy ACL
	}
	return
}

//Tidb related function
func (b Bucket) GetUpdateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.ACL)
	cors, _ := json.Marshal(b.CORS)
	lc, _ := json.Marshal(b.LC)
	bucket_policy, _ := json.Marshal(b.Policy)
	sql := "update buckets set bucketname=?,acl=?,policy=?,cors=?,lc=?,uid=?,versioning=? where bucketname=?"
	args := []interface{}{b.Name, acl, bucket_policy, cors, lc, b.OwnerId, b.Versioning, b.Name}
	return sql, args
}

func (b Bucket) GetCreateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.ACL)
	cors, _ := json.Marshal(b.CORS)
	lc, _ := json.Marshal(b.LC)
	bucket_policy, _ := json.Marshal(b.Policy)
	createTime := b.CreateTime.Format(TIME_LAYOUT_TIDB)

	sql := "insert into buckets(bucketname,acl,cors,lc,uid,policy,createtime,usages,fileNum,versioning) " +
		"values(?,?,?,?,?,?,?,?,?,?);"
	args := []interface{}{b.Name, acl, cors, lc, b.OwnerId, bucket_policy, createTime, b.Usage, b.FileNum, b.Versioning}
	return sql, args
}

type BucketInfo struct {
	BucketName string
	Usage      int64
	FileNum    int64
}
