package datatype

import (
	"encoding/xml"
	//	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

const (
	DEFAULT_RESTORE_DAYS = int64(1)
)

type LcRule struct {
	ID             string `xml:"ID"`
	Prefix         string `xml:"Filter>Prefix"`
	Status         string `xml:"Status"`
	Expiration     string `xml:"Expiration>Days"`
	TransitionStorageClass string `xml:"Transition>StorageClass,omitempty"` // TODO. In fact the following two params should appear at the same time.
	TransitionDays int64  `xml:"Transition>Days,omitempty"`         // int in aws S3 is "8-byte signed integer".
}

type Lc struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rule    []LcRule `xml:"Rule"`
}

var ValidRestoreTier = []string{
	"Expedited",
	"Standard",
	"Bulk",
}

type RestoreRequest struct {
	XMLName xml.Name `xml:"RestoreRequest"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Days    int64   `xml:"Days"`
	Tier    string   `xml:"GlacierJobParameters>Tier"`
	Type    string   `xml:"Type"`
}

func IsValidRestoreTier(tier string) bool {
	return helper.StringInSlice(tier, ValidRestoreTier)
}
