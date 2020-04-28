package datatype

import (
	"context"
	"encoding/xml"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

const (
	MAX_LIFCYCLE_RULES = 1000
)

type LcRule struct {
	ID         string `xml:"ID"`
	Prefix     string `xml:"Prefix"`
	Status     string `xml:"Status"`
	Expiration string `xml:"Expiration>Days"`
}

type Lc struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rule    []LcRule `xml:"Rule"`
}

func LifcycleFromXml(ctx context.Context, lifcycleBuffer []byte) (lc Lc, err error) {
	helper.Logger.Info(ctx, "Incoming Lifcycle XML:", string(lifcycleBuffer))
	err = xml.Unmarshal(lifcycleBuffer, &lc)
	if err != nil {
		helper.ErrorIf(err, "Unable to unmarshal Lifcycle XML")
		return lc, ErrInvalidLifcycleDocument
	}
	if len(lc.Rule) == 0 || len(lc.Rule) > MAX_LIFCYCLE_RULES {
		helper.ErrorIf(nil, "Number of rules is invalid, rules = ", len(lc.Rule))
		return lc, ErrInvalidNumberOfRules
	}
	return lc, nil
}
