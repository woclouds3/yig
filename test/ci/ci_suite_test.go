package ci

import (
	"testing"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type CISuite struct {
	// this YigStorage is readonly.
	// we create it just for verify data.
	storage *storage.YigStorage
}

var _ = Suite(&CISuite{})

var logger log.Logger

func (cs *CISuite) SetUpSuite(c *C) {
	helper.SetupConfig()
	logLevel := log.ParseLevel("DEBUG")
	logger = log.NewFileLogger("./ci.log", logLevel)
	helper.Logger = logger
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
	}

	cs.storage = storage.New(logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)
}

func (cs *CISuite) TearDownSuite(c *C) {
	cs.storage.Stop()
}
