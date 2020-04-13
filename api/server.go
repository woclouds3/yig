package api

import (
	"net/http"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
)

const RequestContextKey = "RequestContext"

type RequestContext struct {
	RequestId  string
	BucketInfo *types.Bucket
	ObjectInfo *types.Object
}

type Server struct {
	Server *http.Server
}

func (s *Server) Stop() {
	helper.Logger.Info(nil, "Server stopped")
}
