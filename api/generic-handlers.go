/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"
)

// HandlerFunc - useful to chain different middleware http.Handler
type HandlerFunc func(http.Handler, *meta.Meta) http.Handler

func RegisterHandlers(router *mux.Router, metadata *meta.Meta, handlerFns ...HandlerFunc) http.Handler {
	var f http.Handler
	f = router
	for _, hFn := range handlerFns {
		f = hFn(f, metadata)
	}
	return f
}

// Common headers among ALL the requests, including "Server", "Accept-Ranges",
// "Cache-Control" and more to be added
type commonHeaderHandler struct {
	handler http.Handler
}

func (h commonHeaderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Accept-Ranges", "bytes")
	h.handler.ServeHTTP(w, r)
}

func SetCommonHeaderHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return commonHeaderHandler{h}
}

type corsHandler struct {
	handler http.Handler
}

func (h corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")

	ctx := getRequestContext(r)
	bucket := ctx.BucketInfo

	if bucket != nil {
		for _, rule := range bucket.CORS.CorsRules {
			if rule.OriginMatched(origin) {
				rule.SetResponseHeaders(w, r)
				break
			}
		}
	}

	if r.Method == "OPTIONS" {
		if origin == "" || r.Header.Get("Access-Control-Request-Method") == "" {
			WriteErrorResponse(w, r, ErrInvalidHeader)
			return
		}
		if InReservedOrigins(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
			w.Header().Set("Access-Control-Allow-Methods", r.Header.Get("Access-Control-Request-Method"))
			if headers := w.Header().Get("Access-Control-Expose-Headers"); headers != "" {
				w.Header().Set("Access-Control-Expose-Headers", strings.Join(append(CommonS3ResponseHeaders, headers), ","))
			} else {
				w.Header().Set("Access-Control-Expose-Headers", strings.Join(CommonS3ResponseHeaders, ","))
			}
		}
		WriteSuccessResponse(w, nil)
		return
	}

	h.handler.ServeHTTP(w, r)
	return
}

// setCorsHandler handler for CORS (Cross Origin Resource Sharing)
func SetCorsHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return corsHandler{h}
}

type resourceHandler struct {
	handler http.Handler
}

// Resource handler ServeHTTP() wrapper
func (h resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Skip the first element which is usually '/' and split the rest.
	tstart := time.Now()
	bucketName, objectName, _ := GetBucketAndObjectInfoFromRequest(r)
	helper.Logger.Info(r.Context(), "ServeHTTP", bucketName, objectName, "Hostname:", strings.Split(r.Host, ":"))

	// Check host name with configuration.
	if isValidHostName(r) == false {
		helper.Logger.Error(r.Context(), "resourceHandler invalid host name. Host:", r.Host)
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	}

	// If bucketName is present and not objectName check for bucket
	// level resource queries.
	if bucketName != "" && objectName == "" {
		if ignoreNotImplementedBucketResources(r) {
			WriteErrorResponse(w, r, ErrNotImplemented)
			return
		}
	}
	// If bucketName and objectName are present check for its resource queries.
	if bucketName != "" && objectName != "" {
		if ignoreNotImplementedObjectResources(r) {
			WriteErrorResponse(w, r, ErrNotImplemented)
			return
		}
	}
	// A put method on path "/" doesn't make sense, ignore it.
	if r.Method == "PUT" && r.URL.Path == "/" && bucketName == "" {
		helper.Logger.Error(r.Context(), "Host:", r.Host, "Path:", r.URL.Path, "Bucket:", bucketName)
		WriteErrorResponse(w, r, ErrMethodNotAllowed)
		return
	}
	h.handler.ServeHTTP(w, r)
	tend := time.Now()
	dur := tend.Sub(tstart).Nanoseconds() / 1000000
	if dur >= 100 {
		helper.Logger.Info(r.Context(), fmt.Sprintf("slow log resouce_handler(%s, %s) spent %d", bucketName, objectName, dur))
	}
}

// setIgnoreResourcesHandler -
// Ignore resources handler is wrapper handler used for API request resource validation
// Since we do not support all the S3 queries, it is necessary for us to throw back a
// valid error message indicating that requested feature is not implemented.
func SetIgnoreResourcesHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return resourceHandler{h}
}

// authHandler - handles all the incoming authorization headers and
// validates them if possible.
type AuthHandler struct {
	handler http.Handler
}

// handler for validating incoming authorization headers.
func (a AuthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch signature.GetRequestAuthType(r) {
	case signature.AuthTypeUnknown:
		WriteErrorResponse(w, r, ErrSignatureVersionNotSupported)
		return
	default:
		// Let top level caller validate for anonymous and known
		// signed requests.
		a.handler.ServeHTTP(w, r)
		return
	}
}

// setAuthHandler to validate authorization header for the incoming request.
func SetAuthHandler(h http.Handler, _ *meta.Meta) http.Handler {
	return AuthHandler{h}
}

// authHandler - handles all the incoming authorization headers and
// validates them if possible.
type GenerateContextHandler struct {
	handler http.Handler
	meta    *meta.Meta
}

// handler for validating incoming authorization headers.
func (h GenerateContextHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var bucketInfo *types.Bucket
	var objectInfo *types.Object
	var err error
	tstart := time.Now()
	requestId := string(helper.GenerateRandomId())
	bucketName, objectName, isBucketDomain := GetBucketAndObjectInfoFromRequest(r)
	helper.Logger.Info(nil, "GenerateContextHandler. RequestId:", requestId, "BucketName:", bucketName, "ObjectName:", objectName)

	ctx := context.WithValue(r.Context(), "RequestId", requestId)

	if bucketName != "" {
		bucketInfo, err = h.meta.GetBucket(ctx, bucketName, true)
		if err != nil && err != ErrNoSuchBucket {
			WriteErrorResponse(w, r, err)
			return
		}
		if bucketInfo != nil && objectName != "" {
			objectInfo, err = h.meta.GetObject(ctx, bucketInfo.Name, objectName, true)
			if err != nil && err != ErrNoSuchKey {
				WriteErrorResponse(w, r, err)
				return
			}
		}
	}

	authType := signature.GetRequestAuthType(r)
	if authType == signature.AuthTypeUnknown {
		WriteErrorResponse(w, r, ErrSignatureVersionNotSupported)
		return
	}

	ctx = context.WithValue(
		ctx,
		RequestContextKey,
		RequestContext{
			RequestID:      requestId,
			BucketName:     bucketName,
			ObjectName:     objectName,
			BucketInfo:     bucketInfo,
			ObjectInfo:     objectInfo,
			AuthType:       authType,
			IsBucketDomain: isBucketDomain,
		})
	h.handler.ServeHTTP(w, r.WithContext(ctx))
	tend := time.Now()
	dur := tend.Sub(tstart).Nanoseconds() / 1000000
	if dur >= 100 {
		helper.Logger.Info(ctx, fmt.Sprintf("slow log: generic_context(%s, %s) spent %d", bucketName, objectName, dur))
	}

}

// setAuthHandler to validate authorization header for the incoming request.
func SetGenerateContextHandler(h http.Handler, meta *meta.Meta) http.Handler {
	return GenerateContextHandler{h, meta}
}

func InReservedOrigins(origin string) bool {
	if len(helper.CONFIG.ReservedOrigins) == 0 {
		return false
	}
	OriginsSplit := strings.Split(helper.CONFIG.ReservedOrigins, ",")
	for _, r := range OriginsSplit {
		if strings.Contains(origin, r) {
			return true
		}
	}
	return false
}

// guessIsBrowserReq - returns true if the request is browser.
// This implementation just validates user-agent and
// looks for "Mozilla" string. This is no way certifiable
// way to know if the request really came from a browser
// since User-Agent's can be arbitrary. But this is just
// a best effort function.
func guessIsBrowserReq(req *http.Request) bool {
	if req == nil {
		return false
	}
	return true
	//return strings.Contains(req.Header.Get("User-Agent"), "Mozilla")
}

//// helpers

// Checks requests for not implemented Bucket resources
func ignoreNotImplementedBucketResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notimplementedBucketResourceNames[name] {
			return true
		}
	}
	return false
}

// Checks requests for not implemented Object resources
func ignoreNotImplementedObjectResources(req *http.Request) bool {
	for name := range req.URL.Query() {
		if notimplementedObjectResourceNames[name] {
			return true
		}
	}
	return false
}

// List of not implemented bucket queries
var notimplementedBucketResourceNames = map[string]bool{
	"logging":        true,
	"notification":   true,
	"replication":    true,
	"tagging":        true,
	"requestPayment": true,
}

// List of not implemented object queries
var notimplementedObjectResourceNames = map[string]bool{
	"torrent": true,
	"tagging": true,
}

func GetBucketAndObjectInfoFromRequest(r *http.Request) (bucketName string, objectName string, isBucketDomain bool) {
	splits := strings.SplitN(r.URL.Path[1:], "/", 2)
	v := strings.Split(r.Host, ":")
	hostWithOutPort := v[0]
	isBucketDomain, bucketName = helper.HasBucketInDomain(hostWithOutPort, ".", helper.CONFIG.S3Domain)
	if isBucketDomain {
		objectName = r.URL.Path[1:]
	} else {
		if len(splits) == 1 {
			bucketName = splits[0]
		}
		if len(splits) == 2 {
			bucketName = splits[0]
			objectName = splits[1]
		}
	}
	return bucketName, objectName, isBucketDomain
}

func getRequestContext(r *http.Request) RequestContext {
	ctx, ok := r.Context().Value(RequestContextKey).(RequestContext)
	if ok {
		return ctx
	}
	helper.Logger.Warn(r.Context(), "getRequestContext() faile, key:", RequestContextKey, r.Context())
	return RequestContext{
		RequestID: r.Context().Value(RequestIdKey).(string),
	}
}

func isValidHostName(r *http.Request) bool {
	if r == nil {
		return false
	}

	v := strings.Split(r.Host, ":")
	hostWithOutPort := v[0]

	for _, d := range helper.CONFIG.S3Domain {
		// Host style, like "bucketname.s3.test.com" .
		if strings.HasSuffix(hostWithOutPort, "."+d) {
			return true
		}

		// Path style, like "s3.test.com" .
		if hostWithOutPort == d {
			return true
		}
	}

	return false
}
