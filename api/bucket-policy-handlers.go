/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
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
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	. "git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/yig/yig/signature"
	mux "github.com/gorilla/mux"
)

// maximum supported access policy size.
const maxAccessPolicySize = 20 * 1024 * 1024 // 20KiB.

// Verify if a given action is valid for the url path based on the
// existing bucket access policy.
func bucketPolicyEvalStatements(action string, resource string, conditions map[string]string, statements []policyStatement) bool {
	for _, statement := range statements {
		if bucketPolicyMatchStatement(action, resource, conditions, statement) {
			if statement.Effect == "Allow" {
				return true
			}
			// Do not uncomment kept here for readability.
			// else statement.Effect == "Deny"
			return false
		}
	}
	// None match so deny.
	return false
}

// Verify if action, resource and conditions match input policy statement.
func bucketPolicyMatchStatement(action string, resource string, conditions map[string]string, statement policyStatement) bool {
	// Verify if action matches.
	if bucketPolicyActionMatch(action, statement) {
		// Verify if resource matches.
		if bucketPolicyResourceMatch(resource, statement) {
			// Verify if condition matches.
			if bucketPolicyConditionMatch(conditions, statement) {
				return true
			}
		}
	}
	return false
}

// Verify if given action matches with policy statement.
func bucketPolicyActionMatch(action string, statement policyStatement) bool {
	for _, policyAction := range statement.Actions {
		// Policy action can be a regex, validate the action with matching string.
		matched, err := regexp.MatchString(policyAction, action)
		helper.FatalIf(err, "Invalid action \"%s\" in bucket policy.", action)
		if matched {
			return true
		}
	}
	return false
}

// Match function matches wild cards in 'pattern' for resource.
func resourceMatch(pattern, resource string) bool {
	if pattern == "" {
		return resource == pattern
	}
	if pattern == "*" {
		return true
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return resource == pattern
	}
	tGlob := strings.HasSuffix(pattern, "*")
	end := len(parts) - 1
	if !strings.HasPrefix(resource, parts[0]) {
		return false
	}
	for i := 1; i < end; i++ {
		if !strings.Contains(resource, parts[i]) {
			return false
		}
		idx := strings.Index(resource, parts[i]) + len(parts[i])
		resource = resource[idx:]
	}
	return tGlob || strings.HasSuffix(resource, parts[end])
}

// Verify if given resource matches with policy statement.
func bucketPolicyResourceMatch(resource string, statement policyStatement) bool {
	for _, resourcep := range statement.Resources {
		// the resource rule for object could contain "*" wild card.
		// the requested object can be given access based on the already set bucket policy if
		// the match is successful.
		// More info: http://docs.aws.amazon.com/AmazonS3/latest/dev/s3-arn-format.html .
		if matched := resourceMatch(resourcep, resource); !matched {
			return false
		}
	}
	return true
}

// Verify if given condition matches with policy statement.
func bucketPolicyConditionMatch(conditions map[string]string, statement policyStatement) bool {
	// Supports following conditions.
	// - StringEquals
	// - StringNotEquals
	//
	// Supported applicable condition keys for each conditions.
	// - s3:prefix
	// - s3:max-keys
	var conditionMatches = true
	for condition, conditionKeys := range statement.Conditions {
		if condition == "StringEquals" {
			if conditionKeys["s3:prefix"] != conditions["prefix"] {
				conditionMatches = false
				break
			}
			if conditionKeys["s3:max-keys"] != conditions["max-keys"] {
				conditionMatches = false
				break
			}
		} else if condition == "StringNotEquals" {
			if conditionKeys["s3:prefix"] == conditions["prefix"] {
				conditionMatches = false
				break
			}
			if conditionKeys["s3:max-keys"] == conditions["max-keys"] {
				conditionMatches = false
				break
			}
		}
	}
	return conditionMatches
}

// PutBucketPolicyHandler - PUT Bucket policy
// -----------------
// This implementation of the PUT operation uses the policy
// subresource to add to or replace a policy on a bucket
func (api ObjectAPIHandlers) PutBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4:
		if _, s3Error := signature.IsReqAuthenticated(r); s3Error != nil {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// If Content-Length is unknown or zero, deny the
	// request. PutBucketPolicy always needs a Content-Length if
	// incoming request is not chunked.
	if !contains(r.TransferEncoding, "chunked") {
		if r.ContentLength == -1 || r.ContentLength == 0 {
			WriteErrorResponse(w, r, ErrMissingContentLength, r.URL.Path)
			return
		}
		// If Content-Length is greater than maximum allowed policy size.
		if r.ContentLength > maxAccessPolicySize {
			WriteErrorResponse(w, r, ErrEntityTooLarge, r.URL.Path)
			return
		}
	}

	// Read access policy up to maxAccessPolicySize.
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html
	// bucket policies are limited to 20KB in size, using a limit reader.
	bucketPolicyBuf, err := ioutil.ReadAll(io.LimitReader(r.Body, maxAccessPolicySize))
	if err != nil {
		helper.ErrorIf(err, "Unable to read bucket policy.")
		WriteErrorResponse(w, r, ErrInternalError, r.URL.Path)
		return
	}

	// Parse bucket policy.
	bucketPolicy, err := parseBucketPolicy(bucketPolicyBuf)
	if err != nil {
		helper.ErrorIf(err, "Unable to parse bucket policy.")
		WriteErrorResponse(w, r, ErrInvalidPolicyDocument, r.URL.Path)
		return
	}

	// Parse check bucket policy.
	if s3Error := checkBucketPolicyResources(bucket, bucketPolicy); s3Error != nil {
		WriteErrorResponse(w, r, s3Error, r.URL.Path)
		return
	}

	// Save bucket policy.
	if err := writeBucketPolicy(bucket, bucketPolicyBuf); err != nil {
		helper.ErrorIf(err, "Unable to write bucket policy.")
		switch err.(type) {
		case meta.BucketNameInvalid:
			WriteErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		default:
			WriteErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	WriteSuccessNoContent(w)
}

// DeleteBucketPolicyHandler - DELETE Bucket policy
// -----------------
// This implementation of the DELETE operation uses the policy
// subresource to add to remove a policy on a bucket.
func (api ObjectAPIHandlers) DeleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4:
		if _, s3Error := signature.IsReqAuthenticated(r); s3Error != nil {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// Delete bucket access policy.
	if err := removeBucketPolicy(bucket); err != nil {
		helper.ErrorIf(err, "Unable to remove bucket policy.")
		switch err.(type) {
		case meta.BucketNameInvalid:
			WriteErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case meta.BucketPolicyNotFound:
			WriteErrorResponse(w, r, ErrNoSuchBucketPolicy, r.URL.Path)
		default:
			WriteErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	WriteSuccessNoContent(w)
}

// GetBucketPolicyHandler - GET Bucket policy
// -----------------
// This operation uses the policy
// subresource to return the policy of a specified bucket.
func (api ObjectAPIHandlers) GetBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied, r.URL.Path)
		return
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4:
		if _, s3Error := signature.IsReqAuthenticated(r); s3Error != nil {
			WriteErrorResponse(w, r, s3Error, r.URL.Path)
			return
		}
	}

	// Read bucket access policy.
	p, err := readBucketPolicy(bucket)
	if err != nil {
		helper.ErrorIf(err, "Unable to read bucket policy.")
		switch err.(type) {
		case meta.BucketNameInvalid:
			WriteErrorResponse(w, r, ErrInvalidBucketName, r.URL.Path)
		case meta.BucketPolicyNotFound:
			WriteErrorResponse(w, r, ErrNoSuchBucketPolicy, r.URL.Path)
		default:
			WriteErrorResponse(w, r, ErrInternalError, r.URL.Path)
		}
		return
	}
	io.Copy(w, bytes.NewReader(p))
}