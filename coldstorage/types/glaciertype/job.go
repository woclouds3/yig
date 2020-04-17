package glaciertype

/* copied from aws glacier, aws-sdk-go/service/glacier/api.go  */
const (
	// StatusCodeInProgress is a StatusCode enum value
	StatusCodeInProgress = "InProgress"

	// StatusCodeSucceeded is a StatusCode enum value
	StatusCodeSucceeded = "Succeeded"

	// StatusCodeFailed is a StatusCode enum value
	StatusCodeFailed = "Failed"
)

type JobStatus struct {
	Completed  bool
	StatusCode string
}
