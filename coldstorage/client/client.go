package client

import (
	"io"

	"github.com/journeymidnight/yig/coldstorage/client/glacierclient"
	. "github.com/journeymidnight/yig/coldstorage/types/glaciertype"
	"github.com/journeymidnight/yig/log"
)

func InitiateColdstorageClient(logger *log.Logger) {
	glacierclient.InitiateGlacierClient(logger)
}

//Cold storage Client Interface
type ClientAPI interface {
	//archives
	PutArchive(accountid, vaultname string, ioReader io.Reader) (string, error)
	DeleteArchive(accountid string, archiveid string, vaultname string) error
	CreateMultipart(accountid, partsize, vaultname string) (string, error)
	PutArchivePart(accountid, uploadid, vaultname, partrange string, ioReader io.Reader) error
	CompleteMultipartUpload(accountid, uploadid, vaultname string) (string, error)
	DeleteMultipart(accountid, uploadid, vaultname string) error

	//vaults
	CreateVault(accountid string, vaultname string) error
	GetVaultInfo(accountid string, vaultname string) (*VaultInfo, error)
	DeleteVault(accountid string, vaultname string) error

	//job
	PostJob(accountid, vaultname, archiveid, snstopic, tier, outputbucket string) (string, error)
	GetJobStatus(accountid string, jobid string, vaultname string) (*JobStatus, error)
	GetJobOutput(accountid string, jobid string, vaultname string) (io.ReadCloser, error)
}

type Client struct {
	GlacierAPI ClientAPI
}

func NewClient(endpoint, region, ak, sk string) *Client {
	return &Client{GlacierAPI: glacierclient.NewGlacierClient(endpoint, region, ak, sk)}
}
