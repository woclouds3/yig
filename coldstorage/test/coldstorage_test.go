package test

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/client/glacierclient"
	. "github.com/journeymidnight/yig/coldstorage/types/glaciertype"
)

type Archive struct {
	ArchiveId          string
	ArchiveDescription string
	CreationDate       string
	Size               int64
	SHA256TreeHash     string
}

type InventoryRetrieval struct {
	VaultARN      string
	VaultName     string
	InventoryDate string
	Marker        string
	ArchiveList   []Archive
}

func TestColdStorage(t *testing.T) {
	gcli := NewGlacierClient("XXX:XX", "XXX", "XXX", "XXX")
	err := gcli.CreateVault("-", "VaultTest")
	if err != nil {
		t.Fatal("Create vault fail:", err)
	} else {
		t.Log("Create vault success!")
	}

	f, err := os.Create("filetest")
	if err != nil {
		t.Fatal("Create test file fail:", err)
	} else {
		content := "This file is used for cold storage function test."
		_, err = f.Write([]byte(content))
		if err != nil {
			t.Fatal("Write file fail:", err)
		}
	}
	file, err := os.Open("filetest")
	if err != nil {
		t.Fatal("Open file fail:", err)
		os.Exit(1)
	}
	fileInfo, _ := file.Stat()
	var size int64 = fileInfo.Size()
	var fileBytes io.ReadSeeker
	buffer := make([]byte, size)
	_, err = file.Read(buffer)
	if err != nil {
		t.Fatal("Read file content to buffer fail:", err)
	} else {
		fileBytes = bytes.NewReader(buffer) // Converted to io.ReadSeeker type.
	}
	archiveid, err := gcli.PutArchive("-", "VaultTest", fileBytes)
	if err != nil {
		t.Fatal("Upload archive fail:", err)
	} else {
		t.Log("Upload archive success preliminary!")
	}

	jobpara := &glacier.JobParameters{
		ArchiveId: aws.String(*archiveid),
		Type:      aws.String("archive-retrieval"),
	}
	jobid, err := gcli.PostJob("-", jobpara, "VaultTest")
	if err != nil {
		t.Fatal("Initiate job archive-retrieval fail:", err)
	} else {
		t.Log("Initiate job archive-retrieval success!")
	}

	var ioReadCloser io.ReadCloser
	var jobstatus *JobStatus
	for {
		jobstatus, err = gcli.GetJobStatus("-", *jobid, "VaultTest")
		if err != nil {
			t.Fatal("GetJobStatus archive-retrieval fail:", err)
		} else {
			t.Log("GetJobStatus archive-retrieval success!")
		}
		if *jobstatus.Completed == true {
			ioReadCloser, err = gcli.GetJobOutput("-", *jobid, "VaultTest")
			if err != nil {
				t.Fatal("GetJobOutput archive-retrieval fail:", err)
			} else {
				t.Log("GetJobOutput archive-retrieval success！")
			}
			break
		} else {
			time.Sleep(time.Second * 3)
		}
	}

	var s string
	archivebuf := new(bytes.Buffer)
	_, err = archivebuf.ReadFrom(ioReadCloser)
	if err != nil {
		t.Fatal("ReadFrom(ioReadCloser) fail:", err)
	} else {
		s = archivebuf.String()
	}
	if strings.Compare("This file is used for cold storage function test.", s) == 0 {
		t.Log("Upload archive success further!")
	} else {
		t.Fatal("Upload archive fail!")
	}

	jobpara = &glacier.JobParameters{
		Type: aws.String("inventory-retrieval"),
	}
	jobid, err = gcli.PostJob("-", jobpara, "VaultTest")
	if err != nil {
		t.Fatal("Initiate job inventory-retrieval fail:", err)
	} else {
		t.Log("Initiate job inventory-retrieval success!")
	}

	for {
		jobstatus, err = gcli.GetJobStatus("-", *jobid, "VaultTest")
		if err != nil {
			t.Fatal("GetJobStatus inventory-retrieval fail:", err)
		} else {
			t.Log("GetJobStatus inventory-retrieval success!")
		}
		if *jobstatus.Completed == true {
			ioReadCloser, err = gcli.GetJobOutput("-", *jobid, "VaultTest")
			if err != nil {
				t.Fatal("GetJobOutput inventory-retrieval fail:", err)
			} else {
				t.Log("GetJobOutput inventory-retrieval success！")
			}
			break
		} else {
			time.Sleep(time.Second * 3)
		}
	}
	body, err := ioutil.ReadAll(ioReadCloser)
	if err != nil {
		t.Fatal("ioutil.ReadAll fail:", err)
	}
	var inventoryretrieval InventoryRetrieval
	err = json.Unmarshal(body, &inventoryretrieval)
	if err != nil {
		t.Fatal("json.Unmarshal fail:", err)
	} else {
		if inventoryretrieval.VaultName == "VaultTest" {
			t.Log("GetJobOutput inventory-retrieval success further!")
		} else {
			t.Fatal("GetJobOutput inventory-retrieval fail!")
		}
	}

	err = gcli.DeleteArchive("-", *archiveid, "VaultTest")
	if err != nil {
		t.Fatal("DelArchive fail:", err)
	} else {
		t.Log("DelArchive success!")
	}

	var vaultinfo *VaultInfo
	for {
		vaultinfo, err = gcli.GetVaultInfo("-", "VaultTest")
		if *vaultinfo.NumberOfArchives == 0 {
			err = gcli.DeleteVault("-", "VaultTest")
			if err != nil {
				t.Fatal("DelVault fail:", err)
			} else {
				t.Log("DelVault success!")
			}
			break
		} else {
			for _, value := range inventoryretrieval.ArchiveList {
				err = gcli.DeleteArchive("-", value.ArchiveId, "VaultTest")
				if err != nil {
					t.Fatal("DelArchive fail:", err)
				} else {
					t.Log("DelArchive success!")
				}
			}
		}
	}
	err = f.Close()
	if err != nil {
		t.Fatal("Close file fail:", err)
	}
	err = file.Close()
	if err != nil {
		t.Fatal("Close file fail:", err)
	}
	err = os.Remove("filetest")
	if err != nil {
		t.Fatal("Delete test file fail:", err)
	} else {
		t.Log("Delete test file success!")
	}
}
