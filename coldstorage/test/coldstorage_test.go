package test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"strconv"
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

type ReadSeeker struct {
	b        []byte
	i        int64
	prevRune int
}

func (rs *ReadSeeker) Read(b []byte) (n int, err error) {
	if rs.i >= int64(len(rs.b)) {
		return 0, io.EOF
	}
	rs.prevRune = -1
	n = copy(b, rs.b[rs.i:])
	rs.i += int64(n)
	return
}

func (rs *ReadSeeker) Seek(offset int64, whence int) (int64, error) {
	rs.prevRune = -1
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = rs.i + offset
	case io.SeekEnd:
		abs = int64(len(rs.b)) + offset
	default:
		return 0, errors.New("strings.Reader.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("strings.Reader.Seek: negative position")
	}
	rs.i = abs
	return abs, nil
}

func NewReadSeeker(b []byte) *ReadSeeker {
	return &ReadSeeker{b, 0, -1}
}

func TestColdStorage(t *testing.T) {
	gcli := NewGlacierClient("10.3.221.31:9100", "us-east-1", "I4HZTE4SI1P53CZ1FVY8", "rKsJ3tEzIWvIfLyI7GK1ea2UCwVBRep7Mli3oGMZ")

	err := gcli.CreateVault("-", "VaultTest")
	if err != nil {
		t.Fatal("Create vault fail:", err)
	} else {
		t.Log("Create vault success!")
	}

	content := "This file is used for cold storage single archive function test."
	ioreadseeker := aws.ReadSeekCloser(strings.NewReader(content))
	archiveid, err := gcli.PutArchive("-", "VaultTest", ioreadseeker)
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
		t.Fatal("ReadFromioReadCloser fail:", err)
	}
	s = archivebuf.String()
	if strings.Compare(content, s) == 0 {
		t.Log("Upload archive success further!")
	} else {
		t.Fatal("Upload archive fail!")
	}

	uploadid, err := gcli.CreateMultipart("-", "8388608", "VaultTest")
	if err != nil {
		t.Fatal("CreateMultipart fail:", err)
	} else {
		t.Log("CreateMultipart success!")
	}

	for i := 0; i < 5; i++ {
		var rs io.ReadSeeker
		var endpart string
		var c = byte(i)
		startpart := strconv.Itoa(8388608 * i)
		if i < 4 {
			frontBigArr := make([]byte, 8388608)
			for j := 0; j < 8388608; j++ {
				frontBigArr[j] = c
			}
			rs = NewReadSeeker(frontBigArr)
			endpart = strconv.Itoa(8388608*(i+1) - 1)
		} else {
			lastBigArr := make([]byte, 4194304)
			for j := 0; j < 4194304; j++ {
				lastBigArr[j] = c
			}
			rs = NewReadSeeker(lastBigArr)
			endpart = strconv.Itoa(37748735)
		}
		partrange := "bytes " + startpart + "-" + endpart + "/*"
		err = gcli.PutArchivePart("-", *uploadid, "VaultTest", partrange, rs)
		if err != nil {
			t.Fatal("Put"+strconv.Itoa(i+1)+"ArchivePart fail:", err)
		} else {
			t.Log("Put" + strconv.Itoa(i+1) + "ArchivePart success!")
		}
	}

	_, err = gcli.GetMultipartFromArchive("-", *uploadid, "VaultTest")
	if err != nil {
		t.Fatal("GetMultipartFromArchive fail:", err)
	} else {
		t.Log("GetMultipartFromArchive success preliminary!")
	}

	archiveid, err = gcli.CompleteMultipartUpload("-", *uploadid, "VaultTest")
	if err != nil {
		t.Fatal("CompleteMultipartUpload fail:", err)
	} else {
		t.Log("CompleteMultipartUpload success!")
	}

	_, err = gcli.GetMultipartFromVault("-", "VaultTest")
	if err != nil {
		t.Fatal("GetMultipartFromVault fail:", err)
	} else {
		t.Log("GetMultipartFromVault success preliminary!")
	}

	jobpara = &glacier.JobParameters{
		Type: aws.String("inventory-retrieval"),
		InventoryRetrievalParameters: &glacier.InventoryRetrievalJobInput{
			StartDate: aws.String("2013-03-20T17:03:43Z"),
			EndDate:   aws.String("2030-03-20T17:03:43Z"),
			Limit:     aws.String(""),
			Marker:    aws.String(""),
		},
	}
	jobid, err = gcli.PostJob("-", jobpara, "VaultTest")
	if err != nil {
		t.Fatal("Initiate job inventory-retrieval fail:", err)
	} else {
		t.Log("Initiate job inventory-retrieval success!")
	}
	for {
		jobstatus, err := gcli.GetJobStatus("-", *jobid, "VaultTest")
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
		t.Fatal("ioutil.ReadAll(ioReadCloser) fail:", err)
	}
	var inventoryretrieval InventoryRetrieval
	err = json.Unmarshal(body, &inventoryretrieval)
	if err != nil {
		t.Fatal("json.Unmarshal(inventoryretrieval) fail:", err)
	}
	if inventoryretrieval.VaultName == "VaultTest" {
		t.Log("GetJobOutput inventory-retrieval success further!")
	} else {
		t.Fatal("GetJobOutput inventory-retrieval fail!")
	}

	var vaultinfo *VaultInfo
	for {
		vaultinfo, err = gcli.GetVaultInfo("-", "VaultTest")
		if err != nil {
			t.Fatal("GetVaultInfo fail:", err)
		}
		if *vaultinfo.NumberOfArchives == 0 {
			err = gcli.DeleteVault("-", "VaultTest")
			if err != nil {
				t.Fatal("DeleteVault fail:", err)
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
}
