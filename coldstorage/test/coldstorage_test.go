package test

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/client"
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

func createSmallFile(content string, filename string) {
	f, err := os.Create(filename)
	if err != nil {
		Logger.Println(5, "With error: ", err)
	}
	_, err = f.Write([]byte(content))
	if err != nil {
		Logger.Println(5, "With error: ", err)
	}
}

func createBigFile(size int, filename string) {
	f, err := os.Create(filename)
	if err != nil {
		Logger.Println(5, "With error: ", err)
	}
	f, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModeAppend|os.ModeSetuid)
	if err != nil {
		Logger.Println(5, "With error: ", err)
	}
	count := math.Ceil(float64(size) / 1000)
	count_64 := int64(int(count))
	var i int64
	var length int
	for i = 0; i < count_64; i++ {
		if i == (count_64 - 1) {
			length = int(int64(size) - (i)*1000)
		} else {
			length = 1000
		}
		_, err = f.WriteAt([]byte(strings.Repeat("A", length)), i*1000)
		if err != nil {
			Logger.Println(5, "With error: ", err)
		}
	}
}

func splitBigFile(filename string, chunkSize int64) int {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		Logger.Println(5, "With error: ", err)
	}
	num := int(math.Ceil(float64(fileInfo.Size()) / float64(chunkSize)))
	fi, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		Logger.Println(5, "With error: ", err)
	}
	b := make([]byte, chunkSize)
	var i int64 = 1
	for ; i <= int64(num); i++ {
		_, err = fi.Seek((i-1)*(chunkSize), 0)
		if err != nil {
			Logger.Println(5, "With error: ", err)
		}
		if len(b) > int((fileInfo.Size() - (i-1)*chunkSize)) {
			b = make([]byte, fileInfo.Size()-(i-1)*chunkSize)
		}
		_, err = fi.Read(b)
		if err != nil {
			Logger.Println(5, "With error: ", err)
		}
		f, err := os.OpenFile("./"+strconv.Itoa(int(i))+".txt", os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			Logger.Println(5, "With error: ", err)
		}
		_, err = f.Write(b)
		if err != nil {
			Logger.Println(5, "With error: ", err)
		}
		err = f.Close()
		if err != nil {
			Logger.Println(5, "With error: ", err)
		}
	}
	err = fi.Close()
	if err != nil {
		Logger.Println(5, "With error: ", err)
	}
	return num
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
	createSmallFile(content, "SmallFile.txt")
	sf, err := os.Open("SmallFile.txt")
	if err != nil {
		t.Fatal("OpenSmallFile fail:", err)
	}
	archiveid, err := gcli.PutArchive("-", "VaultTest", sf)
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

	err = os.Remove("SmallFile.txt")
	if err != nil {
		t.Fatal("DeleteSmallFile fail:", err)
	}

	uploadid, err := gcli.CreateMultipart("-", "8388608", "VaultTest")
	if err != nil {
		t.Fatal("CreateMultipart fail:", err)
	} else {
		t.Log("CreateMultipart success!")
	}

	var bytesize = 25690112
	var chunkSize int64 = 8388608
	createBigFile(bytesize, "BigFile.txt")
	num := splitBigFile("BigFile.txt", chunkSize)
	for i := 1; i <= num; i++ {
		fi, err := os.Open("./" + strconv.Itoa(int(i)) + ".txt")
		if err != nil {
			t.Fatal("Open "+strconv.Itoa(int(i))+".txt fail:", err)
		}
		startpart := strconv.Itoa(8388608 * (i - 1))
		var endpart string
		if i < num {
			endpart = strconv.Itoa(8388608*i - 1)
		} else {
			endpart = strconv.Itoa(bytesize - 1)
		}
		partrange := "bytes " + startpart + "-" + endpart + "/*"
		err = gcli.PutArchivePart("-", *uploadid, "VaultTest", partrange, fi)
		if err != nil {
			t.Fatal("Put"+strconv.Itoa(i)+"ArchivePart fail:", err)
		} else {
			t.Log("Put" + strconv.Itoa(i) + "ArchivePart success!")
		}
		err = fi.Close()
		if err != nil {
			t.Fatal("Close "+strconv.Itoa(int(i))+".txt fail:", err)
		}
		err = os.Remove("./" + strconv.Itoa(int(i)) + ".txt")
		if err != nil {
			t.Fatal("Delete "+strconv.Itoa(int(i))+".txt fail:", err)
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

	err = os.Remove("BigFile.txt")
	if err != nil {
		t.Fatal("DeleteBigFile fail:", err)
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
