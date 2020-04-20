package storage

import (
	"io"
	"strconv"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	meta "github.com/journeymidnight/yig/meta/types"
)

// To implement a reader for each Glacier part.
// The point is how to adapt AWS S3 parts in various size to Glacier part in (2^n * 1024 KB).
// https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-archive-mpu.html
// https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html

const (
	KB                              = 1024
	MB                              = (1024 * KB)
	TB								= (1024 * MB)
	DEFAULT_PART_SIZE               = (512 * MB) //  Default part size to Glacier.
	MAX_GLACIER_PART_NUM_PER_UPLOAD = 10000      // In fact it's max part number per upload,
	// but 512MB * 10000 should be enough for S3 object (5TB max).
)

// PartReaderBuilder is used to set up a new PartReader only.
type PartReaderBuilder struct {
	glacierPartSize int64 // const 2^n * 1024 KB
	glacierPartNum  int   // const.
	size            int64 // const Object size.

	glacierPartIndex int         // From 1 to glacierPartNum. The part index in Glacier multipart.
	partReader       *PartReader // Glacier part reader for each part. It may cross multiple s3 parts.
}

// The reader to AWS for each Glacier part.
// To avoid start from very beginning every time, startS3PartIndex and startS3ObjectOffset are defined for each Glacier part.
// S3 part              |==256M==|=100M=|======512M=======|===256M===|
// Glacier part         |======512M========|=======512M=======|=100M=|
//                                         /\ PartReaderBuilder.glacierPartIndex = 2
//                                         /\ PartReader.startS3Index = 3
//                                         /\ PartReader.startS3ObjectOffset = 512M
//                                                         /\ PartReader.currentS3PartIndex = 4
//                                                         /\ PartReader.offset = 360M
//                                                         /\ PartReader.size = 512M

type PartReader struct {
	s3Parts     *map[int]*meta.Part // const S3 Object.Parts
	cephCluster *CephStorage

	startS3PartIndex    int   // Move for each Glacier part. Const in a certain part.From 1.
	startS3ObjectOffset int64 // Startoffset to object. Const in a certain part. Move for each Glacier part.

	currentS3PartIndex int // Move in Read and Seek. From 1. Use it to avoid too much move when Read is called repeatedly.

	offset int64 // offset of this part reader, 0 to (size - 1) .
	size   int64 // part size, either PartReaderBuilder.glacierPartSize or last Glacier part.
}

func calculatePart(size int64) (partSize int64, partNum int, err error) {
	var n int64

	if size%DEFAULT_PART_SIZE == 0 {
		n = size / DEFAULT_PART_SIZE
	} else {
		n = size/DEFAULT_PART_SIZE + 1
	}

	if n > MAX_GLACIER_PART_NUM_PER_UPLOAD {
		return 0, 0, io.EOF
	}

	return min(DEFAULT_PART_SIZE, size), int(n), nil
}

func getNewPartReaderBuilder(size int64, s3Parts *map[int]*meta.Part, cephCluster *CephStorage) *PartReaderBuilder {
	if size <= int64(0) || s3Parts == nil || cephCluster == nil {
		return nil
	}

	partSize, partNum, err := calculatePart(size)
	if err != nil {
		return nil
	}

	builder := &PartReaderBuilder{}
	builder.build(partSize, partNum, size, s3Parts, cephCluster)

	return builder
}

func (b *PartReaderBuilder) build(glacierPartSize int64, glacierPartNum int, size int64, s3Parts *map[int]*meta.Part, cephCluster *CephStorage) {
	b.glacierPartSize = glacierPartSize
	b.glacierPartNum = glacierPartNum
	b.size = size

	b.glacierPartIndex = 0

	b.partReader = &PartReader{
		startS3PartIndex:    0,
		startS3ObjectOffset: -1,
		currentS3PartIndex:  0,
		s3Parts:             s3Parts,
		cephCluster:         cephCluster,
		offset:              0,
		size:                -1,
	}
}

func (b *PartReaderBuilder) buildNextPart() error {
	helper.Logger.Println(20, "[  ] enter buildNextPart", b)

	if b.partReader == nil || (int64(b.glacierPartIndex)*b.glacierPartSize >= b.size) {
		return io.EOF
	}

	if b.glacierPartIndex == 0 || b.partReader.startS3PartIndex == 0 {
		// First Glacier part. Point to first S3 part.
		b.glacierPartIndex = 1
		b.partReader.startS3PartIndex = 1
		b.partReader.startS3ObjectOffset = 0
	} else {
		// Skip a Glacier part size. Find where to start in object Parts.
		targetOffset := b.glacierPartSize * int64(b.glacierPartIndex)
		index, err := b.partReader.getS3Index(b.partReader.startS3PartIndex, targetOffset)
		if err != nil {
			return err
		}

		b.glacierPartIndex++
		b.partReader.startS3PartIndex = index
		b.partReader.startS3ObjectOffset = targetOffset
	}

	b.partReader.size = min(b.glacierPartSize, b.size - b.partReader.startS3ObjectOffset)
	b.partReader.reset() // Reset offset and currentS3PartIndex.

	helper.Logger.Println(20, "[  ] leave buildNextPart", b)

	return nil
}

func (b *PartReaderBuilder) getReader() io.ReadCloser {
	return b.partReader
}

// Convert (Glacier part) offset to Object.offset, and find corresonding (S3) part to do read.
// (Seems aws-sdk read 1M for hash or 4096 byte for Body to send.)
func (r *PartReader) Read(p []byte) (int, error) {
	//helper.Logger.Info(nil, "[ ] enter Read %p len %d r.offset %d / %d index %d / %d", p, len(p), r.offset, r.startS3ObjectOffset, r.currentS3PartIndex,r.startS3PartIndex)

	if r.offset >= r.size || r.currentS3PartIndex > len(*(r.s3Parts)) {
		helper.Logger.Info(nil, "[ ] leave Read 1 %p len %d r.offset %d / %d index %d / %d", p, len(p), r.offset, r.startS3ObjectOffset, r.currentS3PartIndex,r.startS3PartIndex)
		return 0, io.EOF
	}

	remaining := r.size - r.offset
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	s3ObjectOffset := r.offsetToObjectOffset(r.offset) // Glacier part offset to Object offset.

	count, err := r.readAndCloseFromS3Part(p, r.currentS3PartIndex, s3ObjectOffset)
	if count == 0 && err != nil {
		helper.Logger.Info(nil, "[ ] leave Read 2 %p len %d r.offset %d / %d index %d / %d", p, len(p), r.offset, r.startS3ObjectOffset, r.currentS3PartIndex,r.startS3PartIndex)
		return 0, err
	}

	// Move to next S3 part. Move r.currentS3PartIndex and r.offset.
	s3ObjectOffset += int64(count) 
	partEndOffset, err := r.getEndOffsetInObject(r.currentS3PartIndex)
	if err != nil || s3ObjectOffset > (partEndOffset + 1) {
		// Should not happen. Should not happen if it increase bigger than 1 part.
		helper.Logger.Info(nil, "[ ] leave Read 3 %p len %d r.offset %d / %d index %d / %d s3ObjectOffset %d err %v", 
			p, len(p), r.offset, r.startS3ObjectOffset, r.currentS3PartIndex,r.startS3PartIndex, s3ObjectOffset, err)
		return 0, io.EOF
	}
	if s3ObjectOffset > partEndOffset {
		r.currentS3PartIndex++ 
	}

	r.offset += int64(count)  // In last part, it's possible bigger than r.size

	//helper.Logger.Info(nil, "[ ] leave Read %p len %d r.offset %d / %d index %d / %d", p, len(p), r.offset, r.startS3ObjectOffset, r.currentS3PartIndex,r.startS3PartIndex)

	return count, err
}

func (r *PartReader) reset() {
	r.offset = 0
	r.currentS3PartIndex = r.startS3PartIndex
}

// Only move the offset and currentS3PartIndex in PartReader, won't connect Ceph.
func (r *PartReader) Seek(offset int64, whence int) (int64, error) {
	// In current test, only Seek(0, 0) called by AWS.
	// helper.Logger.Info(nil, "[ ] enter Seek %d %d", offset, whence)

	switch whence {
	case 0:
		if offset == 0 {
			r.reset()
			return 0, nil
		}
		// Do nothing
	case 1:
		offset += r.offset
	default:
		return 0, ErrInternalError
	}

	if offset < 0 || offset >= r.size {
		return 0, io.EOF
	}

	if offset > 0 {
		// Iterate Parts to find the one matches (object) offset.
		index, err := r.getS3Index(r.startS3PartIndex, r.offsetToObjectOffset(offset))
		if err != nil {
			return 0, err
		}

		r.currentS3PartIndex = index
	} else {
		r.currentS3PartIndex = r.startS3PartIndex
	}

	r.offset = offset

	helper.Logger.Info(nil, "[ ] leave Seek offset %d size: %d index %d / %d", r.offset, r.size, r.currentS3PartIndex, r.startS3PartIndex)

	return r.offset, nil
}

func (r *PartReader) Len() int {
	helper.Logger.Info(nil, "[ ] enter Len() offset %d size %d", r.offset, r.size)

	if r.offset >= r.size {
		return 0
	}

	return int(r.size - r.offset)
}

func (r *PartReader) Close() error {
	// Every AWS S3 part in Ceph is closed in Read=>readAndCloseFromS3Part.
	// So just reset the offset here.
	r.offset = 0
	r.currentS3PartIndex = r.startS3PartIndex
	return nil
}

func (r *PartReader) readAndCloseFromS3Part(p []byte, s3PartIndex int, s3ObjectOffset int64) (int, error) {
	//helper.Logger.Info(nil, "[ ] enter readAndCloseFromS3Part %p %d %d", p, s3PartIndex, s3ObjectOffset)

	if s3PartIndex > len(*(r.s3Parts)) {
		helper.Logger.Info(nil, "err in readAndCloseFromS3Part 1 %d", s3PartIndex)
		return 0, io.EOF
	}

	part, ok := (*(r.s3Parts))[s3PartIndex]
	if !ok {
		helper.Logger.Info(nil, "err in readAndCloseFromS3Part 2 %d %t %v", s3PartIndex, ok, (*(r.s3Parts)))
		return 0, io.EOF
	}

	partStartOffset := s3ObjectOffset - part.Offset

	ioReadCloser, err := r.cephCluster.getReader(BIG_FILE_POOLNAME,
												part.ObjectId,
												partStartOffset,
												part.Size-partStartOffset)
	defer ioReadCloser.Close()

	if err != nil {
		helper.Logger.Error(nil, "[ ] err in cephCluster.getReader for pool %s part[%d] %s offset %d length %d",
			BIG_FILE_POOLNAME, s3PartIndex, part.ObjectId, partStartOffset,
			part.Size-partStartOffset)
		return 0, err
	}

	len, ok := getLen(ioReadCloser)
	if !ok {
		return 0, io.EOF
	}
	count, err := ioReadCloser.Read(p)
	if err != nil { // TODO
		helper.Logger.Info(nil, "[ ] err in readAndCloseFromS3Part err %s", err)
	}
	if count > len {
		helper.Logger.Error(nil, "[ ] err in readAndCloseFromS3Part count %d > len %d", count, len)
		return 0, io.EOF
	}

	return count, err
}

func (r *PartReader) offsetToObjectOffset(offset int64) int64 {
	return offset + r.startS3ObjectOffset
}

func (r *PartReader) getEndOffsetInObject(index int) (int64, error) {
	var part *meta.Part
	var ok bool

	if index > len(*(r.s3Parts)) {
		helper.Logger.Println(10, "[  ] getEndOffsetInObject failed for input", index, len(*(r.s3Parts)))
		return int64(0), ErrInternalError
	} 

	if part, ok = (*(r.s3Parts))[index]; !ok {
		helper.Logger.Println(10, "[  ] getEndOffsetInObject failed for", index, (*(r.s3Parts)), ok)
		return int64(0), ErrInternalError
	}

	return (part.Offset + part.Size - 1), nil
}

func (r *PartReader) getS3Index(startS3Index int, objectOffset int64) (int, error) {
	var index int
	for index = startS3Index; index <= len(*(r.s3Parts)); index++ {
		partEndOffset, err := r.getEndOffsetInObject(index)
		if err != nil {
			return 0, io.EOF
		}
		if partEndOffset >= objectOffset {
			break
		}
	}

	if index > len(*(r.s3Parts)) {
		return 0, io.EOF
	}

	return index, nil
}

func min(a int64, b int64) int64 {
	if a <= b {
		return a
	}
	return b
}

func getLen(reader io.ReadCloser) (int, bool) {
	type lenner interface {
		Len() int
	}

	if lr, ok := reader.(lenner); ok {
		return lr.Len(), true
	}

	return 0, false
}

// Part index from 1, offset / range from 0. 
func getContentRange(partIndex int, partSize int64, size int64) (string, error) {
	if partIndex < 1 {
		helper.Logger.Println(10, "[  ] getContentRange failed for input", partIndex, partSize, size)
		return "", ErrInternalError
	}
	startOffset := int64(partIndex - 1) * partSize
	endOffset := min(startOffset + partSize, size) - 1

	if startOffset >= size || startOffset < 0 || endOffset < startOffset {
		return "", ErrInternalError
	}

	return "bytes " + strconv.FormatInt(startOffset, 10) + "-" + strconv.FormatInt(endOffset, 10) + "/*", nil
}
