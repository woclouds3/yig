package conf

import (
	awscredentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws"
)

type GlacierConf struct {
	AccessKeyID           string
	SecretAccessKey       string
	Endpoint              string
	Region                string
	DisableSSL            bool
	S3ForcePathStyle      bool
}

func Tos3Config(gc GlacierConf) *aws.Config {
	s3Config := &aws.Config{
		Credentials:      awscredentials.NewStaticCredentials(gc.AccessKeyID, gc.SecretAccessKey, ""),
		Endpoint:         aws.String(gc.Endpoint),
		Region:           aws.String(gc.Region),
		DisableSSL:       aws.Bool(gc.DisableSSL),
		S3ForcePathStyle: aws.Bool(gc.S3ForcePathStyle),
	}
	return s3Config
}
