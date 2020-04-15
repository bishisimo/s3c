/*
@author '彼时思默'
@time 2020/4/8 15:28
@describe:
*/
package s3c

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

type S3Connector struct {
	Sess         *session.Session
	Svc          *s3.S3
	Endpoint     string
	Region       string
	Bucket       string
	accessId     string
	accessSecret string
}

func NewS3Connector() *S3Connector {
	accessId := os.Getenv("S3Id")
	accessSecret := os.Getenv("S3Secret")
	endpoint := os.Getenv("S3Endpoint")
	region := os.Getenv("S3Region")
	bucket := os.Getenv("S3Bucket")
	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(accessId, accessSecret, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(false), //virtual-host style方式，不要修改
	})
	if err != nil {
		logrus.Panic("aws session error:", err)
	}
	svc := s3.New(sess)
	return &S3Connector{
		Sess:         sess,
		Svc:          svc,
		Endpoint:     endpoint,
		Region:       region,
		Bucket:       bucket,
		accessId:     accessId,
		accessSecret: accessSecret,
	}
}

func (s S3Connector) ListBucket() {
	result, err := s.Svc.ListBuckets(nil)
	if err != nil {
		logrus.Panic("Unable to list buckets:", err)
	}

	fmt.Println("Buckets:")

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}
}

func (s S3Connector) ListFile(bucket string) {
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
	}
	resp, err := s.Svc.ListObjects(params)
	if err != nil {
		logrus.Panic("Unable to list buckets:", err)
	}
	for _, item := range resp.Contents {
		fmt.Println(*item.Key, *item.LastModified, *item.Size, *item.StorageClass)
	}
}

/*
上传一个文件
*/
func (s S3Connector) UploadFil2eByPath(filePath string, descPath string) {
	fp, err := os.Open(filePath)
	if err != nil {
		logrus.Panic(filePath, "打开错误! ", err)
	} else if fp != nil {
		defer func() {
			err := fp.Close()
			if err != nil {
				logrus.Panic(err)
			}
		}()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	res, _ := s.Svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(descPath),
		Body:   fp,
	})
	fmt.Println(res)
}

func (s S3Connector) UploadFileByFP(fp *os.File, descPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	res, err := s.Svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(descPath),
		Body:   fp,
	})
	if err != nil {
		logrus.Error("上传文件失败:", err)
	} else {
		logrus.Infof("上传s3文件成功:%v", *res.ETag)
	}
}

/*
上传字符串保存到文件
*/
func (s S3Connector) UploadString(msg string, desPath string) {
	fp := strings.NewReader(msg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()
	res, err := s.Svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(desPath),
		Body:   fp,
	})
	if err != nil {
		logrus.Error("上传文件失败:", err)
	} else {
		logrus.Infof("上传s3文件成功:%v", *res.ETag)
	}
}
