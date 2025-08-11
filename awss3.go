package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	a "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	creds "github.com/aws/aws-sdk-go-v2/credentials"
	as3 "github.com/aws/aws-sdk-go-v2/service/s3"
	ts3 "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (s3 *S3) Get(ctx context.Context, file *File, body io.Writer) error {
	err := s3.Check()
	if err != nil {
		return err
	}
	p := path.Join(s3.bucketPrefix, file.Path)
	o, err := s3.c.GetObject(ctx, &as3.GetObjectInput{
		Bucket: a.String(s3.bucket),
		Key:    a.String(p),
	})
	if err != nil {
		var NoSuchKeyError *ts3.NoSuchKey
		if errors.As(err, &NoSuchKeyError) {
			return ErrNoSuchKey
		}
		return err
	}
	defer o.Body.Close()
	wBytes, err := io.Copy(body, o.Body)
	if err != nil {
		return err
	}
	if wBytes != *o.ContentLength {
		return fmt.Errorf("file size mismatch")
	}
	return nil
}

func (s3 *S3) GetDest(file *File) string {
	if s3.flatten {
		return path.Join(s3.bucketPrefix, path.Base(file.Path))
	}
	if strings.HasPrefix(file.Path, "http://") || strings.HasPrefix(file.Path, "https://") || strings.HasPrefix(file.Path, "oci://") {
		u, err := url.Parse(file.Path)
		if err != nil {
			return path.Join(s3.bucketPrefix, path.Base(file.Path))
		}
		return path.Join(s3.bucketPrefix, u.Path)
	}
	return path.Join(s3.bucketPrefix, file.Path)
}

func (s3 *S3) Put(ctx context.Context, file *File, body io.Reader) error {
	err := s3.Check()
	if err != nil {
		return err
	}
	contentType := mime.TypeByExtension(filepath.Ext(file.Path))
	if contentType == "" {
		// Fallback to a default if the type can't be determined
		contentType = "application/octet-stream"
	}
	_, err = s3.c.PutObject(ctx, &as3.PutObjectInput{
		Bucket:            a.String(s3.bucket),
		Key:               a.String(s3.GetDest(file)),
		ACL:               ts3.ObjectCannedACLPublicRead,
		ChecksumAlgorithm: ts3.ChecksumAlgorithmSha256,
		ChecksumSHA256:    a.String(file.ChSum.Base64()),
		Body:              body,
		ContentType:       a.String(contentType),
	})
	return err
}

type S3 struct {
	endpoint        string
	bucket          string
	bucketPrefix    string
	accessKeyID     string
	secretAccessKey string
	region          string
	secure          bool
	flatten         bool
	publicRead      bool
	pathStyle       bool
	c               *as3.Client
}

func New(endpoint, bucket string) *S3 {
	return &S3{
		endpoint: endpoint,
		bucket:   bucket,
	}
}

func (s3 *S3) SetPrefix(p string) *S3 {
	s3.bucketPrefix = p
	return s3
}

func (s3 *S3) SetFlatten(f bool) *S3 {
	s3.flatten = f
	return s3
}

func (s3 *S3) SetPublicRead(p bool) *S3 {
	s3.publicRead = p
	return s3
}

func (s3 *S3) SetPathStyle(ps bool) *S3 {
	s3.pathStyle = ps
	return s3
}

func (s3 *S3) SetRegion(r string) *S3 {
	s3.region = r
	return s3
}

func (s3 *S3) SetCreds(keyID, secret string) *S3 {
	s3.accessKeyID = keyID
	s3.secretAccessKey = secret
	return s3
}

func (s3 *S3) Connect() error {
	if s3.endpoint == "" || s3.bucket == "" || s3.accessKeyID == "" || s3.secretAccessKey == "" {
		return fmt.Errorf("Missing required parameters")
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithBaseEndpoint(s3.endpoint),
		config.WithRegion(s3.region),
		config.WithCredentialsProvider(creds.NewStaticCredentialsProvider(s3.accessKeyID, s3.secretAccessKey, "")),
		config.WithClientLogMode(a.LogRetries|a.LogRequest|a.LogResponse|a.LogSigning),
	)
	if err != nil {
		return nil
	}
	var opts []func(*as3.Options)
	if s3.pathStyle {
		opts = append(opts,
			func(o *as3.Options) {
				o.UsePathStyle = true
			},
		)
	}
	s3.c = as3.NewFromConfig(cfg, opts...)
	return nil
}

func (s3 *S3) Check() error {
	if s3.c == nil {
		return s3.Connect()
	}
	//_, err := s3.c.HealthCheck(time.Second * 10)
	//if err != nil {
	//	return s3.Connect()
	//}
	return nil
}
