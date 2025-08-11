package main

import (
	"context"
	"log"
	"os"
	"path"
	"strings"
)

var (
	AWSRegion       string = os.Getenv("AWS_REGION")
	AWSS3Endpoint   string = os.Getenv("AWS_S3_ENDPOINT")
	AWSS3Bucket     string = os.Getenv("AWS_S3_BUCKET")
	AWSS3Key        string = os.Getenv("AWS_S3_KEY")
	AWSS3Secret     string = os.Getenv("AWS_S3_SECRET")
	AWSS3Prefix     string = os.Getenv("AWS_S3_PREFIX")
	AWSS3Flatten    bool   = false
	AWSS3PathStyle  bool   = false
	AWSS3PublicRead bool   = false
)

func init() {
	if strings.EqualFold(os.Getenv("AWS_S3_FLATTEN"), "true") {
		AWSS3Flatten = true
	}
	if strings.EqualFold(os.Getenv("AWS_S3_PATH_STYLE"), "true") {
		AWSS3PathStyle = true
	}
	if strings.EqualFold(os.Getenv("AWS_S3_PUBLIC_READ"), "true") {
		AWSS3PublicRead = true
	}
}

func main() {
	ctx := context.Background()
	dt := New(AWSS3Endpoint, AWSS3Bucket).SetRegion(AWSRegion)
	dt.SetCreds(AWSS3Key, AWSS3Secret)
	if AWSS3Flatten {
		dt.SetFlatten(true)
	}
	if len(AWSS3Prefix) > 0 {
		dt.SetPrefix(AWSS3Prefix)
	}
	if AWSS3PathStyle {
		dt.SetPathStyle(true)
	}
	if AWSS3PublicRead {
		dt.SetPublicRead(true)
	}
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	rPath := path.Join(pwd, "index.yaml")
	digest, err := NewDigestFile(rPath)
	if err != nil {
		log.Fatal(err)
	}
	size, err := GetSize(rPath)
	if err != nil {
		log.Fatal(err)
	}
	file := &File{
		Path:  "index.yaml",
		Size:  size,
		ChSum: digest,
	}
	r, err := os.Open(rPath)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()
	log.Printf("Put %s, %d, %s", file.Path, size, digest.String())
	err = dt.Put(ctx, file, r)
	if err != nil {
		log.Fatal(err)
	}

	wPath := path.Join(pwd, "index2.yaml")
	w, err := os.Create(wPath)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	err = dt.Get(ctx, file, w)
	if err != nil {
		log.Fatal(err)
	}
	size, err = GetSize(wPath)
	if err != nil {
		log.Fatal(err)
	}
	digest, err = NewDigestFile(wPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Getted %s, %d, %s", file.Path, size, digest.String())
	return
}
