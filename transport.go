package main

import (
	"context"
	"crypto"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"
)

var (
	ErrNoSuchKey = errors.New("NoSuchKey")
)

type Digest struct {
	sum []byte
}

type File struct {
	Size  int64
	Path  string
	ChSum Digest
}

type Result struct {
	Message string
	Error   error
}

type Transport interface {
	GetDest(file *File) string
	Get(ctx context.Context, file *File, body io.Writer) error
	Put(ctx context.Context, file *File, body io.Reader) error
}

func Copy(ctx context.Context, to, from Transport, f *File) error {
	pr, pw := io.Pipe()
	ch := make(chan error, 2)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer pr.Close()
		err := to.Put(ctx, f, pr)
		ch <- err
	}()
	go func() {
		defer wg.Done()
		defer pw.Close()
		err := from.Get(ctx, f, pw)
		ch <- err
	}()
	go func() {
		wg.Wait()
		close(ch)
	}()
	for err := range ch {
		if err != nil {
			return err
		}
	}
	return nil
}

func GetSize(file string) (int64, error) {
	s, err := os.Stat(file)
	if err != nil {
		return 0, err
	}
	return s.Size(), nil
}

func NewDigestFile(filename string) (Digest, error) {
	f, err := os.Open(filename)
	if err != nil {
		return Digest{}, err
	}
	defer f.Close()
	return NewDigest(f)
}

func NewDigestString(ds string) (Digest, error) {
	d, err := hex.DecodeString(ds)
	if err != nil {
		return Digest{}, err
	}
	return Digest{sum: d}, nil
}

// Helm uses SHA256 as its default hash for all non-cryptographic applications.
func NewDigest(in io.Reader) (Digest, error) {
	hash := crypto.SHA256.New()
	if _, err := io.Copy(hash, in); err != nil {
		return Digest{}, err
	}
	return Digest{sum: hash.Sum(nil)}, nil
}

func (d *Digest) String() string {
	return hex.EncodeToString(d.sum)
}

func (d *Digest) Base64() string {
	return base64.StdEncoding.EncodeToString(d.sum)
}
