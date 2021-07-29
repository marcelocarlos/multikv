package gcs

import (
	"context"
	"io/ioutil"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GCSBackend struct {
	client     *storage.Client
	bucketName string
	context    context.Context
}

func NewGCSBackend(client *storage.Client, bucketName string, ctx context.Context) GCSBackend {
	return GCSBackend{
		client:     client,
		bucketName: bucketName,
		context:    ctx,
	}
}

func (c GCSBackend) WriteFile(path string, value []byte) error {
	bucket := c.client.Bucket(c.bucketName)
	obj := bucket.Object(path)
	w := obj.NewWriter(c.context)
	_, err := w.Write(value)
	if err != nil {
		return err
	}
	return w.Close()
}

func (c GCSBackend) ReadFile(path string) ([]byte, error) {
	bucket := c.client.Bucket(c.bucketName)
	obj := bucket.Object(path)
	rc, err := obj.NewReader(c.context)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return ioutil.ReadAll(rc)
}

func (c GCSBackend) DeleteFile(path string) error {
	bucket := c.client.Bucket(c.bucketName)
	return bucket.Object(path).Delete(c.context)
}

func (c GCSBackend) DeleteDir(path string) error {
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	it := c.client.Bucket(c.bucketName).Objects(c.context, &storage.Query{Prefix: path})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		// Need to check further this is the best to way to skip the current "directory"
		if attrs.Name != "" {
			err = c.client.Bucket(c.bucketName).Object(attrs.Name).Delete(c.context)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c GCSBackend) ListDir(path string) ([]string, error) {
	it := c.client.Bucket(c.bucketName).Objects(c.context, &storage.Query{Prefix: path + "/", Delimiter: "/"})
	var fileNames []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if attrs.Name != "" {
			fileNames = append(fileNames, attrs.Name)
		}
	}
	return fileNames, nil
}

func (c GCSBackend) Exist(path string) (bool, error) {
	bucket := c.client.Bucket(c.bucketName)
	obj := bucket.Object(path)
	_, err := obj.Attrs(c.context)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
