package gcs

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const bucketName = "test"

func TestWriteFile(t *testing.T) {
	path := "svk-test-file"
	testWriteFile(t, path)
}

func TestWriteFile_SubDir(t *testing.T) {
	path := "svk-test-dir/test-file"
	testWriteFile(t, path)
}

func testWriteFile(t *testing.T, path string) {
	client := newClient(t)
	defer cleanupBucketPath(bucketName, path, client, t)

	ctx := context.Background()
	backend := NewGCSBackend(client, bucketName, ctx)
	contents := []byte("test")
	err := backend.WriteFile(path, contents)
	if err != nil {
		t.Errorf("WriteFile: should have succeeded (%s)", err)
	}
	// Read it back.
	bucketHandle := client.Bucket(bucketName)
	obj := bucketHandle.Object(path)
	rc, err := obj.NewReader(context.Background())
	if err != nil {
		t.Errorf("WriteFile: should be able to open new file (%s)", err)
	}
	defer rc.Close()
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Errorf("WriteFile: should be able to read new file (%s)", err)
	}
	res := bytes.Compare(data, contents)
	if res != 0 {
		t.Errorf("WriteFile: stored value is different from original (expected '%s' got '%s')", contents, data)
	}
}

func TestReadFile(t *testing.T) {
	contents := []byte("test")
	path := "svk-test-file"
	client := newClient(t)
	defer cleanupBucketPath(bucketName, path, client, t)

	bucketHandle := client.Bucket(bucketName)
	obj := bucketHandle.Object(path)
	w := obj.NewWriter(context.Background())
	_, err := w.Write(contents)
	if err != nil {
		t.Errorf("TestReadFile: should be able to write new file (%s)", err)
	}
	err = w.Close()
	if err != nil {
		t.Errorf("WriteFile: should be able to close file (%s)", err)
	}

	ctx := context.Background()
	backend := NewGCSBackend(client, bucketName, ctx)
	fileBytes, err := backend.ReadFile(path)
	if err != nil {
		t.Errorf("ReadFile: Read should not have failed (%s)", err)
	}
	res := bytes.Compare(fileBytes, contents)
	if res != 0 {
		t.Errorf("ReadFile: stored value is different from original (expected '%s' got '%s')", contents, fileBytes)
	}

}

func TestDeleteFile(t *testing.T) {
	path := "svk-test-file"

	client := newClient(t)
	defer cleanupBucketPath(bucketName, path, client, t)

	bucketHandle := client.Bucket(bucketName)
	obj := bucketHandle.Object(path)
	w := obj.NewWriter(context.Background())
	_, err := w.Write([]byte("test"))
	if err != nil {
		t.Errorf("TestReadFile: should be able to write new file (%s)", err)
	}
	err = w.Close()
	if err != nil {
		t.Errorf("WriteFile: should be able to close file (%s)", err)
	}

	ctx := context.Background()
	backend := NewGCSBackend(client, bucketName, ctx)
	err = backend.DeleteFile(path)
	if err != nil {
		t.Errorf("DeleteFile: should not have failed (%s)", err)
	}
	_, err = obj.Attrs(context.Background())
	if err != storage.ErrObjectNotExist {
		t.Errorf("DeleteFile: should have removed the file (%s)", err)
	}
}

func TestDeleteDir(t *testing.T) {
	client := newClient(t)
	dir := "svk-test-dir"
	path := fmt.Sprintf("%s/file", dir)
	defer cleanupBucketPath(bucketName, path, client, t)

	bucketHandle := client.Bucket(bucketName)

	obj := bucketHandle.Object(path)
	w := obj.NewWriter(context.Background())
	_, err := w.Write([]byte("test"))
	if err != nil {
		t.Errorf("TestReadFile: should be able to write new file (%s)", err)
	}
	err = w.Close()
	if err != nil {
		t.Errorf("WriteFile: should be able to close file (%s)", err)
	}

	ctx := context.Background()
	backend := NewGCSBackend(client, bucketName, ctx)
	err = backend.DeleteDir(dir)
	if err != nil {
		t.Errorf("DeleteFile: should not have failed (%s)", err)
	}
	it := client.Bucket(bucketName).Objects(context.Background(), &storage.Query{Prefix: dir, Delimiter: "/"})
	_, err = it.Next()
	if err != iterator.Done {
		t.Errorf("DeleteFile: should have removed the directory (%s)", dir)
	}
}

func TestDeleteDir_WithSubDirs(t *testing.T) {
	client := newClient(t)
	dir := "svk-test-dir"
	path := fmt.Sprintf("%s/file", dir)
	dir2 := "svk-test-dir/sub-dir"
	path2 := fmt.Sprintf("%s/file", dir2)
	defer cleanupBucketPath(bucketName, path, client, t)

	bucketHandle := client.Bucket(bucketName)

	obj := bucketHandle.Object(path)
	w := obj.NewWriter(context.Background())
	_, err := w.Write([]byte("test"))
	if err != nil {
		t.Errorf("TestReadFile: should be able to write new file (%s)", err)
	}
	err = w.Close()
	if err != nil {
		t.Errorf("WriteFile: should be able to close file (%s)", err)
	}

	obj = bucketHandle.Object(path2)
	w = obj.NewWriter(context.Background())
	_, err = w.Write([]byte("test2"))
	if err != nil {
		t.Errorf("TestReadFile: should be able to write new file (%s)", err)
	}
	err = w.Close()
	if err != nil {
		t.Errorf("WriteFile: should be able to close file (%s)", err)
	}

	ctx := context.Background()
	backend := NewGCSBackend(client, bucketName, ctx)
	err = backend.DeleteDir(dir)
	if err != nil {
		t.Errorf("DeleteFile: should not have failed (%s)", err)
	}
	it := client.Bucket(bucketName).Objects(context.Background(), &storage.Query{Prefix: dir, Delimiter: "/"})
	for {
		attr, err := it.Next()
		fmt.Println(err)
		if err == iterator.Done {
			break
		}
		fmt.Println(attr)
		if attr != nil {
			t.Errorf("DeleteFile: should have removed the directory (%s)", dir)
		}
		fmt.Println("it")
	}
}

func TestList_Empty(t *testing.T) {
	client := newClient(t)
	dir := "svk-test-dir"
	defer cleanupBucketPath(bucketName, dir, client, t)
	ctx := context.Background()
	backend := NewGCSBackend(client, bucketName, ctx)
	files, err := backend.ListDir(dir)
	if err != nil {
		t.Errorf("List should not have failed (%s)", err)
	}
	if len(files) != 0 {
		t.Errorf("List should have return a list with zero elements")
	}
}

func TestList_Single(t *testing.T) {
	client := newClient(t)
	dir := "svk-test-dir"
	path := fmt.Sprintf("%s/file", dir)
	defer cleanupBucketPath(bucketName, path, client, t)

	bucketHandle := client.Bucket(bucketName)

	obj := bucketHandle.Object(path)
	w := obj.NewWriter(context.Background())
	_, err := w.Write([]byte("test"))
	if err != nil {
		t.Errorf("TestReadFile: should be able to write new file (%s)", err)
	}
	err = w.Close()
	if err != nil {
		t.Errorf("WriteFile: should be able to close file (%s)", err)
	}
	ctx := context.Background()
	backend := NewGCSBackend(client, bucketName, ctx)
	files, err := backend.ListDir(dir)
	if err != nil {
		t.Errorf("List should not have failed (%s)", err)
	}
	if len(files) != 1 {
		t.Errorf("List should have return a list with one element")
	}
}

func TestList_Multiple(t *testing.T) {
	client := newClient(t)
	dir := "svk-test-dir"
	path := fmt.Sprintf("%s/file", dir)
	defer cleanupBucketPath(bucketName, path, client, t)

	bucketHandle := client.Bucket(bucketName)

	obj := bucketHandle.Object(path)
	w := obj.NewWriter(context.Background())
	_, err := w.Write([]byte("test"))
	if err != nil {
		t.Errorf("TestReadFile: should be able to write new file (%s)", err)
	}
	err = w.Close()
	if err != nil {
		t.Errorf("WriteFile: should be able to close file (%s)", err)
	}

	obj = bucketHandle.Object(path + "2")
	w = obj.NewWriter(context.Background())
	_, err = w.Write([]byte("test2"))
	if err != nil {
		t.Errorf("TestReadFile: should be able to write new file (%s)", err)
	}
	err = w.Close()
	if err != nil {
		t.Errorf("WriteFile: should be able to close file (%s)", err)
	}

	ctx := context.Background()
	backend := NewGCSBackend(client, bucketName, ctx)
	files, err := backend.ListDir(dir)
	if err != nil {
		t.Errorf("List should not have failed (%s)", err)
	}
	if len(files) != 2 {
		t.Errorf("List should have return a list with two elements")
	}
}

func cleanupBucketPath(bucketName string, path string, client *storage.Client, t *testing.T) {
	ctx := context.Background()
	it := client.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: path})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		err = client.Bucket(bucketName).Object(attrs.Name).Delete(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	defer client.Close()
}

func newClient(t *testing.T) *storage.Client {
	ctx := context.Background()
	// client, err := storage.NewClient(ctx)
	// Using fake GCS server to test GCS: https://github.com/fsouza/fake-gcs-server
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	httpClient := &http.Client{Transport: transCfg}
	client, err := storage.NewClient(ctx, option.WithEndpoint("https://localhost:4443/storage/v1/"), option.WithHTTPClient(httpClient))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Bucket(bucketName).Create(ctx, "test-prj", nil)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
