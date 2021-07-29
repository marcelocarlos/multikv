package multikv

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/marcelocarlos/multikv/backends/local"
)

func TestKvPut(t *testing.T) {
	// For simplicity, we'll use local backend for tests
	baseDir, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)
	path := "test-key"
	keyPath := filepath.Join(baseDir, path)
	backend, err := local.NewLocalBackend(baseDir)
	if err != nil {
		t.Errorf("TestKvPut: failed to prepare (%s)", err)
	}

	kv := KV{Backend: backend}
	contents := []byte("test")
	err = kv.Put(path, contents)
	if err != nil {
		t.Errorf("TestKvPut: Should have succeeded")
	}

	infoFile, err := ioutil.ReadFile(filepath.Join(keyPath, "info"))
	if err != nil {
		t.Errorf("TestKvPut: Should have a info file (%s)", err)
	}
	info := Info{}
	err = json.Unmarshal([]byte(infoFile), &info)
	if err != nil {
		t.Errorf("TestKvPut: Info file should have the correct format (%s)", err)
	}

	dataFile, err := ioutil.ReadFile(filepath.Join(keyPath, "data"))
	if err != nil {
		t.Errorf("TestKvPut: Should have created a data file (%s)", err)
	}
	fileBytes, err := base64.StdEncoding.DecodeString(string(dataFile))
	if err != nil {
		t.Errorf("TestKvPut: Error decoding value (%s)", err)
	}
	res := bytes.Compare(fileBytes, contents)
	if res != 0 {
		t.Errorf("TestKvPut: stored value is different from original (expected '%s' got '%s')", contents, fileBytes)
	}
}

func TestKvPut_NilPayload(t *testing.T) {
	// For simplicity, we'll use local backend for tests
	baseDir, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)
	path := "test-key"
	keyPath := filepath.Join(baseDir, path)
	backend, err := local.NewLocalBackend(baseDir)
	if err != nil {
		t.Errorf("ListDir: failed to prepare (%s)", err)
	}

	kv := KV{Backend: backend}
	err = kv.Put(path, nil)
	if err != nil {
		t.Errorf("TestKvPut_NilPayload: Should have succeeded")
	}

	dataFile, err := ioutil.ReadFile(filepath.Join(keyPath, "data"))
	if err != nil {
		t.Errorf("TestKvPut_NilPayload: Should have created a data file (%s)", err)
	}
	fileBytes, err := base64.StdEncoding.DecodeString(string(dataFile))
	if err != nil {
		t.Errorf("TestKvPut_NilPayload: Error decoding value (%s)", err)
	}
	res := bytes.Compare(fileBytes, []byte(""))
	if res != 0 {
		t.Errorf("TestKvPut_NilPayload: stored value is different from original (expected '%s' got '%s')", "", fileBytes)
	}
}

func TestGet(t *testing.T) {
	// For simplicity, we'll use local backend for tests
	baseDir, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)

	path := filepath.Join(baseDir, "multikv-key")
	backend, err := local.NewLocalBackend(baseDir)
	if err != nil {
		t.Errorf("ListDir: failed to prepare (%s)", err)
	}
	contents := []byte("test")
	kv := KV{Backend: backend}
	err = kv.Put(path, contents)
	if err != nil {
		t.Errorf("TestGet: Put should have succeeded")
	}

	data, err := kv.Get(path)
	if err != nil {
		t.Errorf("TestGet: Get should not have failed (%s)", err)
	}
	res := bytes.Compare(data, contents)
	if res != 0 {
		t.Errorf("TestGet: stored value is different from original (expected '%s' got '%s')", contents, data)
	}
}

func TestGetInfo(t *testing.T) {
	// For simplicity, we'll use local backend for tests
	baseDir, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)

	path := filepath.Join(baseDir, "multikv-key")
	backend, err := local.NewLocalBackend(baseDir)
	if err != nil {
		t.Errorf("ListDir: failed to prepare (%s)", err)
	}
	contents := []byte("test")
	kv := KV{Backend: backend}
	err = kv.Put(path, contents)
	if err != nil {
		t.Errorf("TestGet: Put should have succeeded")
	}
	_, err = kv.GetInfo(path)
	if err != nil {
		t.Errorf("TestGet: Get should not have failed (%s)", err)
	}
}

func TestDelete(t *testing.T) {
	// For simplicity, we'll use local backend for tests
	baseDir, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)

	path := "test-key"
	backend, err := local.NewLocalBackend(baseDir)
	if err != nil {
		t.Errorf("ListDir: failed to prepare (%s)", err)
	}
	contents := []byte("test")
	kv := KV{Backend: backend}
	err = kv.Put(path, contents)
	if err != nil {
		t.Errorf("TestDelete: Put should have succeeded")
	}

	err = kv.Delete(path)
	if err != nil {
		t.Errorf("TestDelete: should not have failed (%s)", err)
	}
	_, err = os.Stat(path)
	if !os.IsNotExist(err) {
		t.Errorf("TestDelete: Path should have been removed")
	}
}

func TestList(t *testing.T) {
	baseDir, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)
	path := "test/sub/key"
	backend, err := local.NewLocalBackend(baseDir)
	if err != nil {
		t.Errorf("ListDir: failed to prepare (%s)", err)
	}
	kv := KV{Backend: backend}
	err = kv.Put(path, []byte("test"))
	if err != nil {
		t.Errorf("TestList: Put should have succeeded")
	}

	keys, err := kv.List("test/sub")
	if err != nil {
		t.Errorf("TestList: List should have succeeded")
	}
	expected := []string{"key"}
	if !reflect.DeepEqual(keys, expected) {
		t.Errorf("TestList: listed keys did not match. Expected: %v; Found: %v", expected, keys)
	}
}
