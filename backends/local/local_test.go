package local

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestNewBackend(t *testing.T) {
	baseDir, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)
	_, err = NewLocalBackend(baseDir)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewBackend_InvalidPath(t *testing.T) {
	_, err := NewLocalBackend("/invalid/path/to/test")
	if err == nil {
		t.Fatal(err)
	}
}

func TestWriteFile(t *testing.T) {
	basePath, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(basePath)
	path := "/test-key"
	testWriteFile(t, basePath, path)
}

func TestWriteFile_SubDir(t *testing.T) {
	basePath, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(basePath)
	path := "/test/sub/dir"
	testWriteFile(t, basePath, path)
}

func testWriteFile(t *testing.T, basePath string, path string) {
	contents := []byte("test")
	backend, err := NewLocalBackend(basePath)
	if err != nil {
		t.Errorf("WriteFile: should have succeeded (%s)", err)
	}
	err = backend.WriteFile(path, contents)
	if err != nil {
		t.Errorf("WriteFile: should have succeeded (%s)", err)
	}

	keyPath := filepath.Join(basePath, path)
	file, err := os.Open(keyPath)
	if err != nil {
		t.Errorf("WriteFile: should be able to open new file (%s)", err)
	}

	fi, err := file.Stat()
	if err != nil {
		t.Errorf("WriteFile: should be able to get file info (%s)", err)
	}

	if fi.Mode() != os.FileMode(0640) {
		t.Errorf("WriteFile: wrong file mode was detected at (%s)", fi.Mode())
	}

	err = file.Close()
	if err != nil {
		t.Errorf("WriteFile: error when closing file (%s)", err)
	}

	fileBytes, err := ioutil.ReadFile(keyPath)
	if err != nil {
		t.Errorf("WriteFile: failed to read file contents (%s)", err)
	}

	res := bytes.Compare(fileBytes, contents)
	if res != 0 {
		t.Errorf("WriteFile: stored value is different from original (expected '%s' got '%s')", contents, fileBytes)
	}
}

func TestReadFile(t *testing.T) {
	basePath, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(basePath)
	path := "/multikv-test-file"
	keyPath := filepath.Join(basePath, path)

	contents := []byte("test")
	err = ioutil.WriteFile(keyPath, contents, 0640)
	if err != nil {
		t.Errorf("ReadFile: failed to prepare (%s)", err)
	}
	backend, err := NewLocalBackend(basePath)
	if err != nil {
		t.Errorf("ReadFile: should have succeeded (%s)", err)
	}
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
	basePath, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(basePath)
	path := "multikv-test-file"

	contents := []byte("test")
	keyPath := filepath.Join(basePath, path)
	err = ioutil.WriteFile(keyPath, contents, 0640)
	if err != nil {
		t.Errorf("DeleteFile: failed to prepare (%s)", err)
	}

	backend, err := NewLocalBackend(basePath)
	if err != nil {
		t.Errorf("DeleteFile: should have succeeded (%s)", err)
	}
	err = backend.DeleteFile(path)
	if err != nil {
		t.Errorf("DeleteFile: should not have failed (%s)", err)
	}
	_, err = os.Stat(path)
	if os.IsExist(err) {
		t.Errorf("DeleteFile: should have removed the file (%s)", err)
	}
}

func TestDeleteDir(t *testing.T) {
	basePath, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(basePath)

	path := "multikv-test-file"
	contents := []byte("test")
	keyPath := filepath.Join(basePath, path)
	err = ioutil.WriteFile(keyPath, contents, 0640)
	if err != nil {
		t.Errorf("DeleteFile: failed to prepare (%s)", err)
	}

	backend, err := NewLocalBackend(basePath)
	if err != nil {
		t.Errorf("WriteFile: should have succeeded (%s)", err)
	}
	err = backend.DeleteDir(path)
	if err != nil {
		t.Errorf("DeleteFile: should not have failed (%s)", err)
	}
	_, err = os.Stat(keyPath)
	if os.IsExist(err) {
		t.Errorf("DeleteFile: should have removed the directory (%s)", err)
	}
}

func TestList_Empty(t *testing.T) {
	basePath, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	path := ""
	defer os.RemoveAll(basePath)
	backend, err := NewLocalBackend(basePath)
	if err != nil {
		t.Errorf("WriteFile: should have succeeded (%s)", err)
	}
	fileNames, err := backend.ListDir(path)
	if err != nil {
		t.Errorf("ListDir: should not have failed (%s)", err)
	}
	if len(fileNames) != 0 {
		t.Errorf("ListDir: should have return a list with zero elements")
	}
}

func TestList(t *testing.T) {
	basePath, err := ioutil.TempDir("", "multikv-test-dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(basePath)
	keyDir := "multikv-test-file"
	path := keyDir + "/key"
	keyPath := filepath.Join(basePath, path)

	contents := []byte("test")
	err = os.MkdirAll(filepath.Join(basePath, keyDir), os.ModePerm)
	if err != nil {
		t.Errorf("ListDir: failed to prepare (%s)", err)
	}
	err = ioutil.WriteFile(keyPath, contents, 0640)
	if err != nil {
		t.Errorf("ListDir: failed to prepare (%s)", err)
	}

	backend, err := NewLocalBackend(basePath)
	if err != nil {
		t.Errorf("WriteFile: should have succeeded (%s)", err)
	}
	fileNames, err := backend.ListDir("multikv-test-file")
	if err != nil {
		t.Errorf("ListDir: should not have failed (%s)", err)
	}
	if len(fileNames) != 1 {
		t.Errorf("ListDir: should have return a list with one element")
	}
}
