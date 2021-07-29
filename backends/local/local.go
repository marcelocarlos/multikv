package local

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type LocalBackend struct {
	BasePath string
}

func NewLocalBackend(basePath string) (LocalBackend, error) {
	backend := LocalBackend{
		BasePath: basePath,
	}
	err := unix.Access(basePath, unix.W_OK)
	if err != nil {
		return backend, err
	}
	return backend, nil
}

func (c LocalBackend) WriteFile(path string, value []byte) error {
	keyPath := filepath.Join(c.BasePath, path)
	_, err := os.Stat(filepath.Dir(keyPath))
	if os.IsNotExist(err) {
		err := os.MkdirAll(filepath.Dir(keyPath), 0750)
		if err != nil {
			return err
		}
	}
	file, err := os.OpenFile(keyPath, os.O_RDWR|os.O_CREATE, 0640) // O_RDONLY mode
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(value)
	return err
}

func (c LocalBackend) ReadFile(path string) ([]byte, error) {
	keyPath := filepath.Join(c.BasePath, path)
	return ioutil.ReadFile(keyPath)
}

func (c LocalBackend) DeleteFile(path string) error {
	keyPath := filepath.Join(c.BasePath, path)
	return os.Remove(keyPath)
}

func (c LocalBackend) DeleteDir(path string) error {
	keyPath := filepath.Join(c.BasePath, path)
	return os.RemoveAll(keyPath)
}

func (c LocalBackend) ListDir(path string) ([]string, error) {
	keyPath := filepath.Join(c.BasePath, path)
	fi, err := ioutil.ReadDir(keyPath)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, f := range fi {
		files = append(files, f.Name())
	}
	return files, nil
}

func (c LocalBackend) Exist(path string) (bool, error) {
	keyPath := filepath.Join(c.BasePath, path)
	_, err := os.Stat(keyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
