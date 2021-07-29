package multikv

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/marcelocarlos/multikv/backends"
)

type KV struct {
	Backend backends.KvBackend
}

type Info struct {
	FormatVersion string    `yaml:"formatVersion"`
	Kind          string    `yaml:"kind"`
	Path          string    `yaml:"path"`
	CreatedAt     time.Time `yaml:"createdAt"`
	UpdatedAt     time.Time `yaml:"updatedAt"`
}

func (kv *KV) NewInfo(path string) Info {
	return Info{
		FormatVersion: "1",
		Kind:          "info",
		Path:          path,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
}

func (kv *KV) Put(path string, value []byte) error {
	// Info File
	info := Info{}
	infoFile, err := kv.Backend.ReadFile(filepath.Join(path, "info"))
	if err != nil {
		info = kv.NewInfo(path)
	} else {
		err = json.Unmarshal([]byte(infoFile), &info)
		if err != nil {
			return fmt.Errorf("failed to parse info file (%s)", err)
		}
		info.UpdatedAt = time.Now()
	}
	infoJSON, err := json.Marshal(&info)
	if err != nil {
		return fmt.Errorf("failed to generate info file (%s)", err)
	}
	err = kv.Backend.WriteFile(filepath.Join(path, "info"), infoJSON)
	if err != nil {
		return fmt.Errorf("failed to write info file (%s)", err)
	}
	// Data File
	return kv.Backend.WriteFile(filepath.Join(path, "data"), []byte(base64.StdEncoding.EncodeToString(value)))
}

func (kv *KV) Get(path string) ([]byte, error) {
	data, err := kv.Backend.ReadFile(filepath.Join(path, "data"))
	if err != nil {
		return nil, err
	}
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func (kv *KV) GetInfo(path string) (Info, error) {
	info := Info{}
	infoFile, err := kv.Backend.ReadFile(filepath.Join(path, "info"))
	if err != nil {
		return info, err
	}
	err = json.Unmarshal([]byte(infoFile), &info)
	return info, err
}

func (kv *KV) Delete(path string) error {
	return kv.Backend.DeleteDir(path)
}

func (kv *KV) List(path string) ([]string, error) {
	dirList, err := kv.Backend.ListDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read path %s (%s)", path, err)
	}
	var keys []string
	for _, f := range dirList {
		dataFileFound, _ := kv.Backend.Exist(filepath.Join(path, "data"))
		infoFileFound, _ := kv.Backend.Exist(filepath.Join(path, "info"))
		if dataFileFound || infoFileFound {
			return nil, fmt.Errorf("cannot list the contents of a key, use Get or GetInfo instead")
		}
		keys = append(keys, f)
	}
	return keys, nil
}
