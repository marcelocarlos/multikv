package backends

type KvBackend interface {
	Exist(path string) (bool, error)
	ListDir(path string) ([]string, error)
	DeleteDir(path string) error
	DeleteFile(path string) error
	ReadFile(path string) ([]byte, error)
	WriteFile(path string, data []byte) error
}
