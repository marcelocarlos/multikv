# multikv

MultiKV is a simple and extensible library to manage file and path-based key/value stores for multiple storage backends, such as local storage, AWS S3 and Google Cloud Storage.

This library is not intended to be high performance, support high volume workloads or store large amounts of data. Instead, the goal is to provide a quick and easy way to create simple key/value stores. Additional features and optimizations such as encryption at rest, replication, authentication, authorization, and others are left to be managed by the storage backends (e.g. KMS encryption in a S3 or GCS bucket).

## Instalation

```shell
go get github.com/marcelocarlos/multikv
```

## Example usage

Initialize the local backend, then write and read from the KV store:

```go
package main

import (
  "fmt"
  "path/filepath"

  "github.com/marcelocarlos/multikv"
  "github.com/marcelocarlos/multikv/backends/local"
)

func main() {
  backend := local.NewLocalBackend()
  kv := multikv.KV{Backend: backend}
  key := "/tmp/test/mkv"

  err := kv.Put(path, []byte("test"))
  if err != nil {
    fmt.Println(err)
  }

  val, err := kv.Get(path)
  if err != nil {
    fmt.Println(err)
  }
  fmt.Println(string(val))
}
```

## Testing

Tests are executed in CI, but if you want to run them locally first, run:

```shell
# Run lint
docker run --rm -v $(pwd):/app -w /app golangci/golangci-lint:v1.41.0 golangci-lint run
# Test
go test -v ./...
```

## Storage format

Regardless the backend, each key/value pair generates 2 types files: `data` and `info`.

The `data` file contains the base64-encoded value of the corresponding `key`. The `info` file contains JSON-encoded metadata about the `key` using the following format:

```json
{
  "formatVersion": "1.0",
  "kind": "info",
  "path": "/path/to/my/key",
  "createdAt": "2021-04-23T18:25:43.511Z",
  "updatedAt": "2021-04-23T18:26:12.312Z"
}
```

## Roadmap

- v0.2
  - gcs backend
- v0.3
  - s3 backend
- v0.4
  - versioning support
- v0.5
  - PBE encryption (client-side encryption)
- future plans
  - Backblaze B2 backend
  - Google Drive backend
  - Dropbox backend
  - Versioning optimizations (e.g. store diffs instead of full data for each version)