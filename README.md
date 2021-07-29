# multikv

MultiKV is a simple and extensible library to manage file and path-based key/value stores for multiple storage backends, such as local storage, AWS S3 and Google Cloud Storage.

This library is not intended to be high performance, support high volume workloads or store large amounts of data. Instead, the goal is to provide a quick and easy way to create simple key/value stores. Additional features and optimizations such as encryption at rest, replication, authentication, authorization, and others are left to be managed by the storage backends (e.g. KMS encryption in a S3 or GCS bucket).

<!-- toc -->

- [Installation](#installation)
- [Example usage](#example-usage)
- [Testing](#testing)
- [Backends](#backends)
  * [Local](#local)
  * [GCS](#gcs)
- [Storage format](#storage-format)
- [Roadmap](#roadmap)

<!-- tocstop -->

## Installation

```shell
go get github.com/marcelocarlos/multikv
```

## Example usage

Initialize the local backend, then write and read from the KV store:

```go
package main

import (
  "fmt"

  "github.com/marcelocarlos/multikv"
  "github.com/marcelocarlos/multikv/backends/local"
)

func main() {
  // Initializing backend
  backend, err := local.NewLocalBackend("/tmp/multikv")
  if err != nil {
    fmt.Println(err)
  }
  // Initializing kv
  kv := multikv.KV{Backend: backend}
  // Put example
  err = kv.Put("test/key", []byte("test"))
  if err != nil {
    fmt.Println(err)
  }
  // Get example
  val, err := kv.Get("test/key")
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

# Test - we use fake-gcs-server to spin up a local GCS server so we can safely run the tests
docker run --rm  -d --name fake-gcs-server -p 4443:4443 fsouza/fake-gcs-server -public-host localhost:4443
go test -v ./...
```

## Backends

### Local

The `local` backend allows `multikv` to use a local filesystem as the key/value storage layer. See the [example usage](#example-usage) we provided earlier see it in practice.

### GCS

The `gcs` backend allows `multikv` to use Google Cloud Storage (GCS) as the key/value storage layer. To use it, you can tweak the [example usage](#example-usage) we provided earlier and change backend, for example:

```go
import (
  // ...
  "cloud.google.com/go/storage"
  "github.com/marcelocarlos/multikv/backends/gcs"
  // ...
)

func main() {
  // ...
  ctx := context.Background()
  client, err := storage.NewClient(ctx)
  backend := gcs.NewGCSBackend(client, "my-gcs-bucket", ctx)
  // Initializing kv
  kv := multikv.KV{Backend: backend}
  // ...
}
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

- v0.3
  - S3 backend
- v0.4
  - versioning support
- v0.5
  - PBE encryption (client-side encryption)
- future plans
  - Backblaze B2 backend
  - Google Drive backend
  - Dropbox backend
  - Versioning optimizations (e.g. store diffs instead of full data for each version)
