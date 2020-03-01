Package dirsync allows to sync specified directory from client to server over GRPC connection.

### Highlights

* Works with recursive directories
* Detects no changes in file with SHA-256 checksum
* Rsync-like approach to sync only modified blocks of files
* Directory tree synchronization on start
* GRPC transport for client-server RPC
* GRPC streaming to upload new file and file changes
* Optional GZIP compression to reduce bandwidth used
* Possibility to set custom block size
* Instant synchronization start by watching file system events inside directory
* Synchronization deduping if currently in process of synchronization

### Quick start

Clone this repo, create two example directories in repo root:

```
mkdir dirsync_from
mkdir dirsync_to
```

Start server (will use `dirsync_to` directory by default):

```
go run cmd/server/main.go
```

And start client (will use `dirsync_from` directory by default):

```
go run cmd/client/main.go
```

Change sth inside `dirsync_from` folder and this will be reflected inside `dirsync_to`.

### TODO

This was a weekend project, it works but still a lot of things can be improved:

* Multitenancy
* Concurrent upload
* Per-operation changes - at this moment we synchronize all structure on every change
* Do not upload the same file twice
* Better error handling - at moment client exits on every error
* More clever decision on when to use rolling checksum upload
* Bidirectional synchronization 🔥
* This list is endless actually ...
