Package dirsync allows to sync specified directory from client to server over GRPC connection.

### Highlights

* Works with subdirectories
* Detects that file did not change using SHA-256 checksum
* Rsync-like approach to sync only modified blocks of files
* Keeps in-memory checksum mapping to prevent full uploading of the same file twice
* Full directory tree synchronization on client start
* Synchronizes only difference of directory tree structure after initial tree sync
* GRPC transport for client-server RPC
* GRPC streaming to upload new file and file changes
* Optional GZIP compression to reduce bandwidth used
* Possibility to set custom block size
* Instant synchronization start by watching file system events inside directory
* Synchronization deduping if currently in process of synchronization
* Uses gogo/protobuf for faster Protobuf marshal/unmarshal
* Works on Unix only at moment
* Works only with directories and regular files
* Does not maintain directory and structure permissions

### How it works

First you need to start server and provide a directory to sync to. Then start a client part and provide a directory to sync changes from.

As soon as client started it synchronizes its directory content with server. First directory structure is synchronized, missing directories get created, unnecessary directories and files removed inside server side folder. Then client starts synchronization of each individual file (concurrently). It requests file information from server providing local file SHA-256 checksum. Server replies with SHA-256 file checksum and a list of rolling checksums (weak adler32 and strong SHA-256 for each file block). The size of each file block can be configured (default is 4kb). Client receives file information. If local file checksum matches server side version checksum then nothing will be sent to sync file state – we consider files are the same. If checksums differ then rsync-like algorithm to search for blocks that already exist in server file version will be used. Thus only references to existing blocks and changed parts of file will be sent over network. For newly created files we just stream contents to server without attempt to utilize rsync algorithm. Also new files have a chance to avoid full uploading due to in-memory cache of file checksum on server. Client process monitors directory for changes (using `fsnotify` library, recursively) and triggers synchronization process again if needed.

At this moment we walk over full tree on every change (though deduping this to do actual sync process once, track file's modification time to not sync files if not needed, also synchronize only difference in directory tree). Ideally we could handle individual `fsnotify` events separately to avoid full directory walk but looks like this is quite tricky due to lot of micromanagement and possible corner cases on different systems. So I decided to go with straighforward approach that works reliably.

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
* Make it cross-platform to handle os specific path separators
* Better error handling - at moment client exits on every error
* More clever decision on when to use rolling checksum upload
* Bidirectional synchronization 🔥
* This list is endless actually ... Just use Rsync or Dropbox maybe? 😄
