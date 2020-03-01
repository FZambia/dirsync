package dirsync

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/FZambia/dirsync/internal/service"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func runTestServer(t *testing.T, dir string, blockSize int64) (*grpc.Server, *bufconn.Listener) {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()

	server, err := NewServer(dir, blockSize)
	require.NoError(t, err)

	service.RegisterDirSyncServer(s, server)
	go func() {
		if err := s.Serve(lis); err != nil {
			lis.Close()
			if err != grpc.ErrServerStopped {
				log.Fatalf("Server exited with error: %v", err)
			}
		}
	}()

	return s, lis
}

func getTestClient(t *testing.T, conn *grpc.ClientConn, dir string, blockSize int64) *Client {
	clientConn := service.NewDirSyncClient(conn)
	client, err := NewClient(clientConn, dir, 64)
	require.NoError(t, err)
	return client
}

func TestSynchronization(t *testing.T) {
	dirTo, err := ioutil.TempDir("", "dirsync_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirTo)

	dirFrom, err := ioutil.TempDir("", "dirsync_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dirFrom)

	server, listener := runTestServer(t, dirTo, 64)
	defer server.Stop()

	ctx := context.Background()
	bufDialer := func(string, time.Duration) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := getTestClient(t, conn, dirFrom, 64)
	require.NotNil(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go client.Sync(ctx)

	// Test creating new file.
	fname := "test.txt"
	f, err := os.Create(filepath.Join(dirFrom, fname))
	require.NoError(t, err)
	defer f.Close()
	f.Sync()
	time.Sleep(time.Second)
	if _, err := os.Stat(filepath.Join(dirTo, fname)); os.IsNotExist(err) {
		require.Fail(t, "file not synced")
	}

	// Test modifying file.
	n, err := f.Write([]byte("test"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	f.Sync()
	time.Sleep(time.Second)
	data, err := ioutil.ReadFile(filepath.Join(dirTo, fname))
	require.NoError(t, err)
	require.Equal(t, []byte("test"), data)

	// Test modifying non-empty file.
	n, err = f.Write([]byte(" more content"))
	require.NoError(t, err)
	require.Equal(t, 13, n)
	f.Sync()
	time.Sleep(time.Second)
	data, err = ioutil.ReadFile(filepath.Join(dirTo, fname))
	require.NoError(t, err)
	require.Equal(t, []byte("test more content"), data)

	// Test removing file.
	err = os.Remove(filepath.Join(dirFrom, fname))
	require.NoError(t, err)
	time.Sleep(time.Second)
	if _, err := os.Stat(filepath.Join(dirTo, fname)); !os.IsNotExist(err) {
		require.Fail(t, "file exists")
	}

	// Test creating directory tree.
	err = os.MkdirAll(filepath.Join(dirFrom, "deeply/nested/folder"), 0755)
	require.NoError(t, err)
	time.Sleep(time.Second)
	if info, err := os.Stat(filepath.Join(dirTo, "deeply/nested/folder")); os.IsNotExist(err) || !info.IsDir() {
		require.Fail(t, "directory not synced")
	}

	// Test removing part of directory tree.
	err = os.RemoveAll(filepath.Join(dirFrom, "deeply/nested"))
	require.NoError(t, err)
	time.Sleep(time.Second)
	if _, err := os.Stat(filepath.Join(dirTo, "deeply")); os.IsNotExist(err) {
		require.Fail(t, "directory must exist")
	}
}
