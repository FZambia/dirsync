package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FZambia/dirsync"
	"github.com/FZambia/dirsync/internal/service"

	"github.com/jessevdk/go-flags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

// Options for client.
type Options struct {
	Addr      string `short:"a" long:"addr" default:":10000" description:"address of server to connect"`
	Dir       string `short:"d" long:"dir" default:"dirsync_from" description:"a directory to sync from"`
	BlockSize int64  `short:"b" long:"block_size" default:"4096" description:"block size in bytes"`
	UseGzip   bool   `short:"c" long:"gzip" description:"use GZIP compression for calls to server"`
}

func grpcConn(opts Options) *grpc.ClientConn {
	var conn *grpc.ClientConn

	dialOpts := []grpc.DialOption{grpc.WithInsecure()}
	if opts.UseGzip {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}

	conn, err := grpc.Dial(opts.Addr, dialOpts...)
	if err != nil {
		log.Fatalf("can not connect: %s", err)
	}
	return conn
}

func main() {
	var opts Options
	var parser = flags.NewParser(&opts, flags.Default)
	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			log.Fatal(err)
		}
	}

	conn := grpcConn(opts)
	defer conn.Close()

	c := service.NewDirSyncClient(conn)

	client, err := dirsync.NewClient(c, opts.Dir, opts.BlockSize)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gracefulStop := make(chan struct{})

	go func() {
		err = client.Sync(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatal(err)
		}
		close(gracefulStop)
	}()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Println("shutting down")
		cancel()
		// Give client several seconds to finish gracefully.
		select {
		case <-gracefulStop:
		case <-time.After(5 * time.Second):
		}
		close(done)
	}()
	<-done
}
