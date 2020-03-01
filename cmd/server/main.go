package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/FZambia/dirsync"
	"github.com/FZambia/dirsync/internal/service"

	"github.com/jessevdk/go-flags"
	"google.golang.org/grpc"

	_ "google.golang.org/grpc/encoding/gzip"
)

// Options for server.
type Options struct {
	Addr      string `short:"a" long:"addr" default:":10000" description:"Address of server to listen on"`
	Dir       string `short:"d" long:"dir" default:"dirsync_to" description:"A directory to sync to"`
	BlockSize int64  `short:"b" long:"block_size" default:"4096" description:"Block size in bytes"`
}

func main() {
	var opts Options
	var parser = flags.NewParser(&opts, flags.Default)
	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}

	grpcServer := grpc.NewServer()
	syncServer, err := dirsync.NewServer(opts.Dir, opts.BlockSize)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	service.RegisterDirSyncServer(grpcServer, syncServer)

	listener, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("starting server on %s", opts.Addr)
	go grpcServer.Serve(listener)

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Println("shutting down")
		grpcServer.GracefulStop()
		close(done)
	}()
	<-done
}
