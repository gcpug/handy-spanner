// Copyright 2019 Masahiro Sano
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/gcpug/handy-spanner/fake"
	"golang.org/x/sync/errgroup"
)

var (
	addr     = flag.String("addr", "0.0.0.0:9999", "address to listen")
	project  = flag.String("project", "fake", "project to apply DDL at startup")
	database = flag.String("database", "fake", "database to apply DDL at startup")
	instance = flag.String("instance", "fake", "instance to apply DDL at startup")
	schema   = flag.String("schema", "", "path to a DDL file")
)

func main() {
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Parse()

	if err := runMain(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)

	}
}

func runMain() error {
	if *addr == "" {
		return fmt.Errorf("addr must be specified")
	}

	var file *os.File
	if *schema != "" {
		f, err := os.Open(*schema)
		if err != nil {
			return fmt.Errorf("failed to open schema file: %v", err)
		}
		file = f
		defer file.Close()
	}

	// root context notifies server shutdown by SIGINT or SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}

	server, err := fake.New(lis)
	if err != nil {
		return fmt.Errorf("failed to setup fake server: %v", err)
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error { return server.Start() })

	if file != nil {
		dbName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", *project, *instance, *database)
		if err := server.ParseAndApplyDDL(ctx, dbName, file); err != nil {
			server.Stop()
			return fmt.Errorf("failed apply DDL: %v", err)
		}
	}

	log.Print("spanner server is ready")

	// Waiting for SIGTERM or Interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, os.Interrupt)
	select {
	case <-sigCh:
		log.Print("received SIGTERM, exiting server")
	case <-ctx.Done():
	}

	// stop server
	server.Stop()

	return wg.Wait()
}
