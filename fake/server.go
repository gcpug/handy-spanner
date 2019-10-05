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

package fake

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"time"

	"github.com/MakeNowJust/memefish/pkg/ast"
	"github.com/MakeNowJust/memefish/pkg/parser"
	"github.com/MakeNowJust/memefish/pkg/token"
	"github.com/gcpug/handy-spanner/server"
	adminv1pb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc"
	channelzsvc "google.golang.org/grpc/channelz/service"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	addr       string
	lis        net.Listener
	grpcServer *grpc.Server
	srv        server.FakeSpannerServer
}

func Run() (*Server, *grpc.ClientConn, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			return nil, nil, err
		}
	}

	srv, err := New(l)
	if err != nil {
		return nil, nil, err
	}
	go func() { _ = srv.Start() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, srv.Addr(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		srv.Stop()
		return nil, nil, err
	}

	return srv, conn, nil
}

// New returns Server for fake spanner.
func New(lis net.Listener) (*Server, error) {
	s := &Server{
		addr:       lis.Addr().String(),
		lis:        lis,
		grpcServer: grpc.NewServer(),
		srv:        server.NewFakeServer(),
	}

	adminv1pb.RegisterDatabaseAdminServer(s.grpcServer, s.srv)
	spannerpb.RegisterSpannerServer(s.grpcServer, s.srv)
	// TODO: support longrunning server

	reflection.Register(s.grpcServer)
	channelzsvc.RegisterChannelzServiceToServer(s.grpcServer)

	return s, nil
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) Start() error {
	return s.grpcServer.Serve(s.lis)
}

func (s *Server) Stop() {
	s.grpcServer.Stop()
	_ = s.lis.Close()
}

func (s *Server) ApplyDDL(ctx context.Context, databaseName string, ddl []ast.DDL) error {
	for _, stmt := range ddl {
		if err := s.srv.ApplyDDL(ctx, databaseName, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) ParseAndApplyDDL(ctx context.Context, databaseName string, r io.Reader) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	ddl, err := (&parser.Parser{
		Lexer: &parser.Lexer{
			File: &token.File{FilePath: "", Buffer: string(b)},
		},
	}).ParseDDLs()
	if err != nil {
		return err
	}

	for _, stmt := range ddl {
		if err := s.srv.ApplyDDL(ctx, databaseName, stmt); err != nil {
			return err
		}
	}

	return nil
}
