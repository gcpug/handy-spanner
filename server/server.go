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

package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/MakeNowJust/memefish/pkg/ast"
	"github.com/MakeNowJust/memefish/pkg/parser"
	"github.com/MakeNowJust/memefish/pkg/token"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	structpb "github.com/golang/protobuf/ptypes/struct"
	iamv1pb "google.golang.org/genproto/googleapis/iam/v1"
	lropb "google.golang.org/genproto/googleapis/longrunning"
	adminv1pb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FakeSpannerServer interface {
	ApplyDDL(ctx context.Context, databaseName string, stmt ast.DDL) error

	spannerpb.SpannerServer
	adminv1pb.DatabaseAdminServer
	lropb.OperationsServer
}

func NewFakeServer() FakeSpannerServer {
	return &server{
		autoCreateDatabase: true,
		db:                 make(map[string]*database),
		sessions:           make(map[string]*session),
	}
}

type server struct {
	autoCreateDatabase bool

	dbMu sync.RWMutex
	db   map[string]*database

	sessionMu sync.RWMutex
	sessions  map[string]*session
}

func (s *server) ApplyDDL(ctx context.Context, databaseName string, stmt ast.DDL) error {
	db, err := s.getOrCreateDatabase(databaseName)
	if err != nil {
		return err
	}

	return db.ApplyDDL(ctx, stmt)
}

// CreateDatabase implements adminv1pb.DatabaseAdminServer.
func (s *server) CreateDatabase(ctx context.Context, req *adminv1pb.CreateDatabaseRequest) (*lropb.Operation, error) {
	ddl, err := (&parser.Parser{
		Lexer: &parser.Lexer{
			File: &token.File{FilePath: "", Buffer: req.GetCreateStatement()},
		},
	}).ParseDDL()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Errors parsing Spanner DDL statement: %s : %s", req.GetCreateStatement(), err)
	}
	createStmt, ok := ddl.(*ast.CreateDatabase)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Create statement is not CREATE DATABASE")
	}

	var stmts []ast.DDL
	for _, s := range req.GetExtraStatements() {
		stmt, err := (&parser.Parser{
			Lexer: &parser.Lexer{
				File: &token.File{FilePath: "", Buffer: s},
			},
		}).ParseDDL()
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Errors parsing Spanner DDL statement: %s : %s", s, err)
		}
		stmts = append(stmts, stmt)
	}

	databaseName := strings.Join([]string{req.GetParent(), "databases", createStmt.Name.Name}, "/")
	if _, err := s.createDatabase(databaseName); err != nil {
		return nil, err
	}
	for _, ddl := range stmts {
		if err := s.ApplyDDL(ctx, databaseName, ddl); err != nil {
			return nil, err
		}
	}

	// TODO: save operation
	resp, _ := ptypes.MarshalAny(&adminv1pb.Database{
		Name:  databaseName,
		State: adminv1pb.Database_READY,
	})
	op := &lropb.Operation{
		Name: fmt.Sprintf("%s/operations/_auto_%d", databaseName, time.Now().UnixNano()/1000),
		Done: true,
		Result: &lropb.Operation_Response{
			Response: resp,
		},
	}
	return op, nil
}

// Gets the state of a Cloud Spanner database.
func (s *server) GetDatabase(ctx context.Context, req *adminv1pb.GetDatabaseRequest) (*adminv1pb.Database, error) {
	_, err := s.getDatabase(req.Name)
	if err != nil {
		return nil, err
	}

	return &adminv1pb.Database{
		Name:  req.Name,
		State: adminv1pb.Database_READY,
	}, nil
}

// Lists Cloud Spanner databases.
func (s *server) ListDatabases(ctx context.Context, req *adminv1pb.ListDatabasesRequest) (*adminv1pb.ListDatabasesResponse, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	// TODO: filter by parent

	dbs := make([]*adminv1pb.Database, 0, len(s.db))
	for name := range s.db {
		dbs = append(dbs, &adminv1pb.Database{
			Name:  name,
			State: adminv1pb.Database_READY,
		})
	}

	// TODO: respect page token

	return &adminv1pb.ListDatabasesResponse{Databases: dbs}, nil
}

func (s *server) UpdateDatabaseDdl(ctx context.Context, req *adminv1pb.UpdateDatabaseDdlRequest) (*lropb.Operation, error) {
	var stmts []ast.DDL
	for _, s := range req.Statements {
		stmt, err := (&parser.Parser{
			Lexer: &parser.Lexer{
				File: &token.File{FilePath: "", Buffer: s},
			},
		}).ParseDDL()
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid ddl %q: %v", s, err)
		}
		stmts = append(stmts, stmt)
	}

	for _, ddl := range stmts {
		_ = s.ApplyDDL(ctx, req.Database, ddl)
	}

	op := &lropb.Operation{
		Name: "TODO:xxx",
	}
	return op, nil
}

// DropDatabase implements adminv1pb.DatabaseAdminServer.
func (s *server) DropDatabase(ctx context.Context, req *adminv1pb.DropDatabaseRequest) (*empty.Empty, error) {
	if err := s.dropDatabase(req.GetDatabase()); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

// Returns the schema of a Cloud Spanner database as a list of formatted
// DDL statements. This method does not show pending schema updates, those may
// be queried using the [Operations][google.longrunning.Operations] API.
func (s *server) GetDatabaseDdl(context.Context, *adminv1pb.GetDatabaseDdlRequest) (*adminv1pb.GetDatabaseDdlResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: ExecuteSql")
}

func (s *server) SetIamPolicy(context.Context, *iamv1pb.SetIamPolicyRequest) (*iamv1pb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: SetIamPolixy")
}

func (s *server) GetIamPolicy(context.Context, *iamv1pb.GetIamPolicyRequest) (*iamv1pb.Policy, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: GetIamPolicy")
}

func (s *server) TestIamPermissions(context.Context, *iamv1pb.TestIamPermissionsRequest) (*iamv1pb.TestIamPermissionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: TestIamPermissions")
}

func (s *server) ListOperations(ctx context.Context, req *lropb.ListOperationsRequest) (*lropb.ListOperationsResponse, error) {
	// TODO
	return &lropb.ListOperationsResponse{}, nil
}

func (s *server) GetOperation(ctx context.Context, req *lropb.GetOperationRequest) (*lropb.Operation, error) {
	name := "TODO:xxx"
	if req.Name != name {
		return nil, status.Errorf(codes.NotFound, "Operation not found: %s", name)
	}

	any, _ := ptypes.MarshalAny(&empty.Empty{})
	op := &lropb.Operation{
		Name: "TODO:xxx",
		Done: true,
		Result: &lropb.Operation_Response{
			Response: any,
		},
	}
	return op, nil
}

func (s *server) DeleteOperation(ctx context.Context, req *lropb.DeleteOperationRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *server) CancelOperation(ctx context.Context, req *lropb.CancelOperationRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *server) WaitOperation(ctx context.Context, req *lropb.WaitOperationRequest) (*lropb.Operation, error) {
	name := "TODO:xxx"
	if req.Name != name {
		return nil, status.Errorf(codes.NotFound, "Operation not found: %s", name)
	}
	any, _ := ptypes.MarshalAny(&empty.Empty{})
	op := &lropb.Operation{
		Name: "TODO:xxx",
		Done: true,
		Result: &lropb.Operation_Response{
			Response: any,
		},
	}
	return op, nil
}

func parseDatabaseName(fullDatabaseName string) ([]string, bool) {
	parts := strings.Split(fullDatabaseName, "/")
	if len(parts) != 6 {
		return nil, false
	}
	if parts[0] != "projects" {
		return nil, false
	}
	if parts[2] != "instances" {
		return nil, false
	}
	if parts[4] != "databases" {
		return nil, false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return nil, false
	}
	return []string{parts[1], parts[3], parts[5]}, true
}

func (s *server) getSession(name string) (*session, error) {
	s.sessionMu.RLock()
	defer s.sessionMu.RUnlock()

	if !validateSessionName(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid BeginTransaction request")
	}
	session, ok := s.sessions[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Session not found: %s", name)
	}
	return session, nil
}

func (s *server) createSession(db *database, dbName string) (*session, error) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	for i := 0; i < 3; i++ {
		session := newSession(db, dbName)
		if _, ok := s.sessions[session.Name()]; ok {
			continue
		}
		s.sessions[session.Name()] = session
		return session, nil
	}

	return nil, status.Errorf(codes.Internal, "create session failed")
}

func (s *server) createDatabase(name string) (*database, error) {
	if _, ok := parseDatabaseName(name); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid CreateDatabase request.")
	}

	// read lock to check database exists
	s.dbMu.RLock()
	_, ok := s.db[name]
	s.dbMu.RUnlock()
	if ok {
		return nil, status.Errorf(codes.AlreadyExists, "Database already exists: %s", name)
	}

	// write lock
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	// re-check after lock
	if _, ok := s.db[name]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "Database already exists: %s", name)
	}

	db := newDatabase()
	s.db[name] = db

	return db, nil
}

func (s *server) getOrCreateDatabase(name string) (*database, error) {
	s.dbMu.RLock()
	db, ok := s.db[name]
	s.dbMu.RUnlock()
	if !ok {
		if !s.autoCreateDatabase {
			return nil, status.Errorf(codes.NotFound, "Database not found: %s", name)
		}

		var err error
		db, err = s.createDatabase(name)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (s *server) getDatabase(name string) (*database, error) {
	s.dbMu.RLock()
	db, ok := s.db[name]
	s.dbMu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Database not found: %s", name)
	}

	return db, nil
}

func (s *server) dropDatabase(name string) error {
	if _, ok := parseDatabaseName(name); !ok {
		return status.Error(codes.InvalidArgument, "Invalid DropDatabase request.")
	}

	s.dbMu.RLock()
	db, found := s.db[name]
	s.dbMu.RUnlock()
	if !found {
		return nil
	}

	if err := db.Close(); err != nil {
		return status.Errorf(codes.Internal, "Failed to close the database: %s", err)
	}

	s.dbMu.Lock()
	delete(s.db, name)
	s.dbMu.Unlock()

	return nil
}

func (s *server) CreateSession(ctx context.Context, req *spannerpb.CreateSessionRequest) (*spannerpb.Session, error) {
	_, ok := parseDatabaseName(req.Database)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid CreateSession request")
	}

	db, err := s.getOrCreateDatabase(req.Database)
	if err != nil {
		return nil, err
	}

	session, err := s.createSession(db, req.Database)
	if err != nil {
		return nil, err
	}

	return session.Proto(), nil
}

func (s *server) BatchCreateSessions(ctx context.Context, req *spannerpb.BatchCreateSessionsRequest) (*spannerpb.BatchCreateSessionsResponse, error) {
	_, ok := parseDatabaseName(req.Database)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid BatchCreateSessions request")
	}

	db, err := s.getOrCreateDatabase(req.Database)
	if err != nil {
		return nil, err
	}

	sessions := make([]*spannerpb.Session, req.SessionCount)
	for i := 0; i < int(req.SessionCount); i++ {
		s, err := s.createSession(db, req.Database)
		if err != nil {
			return nil, err
		}
		sessions[i] = s.Proto()
	}

	return &spannerpb.BatchCreateSessionsResponse{
		Session: sessions,
	}, nil
}

func (s *server) GetSession(ctx context.Context, req *spannerpb.GetSessionRequest) (*spannerpb.Session, error) {
	session, err := s.getSession(req.Name)
	if err != nil {
		return nil, err
	}

	return session.Proto(), nil
}

func (s *server) ListSessions(ctx context.Context, req *spannerpb.ListSessionsRequest) (*spannerpb.ListSessionsResponse, error) {
	s.sessionMu.RLock()
	defer s.sessionMu.RUnlock()

	// TODO: respect page size

	prefix := req.Database + "/"
	var sessions []*spannerpb.Session
	for name, session := range s.sessions {
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		sessions = append(sessions, session.Proto())
	}

	return &spannerpb.ListSessionsResponse{
		Sessions: sessions,
	}, nil
}

func (s *server) DeleteSession(ctx context.Context, req *spannerpb.DeleteSessionRequest) (*emptypb.Empty, error) {
	session, err := s.getSession(req.Name)
	if err != nil {
		return nil, err
	}

	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	delete(s.sessions, session.Name())

	return &emptypb.Empty{}, nil
}

func (s *server) ExecuteSql(ctx context.Context, req *spannerpb.ExecuteSqlRequest) (*spannerpb.ResultSet, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	session, err := s.getSession(req.Session)
	if err != nil {
		return nil, err
	}

	tx, txCreated, err := session.GetTransactionBySelector(req.GetTransaction())
	if err != nil {
		return nil, err
	}

	if tx.SingleUse() {
		tx.Done(TransactionRollbacked)
		msg := "DML statements may not be performed in single-use transactions, to avoid replay."
		return nil, status.Errorf(codes.InvalidArgument, msg)
	}

	checkAvailability := func() error {
		switch tx.Status() {
		case TransactionInvalidated:
			return status.Errorf(codes.FailedPrecondition, "This transaction has been invalidated by a later transaction in the same session.")
		case TransactionCommited, TransactionRollbacked:
			return status.Errorf(codes.FailedPrecondition, "Cannot start a read or query within a transaction after Commit() or Rollback() has been called.")
		case TransactionAborted:
			return status.Errorf(codes.Aborted, "transaction aborted")
		}
		return status.Errorf(codes.Unknown, "unknown status")
	}

	if !tx.Available() {
		return nil, checkAvailability()
	}

	if !tx.ReadWrite() {
		msg := "DML statements can only be performed in a read-write transaction."
		return nil, status.Errorf(codes.FailedPrecondition, msg)
	}

	if req.Sql == "" {
		return nil, status.Error(codes.InvalidArgument, "Invalid ExecuteSql request.")
	}

	dml, err := (&parser.Parser{
		Lexer: &parser.Lexer{
			File: &token.File{FilePath: "", Buffer: req.Sql},
		},
	}).ParseDML()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Syntax error: %q: %v", req.Sql, err)
	}

	fields := req.GetParams().GetFields()
	paramTypes := req.ParamTypes
	params := make(map[string]Value, len(fields))
	defaultType := &spannerpb.Type{Code: spannerpb.TypeCode_INT64}
	for key, val := range fields {
		typ := defaultType
		if paramTypes != nil {
			if t, ok := paramTypes[key]; ok {
				typ = t
			}
		}

		v, err := makeValueFromSpannerValue(key, val, typ)
		if err != nil {
			return nil, err
		}
		params[key] = v
	}

	count, err := session.database.Execute(ctx, tx, dml, params)
	if err != nil {
		if !tx.Available() {
			return nil, checkAvailability()
		}
		return nil, err
	}

	var metadata *spannerpb.ResultSetMetadata
	if txCreated {
		metadata = &spannerpb.ResultSetMetadata{
			Transaction: tx.Proto(),
		}
	}

	return &spannerpb.ResultSet{
		Metadata: metadata,
		Stats: &spannerpb.ResultSetStats{
			RowCount: &spannerpb.ResultSetStats_RowCountExact{
				RowCountExact: count,
			},
		},
	}, nil
}

func (s *server) ExecuteStreamingSql(req *spannerpb.ExecuteSqlRequest, stream spannerpb.Spanner_ExecuteStreamingSqlServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	session, err := s.getSession(req.Session)
	if err != nil {
		return err
	}

	tx, txCreated, err := session.GetTransactionBySelector(req.GetTransaction())
	if err != nil {
		return err
	}

	checkAvailability := func() error {
		switch tx.Status() {
		case TransactionInvalidated:
			return status.Errorf(codes.FailedPrecondition, "This transaction has been invalidated by a later transaction in the same session.")
		case TransactionCommited, TransactionRollbacked:
			return status.Errorf(codes.FailedPrecondition, "Cannot start a read or query within a transaction after Commit() or Rollback() has been called.")
		case TransactionAborted:
			return status.Errorf(codes.Aborted, "transaction aborted")
		}
		return status.Errorf(codes.Unknown, "unknown status")
	}

	if !tx.Available() {
		return checkAvailability()
	}

	if tx.SingleUse() {
		go func(ctx context.Context) {
			// make sure to call tx.Done() after the request finished
			<-ctx.Done()
			tx.Done(TransactionCommited)
		}(stream.Context())
	}

	if req.Sql == "" {
		return status.Error(codes.InvalidArgument, "Invalid ExecuteStreamingSql request.")
	}

	stmt, err := (&parser.Parser{
		Lexer: &parser.Lexer{
			File: &token.File{FilePath: "", Buffer: req.Sql},
		},
	}).ParseQuery()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Syntax error: %q: %v", req.Sql, err)
	}

	fields := req.GetParams().GetFields()
	paramTypes := req.ParamTypes
	params := make(map[string]Value, len(fields))
	defaultType := &spannerpb.Type{Code: spannerpb.TypeCode_INT64}
	for key, val := range fields {
		typ := defaultType
		if paramTypes != nil {
			if t, ok := paramTypes[key]; ok {
				typ = t
			}
		}

		v, err := makeValueFromSpannerValue(key, val, typ)
		if err != nil {
			return err
		}
		params[key] = v
	}

	iter, err := session.database.Query(ctx, tx, stmt, params)
	if err != nil {
		if !tx.Available() {
			return checkAvailability()
		}
		return err
	}

	if err := sendResult(stream, tx, iter, txCreated); err != nil {
		if !tx.Available() {
			return checkAvailability()
		}
		return err
	}

	return nil
}

func (s *server) ExecuteBatchDml(ctx context.Context, req *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: ExecuteBatchDml")
}

func (s *server) Read(ctx context.Context, req *spannerpb.ReadRequest) (*spannerpb.ResultSet, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: Read")
}

func (s *server) StreamingRead(req *spannerpb.ReadRequest, stream spannerpb.Spanner_StreamingReadServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	session, err := s.getSession(req.Session)
	if err != nil {
		return err
	}

	tx, txCreated, err := session.GetTransactionBySelector(req.GetTransaction())
	if err != nil {
		return err
	}

	checkAvailability := func() error {
		switch tx.Status() {
		case TransactionInvalidated:
			return status.Errorf(codes.FailedPrecondition, "This transaction has been invalidated by a later transaction in the same session.")
		case TransactionCommited, TransactionRollbacked:
			return status.Errorf(codes.FailedPrecondition, "Cannot start a read or query within a transaction after Commit() or Rollback() has been called.")
		case TransactionAborted:
			return status.Errorf(codes.Aborted, "transaction aborted")
		}
		return status.Errorf(codes.Unknown, "unknown status")
	}

	if !tx.Available() {
		return checkAvailability()
	}

	if tx.SingleUse() {
		go func(ctx context.Context) {
			// make sure to call tx.Done() after the request finished
			<-ctx.Done()
			tx.Done(TransactionCommited)
		}(stream.Context())
	}

	iter, err := session.database.Read(ctx, tx, req.Table, req.Index, req.Columns, makeKeySet(req.KeySet), req.Limit)
	if err != nil {
		if !tx.Available() {
			return checkAvailability()
		}
		return err
	}

	if err := sendResult(stream, tx, iter, txCreated); err != nil {
		if !tx.Available() {
			return checkAvailability()
		}
		return err
	}
	return nil
}

func sendResult(stream spannerpb.Spanner_StreamingReadServer, tx *transaction, iter RowIterator, returnTx bool) error {
	// Create metadata about columns
	fields := make([]*spannerpb.StructType_Field, len(iter.ResultSet()))
	for i, item := range iter.ResultSet() {
		fields[i] = &spannerpb.StructType_Field{
			Name: item.Name,
			Type: makeSpannerTypeFromValueType(item.ValueType),
		}
	}

	var txProto *spannerpb.Transaction
	if returnTx {
		txProto = tx.Proto()
	}

	metadata := &spannerpb.ResultSetMetadata{
		RowType:     &spannerpb.StructType{Fields: fields},
		Transaction: txProto,
	}

	err := iter.Do(func(row []interface{}) error {
		values := make([]*structpb.Value, len(row))
		for i, x := range row {
			v, err := spannerValueFromValue(x)
			if err != nil {
				return err
			}
			values[i] = v
		}

		if err := stream.Send(&spannerpb.PartialResultSet{
			Metadata: metadata,
			Values:   values,
		}); err != nil {
			return err
		}

		// From documents:
		// Metadata about the result set, such as row type information.
		// Only present in the first response.
		metadata = nil
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *server) BeginTransaction(ctx context.Context, req *spannerpb.BeginTransactionRequest) (*spannerpb.Transaction, error) {
	session, err := s.getSession(req.Session)
	if err != nil {
		return nil, err
	}

	tx, err := session.BeginTransaction(req.GetOptions())
	if err != nil {
		return nil, err
	}

	return tx.Proto(), nil
}

func (s *server) Commit(ctx context.Context, req *spannerpb.CommitRequest) (*spannerpb.CommitResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	session, err := s.getSession(req.Session)
	if err != nil {
		return nil, err
	}

	tx, err := session.GetTransactionForCommit(req.GetTransaction())
	if err != nil {
		return nil, err
	}

	checkAvailability := func() (*spannerpb.CommitResponse, error) {
		switch tx.Status() {
		case TransactionInvalidated:
			return nil, status.Errorf(codes.FailedPrecondition, "This transaction has been invalidated by a later transaction in the same session.")
		case TransactionCommited:
			return &spannerpb.CommitResponse{}, nil
		case TransactionRollbacked:
			return nil, status.Errorf(codes.FailedPrecondition, "Cannot commit a transaction after Rollback() has been called.")
		case TransactionAborted:
			return nil, status.Errorf(codes.Aborted, "transaction aborted")
		}
		return nil, status.Errorf(codes.Unknown, "unknown status")
	}

	if !tx.Available() {
		return checkAvailability()
	}

	if !tx.ReadWrite() {
		var msg string
		if tx.SingleUse() {
			msg = "Cannot commit a single-use read-only transaction."
		} else {
			msg = "Cannot commit a read-only transaction."
		}
		return nil, status.Errorf(codes.FailedPrecondition, msg)
	}

	err = func() error {
		for _, m := range req.Mutations {
			switch op := m.Operation.(type) {
			case *spannerpb.Mutation_Insert:
				mut := op.Insert
				if err := session.database.Insert(ctx, tx, mut.Table, mut.Columns, mut.Values); err != nil {
					return err
				}

			case *spannerpb.Mutation_Update:
				mut := op.Update
				if err := session.database.Update(ctx, tx, mut.Table, mut.Columns, mut.Values); err != nil {
					return err
				}

			case *spannerpb.Mutation_Replace:
				mut := op.Replace
				if err := session.database.Replace(ctx, tx, mut.Table, mut.Columns, mut.Values); err != nil {
					return err
				}

			case *spannerpb.Mutation_InsertOrUpdate:
				mut := op.InsertOrUpdate
				if err := session.database.InsertOrUpdate(ctx, tx, mut.Table, mut.Columns, mut.Values); err != nil {
					return err
				}

			case *spannerpb.Mutation_Delete_:
				mut := op.Delete
				if err := session.database.Delete(ctx, tx, mut.Table, makeKeySet(mut.KeySet)); err != nil {
					return err
				}

			default:
				return fmt.Errorf("unknown mutation operation: %v", op)
			}
		}

		return nil
	}()
	if err != nil {
		if !tx.Available() {
			return checkAvailability()
		}

		tx.Done(TransactionRollbacked)
		return nil, err
	}

	if err := session.database.Commit(tx); err != nil {
		if !tx.Available() {
			return checkAvailability()
		}

		tx.Done(TransactionRollbacked)
		return nil, err // TODO
	}

	tx.Done(TransactionCommited)

	// TODO: more accurate time
	commitTime := time.Now()
	commitTimeProto, _ := ptypes.TimestampProto(commitTime)

	return &spannerpb.CommitResponse{
		CommitTimestamp: commitTimeProto,
	}, nil
}

func (s *server) Rollback(ctx context.Context, req *spannerpb.RollbackRequest) (*emptypb.Empty, error) {
	session, err := s.getSession(req.Session)
	if err != nil {
		return nil, err
	}
	tx, ok := session.GetTransaction(req.TransactionId)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Transaction was started in a different session")
	}

	if !tx.Available() {
		switch tx.Status() {
		case TransactionInvalidated:
			return nil, status.Errorf(codes.FailedPrecondition, "This transaction has been invalidated by a later transaction in the same session.")
		case TransactionCommited:
			return nil, status.Errorf(codes.FailedPrecondition, "Cannot rollback a transaction after Commit() has been called.")
		case TransactionRollbacked:
			return &emptypb.Empty{}, nil
		case TransactionAborted:
			return nil, status.Errorf(codes.Aborted, "transaction aborted")
		}
	}

	if !tx.ReadWrite() {
		return nil, status.Errorf(codes.FailedPrecondition, "Cannot rollback a read-only transaction.")
	}

	if err := session.database.Rollback(tx); err != nil {
		return nil, err // TODO
	}
	tx.Done(TransactionRollbacked)

	return &emptypb.Empty{}, nil
}

func (s *server) PartitionQuery(ctx context.Context, req *spannerpb.PartitionQueryRequest) (*spannerpb.PartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: PartitionQuery")
}

func (s *server) PartitionRead(ctx context.Context, req *spannerpb.PartitionReadRequest) (*spannerpb.PartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: PartitionRead")
}
