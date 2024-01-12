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

	iamv1pb "cloud.google.com/go/iam/apiv1/iampb"
	lropb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	adminv1pb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	spannerpb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/cloudspannerecosystem/memefish/token"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	ddl, err := (&memefish.Parser{
		Lexer: &memefish.Lexer{
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
		stmt, err := (&memefish.Parser{
			Lexer: &memefish.Lexer{
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
	resp, _ := anypb.New(&adminv1pb.Database{
		Name:  databaseName,
		State: adminv1pb.Database_READY,
	})
	op := &lropb.Operation{
		Name:     fmt.Sprintf("%s/operations/_auto_%d", databaseName, time.Now().UnixNano()/1000),
		Metadata: resp,
		Done:     true,
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
		stmt, err := (&memefish.Parser{
			Lexer: &memefish.Lexer{
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

func (s *server) UpdateDatabase(context.Context, *adminv1pb.UpdateDatabaseRequest) (*lropb.Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: UpdateDatabase")
}

// DropDatabase implements adminv1pb.DatabaseAdminServer.
func (s *server) DropDatabase(ctx context.Context, req *adminv1pb.DropDatabaseRequest) (*emptypb.Empty, error) {
	if err := s.dropDatabase(req.GetDatabase()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
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

	any, _ := anypb.New(&emptypb.Empty{})
	op := &lropb.Operation{
		Name:     "TODO:xxx",
		Metadata: any,
		Done:     true,
		Result: &lropb.Operation_Response{
			Response: any,
		},
	}
	return op, nil
}

func (s *server) DeleteOperation(ctx context.Context, req *lropb.DeleteOperationRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *server) CancelOperation(ctx context.Context, req *lropb.CancelOperationRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *server) WaitOperation(ctx context.Context, req *lropb.WaitOperationRequest) (*lropb.Operation, error) {
	name := "TODO:xxx"
	if req.Name != name {
		return nil, status.Errorf(codes.NotFound, "Operation not found: %s", name)
	}
	any, _ := anypb.New(&emptypb.Empty{})
	op := &lropb.Operation{
		Name:     "TODO:xxx",
		Metadata: any,
		Done:     true,
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
		return nil, newSpannerSessionNotFoundError(name)
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

	return nil, status.Errorf(codes.Unknown, "create session failed")
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
			return nil, newSpannerDatabaseNotFoundError(name)
		}

		var err error
		db, err = s.createDatabase(name)
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.AlreadyExists {
				return s.getOrCreateDatabase(name)
			}

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
		return nil, newSpannerDatabaseNotFoundError(name)
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
		return status.Errorf(codes.Unknown, "Failed to close the database: %s", err)
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

	rollbackCreatedTx := func() {
		if txCreated {
			tx.Done(TransactionAborted)
		}
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

	result, err := s.executeDML(ctx, session, tx, &spannerpb.ExecuteBatchDmlRequest_Statement{
		Sql:        req.GetSql(),
		Params:     req.GetParams(),
		ParamTypes: req.GetParamTypes(),
	})
	if err != nil {
		rollbackCreatedTx()
		if !tx.Available() {
			return nil, checkAvailability()
		}
		return nil, err
	}

	if txCreated {
		result.Metadata = &spannerpb.ResultSetMetadata{
			Transaction: tx.Proto(),
		}
	}

	return result, nil
}

func (s *server) ExecuteStreamingSql(req *spannerpb.ExecuteSqlRequest, stream spannerpb.Spanner_ExecuteStreamingSqlServer) error {
	receivedAt := time.Now().UTC()
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

	rollbackCreatedTx := func() {
		if txCreated {
			tx.Done(TransactionAborted)
		}
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

	stmt, err := (&memefish.Parser{
		Lexer: &memefish.Lexer{
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
		rollbackCreatedTx()
		if !tx.Available() {
			return checkAvailability()
		}
		return err
	}

	stats := queryStats{
		Mode:       req.QueryMode,
		ReceivedAt: receivedAt,
		QueryText:  req.Sql,
	}
	if err := sendResult(stream, tx, iter, txCreated, stats); err != nil {
		if !tx.Available() {
			return checkAvailability()
		}
		return err
	}

	return nil
}

func (s *server) ExecuteBatchDml(ctx context.Context, req *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
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

	if len(req.Statements) == 0 {
		msg := "No statements in batch DML request."
		return nil, status.Errorf(codes.InvalidArgument, msg)
	}

	var resultSets []*spannerpb.ResultSet
	var resultStatus rpcstatus.Status
	for i, stmt := range req.Statements {
		result, err := s.executeDML(ctx, session, tx, stmt)
		if err != nil {
			// TODO: confirm to require rollback transaction in partial success
			if !tx.Available() {
				return nil, checkAvailability()
			}
			st := status.Convert(err)
			resultStatus.Code = int32(st.Code())
			resultStatus.Message = fmt.Sprintf("Statement %d: %s", i, st.Message())
			break
		}

		resultSets = append(resultSets, result)
	}

	if txCreated {
		if len(resultSets) > 0 {
			resultSets[0].Metadata = &spannerpb.ResultSetMetadata{
				Transaction: tx.Proto(),
			}
		}
	}

	return &spannerpb.ExecuteBatchDmlResponse{
		ResultSets: resultSets,
		Status:     &resultStatus,
	}, nil
}

func (s *server) executeDML(ctx context.Context, session *session, tx *transaction, stmt *spannerpb.ExecuteBatchDmlRequest_Statement) (*spannerpb.ResultSet, error) {
	dml, err := (&memefish.Parser{
		Lexer: &memefish.Lexer{
			File: &token.File{FilePath: "", Buffer: stmt.Sql},
		},
	}).ParseDML()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%q is not valid DML: %v", stmt.Sql, err)
	}

	fields := stmt.GetParams().GetFields()
	paramTypes := stmt.ParamTypes
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
		return nil, err
	}

	return &spannerpb.ResultSet{
		Stats: &spannerpb.ResultSetStats{
			RowCount: &spannerpb.ResultSetStats_RowCountExact{
				RowCountExact: count,
			},
		},
	}, nil
}

func (s *server) Read(ctx context.Context, req *spannerpb.ReadRequest) (*spannerpb.ResultSet, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: Read")
}

func (s *server) StreamingRead(req *spannerpb.ReadRequest, stream spannerpb.Spanner_StreamingReadServer) error {
	receivedAt := time.Now().UTC()
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

	rollbackCreatedTx := func() {
		if txCreated {
			tx.Done(TransactionAborted)
		}
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
		rollbackCreatedTx()
		if !tx.Available() {
			return checkAvailability()
		}
		return err
	}

	stats := queryStats{
		Mode:       spannerpb.ExecuteSqlRequest_NORMAL,
		ReceivedAt: receivedAt,
		QueryText:  "",
	}
	if err := sendResult(stream, tx, iter, txCreated, stats); err != nil {
		if !tx.Available() {
			return checkAvailability()
		}
		return err
	}
	return nil
}

func sendResult(stream spannerpb.Spanner_StreamingReadServer, tx *transaction, iter RowIterator, returnTx bool, qs queryStats) error {
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

	var rowCount int64
	values := make([]*structpb.Value, 0, 100)
	err := iter.Do(func(row []interface{}) error {
		rowCount++
		for _, x := range row {
			v, err := spannerValueFromValue(x)
			if err != nil {
				return err
			}
			values = append(values, v)
		}

		if len(values) > 100 {
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

			// // Stats is only present once in the last response.
			// // But set the first response for now.
			// stats = nil

			values = values[:0]
		}
		return nil
	})
	if err != nil {
		return err
	}

	qs.RowCount = rowCount
	stats := &spannerpb.ResultSetStats{
		QueryStats: createQueryStats(qs),
	}

	if err := stream.Send(&spannerpb.PartialResultSet{
		Metadata: metadata,
		Values:   values,
		Stats:    stats,
	}); err != nil {
		return err
	}

	return nil
}

type queryStats struct {
	Mode       spannerpb.ExecuteSqlRequest_QueryMode
	ReceivedAt time.Time
	QueryText  string
	RowCount   int64
}

func toMillisecondString(d time.Duration) string {
	return fmt.Sprintf("%d.%d msecs", d/time.Millisecond, (d%time.Millisecond)/time.Microsecond)
}

func createQueryStats(stats queryStats) *structpb.Struct {
	elapsedTime := time.Since(stats.ReceivedAt)
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"cpu_time": {
				Kind: &structpb.Value_StringValue{
					StringValue: toMillisecondString(elapsedTime),
				},
			},
			"remote_server_calls": {
				Kind: &structpb.Value_StringValue{
					StringValue: "0/0",
				},
			},
			"bytes_returned": {
				Kind: &structpb.Value_StringValue{
					StringValue: "8",
				},
			},
			"query_text": {
				Kind: &structpb.Value_StringValue{
					StringValue: stats.QueryText,
				},
			},
			"query_plan_creation_time": {
				Kind: &structpb.Value_StringValue{
					StringValue: "0 msecs",
				},
			},
			"runtime_creation_time": {
				Kind: &structpb.Value_StringValue{
					StringValue: "0 msecs",
				},
			},
			"deleted_rows_scanned": {
				Kind: &structpb.Value_StringValue{
					StringValue: "0",
				},
			},
			"optimizer_version": {
				Kind: &structpb.Value_StringValue{
					StringValue: "2",
				},
			},
			"elapsed_time": {
				Kind: &structpb.Value_StringValue{
					StringValue: toMillisecondString(elapsedTime),
				},
			},
			"rows_returned": {
				Kind: &structpb.Value_StringValue{
					StringValue: fmt.Sprintf("%d", stats.RowCount),
				},
			},
			"filesystem_delay_seconds": {
				Kind: &structpb.Value_StringValue{
					StringValue: "0 msecs",
				},
			},
			"rows_scanned": {
				Kind: &structpb.Value_StringValue{
					StringValue: "0",
				},
			},
		},
	}
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
	commitTimeProto := timestamppb.New(commitTime)

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

func (s *server) CreateBackup(ctx context.Context, req *adminv1pb.CreateBackupRequest) (*lropb.Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: CreateBackup")
}

func (s *server) GetBackup(ctx context.Context, req *adminv1pb.GetBackupRequest) (*adminv1pb.Backup, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: GetBackup")
}

func (s *server) UpdateBackup(ctx context.Context, req *adminv1pb.UpdateBackupRequest) (*adminv1pb.Backup, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: UpdateBackup")
}

func (s *server) DeleteBackup(ctx context.Context, req *adminv1pb.DeleteBackupRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: DeleteBackup")
}

func (s *server) ListBackups(ctx context.Context, req *adminv1pb.ListBackupsRequest) (*adminv1pb.ListBackupsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: ListBackups")
}

func (s *server) RestoreDatabase(ctx context.Context, req *adminv1pb.RestoreDatabaseRequest) (*lropb.Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: RestoreDatabase")
}

func (s *server) ListDatabaseOperations(ctx context.Context, req *adminv1pb.ListDatabaseOperationsRequest) (*adminv1pb.ListDatabaseOperationsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: ListDatabaseOperations")
}

func (s *server) ListBackupOperations(ctx context.Context, req *adminv1pb.ListBackupOperationsRequest) (*adminv1pb.ListBackupOperationsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: ListBackupOperations")
}

func (s *server) CopyBackup(ctx context.Context, req *adminv1pb.CopyBackupRequest) (*lropb.Operation, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: CopyBackup")
}

func (s *server) ListDatabaseRoles(ctx context.Context, req *adminv1pb.ListDatabaseRolesRequest) (*adminv1pb.ListDatabaseRolesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: ListDatabaseRoles")
}

func (*server) BatchWrite(*spannerpb.BatchWriteRequest, spannerpb.Spanner_BatchWriteServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented yet: BatchWrite")
}
