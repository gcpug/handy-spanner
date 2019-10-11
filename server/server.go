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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MakeNowJust/memefish/pkg/ast"
	"github.com/MakeNowJust/memefish/pkg/parser"
	"github.com/MakeNowJust/memefish/pkg/token"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	structpb "github.com/golang/protobuf/ptypes/struct"
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

	adminv1pb.DatabaseAdminServer
}

func (s *server) ApplyDDL(ctx context.Context, databaseName string, stmt ast.DDL) error {
	db, err := s.getOrCreateDatabase(databaseName)
	if err != nil {
		return err
	}

	return db.ApplyDDL(ctx, stmt)
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

func (s *server) getOrCreateDatabase(name string) (*database, error) {
	s.dbMu.RLock()
	db, ok := s.db[name]
	s.dbMu.RUnlock()
	if !ok {
		if !s.autoCreateDatabase {
			return nil, status.Errorf(codes.NotFound, "Database not found: %s", name)
		}

		db = newDatabase()
		s.dbMu.Lock()
		s.db[name] = db
		s.dbMu.Unlock()
	}

	return db, nil
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
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: ExecuteSql")
}

func (s *server) ExecuteStreamingSql(req *spannerpb.ExecuteSqlRequest, stream spannerpb.Spanner_ExecuteStreamingSqlServer) error {
	ctx := stream.Context()

	session, err := s.getSession(req.Session)
	if err != nil {
		return err
	}

	stmt, err := (&parser.Parser{
		Lexer: &parser.Lexer{
			File: &token.File{FilePath: "", Buffer: req.Sql},
		},
	}).ParseQuery()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid sql %q: %v", req.Sql, err)
	}

	fields := req.GetParams().GetFields()
	params := make(map[string]Value, len(fields))
	for key, val := range fields {
		paramType, ok := req.ParamTypes[key]
		if !ok {
			return status.Error(codes.InvalidArgument, "Invalid ExecuteStreamingSql request")
		}

		v, err := makeValueFromSpannerValue(val, paramType)
		if err != nil {
			// TODO: check InvalidArgument or Unimplemented
			return err
		}
		params[key] = v
	}

	iter, err := session.database.Query(ctx, stmt, params)
	if err != nil {
		return err
	}

	return sendResult(stream, iter)
}

func (s *server) ExecuteBatchDml(ctx context.Context, req *spannerpb.ExecuteBatchDmlRequest) (*spannerpb.ExecuteBatchDmlResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: ExecuteBatchDml")
}

func (s *server) Read(ctx context.Context, req *spannerpb.ReadRequest) (*spannerpb.ResultSet, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: Read")
}

func (s *server) StreamingRead(req *spannerpb.ReadRequest, stream spannerpb.Spanner_StreamingReadServer) error {
	ctx := stream.Context()

	session, err := s.getSession(req.Session)
	if err != nil {
		return err
	}

	var tx *transaction
	switch sel := req.GetTransaction().GetSelector().(type) {
	case nil:
		// From documents: If none is provided, the default is a
		// temporary read-only transaction with strong concurrency.
		tx = &transaction{}
	case *spannerpb.TransactionSelector_SingleUse:
		tx = &transaction{}
	case *spannerpb.TransactionSelector_Id:
		txx, ok := session.GetTransaction(sel.Id)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "Transaction was started in a different session")
		}
		tx = txx
	case *spannerpb.TransactionSelector_Begin:
		return status.Errorf(codes.Unimplemented, "transaction selector begin is not supported yet")
	default:
		return fmt.Errorf("unknown transaction selector: %v", sel)
	}
	_ = tx

	iter, err := session.database.Read(ctx, req.Table, req.Index, req.Columns, makeKeySet(req.KeySet), req.Limit)
	if err != nil {
		return err
	}

	return sendResult(stream, iter)
}

func sendResult(stream spannerpb.Spanner_StreamingReadServer, iter RowIterator) error {
	// Create metadata about columns
	fields := make([]*spannerpb.StructType_Field, len(iter.ResultSet()))
	for i, item := range iter.ResultSet() {
		fields[i] = &spannerpb.StructType_Field{
			Name: item.Name,
			Type: makeSpannerTypeFromValueType(item.ValueType),
		}
	}
	metadata := &spannerpb.ResultSetMetadata{
		RowType:     &spannerpb.StructType{Fields: fields},
		Transaction: nil, // TODO
	}

	for {
		row, ok := iter.Next()
		if !ok {
			break
		}

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
	}

	return nil
}

func (s *server) BeginTransaction(ctx context.Context, req *spannerpb.BeginTransactionRequest) (*spannerpb.Transaction, error) {
	session, err := s.getSession(req.Session)
	if err != nil {
		return nil, err
	}

	var txMode transactionMode
	switch v := req.GetOptions().GetMode().(type) {
	case *spannerpb.TransactionOptions_ReadWrite_:
		txMode = txReadWrite
	case *spannerpb.TransactionOptions_ReadOnly_:
		txMode = txReadOnly
	case *spannerpb.TransactionOptions_PartitionedDml_:
		txMode = txPartitionedDML
	case nil:
		// TransactionOptions is required
		return nil, status.Errorf(codes.InvalidArgument, "Invalid BeginTransaction request")
	default:
		return nil, status.Errorf(codes.Unknown, "unknown transaction mode: %v", v)
	}

	tx, err := session.CreateTransaction(txMode)
	if err != nil {
		return nil, err
	}

	return tx.Proto(), nil
}

func (s *server) Commit(ctx context.Context, req *spannerpb.CommitRequest) (*spannerpb.CommitResponse, error) {
	session, err := s.getSession(req.Session)
	if err != nil {
		return nil, err
	}

	var tx *transaction
	switch v := req.GetTransaction().(type) {
	case *spannerpb.CommitRequest_TransactionId:
		txx, ok := session.GetTransaction(v.TransactionId)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Transaction was started in a different session")
		}
		tx = txx
	case *spannerpb.CommitRequest_SingleUseTransaction:
		tx = &transaction{}
	default:
		return nil, status.Errorf(codes.Unknown, "unknown transaction: %v", v)
	}
	_ = tx

	for _, m := range req.Mutations {
		switch op := m.Operation.(type) {
		case *spannerpb.Mutation_Insert:
			mut := op.Insert
			if err := session.database.Insert(ctx, mut.Table, mut.Columns, mut.Values); err != nil {
				return nil, err
			}

		case *spannerpb.Mutation_Update:
			mut := op.Update
			if err := session.database.Update(ctx, mut.Table, mut.Columns, mut.Values); err != nil {
				return nil, err
			}

		case *spannerpb.Mutation_Replace:
			mut := op.Replace
			if err := session.database.Replace(ctx, mut.Table, mut.Columns, mut.Values); err != nil {
				return nil, err
			}

		case *spannerpb.Mutation_InsertOrUpdate:
			mut := op.InsertOrUpdate
			if err := session.database.InsertOrUpdate(ctx, mut.Table, mut.Columns, mut.Values); err != nil {
				return nil, err
			}

		case *spannerpb.Mutation_Delete_:
			mut := op.Delete
			if err := session.database.Delete(ctx, mut.Table, makeKeySet(mut.KeySet)); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unknown mutation operation: %v", op)
		}
	}

	return &spannerpb.CommitResponse{}, nil
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
	tx.done = true

	return &emptypb.Empty{}, nil
}

func (s *server) PartitionQuery(ctx context.Context, req *spannerpb.PartitionQueryRequest) (*spannerpb.PartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: PartitionQuery")
}

func (s *server) PartitionRead(ctx context.Context, req *spannerpb.PartitionReadRequest) (*spannerpb.PartitionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented yet: PartitionRead")
}

func makeSpannerTypeFromValueType(typ ValueType) *spannerpb.Type {
	var code spannerpb.TypeCode
	switch typ.Code {
	case TCBool:
		code = spannerpb.TypeCode_BOOL
	case TCInt64:
		code = spannerpb.TypeCode_INT64
	case TCFloat64:
		code = spannerpb.TypeCode_FLOAT64
	case TCTimestamp:
		code = spannerpb.TypeCode_TIMESTAMP
	case TCDate:
		code = spannerpb.TypeCode_DATE
	case TCString:
		code = spannerpb.TypeCode_STRING
	case TCBytes:
		code = spannerpb.TypeCode_BYTES
	case TCArray:
		code = spannerpb.TypeCode_ARRAY
	case TCStruct:
		code = spannerpb.TypeCode_STRUCT
	}

	st := &spannerpb.Type{Code: code}
	if code == spannerpb.TypeCode_ARRAY {
		st = &spannerpb.Type{
			Code:             code,
			ArrayElementType: makeSpannerTypeFromValueType(*typ.ArrayType),
		}
	}
	if code == spannerpb.TypeCode_STRUCT {
		panic("struct type not supported")
	}
	return st
}

func makeValueTypeFromSpannerType(typ *spannerpb.Type) (ValueType, error) {
	switch typ.Code {
	case spannerpb.TypeCode_BOOL:
		return ValueType{
			Code: TCBool,
		}, nil
	case spannerpb.TypeCode_INT64:
		return ValueType{
			Code: TCInt64,
		}, nil
	case spannerpb.TypeCode_FLOAT64:
		return ValueType{
			Code: TCFloat64,
		}, nil
	case spannerpb.TypeCode_TIMESTAMP:
		return ValueType{
			Code: TCTimestamp,
		}, nil
	case spannerpb.TypeCode_DATE:
		return ValueType{
			Code: TCDate,
		}, nil
	case spannerpb.TypeCode_STRING:
		return ValueType{
			Code: TCString,
		}, nil
	case spannerpb.TypeCode_BYTES:
		return ValueType{
			Code: TCBytes,
		}, nil
	case spannerpb.TypeCode_ARRAY:
		var array *ValueType
		if typ.ArrayElementType != nil {
			vt, err := makeValueTypeFromSpannerType(typ.ArrayElementType)
			if err != nil {
				return ValueType{}, err
			}
			array = &vt
		}
		return ValueType{
			Code:      TCArray,
			ArrayType: array,
		}, nil
	case spannerpb.TypeCode_STRUCT:
		//code = TCStruct
		return ValueType{}, fmt.Errorf("Struct for spanner.Type is not supported")
	}

	return ValueType{}, fmt.Errorf("unknown code for spanner.Type: %v", typ.Code)
}

func spannerValueFromValue(x interface{}) (*structpb.Value, error) {
	switch x := x.(type) {
	case bool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{x}}, nil
	case int64:
		// The Spanner int64 is actually a decimal string.
		s := strconv.FormatInt(x, 10)
		return &structpb.Value{Kind: &structpb.Value_StringValue{s}}, nil
	case float64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{x}}, nil
	case string:
		return &structpb.Value{Kind: &structpb.Value_StringValue{x}}, nil
	case []byte:
		return &structpb.Value{Kind: &structpb.Value_StringValue{string(x)}}, nil
	case nil:
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}, nil
	case []interface{}:
		var vs []*structpb.Value
		for _, elem := range x {
			v, err := spannerValueFromValue(elem)
			if err != nil {
				return nil, err
			}
			vs = append(vs, v)
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{
			&structpb.ListValue{Values: vs},
		}}, nil
	default:
		return nil, fmt.Errorf("unknown database value type %T", x)
	}
}

func makeDataFromSpannerValue(v *structpb.Value, typ ValueType) (interface{}, error) {
	if typ.StructType != nil {
		return nil, fmt.Errorf("Struct type is not supported yet")
	}

	if typ.Code == TCArray {
		if typ.ArrayType == nil {
			return nil, fmt.Errorf("TODO: ArrayType should not be nil in Array")
		}

		if _, ok := v.Kind.(*structpb.Value_NullValue); ok {
			switch typ.ArrayType.Code {
			case TCBool:
				return []bool(nil), nil
			case TCInt64:
				return []int64(nil), nil
			case TCFloat64:
				return []float64(nil), nil
			case TCTimestamp, TCDate, TCString:
				return []string(nil), nil
			case TCBytes:
				return [][]byte(nil), nil
			case TCArray, TCStruct:
				return nil, fmt.Errorf("nested Array or Struct for Array is not supported yet")
			default:
				return nil, fmt.Errorf("unexpected type %d for Null value as Array", typ.ArrayType.Code)
			}
		}

		vv, ok := v.Kind.(*structpb.Value_ListValue)
		if !ok {
			return nil, fmt.Errorf("unexpected value %T and type %d as Array", v.Kind, typ.Code)
		}

		n := len(vv.ListValue.Values)
		switch typ.ArrayType.Code {
		case TCBool:
			ret := make([]bool, n)
			for i, vv := range vv.ListValue.Values {
				vvv, err := makeDataFromSpannerValue(vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				vvvv, ok := vvv.(bool)
				if !ok {
					panic(fmt.Sprintf("unexpected value type: %T", vvv))
				}
				ret[i] = vvvv
			}
			return ret, nil
		case TCInt64:
			ret := make([]int64, n)
			for i, vv := range vv.ListValue.Values {
				vvv, err := makeDataFromSpannerValue(vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				vvvv, ok := vvv.(int64)
				if !ok {
					panic(fmt.Sprintf("unexpected value type: %T", vvv))
				}
				ret[i] = vvvv
			}
			return ret, nil
		case TCFloat64:
			ret := make([]float64, n)
			for i, vv := range vv.ListValue.Values {
				vvv, err := makeDataFromSpannerValue(vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				vvvv, ok := vvv.(float64)
				if !ok {
					panic(fmt.Sprintf("unexpected value type: %T", vvv))
				}
				ret[i] = vvvv
			}
			return ret, nil
		case TCTimestamp, TCDate, TCString:
			ret := make([]string, n)
			for i, vv := range vv.ListValue.Values {
				vvv, err := makeDataFromSpannerValue(vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				vvvv, ok := vvv.(string)
				if !ok {
					panic(fmt.Sprintf("unexpected value type: %T", vvv))
				}
				ret[i] = vvvv
			}
			return ret, nil
		case TCBytes:
			ret := make([][]byte, n)
			for i, vv := range vv.ListValue.Values {
				vvv, err := makeDataFromSpannerValue(vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				vvvv, ok := vvv.([]byte)
				if !ok {
					panic(fmt.Sprintf("unexpected value type: %T", vvv))
				}
				ret[i] = vvvv
			}
			return ret, nil
		case TCArray, TCStruct:
			return nil, fmt.Errorf("nested Array or Struct for Array is not supported yet")
		default:
			return nil, fmt.Errorf("unknown TypeCode for ArrayElement %v", typ.Code)
		}
	}

	if _, ok := v.Kind.(*structpb.Value_NullValue); ok {
		return nil, nil
	}

	switch typ.Code {
	case TCBool:
		switch vv := v.Kind.(type) {
		case *structpb.Value_BoolValue:
			return vv.BoolValue, nil
		}
	case TCInt64:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			// base is always 10
			n, err := strconv.ParseInt(vv.StringValue, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("unexpected format %q as int64: %v", vv.StringValue, err)
			}
			return n, nil
		}

	case TCFloat64:
		switch vv := v.Kind.(type) {
		case *structpb.Value_NumberValue:
			return vv.NumberValue, nil
		}

	case TCTimestamp:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			s := vv.StringValue
			if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
				return nil, fmt.Errorf("unexpected format %q as timestamp: %v", s, err)
			}
			return s, nil
		}

	case TCDate:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			s := vv.StringValue
			if _, err := time.Parse("2006-01-02", s); err != nil {
				return nil, fmt.Errorf("unexpected format for %q as date: %v", s, err)
			}
			return s, nil
		}

	case TCString:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			return vv.StringValue, nil
		}
	case TCBytes:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			return []byte(vv.StringValue), nil
		}
	case TCArray:
		return nil, fmt.Errorf("Array type is not supported")
	case TCStruct:
		return nil, fmt.Errorf("Struct type is not supported")
	default:
		return nil, fmt.Errorf("unknown TypeCode %v", typ.Code)
	}

	return nil, fmt.Errorf("unexpected value %T and type %d", v.Kind, typ.Code)
}

func makeValueFromSpannerValue(v *structpb.Value, typ *spannerpb.Type) (Value, error) {
	vt, err := makeValueTypeFromSpannerType(typ)
	if err != nil {
		return Value{}, err
	}

	data, err := makeDataFromSpannerValue(v, vt)
	if err != nil {
		return Value{}, err
	}

	return Value{
		Data: data,
		Type: vt,
	}, nil
}
