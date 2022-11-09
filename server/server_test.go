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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	cmp "github.com/google/go-cmp/cmp"
	uuidpkg "github.com/google/uuid"
	lropb "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	adminv1pb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	simpleFields = []*spannerpb.StructType_Field{
		{
			Name: "Id",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
		},
		{
			Name: "Value",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_STRING,
			},
		},
	}
	fullTypesFields = []*spannerpb.StructType_Field{
		{
			Name: "PKey",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_STRING,
			},
		},
		{
			Name: "FTString",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_STRING,
			},
		},
		{
			Name: "FTStringNull",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_STRING,
			},
		},
		{
			Name: "FTBool",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_BOOL,
			},
		},
		{
			Name: "FTBoolNull",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_BOOL,
			},
		},
		{
			Name: "FTBytes",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_BYTES,
			},
		},
		{
			Name: "FTBytesNull",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_BYTES,
			},
		},
		{
			Name: "FTTimestamp",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_TIMESTAMP,
			},
		},
		{
			Name: "FTTimestampNull",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_TIMESTAMP,
			},
		},
		{
			Name: "FTInt",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
		},
		{
			Name: "FTIntNull",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
		},
		{
			Name: "FTFloat",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_FLOAT64,
			},
		},
		{
			Name: "FTFloatNull",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_FLOAT64,
			},
		},
		{
			Name: "FTDate",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_DATE,
			},
		},
		{
			Name: "FTDateNull",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_DATE,
			},
		},
	}
	arrayTypesFields = []*spannerpb.StructType_Field{
		{
			Name: "Id",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
		},
		{
			Name: "ArrayString",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_STRING,
				},
			},
		},
		{
			Name: "ArrayBool",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_BOOL,
				},
			},
		},
		{
			Name: "ArrayBytes",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_BYTES,
				},
			},
		},
		{
			Name: "ArrayTimestamp",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_TIMESTAMP,
				},
			},
		},
		{
			Name: "ArrayInt",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
		},
		{
			Name: "ArrayFloat",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_FLOAT64,
				},
			},
		},
		{
			Name: "ArrayDate",
			Type: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_DATE,
				},
			},
		},
	}
)

func newTestServer() *server {
	return NewFakeServer().(*server)
}

func assertGRPCError(t *testing.T, err error, code codes.Code, msg string) {
	t.Helper()
	st := status.Convert(err)
	if st.Code() != code {
		t.Errorf("expect code to be %v but got %v", code, st.Code())
	}
	if st.Message() != msg {
		t.Errorf("unexpected error message: \n %q\n expected:\n %q", st.Message(), msg)
	}
}

func assertStatusCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	st := status.Convert(err)
	if st.Code() != code {
		t.Errorf("expect code to be %v but got %v: msg=%v", code, st.Code(), st.Message())
	}
}

func assertResourceInfo(t *testing.T, err error, expected *errdetails.ResourceInfo) {
	t.Helper()
	st := status.Convert(err)
	details := st.Details()
	if len(details) != 1 {
		t.Fatalf("error should have a detail: %v", len(details))
	}
	ri, ok := details[0].(*errdetails.ResourceInfo)
	if !ok {
		t.Fatalf("error detail should be ResourceInfo: %T", details[0])
	}
	if diff := cmp.Diff(expected, ri, protocmp.Transform()); diff != "" {
		t.Errorf("(-got, +want)\n%s", diff)
	}
}

func testCreateSession(t *testing.T, s *server) (*spannerpb.Session, string) {
	name := fmt.Sprintf("projects/fake/instances/fakse/databases/%s", uuidpkg.New().String())
	session, err := s.CreateSession(context.Background(), &spannerpb.CreateSessionRequest{
		Database: name,
	})
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	return session, name
}

func testInvalidatedTransaction(t *testing.T, s *server, session *spannerpb.Session) *spannerpb.Transaction {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
		Session: session.Name,
		Options: &spannerpb.TransactionOptions{
			Mode: &spannerpb.TransactionOptions_ReadWrite_{
				ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tx.Id) == 0 {
		t.Error("transaction id must not be empty")
	}

	for i := 0; i < 32; i++ {
		tx2, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(tx2.Id) == 0 {
			t.Error("transaction id must not be empty")
		}
	}

	return tx
}

func TestCreateSession(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	validDatabaseName := "projects/fake/instances/fake/databases/fake"

	t.Run("Success", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			session, err := s.CreateSession(ctx, &spannerpb.CreateSessionRequest{
				Database: validDatabaseName,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if session.Name == "" {
				t.Error("session name must not be empty")
			}
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		names := []string{
			"projects/fake/instances/fake/databases/",
			"projects/fake/instances//databases/fake",
			"projects//instances/fake/databases/fake",
			"xx/fake/instances/fake/databases/fake",
			"projects/fake/xx/fake/databases/fake",
			"projects/fake/instances/fake/xx/fake",
			"xxx",
		}
		for _, name := range names {
			_, err := s.CreateSession(ctx, &spannerpb.CreateSessionRequest{
				Database: name,
			})
			st := status.Convert(err)
			if want, got := codes.InvalidArgument, st.Code(); want != got {
				t.Errorf("expect %v but got %v for %v", want, got, name)
			}
		}
	})
}

func TestBatchCreateSessions(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	validDatabaseName := "projects/fake/instances/fake/databases/fake"

	t.Run("Success", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			sessions, err := s.BatchCreateSessions(ctx, &spannerpb.BatchCreateSessionsRequest{
				Database:     validDatabaseName,
				SessionCount: 3,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(sessions.Session) != 3 {
				t.Error("the number of sessions should be 3")
			}
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		names := []string{
			"projects/fake/instances/fake/databases/",
			"projects/fake/instances//databases/fake",
			"projects//instances/fake/databases/fake",
			"xx/fake/instances/fake/databases/fake",
			"projects/fake/xx/fake/databases/fake",
			"projects/fake/instances/fake/xx/fake",
			"xxx",
		}
		for _, name := range names {
			_, err := s.BatchCreateSessions(ctx, &spannerpb.BatchCreateSessionsRequest{
				Database:     name,
				SessionCount: 1,
			})
			st := status.Convert(err)
			if want, got := codes.InvalidArgument, st.Code(); want != got {
				t.Errorf("expect %v but got %v for %v", want, got, name)
			}
		}
	})
}

func TestGetSession(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()
	session, _ := testCreateSession(t, s)

	_, err := s.GetSession(ctx, &spannerpb.GetSessionRequest{
		Name: session.Name,
	})
	if err != nil {
		t.Fatalf("GetSession must success: %v", err)

		_, err = s.GetSession(ctx, &spannerpb.GetSessionRequest{
			Name: "xx",
		})
		st := status.Convert(err)
		if want, got := codes.InvalidArgument, st.Code(); want != got {
			t.Errorf("expect %v but got %v", want, got)
		}
	}

	_, err = s.GetSession(ctx, &spannerpb.GetSessionRequest{
		Name: session.Name + "x",
	})
	st := status.Convert(err)
	if want, got := codes.NotFound, st.Code(); want != got {
		t.Errorf("expect %v but got %v", want, got)
	}
	expectedRI := &errdetails.ResourceInfo{
		ResourceType: "type.googleapis.com/google.spanner.v1.Session",
		ResourceName: session.Name + "x",
		Description:  "Session does not exist.",
	}
	assertResourceInfo(t, err, expectedRI)
}

func TestListSessions(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	dbName1 := fmt.Sprintf("projects/fake/instances/fakse/databases/%s", uuidpkg.New().String())
	dbName2 := fmt.Sprintf("projects/fake/instances/fakse/databases/%s", uuidpkg.New().String())

	var sessions1 []*spannerpb.Session
	var sessions2 []*spannerpb.Session

	for i := 0; i < 3; i++ {
		session, err := s.CreateSession(context.Background(), &spannerpb.CreateSessionRequest{
			Database: dbName1,
		})
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		sessions1 = append(sessions1, session)
	}
	for i := 0; i < 2; i++ {
		session, err := s.CreateSession(context.Background(), &spannerpb.CreateSessionRequest{
			Database: dbName2,
		})
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		sessions2 = append(sessions2, session)
	}

	res1, err := s.ListSessions(ctx, &spannerpb.ListSessionsRequest{
		Database: dbName1,
	})
	if err != nil {
		t.Fatalf("ListSession must succeed: %v", err)
	}
	for _, s := range res1.Sessions {
		var found bool
		for _, s2 := range sessions1 {
			if s.Name == s2.Name {
				found = true
				continue
			}
		}
		if !found {
			t.Errorf("session %s not found", s.Name)
		}
	}

	res2, err := s.ListSessions(ctx, &spannerpb.ListSessionsRequest{
		Database: dbName2,
	})
	if err != nil {
		t.Fatalf("ListSession must succeed: %v", err)
	}
	for _, s := range res2.Sessions {
		var found bool
		for _, s2 := range sessions2 {
			if s.Name == s2.Name {
				found = true
				continue
			}
		}
		if !found {
			t.Errorf("session %s not found", s.Name)
		}
	}
}

func TestDeleteSession(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()
	session, _ := testCreateSession(t, s)

	_, err := s.DeleteSession(ctx, &spannerpb.DeleteSessionRequest{
		Name: session.Name,
	})
	if err != nil {
		t.Fatalf("DeleteSession must success: %v", err)
	}

	_, err = s.DeleteSession(ctx, &spannerpb.DeleteSessionRequest{
		Name: session.Name,
	})
	st := status.Convert(err)
	if want, got := codes.NotFound, st.Code(); want != got {
		t.Errorf("expect %v but got %v", want, got)
	}
	expectedRI := &errdetails.ResourceInfo{
		ResourceType: "type.googleapis.com/google.spanner.v1.Session",
		ResourceName: session.Name,
		Description:  "Session does not exist.",
	}
	assertResourceInfo(t, err, expectedRI)
}

func TestBeginTransaction(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	t.Run("Success", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(tx.Id) == 0 {
			t.Error("transaction id must not be empty")
		}

		tx2, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadOnly_{
					ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(tx2.Id) == 0 {
			t.Error("transaction id must not be empty")
		}

		tx3, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_PartitionedDml_{
					PartitionedDml: &spannerpb.TransactionOptions_PartitionedDml{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(tx3.Id) == 0 {
			t.Error("transaction id must not be empty")
		}
	})

	t.Run("NoOption", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		_, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
		})
		if err == nil {
			t.Fatalf("unexpected success")
		}
		st := status.Convert(err)
		if st.Code() != codes.InvalidArgument {
			t.Fatalf("err codes must be InvalidArgument but got %v", st.Code())
		}
	})
}

func NewFakeExecuteStreamingSqlServer(ctx context.Context) *fakeExecuteStreamingSqlServer {
	return &fakeExecuteStreamingSqlServer{ctx: ctx}
}

type fakeExecuteStreamingSqlServer struct {
	ctx    context.Context
	cancel func()

	sets []*spannerpb.PartialResultSet

	grpc.ServerStream
}

func (s *fakeExecuteStreamingSqlServer) Send(set *spannerpb.PartialResultSet) error {
	s.sets = append(s.sets, set)
	return nil
}

func (s *fakeExecuteStreamingSqlServer) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func TestExecuteStreamingSql_Success(t *testing.T) {
	ctx := context.Background()
	s := newTestServer()
	session, dbName := testCreateSession(t, s)

	// TODO: prepare initial data
	db, ok := s.db[dbName]
	if !ok {
		t.Fatalf("database not found")
	}
	for _, s := range allSchema {
		ddls := parseDDL(t, s)
		for _, ddl := range ddls {
			db.ApplyDDL(ctx, ddl)
		}
	}
	for _, query := range []string{
		`INSERT INTO Simple VALUES(100, "xxx")`,
		`INSERT INTO Simple VALUES(200, "yyy")`,
		`INSERT INTO Simple VALUES(300, "zzz")`,

		`INSERT INTO CompositePrimaryKeys VALUES(1, "aaa", 1, 0, "x1", "y1", "z")`,
		`INSERT INTO CompositePrimaryKeys VALUES(2, "bbb", 2, 0, "x1", "y2", "z")`,
		`INSERT INTO CompositePrimaryKeys VALUES(3, "bbb", 3, 0, "x1", "y3", "z")`,
		`INSERT INTO CompositePrimaryKeys VALUES(4, "ccc", 3, 0, "x2", "y4", "z")`,
		`INSERT INTO CompositePrimaryKeys VALUES(5, "ccc", 4, 0, "x2", "y5", "z")`,

		`INSERT INTO FullTypes VALUES("xxx",
            "xxx", "xxx",
            true, true,
            "eHl6", "eHl6",
            "2012-03-04T12:34:56.123456789Z", "2012-03-04T12:34:56.123456789Z",
            100, 100,
            0.5, 0.5,
            "2012-03-04", "2012-03-04"
        )`,

		`INSERT INTO ArrayTypes VALUES(100,
           json_array("xxx1", "xxx2"),
           json_array(true, false),
           json_array("eHl6", "eHl6"),
           json_array("2012-03-04T12:34:56.123456789Z", "2012-03-04T12:34:56.999999999Z"),
           json_array(1, 2),
           json_array(0.1, 0.2),
           json_array("2012-03-04", "2012-03-05")
        )`,
	} {
		if _, err := db.db.ExecContext(ctx, query); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	table := map[string]struct {
		sql      string
		types    map[string]*spannerpb.Type
		params   *structpb.Struct
		fields   []*spannerpb.StructType_Field
		expected [][]*structpb.Value
	}{
		"Simple": {
			sql:    `SELECT * FROM Simple`,
			fields: simpleFields,
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("yyy"),
					makeStringValue("300"), makeStringValue("zzz"),
				},
			},
		},

		"Simple_QueryParam": {
			sql: `SELECT * FROM Simple WHERE Id IN (@foo, @bar)`,
			types: map[string]*spannerpb.Type{
				"foo": &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
				"bar": &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": makeStringValue("100"),
					"bar": makeStringValue("200"),
				},
			},
			fields: simpleFields,
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("yyy"),
				},
			},
		},

		"Simple_QueryParam_ImplicitTypeAsInt64": {
			sql: `SELECT @foo`,
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": makeStringValue("100"),
				},
			},
			fields: []*spannerpb.StructType_Field{
				{
					Name: "",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"),
				},
			},
		},

		"Simple_UnusedTypes": {
			sql: `SELECT 100 v`,
			types: map[string]*spannerpb.Type{
				"foo": &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
			fields: []*spannerpb.StructType_Field{
				{
					Name: "v",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"),
				},
			},
		},

		"Simple_UnusedParams": {
			sql: `SELECT 100 v`,
			types: map[string]*spannerpb.Type{
				"foo": &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": makeStringValue("200"),
				},
			},
			fields: []*spannerpb.StructType_Field{
				{
					Name: "v",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"),
				},
			},
		},

		"FromUnnest_ArrayLiteral": {
			sql: `SELECT x, y FROM UNNEST (["xxx", "yyy"]) AS x WITH OFFSET y`,
			fields: []*spannerpb.StructType_Field{
				{
					Name: "x",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_STRING,
					},
				},
				{
					Name: "y",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			expected: [][]*structpb.Value{
				{
					makeStringValue("xxx"), makeStringValue("0"),
					makeStringValue("yyy"), makeStringValue("1"),
				},
			},
		},
		"FromUnnest_Params": {
			sql: `SELECT x, y FROM UNNEST (@foo) AS x WITH OFFSET y`,
			types: map[string]*spannerpb.Type{
				"foo": &spannerpb.Type{
					Code: spannerpb.TypeCode_ARRAY,
					ArrayElementType: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": makeListValueAsValue(makeListValue(makeStringValue("100"), makeStringValue("200"))),
				},
			},
			fields: []*spannerpb.StructType_Field{
				{
					Name: "x",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
				{
					Name: "y",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("0"),
					makeStringValue("200"), makeStringValue("1"),
				},
			},
		},

		"Simple_Unnest_Array": {
			sql:    `SELECT * FROM Simple WHERE Id IN UNNEST([100, 200])`,
			fields: simpleFields,
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("yyy"),
				},
			},
		},

		"Simple_Unnest_Params": {
			sql: `SELECT * FROM Simple WHERE Id IN UNNEST(@ids)`,
			types: map[string]*spannerpb.Type{
				"ids": &spannerpb.Type{
					Code: spannerpb.TypeCode_ARRAY,
					ArrayElementType: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"ids": makeListValueAsValue(makeListValue(makeStringValue("100"), makeStringValue("200"))),
				},
			},
			fields: simpleFields,
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("yyy"),
				},
			},
		},

		"Simple_Unnest_Params_EmptyArray": {
			sql: `SELECT * FROM Simple WHERE Id IN UNNEST(@ids)`,
			types: map[string]*spannerpb.Type{
				"ids": &spannerpb.Type{
					Code: spannerpb.TypeCode_ARRAY,
					ArrayElementType: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"ids": makeNullValue(),
				},
			},
			fields: nil,
			expected: [][]*structpb.Value{
				{},
			},
		},

		"CompositePrimaryKeys_Condition": {
			sql: `SELECT Id, PKey1, PKey2 FROM CompositePrimaryKeys WHERE PKey1 = "bbb" AND (PKey2 = 3 OR PKey2 = 4)`,
			fields: []*spannerpb.StructType_Field{
				{
					Name: "Id",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
				{
					Name: "PKey1",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_STRING,
					},
				},
				{
					Name: "PKey2",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
			},
			expected: [][]*structpb.Value{
				{makeStringValue("3"), makeStringValue("bbb"), makeStringValue("3")},
			},
		},
		"CompositePrimaryKeys_Condition2": {
			sql: fmt.Sprintf(`SELECT %s FROM CompositePrimaryKeys`,
				strings.Join(strings.Split(strings.Repeat("XYZ", 10), ""), ", ")),
			expected: [][]*structpb.Value{
				{
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y2"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x1"), makeStringValue("y3"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y4"), makeStringValue("z"),
				},
				{
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
					makeStringValue("x2"), makeStringValue("y5"), makeStringValue("z"),
				},
			},
		},
		"ArrayOfStruct": {
			sql: `SELECT ARRAY(SELECT STRUCT<Id int64, Value string>(1,"xx") x)`,
			fields: []*spannerpb.StructType_Field{
				{
					Name: "",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_ARRAY,
						ArrayElementType: &spannerpb.Type{
							Code: spannerpb.TypeCode_STRUCT,
							StructType: &spannerpb.StructType{
								Fields: []*spannerpb.StructType_Field{
									{
										Name: "Id",
										Type: &spannerpb.Type{
											Code: spannerpb.TypeCode_INT64,
										},
									},
									{
										Name: "Value",
										Type: &spannerpb.Type{
											Code: spannerpb.TypeCode_STRING,
										},
									},
								},
							},
						},
					},
				},
			},
			expected: [][]*structpb.Value{
				{
					makeListValueAsValue(makeListValue(
						makeStructValue(map[string]*structpb.Value{
							"Id":    makeStringValue("1"),
							"Value": makeStringValue("xx"),
						}),
					)),
				},
			},
		},
		"ArrayOfStruct_FullTypes": {
			sql: `SELECT ARRAY(SELECT AS STRUCT * FROM FullTypes)`,
			fields: []*spannerpb.StructType_Field{
				{
					Name: "",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_ARRAY,
						ArrayElementType: &spannerpb.Type{
							Code: spannerpb.TypeCode_STRUCT,
							StructType: &spannerpb.StructType{
								Fields: fullTypesFields,
							},
						},
					},
				},
			},
			// fields: simpleFields,
			expected: [][]*structpb.Value{
				{
					makeListValueAsValue(makeListValue(
						makeStructValue(map[string]*structpb.Value{
							"PKey":            makeStringValue("xxx"),
							"FTString":        makeStringValue("xxx"),
							"FTStringNull":    makeStringValue("xxx"),
							"FTBool":          makeBoolValue(true),
							"FTBoolNull":      makeBoolValue(true),
							"FTBytes":         makeStringValue("eHl6"),
							"FTBytesNull":     makeStringValue("eHl6"),
							"FTTimestamp":     makeStringValue("2012-03-04T12:34:56.123456789Z"),
							"FTTimestampNull": makeStringValue("2012-03-04T12:34:56.123456789Z"),
							"FTInt":           makeStringValue("100"),
							"FTIntNull":       makeStringValue("100"),
							"FTFloat":         makeNumberValue(0.5),
							"FTFloatNull":     makeNumberValue(0.5),
							"FTDate":          makeStringValue("2012-03-04"),
							"FTDateNull":      makeStringValue("2012-03-04"),
						}),
					)),
				},
			},
		},
		"ArrayOfStruct_ArrayTypes": {
			sql: `SELECT ARRAY(SELECT AS STRUCT * FROM ArrayTypes)`,
			fields: []*spannerpb.StructType_Field{
				{
					Name: "",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_ARRAY,
						ArrayElementType: &spannerpb.Type{
							Code: spannerpb.TypeCode_STRUCT,
							StructType: &spannerpb.StructType{
								Fields: arrayTypesFields,
							},
						},
					},
				},
			},
			// fields: simpleFields,
			expected: [][]*structpb.Value{
				{
					makeListValueAsValue(makeListValue(
						makeStructValue(map[string]*structpb.Value{
							"Id": makeStringValue("100"),
							"ArrayString": makeListValueAsValue(makeListValue(
								makeStringValue("xxx1"), makeStringValue("xxx2"),
							)),
							"ArrayBool": makeListValueAsValue(makeListValue(
								makeBoolValue(true), makeBoolValue(false),
							)),
							"ArrayBytes": makeListValueAsValue(makeListValue(
								makeStringValue("eHl6"), makeStringValue("eHl6"),
							)),
							"ArrayTimestamp": makeListValueAsValue(makeListValue(
								makeStringValue("2012-03-04T12:34:56.123456789Z"),
								makeStringValue("2012-03-04T12:34:56.999999999Z"),
							)),
							"ArrayInt": makeListValueAsValue(makeListValue(
								makeStringValue("1"), makeStringValue("2"),
							)),
							"ArrayFloat": makeListValueAsValue(makeListValue(
								makeNumberValue(0.1), makeNumberValue(0.2),
							)),
							"ArrayDate": makeListValueAsValue(makeListValue(
								makeStringValue("2012-03-04"), makeStringValue("2012-03-05"),
							)),
						}),
					)),
				},
			},
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			fake := &fakeExecuteStreamingSqlServer{}
			if err := s.ExecuteStreamingSql(&spannerpb.ExecuteSqlRequest{
				Session:     session.Name,
				Transaction: &spannerpb.TransactionSelector{},
				Sql:         tc.sql,
				ParamTypes:  tc.types,
				Params:      tc.params,
			}, fake); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var results [][]*structpb.Value
			for _, set := range fake.sets {
				results = append(results, set.Values)
			}

			if tc.fields != nil {
				if len(results) == 0 {
					t.Fatalf("unexpected number of results")
				}
				if diff := cmp.Diff(tc.fields, fake.sets[0].Metadata.RowType.Fields, protocmp.Transform()); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			}

			if diff := cmp.Diff(tc.expected, results, protocmp.Transform()); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestStreamingRead_ValueType(t *testing.T) {
	table := map[string]struct {
		table    string
		wcols    []string
		values   []*structpb.Value
		rcols    []string
		fields   []*spannerpb.StructType_Field
		expected []*structpb.Value
	}{
		"Simple": {
			table: "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("300"),
				makeStringValue("zzz"),
			},
			rcols: []string{"Id", "Value"},
			fields: []*spannerpb.StructType_Field{
				{
					Name: "Id",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_INT64,
					},
				},
				{
					Name: "Value",
					Type: &spannerpb.Type{
						Code: spannerpb.TypeCode_STRING,
					},
				},
			},
			expected: []*structpb.Value{
				makeStringValue("300"),
				makeStringValue("zzz"),
			},
		},
		"FullTypes": {
			table: "FullTypes",
			wcols: fullTypesKeys,
			values: []*structpb.Value{
				makeStringValue("xxx"),  // PKey STRING(32) NOT NULL,
				makeStringValue("xxx"),  // FTString STRING(32) NOT NULL,
				makeNullValue(),         // FTStringNull STRING(32),
				makeBoolValue(true),     // FTBool BOOL NOT NULL,
				makeNullValue(),         // FTBoolNull BOOL,
				makeStringValue("eHh4"), // FTBytes BYTES(32) NOT NULL,
				makeNullValue(),         // FTBytesNull BYTES(32),
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
				makeNullValue(),               // FTTimestampNull TIMESTAMP,
				makeStringValue("100"),        // FTInt INT64 NOT NULL,
				makeNullValue(),               // FTIntNull INT64,
				makeNumberValue(0.5),          // FTFloat FLOAT64 NOT NULL,
				makeNullValue(),               // FTFloatNull FLOAT64,
				makeStringValue("2012-03-04"), // FTDate DATE NOT NULL,
				makeNullValue(),               // FTDateNull DATE,
			},
			rcols:  fullTypesKeys,
			fields: fullTypesFields,
			expected: []*structpb.Value{
				makeStringValue("xxx"),  // PKey STRING(32) NOT NULL,
				makeStringValue("xxx"),  // FTString STRING(32) NOT NULL,
				makeNullValue(),         // FTStringNull STRING(32),
				makeBoolValue(true),     // FTBool BOOL NOT NULL,
				makeNullValue(),         // FTBoolNull BOOL,
				makeStringValue("eHh4"), // FTBytes BYTES(32) NOT NULL,
				makeNullValue(),         // FTBytesNull BYTES(32),
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
				makeNullValue(),               // FTTimestampNull TIMESTAMP,
				makeStringValue("100"),        // FTInt INT64 NOT NULL,
				makeNullValue(),               // FTIntNull INT64,
				makeNumberValue(0.5),          // FTFloat FLOAT64 NOT NULL,
				makeNullValue(),               // FTFloatNull FLOAT64,
				makeStringValue("2012-03-04"), // FTDate DATE NOT NULL,
				makeNullValue(),               // FTDateNull DATE,
			},
		},

		// Spanner returns array but it possiblly includes NullValue in the elements
		"ArrayTypes_NonNull": {
			table: "ArrayTypes",
			wcols: arrayTypesKeys,
			values: []*structpb.Value{
				makeStringValue("100"),
				makeListValueAsValue(makeListValue(
					makeStringValue("xxx"),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeBoolValue(true),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("eHh4"),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("2012-03-04T12:34:56.123456789Z"),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("100"),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeNumberValue(0.5),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("2012-03-04"),
					makeNullValue(),
				)),
			},
			rcols:  arrayTypesKeys,
			fields: arrayTypesFields,
			expected: []*structpb.Value{
				makeStringValue("100"),
				makeListValueAsValue(makeListValue(
					makeStringValue("xxx"),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeBoolValue(true),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("eHh4"),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("2012-03-04T12:34:56.123456789Z"),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("100"),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeNumberValue(0.5),
					makeNullValue(),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("2012-03-04"),
					makeNullValue(),
				)),
			},
		},

		// Spanner returns NullValue if the value is null
		"ArrayTypes_Null": {
			table: "ArrayTypes",
			wcols: arrayTypesKeys,
			values: []*structpb.Value{
				makeStringValue("101"),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
			},
			rcols:  arrayTypesKeys,
			fields: arrayTypesFields,
			expected: []*structpb.Value{
				makeStringValue("101"),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
				makeNullValue(),
			},
		},

		// Spanner returns empty list as array if the value is empty not null
		"ArrayTypes_Empty": {
			table: "ArrayTypes",
			wcols: arrayTypesKeys,
			values: []*structpb.Value{
				makeStringValue("100"),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
			},
			rcols:  arrayTypesKeys,
			fields: arrayTypesFields,
			expected: []*structpb.Value{
				makeStringValue("100"),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
				makeListValueAsValue(&structpb.ListValue{Values: []*structpb.Value{}}),
			},
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := newTestServer()
			session, dbName := testCreateSession(t, s)

			db, ok := s.db[dbName]
			if !ok {
				t.Fatalf("database not found")
			}
			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			_, err := s.Commit(ctx, &spannerpb.CommitRequest{
				Session: session.Name,
				Transaction: &spannerpb.CommitRequest_SingleUseTransaction{
					SingleUseTransaction: &spannerpb.TransactionOptions{
						Mode: &spannerpb.TransactionOptions_ReadWrite_{
							ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
						},
					},
				},
				Mutations: []*spannerpb.Mutation{
					{
						Operation: &spannerpb.Mutation_Insert{
							Insert: &spannerpb.Mutation_Write{
								Table:   tc.table,
								Columns: tc.wcols,
								Values: []*structpb.ListValue{
									{
										Values: tc.values,
									},
								},
							},
						}},
				},
			})
			if err != nil {
				t.Fatalf("commit failed: %v", err)
			}

			fake := &fakeExecuteStreamingSqlServer{}
			if err := s.StreamingRead(&spannerpb.ReadRequest{
				Session:     session.Name,
				Transaction: &spannerpb.TransactionSelector{},
				Table:       tc.table,
				Columns:     tc.rcols,
				KeySet:      &spannerpb.KeySet{All: true},
			}, fake); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var results [][]*structpb.Value
			for _, set := range fake.sets {
				results = append(results, set.Values)
			}

			if len(results) != 1 {
				t.Errorf("results should be 1 record but got %v", len(results))
			}

			if diff := cmp.Diff(tc.fields, fake.sets[0].Metadata.RowType.Fields, protocmp.Transform()); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}

			if diff := cmp.Diff(tc.expected, results[0], protocmp.Transform()); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestExecuteStreamingSql_Error(t *testing.T) {
	ctx := context.Background()
	s := newTestServer()
	session, dbName := testCreateSession(t, s)

	// TODO: prepare initial data
	db, ok := s.db[dbName]
	if !ok {
		t.Fatalf("database not found")
	}
	for _, s := range allSchema {
		ddls := parseDDL(t, s)
		for _, ddl := range ddls {
			db.ApplyDDL(ctx, ddl)
		}
	}

	table := map[string]struct {
		sql    string
		types  map[string]*spannerpb.Type
		params *structpb.Struct
		code   codes.Code
		msg    *regexp.Regexp
	}{
		"NoParameter": {
			sql:    `SELECT @foo`,
			types:  nil,
			params: nil,
			code:   codes.InvalidArgument,
			msg:    regexp.MustCompile(`No parameter found for binding: foo`),
		},
		"NoType": {
			sql:   `SELECT @foo`,
			types: nil,
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": &structpb.Value{
						Kind: &structpb.Value_StringValue{StringValue: "x"},
					},
				},
			},
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Invalid value for bind parameter foo: Expected INT64.`),
		},

		"InvalidValueForBinding": {
			sql: `SELECT @foo`,
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"foo": &structpb.Value{
						Kind: &structpb.Value_StringValue{StringValue: "x"},
					},
				},
			},
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Invalid value for bind parameter foo: Expected INT64.`),
		},

		"EmptySQL": {
			sql:  ``,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Invalid ExecuteStreamingSql request.`),
		},
		"SQLSyntxError": {
			sql:  `SELECT`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Syntax error: .+`),
		},

		"BindStruct": {
			sql: `SELECT @bar`,
			types: map[string]*spannerpb.Type{
				"bar": &spannerpb.Type{
					Code: spannerpb.TypeCode_STRUCT,
					StructType: &spannerpb.StructType{
						Fields: []*spannerpb.StructType_Field{
							{
								Name: "xxx",
								Type: &spannerpb.Type{
									Code: spannerpb.TypeCode_STRING,
								},
							},
							{
								Name: "yyy",
								Type: &spannerpb.Type{
									Code: spannerpb.TypeCode_INT64,
								},
							},
						},
					},
				},
			},
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"bar": &structpb.Value{
						Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
							Fields: map[string]*structpb.Value{
								"xxx": &structpb.Value{
									Kind: &structpb.Value_StringValue{StringValue: "xxx"},
								},
								"yyy": &structpb.Value{
									Kind: &structpb.Value_StringValue{StringValue: "100"},
								},
							},
						}},
					},
				},
			},
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Invalid value for bind parameter bar: Expected STRUCT<xxx STRING, yyy INT64>`),
		},

		"BindArrayStruct": {
			sql: `SELECT @bar`,
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"bar": &structpb.Value{
						Kind: &structpb.Value_ListValue{
							ListValue: &structpb.ListValue{
								Values: []*structpb.Value{
									{
										Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
											Fields: map[string]*structpb.Value{
												"xxx": &structpb.Value{
													Kind: &structpb.Value_StringValue{StringValue: "xxx"},
												},
												"yyy": &structpb.Value{
													Kind: &structpb.Value_StringValue{StringValue: "100"},
												},
											},
										}},
									},
								},
							},
						},
					},
				},
			},
			types: map[string]*spannerpb.Type{
				"bar": &spannerpb.Type{
					Code: spannerpb.TypeCode_ARRAY,
					ArrayElementType: &spannerpb.Type{
						Code: spannerpb.TypeCode_STRUCT,
						StructType: &spannerpb.StructType{
							Fields: []*spannerpb.StructType_Field{
								{
									Name: "xxx",
									Type: &spannerpb.Type{
										Code: spannerpb.TypeCode_STRING,
									},
								},
								{
									Name: "yyy",
									Type: &spannerpb.Type{
										Code: spannerpb.TypeCode_INT64,
									},
								},
							},
						},
					},
				},
			},
			code: codes.InvalidArgument,
			// TODO: return correct error message
			// msg:  regexp.MustCompile(`Invalid value for array element bar[0]: Expected STRUCT<xxx STRING, yyy INT64>.`),
			msg: regexp.MustCompile(`^Invalid value for bind parameter bar\[0\]: Expected STRUCT<xxx STRING, yyy INT64>.`),
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			fake := &fakeExecuteStreamingSqlServer{}
			err := s.ExecuteStreamingSql(&spannerpb.ExecuteSqlRequest{
				Session:     session.Name,
				Transaction: &spannerpb.TransactionSelector{},
				Sql:         tc.sql,
				ParamTypes:  tc.types,
				Params:      tc.params,
			}, fake)
			st := status.Convert(err)
			if st.Code() != tc.code {
				t.Errorf("expect code to be %v but got %v", tc.code, st.Code())
			}
			if !tc.msg.MatchString(st.Message()) {
				t.Errorf("unexpected error message: \n %q\n expected:\n %q", st.Message(), tc.msg)
			}
		})
	}
}

func TestExecuteSql_Success(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	preareDBAndSession := func(t *testing.T) *spannerpb.Session {
		session, dbName := testCreateSession(t, s)
		db, ok := s.db[dbName]
		if !ok {
			t.Fatalf("database not found")
		}
		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		for _, query := range []string{
			`INSERT INTO Simple VALUES(100, "xxx")`,
			`INSERT INTO Simple VALUES(200, "yyy")`,
			`INSERT INTO Simple VALUES(300, "zzz")`,
		} {
			if _, err := db.db.ExecContext(ctx, query); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		return session
	}

	begin := func(session *spannerpb.Session) *spannerpb.Transaction {
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return tx
	}
	commit := func(session *spannerpb.Session, tx *spannerpb.Transaction) error {
		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		return err
	}

	table := map[string]struct {
		sql    string
		types  map[string]*spannerpb.Type
		params *structpb.Struct
		fields []*spannerpb.StructType_Field

		table    string
		columns  []string
		expected [][]*structpb.Value
	}{
		"Update": {
			sql:     `UPDATE Simple SET Value = "xyz" WHERE Id = 200`,
			table:   "Simple",
			columns: []string{"Id", "Value"},
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("xyz"),
					makeStringValue("300"), makeStringValue("zzz"),
				},
			},
		},
		"Update_ParamInWhere": {
			sql: `UPDATE Simple SET Value = "xyz" WHERE Id = @id`,
			types: map[string]*spannerpb.Type{
				"id": &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
			params: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"id": &structpb.Value{
						Kind: &structpb.Value_StringValue{StringValue: "200"},
					},
				},
			},
			table:   "Simple",
			columns: []string{"Id", "Value"},
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("xyz"),
					makeStringValue("300"), makeStringValue("zzz"),
				},
			},
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			session := preareDBAndSession(t)
			tx := begin(session)

			if _, err := s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
				Session: session.Name,
				Transaction: &spannerpb.TransactionSelector{
					Selector: &spannerpb.TransactionSelector_Id{
						Id: tx.Id,
					},
				},
				Sql:        tc.sql,
				ParamTypes: tc.types,
				Params:     tc.params,
			}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if err := commit(session, tx); err != nil {
				t.Fatalf("commit error: %v", err)
			}

			fake := NewFakeExecuteStreamingSqlServer(ctx)
			if err := s.StreamingRead(&spannerpb.ReadRequest{
				Session: session.Name,
				Transaction: &spannerpb.TransactionSelector{
					Selector: &spannerpb.TransactionSelector_SingleUse{
						SingleUse: &spannerpb.TransactionOptions{
							Mode: &spannerpb.TransactionOptions_ReadOnly_{
								ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
							},
						},
					},
				},
				Table:   tc.table,
				Columns: tc.columns,
				KeySet:  &spannerpb.KeySet{All: true},
			}, fake); err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			var results [][]*structpb.Value
			for _, set := range fake.sets {
				results = append(results, set.Values)
			}
			if diff := cmp.Diff(tc.expected, results, protocmp.Transform()); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestExecuteSql_Error(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()
	begin := func(ctx context.Context, t *testing.T, session *spannerpb.Session) *spannerpb.Transaction {
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return tx
	}

	t.Run("RollbackAfterRollback", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx := begin(ctx, t, session)

		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Sql: `UPDATE Simple SET Value = "x" WHERE Id = 1000`,
		})
		expected := "Cannot start a read or query within a transaction after Commit() or Rollback() has been called."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("CommitAfterRollback", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx := begin(ctx, t, session)

		_, err := s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Sql: `UPDATE Simple SET Value = "x" WHERE Id = 1000`,
		})
		expected := "Cannot start a read or query within a transaction after Commit() or Rollback() has been called."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("ReadOnlyTransaction", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadOnly_{
					ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Sql: `UPDATE Simple SET Value = "x" WHERE Id = 1000`,
		})
		expected := "DML statements can only be performed in a read-write transaction."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("SingleUseTransaction", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		_, err := s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_SingleUse{
					SingleUse: &spannerpb.TransactionOptions{
						Mode: &spannerpb.TransactionOptions_ReadWrite_{
							ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
						},
					},
				},
			},
			Sql: `UPDATE Simple SET Value = "x" WHERE Id = 1000`,
		})
		expected := "DML statements may not be performed in single-use transactions, to avoid replay."
		assertGRPCError(t, err, codes.InvalidArgument, expected)
	})
}

func TestExecuteSql_Transaction(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	preareDBAndSession := func(t *testing.T) *spannerpb.Session {
		session, dbName := testCreateSession(t, s)
		db, ok := s.db[dbName]
		if !ok {
			t.Fatalf("database not found")
		}
		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		for _, query := range []string{
			`INSERT INTO Simple VALUES(100, "xxx")`,
		} {
			if _, err := db.db.ExecContext(ctx, query); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		return session
	}

	begin := func(session *spannerpb.Session) *spannerpb.Transaction {
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return tx
	}
	commit := func(session *spannerpb.Session, tx *spannerpb.Transaction) error {
		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		return err
	}
	readSimple := func(ctx context.Context, session *spannerpb.Session, tx *spannerpb.Transaction) ([]*structpb.Value, error) {
		fake := NewFakeExecuteStreamingSqlServer(ctx)
		err := s.StreamingRead(&spannerpb.ReadRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Table:   "Simple",
			Columns: []string{"Id", "Value"},
			KeySet:  &spannerpb.KeySet{All: true},
		}, fake)
		if err != nil {
			return nil, err
		}

		var results []*structpb.Value
		for _, set := range fake.sets {
			results = append(results, set.Values...)
		}

		return results, nil
	}

	initialValueSet := []*structpb.Value{
		makeStringValue("100"),
		makeStringValue("xxx"),
	}

	t.Run("ReadBeforeCommit_SameTransaction", func(t *testing.T) {
		session := preareDBAndSession(t)
		tx := begin(session)
		_, err := s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Sql:        `UPDATE Simple SET Value = "zzz" WHERE Id = 100`,
			Params:     nil,
			ParamTypes: nil,
		})
		assertStatusCode(t, err, codes.OK)

		expected := []*structpb.Value{
			makeStringValue("100"),
			makeStringValue("zzz"),
		}

		results, err := readSimple(ctx, session, tx)
		assertStatusCode(t, err, codes.OK)
		if diff := cmp.Diff(results, expected, protocmp.Transform()); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

		if err := commit(session, tx); err != nil {
			t.Fatalf("commit failed: %v", err)
		}
	})

	t.Run("ReadBeforeCommit_AnotherTransaction", func(t *testing.T) {
		session := preareDBAndSession(t)
		tx1 := begin(session)
		tx2 := begin(session)
		defer commit(session, tx1)
		defer commit(session, tx2)

		_, err := s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx1.Id,
				},
			},
			Sql:        `UPDATE Simple SET Value = "zzz" WHERE Id = 100`,
			Params:     nil,
			ParamTypes: nil,
		})
		assertStatusCode(t, err, codes.OK)

		expected := []*structpb.Value{
			makeStringValue("100"),
			makeStringValue("zzz"),
		}

		// read in another transaction timeouts
		ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		_, err = readSimple(ctx2, session, tx2)
		assertStatusCode(t, err, codes.DeadlineExceeded)

		if err := commit(session, tx1); err != nil {
			t.Fatalf("commit failed: %v", err)
		}

		// read in another transaction succeeds after commit
		ctx3, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		results, err := readSimple(ctx3, session, tx2)
		assertStatusCode(t, err, codes.OK)
		if diff := cmp.Diff(results, expected, protocmp.Transform()); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}
	})

	t.Run("ReadBeforeWrite_AnotherTransaction", func(t *testing.T) {
		session := preareDBAndSession(t)
		tx1 := begin(session)
		tx2 := begin(session)
		defer commit(session, tx1)
		defer commit(session, tx2)

		expected := []*structpb.Value{
			makeStringValue("100"),
			makeStringValue("zzz"),
		}

		// read in transaction2
		results, err := readSimple(ctx, session, tx2)
		assertStatusCode(t, err, codes.OK)
		if diff := cmp.Diff(results, initialValueSet, protocmp.Transform()); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

		// write in transaction1 (aborting tx1)
		_, err = s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx1.Id,
				},
			},
			Sql:        `UPDATE Simple SET Value = "zzz" WHERE Id = 100`,
			Params:     nil,
			ParamTypes: nil,
		})
		assertStatusCode(t, err, codes.OK)

		if err := commit(session, tx1); err != nil {
			t.Fatalf("commit failed: %v", err)
		}

		// read in transaction fails because of abort
		_, err = readSimple(ctx, session, tx2)
		assertStatusCode(t, err, codes.Aborted)

		// read in another transaction succeeds after commit
		tx3 := begin(session)
		defer commit(session, tx3)
		results, err = readSimple(ctx, session, tx3)
		assertStatusCode(t, err, codes.OK)
		if diff := cmp.Diff(results, expected, protocmp.Transform()); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}
	})

	t.Run("WriteBeforeWrite_AnotherTransaction", func(t *testing.T) {
		session := preareDBAndSession(t)
		tx1 := begin(session)
		tx2 := begin(session)
		defer commit(session, tx1)
		defer commit(session, tx2)

		expected1 := []*structpb.Value{
			makeStringValue("100"),
			makeStringValue("xxx"),
			makeStringValue("101"),
			makeStringValue("yyy"),
		}

		// write in transaction2
		_, err := s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx2.Id,
				},
			},
			Sql: `INSERT INTO Simple (Id, Value) VALUES(101, "yyy")`,
		})
		assertStatusCode(t, err, codes.OK)

		// write in transaction1 timeouts because tx2 holds write lock
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		_, err = s.ExecuteSql(ctx, &spannerpb.ExecuteSqlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx1.Id,
				},
			},
			Sql:        `UPDATE Simple SET Value = "zzz" WHERE Id = 100`,
			Params:     nil,
			ParamTypes: nil,
		})
		assertStatusCode(t, err, codes.DeadlineExceeded)

		// commit in transaction2
		if err := commit(session, tx2); err != nil {
			t.Fatalf("commit failed: %v", err)
		}

		// read in another transaction succeeds after commit
		tx3 := begin(session)
		defer commit(session, tx3)
		results, err := readSimple(ctx, session, tx3)
		assertStatusCode(t, err, codes.OK)
		if diff := cmp.Diff(results, expected1, protocmp.Transform()); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}
	})
}

func TestExecuteBatchDml_Success(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	preareDBAndSession := func(t *testing.T) *spannerpb.Session {
		session, dbName := testCreateSession(t, s)
		db, ok := s.db[dbName]
		if !ok {
			t.Fatalf("database not found")
		}
		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		for _, query := range []string{
			`INSERT INTO Simple VALUES(100, "xxx")`,
			`INSERT INTO Simple VALUES(200, "yyy")`,
			`INSERT INTO Simple VALUES(300, "zzz")`,
		} {
			if _, err := db.db.ExecContext(ctx, query); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		return session
	}
	begin := func(session *spannerpb.Session) *spannerpb.Transaction {
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return tx
	}
	commit := func(session *spannerpb.Session, tx *spannerpb.Transaction) error {
		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		return err
	}
	read := func(session *spannerpb.Session, tx *spannerpb.Transaction) ([][]*structpb.Value, error) {
		fake := NewFakeExecuteStreamingSqlServer(ctx)
		if err := s.StreamingRead(&spannerpb.ReadRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_SingleUse{
					SingleUse: &spannerpb.TransactionOptions{
						Mode: &spannerpb.TransactionOptions_ReadOnly_{
							ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
						},
					},
				},
			},
			Table:   "Simple",
			Columns: []string{"Id", "Value"},
			KeySet:  &spannerpb.KeySet{All: true},
		}, fake); err != nil {
			return nil, err
		}

		var results [][]*structpb.Value
		for _, set := range fake.sets {
			results = append(results, set.Values)
		}
		return results, nil
	}

	table := map[string]struct {
		stmts []*spannerpb.ExecuteBatchDmlRequest_Statement

		expectedSetCount      int
		expectedStatusCode    int32
		expectedStatusMessage *regexp.Regexp
		expected              [][]*structpb.Value
	}{
		"UpdateSingle": {
			stmts: []*spannerpb.ExecuteBatchDmlRequest_Statement{
				{
					Sql: `UPDATE Simple SET Value = "xyz" WHERE Id = 200`,
				},
			},
			expectedSetCount:      1,
			expectedStatusCode:    0,
			expectedStatusMessage: regexp.MustCompile(`^$`),
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("xyz"),
					makeStringValue("300"), makeStringValue("zzz"),
				},
			},
		},
		"UpdateMulti": {
			stmts: []*spannerpb.ExecuteBatchDmlRequest_Statement{
				{
					Sql: `UPDATE Simple SET Value = "xyz" WHERE Id = 200`,
				},
				{
					Sql: `UPDATE Simple SET Value = "z" WHERE Id = 300`,
				},
			},
			expectedSetCount:      2,
			expectedStatusCode:    0,
			expectedStatusMessage: regexp.MustCompile(`^$`),
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("xyz"),
					makeStringValue("300"), makeStringValue("z"),
				},
			},
		},
		"PartialSuccess": {
			stmts: []*spannerpb.ExecuteBatchDmlRequest_Statement{
				{
					Sql: `UPDATE Simple SET Value = "xyz" WHERE Id = 200`,
				},
				{
					Sql: `UPD`,
				},
			},
			expectedSetCount:      1,
			expectedStatusCode:    int32(codes.InvalidArgument),
			expectedStatusMessage: regexp.MustCompile(`^Statement 1: .* is not valid DML`),
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("xyz"),
					makeStringValue("300"), makeStringValue("zzz"),
				},
			},
		},
		"PartialSuccessStopped": {
			stmts: []*spannerpb.ExecuteBatchDmlRequest_Statement{
				{
					Sql: `UPD`,
				},
				{
					Sql: `UPDATE Simple SET Value = "xyz" WHERE Id = 200`,
				},
			},
			expectedSetCount:      0,
			expectedStatusCode:    int32(codes.InvalidArgument),
			expectedStatusMessage: regexp.MustCompile(`^Statement 0: .* is not valid DML`),
			expected: [][]*structpb.Value{
				{
					makeStringValue("100"), makeStringValue("xxx"),
					makeStringValue("200"), makeStringValue("yyy"),
					makeStringValue("300"), makeStringValue("zzz"),
				},
			},
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			session := preareDBAndSession(t)
			tx := begin(session)

			result, err := s.ExecuteBatchDml(ctx, &spannerpb.ExecuteBatchDmlRequest{
				Session: session.Name,
				Transaction: &spannerpb.TransactionSelector{
					Selector: &spannerpb.TransactionSelector_Id{
						Id: tx.Id,
					},
				},
				Statements: tc.stmts,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if count := len(result.ResultSets); count != tc.expectedSetCount {
				t.Errorf("expect the number of ResultSets to be %v, but got %v", tc.expectedSetCount, count)
			}

			if result.Status.GetCode() != tc.expectedStatusCode {
				t.Errorf("expect status code to be %v, but got %v", tc.expectedStatusCode, result.Status.GetCode())
			}
			if !tc.expectedStatusMessage.MatchString(result.Status.GetMessage()) {
				t.Errorf("unexpected status message: \n %q\n expected:\n %q", result.Status.GetMessage(), tc.expectedStatusMessage)
			}

			if err := commit(session, tx); err != nil {
				t.Fatalf("commit error: %v", err)
			}

			results, err := read(session, tx)
			if err != nil {
				t.Fatalf("read error: %v", err)
			}

			if diff := cmp.Diff(tc.expected, results, protocmp.Transform()); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestExecuteBatchDml_Error(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()
	begin := func(ctx context.Context, t *testing.T, session *spannerpb.Session) *spannerpb.Transaction {
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return tx
	}

	t.Run("NoStatements", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx := begin(ctx, t, session)
		_, err := s.ExecuteBatchDml(ctx, &spannerpb.ExecuteBatchDmlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{},
		})
		expected := "No statements in batch DML request."
		assertGRPCError(t, err, codes.InvalidArgument, expected)
	})

	t.Run("RollbackAfterRollback", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx := begin(ctx, t, session)

		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.ExecuteBatchDml(ctx, &spannerpb.ExecuteBatchDmlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
				{Sql: `UPDATE Simple SET Value = "x" WHERE Id = 1000`},
			},
		})
		expected := "Cannot start a read or query within a transaction after Commit() or Rollback() has been called."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("CommitAfterRollback", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx := begin(ctx, t, session)

		_, err := s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.ExecuteBatchDml(ctx, &spannerpb.ExecuteBatchDmlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
				{Sql: `UPDATE Simple SET Value = "x" WHERE Id = 1000`},
			},
		})
		expected := "Cannot start a read or query within a transaction after Commit() or Rollback() has been called."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("ReadOnlyTransaction", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadOnly_{
					ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.ExecuteBatchDml(ctx, &spannerpb.ExecuteBatchDmlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_Id{
					Id: tx.Id,
				},
			},
			Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
				{Sql: `UPDATE Simple SET Value = "x" WHERE Id = 1000`},
			},
		})
		expected := "DML statements can only be performed in a read-write transaction."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("SingleUseTransaction", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		_, err := s.ExecuteBatchDml(ctx, &spannerpb.ExecuteBatchDmlRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_SingleUse{
					SingleUse: &spannerpb.TransactionOptions{
						Mode: &spannerpb.TransactionOptions_ReadWrite_{
							ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
						},
					},
				},
			},
			Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
				{Sql: `UPDATE Simple SET Value = "x" WHERE Id = 1000`},
			},
		})
		expected := "DML statements may not be performed in single-use transactions, to avoid replay."
		assertGRPCError(t, err, codes.InvalidArgument, expected)
	})
}

func TestCommit(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	t.Run("Success_MultiUseTransaction", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("Success_SingleUse", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_SingleUseTransaction{
				SingleUseTransaction: &spannerpb.TransactionOptions{
					Mode: &spannerpb.TransactionOptions_ReadWrite_{
						ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
					},
				},
			},
			Mutations: []*spannerpb.Mutation{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("CommitAfterCommit", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("CommitAfterRollback", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		expected := "Cannot commit a transaction after Rollback() has been called."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("ReadOnlyTransaction", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadOnly_{
					ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		expected := "Cannot commit a read-only transaction."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("SingleReadOnlyTransaction", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_SingleUseTransaction{
				SingleUseTransaction: &spannerpb.TransactionOptions{
					Mode: &spannerpb.TransactionOptions_ReadOnly_{
						ReadOnly: &spannerpb.TransactionOptions_ReadOnly{},
					},
				},
			},
			Mutations: []*spannerpb.Mutation{},
		})
		expected := "Cannot commit a single-use read-only transaction."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("TransactionInDifferentSession", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		session2, _ := testCreateSession(t, s)

		_, err = s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session2.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		assertGRPCError(t, err, codes.InvalidArgument, "Transaction was started in a different session")
	})

	t.Run("Invalidated_Commit", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx := testInvalidatedTransaction(t, s, session)

		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		expected := "This transaction has been invalidated by a later transaction in the same session."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})
}

func TestCommitMutations(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	session, dbName := testCreateSession(t, s)
	for _, schema := range allSchema {
		ddls := parseDDL(t, schema)
		for _, ddl := range ddls {
			if err := s.ApplyDDL(ctx, dbName, ddl); err != nil {
				t.Fatalf("ApplyDDL failed: %v", err)
			}
		}
	}

	singleTx := &spannerpb.CommitRequest_SingleUseTransaction{
		SingleUseTransaction: &spannerpb.TransactionOptions{
			Mode: &spannerpb.TransactionOptions_ReadWrite_{
				ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
			},
		},
	}

	_, err := s.Commit(ctx, &spannerpb.CommitRequest{
		Session:     session.Name,
		Transaction: singleTx,
		Mutations: []*spannerpb.Mutation{
			{
				Operation: &spannerpb.Mutation_Insert{
					Insert: &spannerpb.Mutation_Write{
						Table:   "Simple",
						Columns: []string{"Id", "Value"},
						Values: []*structpb.ListValue{
							{
								Values: []*structpb.Value{
									makeStringValue("100"),
									makeStringValue("aaa"),
								},
							},
							{
								Values: []*structpb.Value{
									makeStringValue("200"),
									makeStringValue("bbb"),
								},
							},
							{
								Values: []*structpb.Value{
									makeStringValue("300"),
									makeStringValue("ccc"),
								},
							},
							{
								Values: []*structpb.Value{
									makeStringValue("400"),
									makeStringValue("ddd"),
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Commit1 failed: %v", err)
	}

	singleTx2 := &spannerpb.TransactionSelector{
		Selector: &spannerpb.TransactionSelector_SingleUse{
			SingleUse: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		},
	}

	fake := NewFakeExecuteStreamingSqlServer(ctx)
	err = s.StreamingRead(&spannerpb.ReadRequest{
		Session:     session.Name,
		Transaction: singleTx2,
		Table:       "Simple",
		Columns:     []string{"Id"},
		KeySet:      &spannerpb.KeySet{All: true},
	}, fake)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	_, err = s.Commit(ctx, &spannerpb.CommitRequest{
		Session:     session.Name,
		Transaction: singleTx,
		Mutations: []*spannerpb.Mutation{
			{
				Operation: &spannerpb.Mutation_Insert{
					Insert: &spannerpb.Mutation_Write{
						Table:   "Simple",
						Columns: []string{"Id", "Value"},
						Values: []*structpb.ListValue{
							{
								Values: []*structpb.Value{
									makeStringValue("500"),
									makeStringValue("eee"),
								},
							},
						},
					},
				},
			},
			{
				Operation: &spannerpb.Mutation_Update{
					Update: &spannerpb.Mutation_Write{
						Table:   "Simple",
						Columns: []string{"Id", "Value"},
						Values: []*structpb.ListValue{
							{
								Values: []*structpb.Value{
									makeStringValue("100"),
									makeStringValue("aaa2"),
								},
							},
						},
					},
				},
			},
			{
				Operation: &spannerpb.Mutation_Replace{
					Replace: &spannerpb.Mutation_Write{
						Table:   "Simple",
						Columns: []string{"Id", "Value"},
						Values: []*structpb.ListValue{
							{
								Values: []*structpb.Value{
									makeStringValue("200"),
									makeStringValue("bbb2"),
								},
							},
						},
					},
				},
			},
			{
				Operation: &spannerpb.Mutation_InsertOrUpdate{
					InsertOrUpdate: &spannerpb.Mutation_Write{
						Table:   "Simple",
						Columns: []string{"Id", "Value"},
						Values: []*structpb.ListValue{
							{
								Values: []*structpb.Value{
									makeStringValue("300"),
									makeStringValue("ccc2"),
								},
							},
						},
					},
				},
			},
			{
				Operation: &spannerpb.Mutation_Delete_{
					Delete: &spannerpb.Mutation_Delete{
						Table: "Simple",
						KeySet: &spannerpb.KeySet{
							Keys: []*structpb.ListValue{
								{
									Values: []*structpb.Value{
										makeStringValue("400"),
									},
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Commit2 failed: %v", err)
	}

	fake2 := NewFakeExecuteStreamingSqlServer(ctx)
	err = s.StreamingRead(&spannerpb.ReadRequest{
		Session:     session.Name,
		Transaction: singleTx2,
		Table:       "Simple",
		Columns:     []string{"Id", "Value"},
		KeySet:      &spannerpb.KeySet{All: true},
	}, fake2)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	var results []*structpb.Value
	for _, set := range fake2.sets {
		results = append(results, set.Values...)
	}

	expected := []*structpb.Value{
		makeStringValue("100"),
		makeStringValue("aaa2"),
		makeStringValue("200"),
		makeStringValue("bbb2"),
		makeStringValue("300"),
		makeStringValue("ccc2"),
		makeStringValue("500"),
		makeStringValue("eee"),
	}
	if diff := cmp.Diff(expected, results, protocmp.Transform()); diff != "" {
		t.Errorf("(-got, +want)\n%s", diff)
	}
}

func TestCommitMutations_AtomicOperation(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	preareDBAndSession := func(t *testing.T) *spannerpb.Session {
		session, dbName := testCreateSession(t, s)
		db, ok := s.db[dbName]
		if !ok {
			t.Fatalf("database not found")
		}
		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		for _, query := range []string{
			`INSERT INTO Simple VALUES(100, "xxx")`,
		} {
			if _, err := db.db.ExecContext(ctx, query); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		return session
	}

	session := preareDBAndSession(t)

	begin := func() *spannerpb.Transaction {
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return tx
	}
	readSimple := func(t *testing.T) [][]*structpb.Value {
		fake := NewFakeExecuteStreamingSqlServer(ctx)
		err := s.StreamingRead(&spannerpb.ReadRequest{
			Session: session.Name,
			Transaction: &spannerpb.TransactionSelector{
				Selector: &spannerpb.TransactionSelector_SingleUse{
					SingleUse: &spannerpb.TransactionOptions{
						Mode: &spannerpb.TransactionOptions_ReadWrite_{
							ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
						},
					},
				},
			},
			Table:   "Simple",
			Columns: []string{"Id", "Value"},
			KeySet:  &spannerpb.KeySet{All: true},
		}, fake)
		if err != nil {
			t.Fatalf("StreamingRead error: %v", err)
		}

		var results [][]*structpb.Value
		for _, set := range fake.sets {
			results = append(results, set.Values)
		}

		return results
	}

	expected := [][]*structpb.Value{
		{
			makeStringValue("100"),
			makeStringValue("xxx"),
		},
	}

	t.Run("InsertConflict", func(t *testing.T) {
		tx := begin()
		_, err := s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{
				{
					Operation: &spannerpb.Mutation_Insert{
						Insert: &spannerpb.Mutation_Write{
							Table:   "Simple",
							Columns: []string{"Id", "Value"},
							Values: []*structpb.ListValue{
								{
									Values: []*structpb.Value{
										makeStringValue("300"),
										makeStringValue("ccc2"),
									},
								},
							},
						},
					},
				},
				{
					Operation: &spannerpb.Mutation_Insert{
						Insert: &spannerpb.Mutation_Write{
							Table:   "Simple",
							Columns: []string{"Id", "Value"},
							Values: []*structpb.ListValue{
								{
									Values: []*structpb.Value{
										makeStringValue("100"), // already exists
										makeStringValue("ccc2"),
									},
								},
							},
						},
					},
				},
			},
		})
		assertStatusCode(t, err, codes.AlreadyExists)

		results := readSimple(t)
		if diff := cmp.Diff(results, expected, protocmp.Transform()); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}
	})
}

func TestRollback(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	t.Run("Success", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("RollbackAfterRollback", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("RollbackAfterCommit", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Commit(ctx, &spannerpb.CommitRequest{
			Session: session.Name,
			Transaction: &spannerpb.CommitRequest_TransactionId{
				TransactionId: tx.Id,
			},
			Mutations: []*spannerpb.Mutation{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		expected := "Cannot rollback a transaction after Commit() has been called."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})

	t.Run("TransactionInDifferentSession", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
			Session: session.Name,
			Options: &spannerpb.TransactionOptions{
				Mode: &spannerpb.TransactionOptions_ReadWrite_{
					ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		session2, _ := testCreateSession(t, s)

		_, err = s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session2.Name,
			TransactionId: tx.Id,
		})
		assertGRPCError(t, err, codes.InvalidArgument, "Transaction was started in a different session")
	})

	t.Run("InvalidatedTransaction", func(t *testing.T) {
		session, _ := testCreateSession(t, s)
		tx := testInvalidatedTransaction(t, s, session)

		_, err := s.Rollback(ctx, &spannerpb.RollbackRequest{
			Session:       session.Name,
			TransactionId: tx.Id,
		})
		expected := "This transaction has been invalidated by a later transaction in the same session."
		assertGRPCError(t, err, codes.FailedPrecondition, expected)
	})
}

func TestTransaction(t *testing.T) {
	ctx := context.Background()

	s := newTestServer()

	preareDBAndSession := func(t *testing.T) *spannerpb.Session {
		session, dbName := testCreateSession(t, s)
		db, ok := s.db[dbName]
		if !ok {
			t.Fatalf("database not found")
		}
		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		for _, query := range []string{
			`INSERT INTO Simple VALUES(100, "xxx")`,
		} {
			if _, err := db.db.ExecContext(ctx, query); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		return session
	}

	testcase := map[string]func(session string, tx *spannerpb.TransactionSelector, stream spannerpb.Spanner_StreamingReadServer) error{
		"ExecuteStreamingSql": func(session string, tx *spannerpb.TransactionSelector, stream spannerpb.Spanner_StreamingReadServer) error {
			return s.ExecuteStreamingSql(&spannerpb.ExecuteSqlRequest{
				Session:     session,
				Transaction: tx,
				Sql:         "SELECT 1",
			}, stream)
		},
		"StreamingRead": func(session string, tx *spannerpb.TransactionSelector, stream spannerpb.Spanner_StreamingReadServer) error {
			return s.StreamingRead(&spannerpb.ReadRequest{
				Session:     session,
				Transaction: tx,
				Table:       "Simple",
				Columns:     []string{"Id"},
				KeySet:      &spannerpb.KeySet{All: true},
			}, stream)
		},
		"ExecuteSql": func(session string, tx *spannerpb.TransactionSelector, stream spannerpb.Spanner_StreamingReadServer) error {
			r, err := s.ExecuteSql(context.Background(), &spannerpb.ExecuteSqlRequest{
				Session:     session,
				Transaction: tx,
				Sql:         `UPDATE Simple SET Value = "xxx" WHERE Id = 10000000`,
			})
			if err == nil {
				// set data with stream to make test easir
				stream.Send(&spannerpb.PartialResultSet{
					Metadata: r.Metadata,
					Stats:    r.Stats,
				})
			}
			return err
		},
		"ExecuteBatchDml": func(session string, tx *spannerpb.TransactionSelector, stream spannerpb.Spanner_StreamingReadServer) error {
			r, err := s.ExecuteBatchDml(context.Background(), &spannerpb.ExecuteBatchDmlRequest{
				Session:     session,
				Transaction: tx,
				Statements: []*spannerpb.ExecuteBatchDmlRequest_Statement{
					{Sql: `UPDATE Simple SET Value = "xxx" WHERE Id = 10000000`},
				},
			})
			if err == nil {
				// set data with stream to make test easir
				for i := range r.ResultSets {
					stream.Send(&spannerpb.PartialResultSet{
						Metadata: r.ResultSets[i].Metadata,
						Stats:    r.ResultSets[i].Stats,
					})
				}
			}
			return err
		},
	}

	for name, fn := range testcase {
		t.Run(name, func(t *testing.T) {
			t.Run("UseReturnedTransaction", func(t *testing.T) {
				session := preareDBAndSession(t)

				fake := &fakeExecuteStreamingSqlServer{}
				begin := &spannerpb.TransactionSelector{
					Selector: &spannerpb.TransactionSelector_Begin{
						Begin: &spannerpb.TransactionOptions{
							Mode: &spannerpb.TransactionOptions_ReadWrite_{
								ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
							},
						},
					},
				}
				if err := fn(session.Name, begin, fake); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				newtx := fake.sets[0].GetMetadata().GetTransaction()
				if newtx == nil {
					t.Fatalf("transaction should not be nil")
				}

				fake2 := &fakeExecuteStreamingSqlServer{}
				txsel := &spannerpb.TransactionSelector{
					Selector: &spannerpb.TransactionSelector_Id{
						Id: newtx.Id,
					},
				}
				if err := fn(session.Name, txsel, fake2); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			})

			t.Run("AfterCommit", func(t *testing.T) {
				session, _ := testCreateSession(t, s)

				tx, err := s.BeginTransaction(ctx, &spannerpb.BeginTransactionRequest{
					Session: session.Name,
					Options: &spannerpb.TransactionOptions{
						Mode: &spannerpb.TransactionOptions_ReadWrite_{
							ReadWrite: &spannerpb.TransactionOptions_ReadWrite{},
						},
					},
				})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				_, err = s.Commit(ctx, &spannerpb.CommitRequest{
					Session: session.Name,
					Transaction: &spannerpb.CommitRequest_TransactionId{
						TransactionId: tx.Id,
					},
					Mutations: []*spannerpb.Mutation{},
				})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				fake := &fakeExecuteStreamingSqlServer{}
				txsel := &spannerpb.TransactionSelector{
					Selector: &spannerpb.TransactionSelector_Id{
						Id: tx.Id,
					},
				}
				err = fn(session.Name, txsel, fake)
				expected := "Cannot start a read or query within a transaction after Commit() or Rollback() has been called."
				assertGRPCError(t, err, codes.FailedPrecondition, expected)
			})

			t.Run("InvalidatedTransaction", func(t *testing.T) {
				session, _ := testCreateSession(t, s)
				tx := testInvalidatedTransaction(t, s, session)

				fake := &fakeExecuteStreamingSqlServer{}
				txsel := &spannerpb.TransactionSelector{
					Selector: &spannerpb.TransactionSelector_Id{
						Id: tx.Id,
					},
				}
				err := fn(session.Name, txsel, fake)
				expected := "This transaction has been invalidated by a later transaction in the same session."
				assertGRPCError(t, err, codes.FailedPrecondition, expected)
			})
		})
	}
}

func TestUpdateDatabaseDdl(t *testing.T) {
	ctx := context.Background()
	s := newTestServer()
	_, dbName := testCreateSession(t, s)

	stmts := []string{`CREATE TABLE Test (Id INT64) PRIMARY KEY(Id)`}
	op, err := s.UpdateDatabaseDdl(ctx, &adminv1pb.UpdateDatabaseDdlRequest{
		Database:   dbName,
		Statements: stmts,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	op2, err := s.WaitOperation(ctx, &lropb.WaitOperationRequest{
		Name: op.Name,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !op2.Done {
		t.Errorf("should be success")
	}
}

func TestCreateDatabase(t *testing.T) {
	ctx := context.Background()
	s := newTestServer()

	projectID, instanceID := "fake", "fake"
	parent := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)
	extraStmts := []string{`
CREATE TABLE Ids (
  Id INT64 NOT NULL,
) PRIMARY KEY(Id)`}

	alreadyExistsDatabase := "already-exists"
	s.createDatabase(fmt.Sprintf("%s/databases/%s", parent, alreadyExistsDatabase))

	t.Run("Success", func(t *testing.T) {
		databaseID := strconv.Itoa(int(time.Now().Unix()))
		op, err := s.CreateDatabase(ctx, &adminv1pb.CreateDatabaseRequest{
			Parent:          parent,
			CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
			ExtraStatements: extraStmts,
		})
		if err != nil {
			t.Fatalf("failed to create database: %s", err)
		}
		if _, ok := s.db[fmt.Sprintf("%s/databases/%s", parent, databaseID)]; !ok {
			t.Fatal("failed to get database from map")
		}
		if !op.GetDone() {
			t.Fatal("the operation has not finished yet")
		}
		var db adminv1pb.Database
		if err := anypb.UnmarshalTo(op.GetResponse(), &db, proto.UnmarshalOptions{DiscardUnknown: true}); err != nil {
			t.Fatalf("failed to unmarshal response: %s", err)
		}
		if got, expect := db.GetName(), fmt.Sprintf("%s/databases/%s", parent, databaseID); got != expect {
			t.Fatalf("expected %s but got %s", expect, db.GetName())
		}
	})

	t.Run("Failure", func(t *testing.T) {
		tests := map[string]struct {
			req        *adminv1pb.CreateDatabaseRequest
			expectCode codes.Code
		}{
			"invalid parent": {
				req: &adminv1pb.CreateDatabaseRequest{
					Parent:          "INVALID",
					CreateStatement: fmt.Sprintf("CREATE DATABASE `%d`", time.Now().Unix()),
					ExtraStatements: extraStmts,
				},
				expectCode: codes.InvalidArgument,
			},
			"invalid create statement": {
				req: &adminv1pb.CreateDatabaseRequest{
					Parent:          parent,
					CreateStatement: "INVALID",
					ExtraStatements: extraStmts,
				},
				expectCode: codes.InvalidArgument,
			},
			"create statement is not CREATE DATABASE": {
				req: &adminv1pb.CreateDatabaseRequest{
					Parent:          parent,
					CreateStatement: extraStmts[0],
					ExtraStatements: extraStmts,
				},
				expectCode: codes.InvalidArgument,
			},
			"invalid extra statements": {
				req: &adminv1pb.CreateDatabaseRequest{
					Parent:          parent,
					CreateStatement: fmt.Sprintf("CREATE DATABASE `%d`", time.Now().Unix()),
					ExtraStatements: []string{"INVALID"},
				},
				expectCode: codes.InvalidArgument,
			},
			"already exists": {
				req: &adminv1pb.CreateDatabaseRequest{
					Parent:          parent,
					CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", alreadyExistsDatabase),
					ExtraStatements: extraStmts,
				},
				expectCode: codes.AlreadyExists,
			},
		}
		for name, test := range tests {
			test := test
			t.Run(name, func(t *testing.T) {
				_, err := s.CreateDatabase(ctx, test.req)
				if got, expect := status.Convert(err).Code(), test.expectCode; got != expect {
					t.Errorf("expect error code %s but got %s", expect, got)
				}
			})
		}
	})
}

func TestDropDatabase(t *testing.T) {
	ctx := context.Background()
	s := newTestServer()

	database := "projects/fake/instances/fake/databases/fake"

	t.Run("Success", func(t *testing.T) {
		_, err := s.createDatabase(database)
		if err != nil {
			t.Fatalf("failed to create database: %s", err)
		}

		if _, err := s.DropDatabase(ctx, &adminv1pb.DropDatabaseRequest{
			Database: database,
		}); err != nil {
			t.Fatalf("failed to drop database: %s", err)
		}

		_, err = s.GetDatabase(ctx, &adminv1pb.GetDatabaseRequest{
			Name: database,
		})
		st := status.Convert(err)
		if st.Code() != codes.NotFound {
			t.Fatalf("failed to drop database: %s", err)
		}
		expectedRI := &errdetails.ResourceInfo{
			ResourceType: "type.googleapis.com/google.spanner.admin.database.v1.Database",
			ResourceName: database,
			Description:  "Database does not exist.",
		}
		assertResourceInfo(t, err, expectedRI)

		// DropDatabase returns no error even if the database is not exist
		if _, err := s.DropDatabase(ctx, &adminv1pb.DropDatabaseRequest{
			Database: database,
		}); err != nil {
			t.Fatalf("failed to drop database: %s", err)
		}
	})

	t.Run("Failure", func(t *testing.T) {
		tests := map[string]struct {
			req        *adminv1pb.DropDatabaseRequest
			expectCode codes.Code
		}{
			"invalid database": {
				req: &adminv1pb.DropDatabaseRequest{
					Database: "INVALID",
				},
				expectCode: codes.InvalidArgument,
			},
		}
		for name, test := range tests {
			test := test
			t.Run(name, func(t *testing.T) {
				_, err := s.DropDatabase(ctx, test.req)
				if got, expect := status.Convert(err).Code(), test.expectCode; got != expect {
					t.Errorf("expect error code %s but got %s", expect, got)
				}
			})
		}
	})
}

func TestListDatabases(t *testing.T) {
	ctx := context.Background()
	s := newTestServer()

	databases := []string{
		"projects/fake/instances/fake/databases/fake",
		"projects/fake/instances/fake/databases/fake2",
		"projects/fake/instances/fake/databases/fake3",
	}

	for _, name := range databases {
		_, err := s.createDatabase(name)
		if err != nil {
			t.Fatalf("failed to create database: %s", err)
		}
	}

	res, err := s.ListDatabases(ctx, &adminv1pb.ListDatabasesRequest{
		Parent: "projects/fake/instances/fake",
	})
	if err != nil {
		t.Fatalf("failed to list databases: %s", err)
	}

	sort.Slice(res.Databases, func(i, j int) bool {
		return res.Databases[i].Name < res.Databases[j].Name
	})

	expected := []*adminv1pb.Database{
		{
			Name:  "projects/fake/instances/fake/databases/fake",
			State: adminv1pb.Database_READY,
		},
		{
			Name:  "projects/fake/instances/fake/databases/fake2",
			State: adminv1pb.Database_READY,
		},
		{
			Name:  "projects/fake/instances/fake/databases/fake3",
			State: adminv1pb.Database_READY,
		},
	}
	if diff := cmp.Diff(expected, res.Databases, protocmp.Transform()); diff != "" {
		t.Errorf("(-got, +want)\n%s", diff)
	}
}
