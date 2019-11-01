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
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	structpb "github.com/golang/protobuf/ptypes/struct"
	cmp "github.com/google/go-cmp/cmp"
	uuidpkg "github.com/google/uuid"
	adminv1pb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type fakeExecuteStreamingSqlServer struct {
	sets []*spannerpb.PartialResultSet

	grpc.ServerStream
}

func (s *fakeExecuteStreamingSqlServer) Send(set *spannerpb.PartialResultSet) error {
	s.sets = append(s.sets, set)
	return nil
}

func (s *fakeExecuteStreamingSqlServer) Context() context.Context {
	return context.Background()
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
				{makeStringValue("100"), makeStringValue("xxx")},
				{makeStringValue("200"), makeStringValue("yyy")},
				{makeStringValue("300"), makeStringValue("zzz")},
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
				{makeStringValue("100"), makeStringValue("xxx")},
				{makeStringValue("200"), makeStringValue("yyy")},
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
				{makeStringValue("xxx"), makeStringValue("0")},
				{makeStringValue("yyy"), makeStringValue("1")},
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
				{makeStringValue("100"), makeStringValue("0")},
				{makeStringValue("200"), makeStringValue("1")},
			},
		},

		"Simple_Unnest_Array": {
			sql:    `SELECT * FROM Simple WHERE Id IN UNNEST([100, 200])`,
			fields: simpleFields,
			expected: [][]*structpb.Value{
				{makeStringValue("100"), makeStringValue("xxx")},
				{makeStringValue("200"), makeStringValue("yyy")},
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
				{makeStringValue("100"), makeStringValue("xxx")},
				{makeStringValue("200"), makeStringValue("yyy")},
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
			fields:   nil,
			expected: nil,
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
				if diff := cmp.Diff(tc.fields, fake.sets[0].Metadata.RowType.Fields); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			}

			if diff := cmp.Diff(tc.expected, results); diff != "" {
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

			if diff := cmp.Diff(tc.fields, fake.sets[0].Metadata.RowType.Fields); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}

			if diff := cmp.Diff(tc.expected, results[0]); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestMakeValueFromSpannerValue(t *testing.T) {
	table := map[string]struct {
		value    *structpb.Value
		typ      *spannerpb.Type
		expected Value
	}{
		"Null": {
			value: makeNullValue(),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
			expected: Value{
				Data: nil,
				Type: ValueType{
					Code: TCInt64,
				},
			},
		},
		"Int": {
			value: makeStringValue("100"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
			expected: Value{
				Data: int64(100),
				Type: ValueType{
					Code: TCInt64,
				},
			},
		},
		"String": {
			value: makeStringValue("xx"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_STRING,
			},
			expected: Value{
				Data: "xx",
				Type: ValueType{
					Code: TCString,
				},
			},
		},
		"Bool": {
			value: makeBoolValue(true),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_BOOL,
			},
			expected: Value{
				Data: true,
				Type: ValueType{
					Code: TCBool,
				},
			},
		},
		"Number": {
			value: makeNumberValue(0.123),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_FLOAT64,
			},
			expected: Value{
				Data: 0.123,
				Type: ValueType{
					Code: TCFloat64,
				},
			},
		},
		"Timestamp": {
			value: makeStringValue("2012-03-04T00:00:00.123456789Z"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_TIMESTAMP,
			},
			expected: Value{
				Data: "2012-03-04T00:00:00.123456789Z",
				Type: ValueType{
					Code: TCTimestamp,
				},
			},
		},
		"Date": {
			value: makeStringValue("2012-03-04"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_DATE,
			},
			expected: Value{
				Data: "2012-03-04",
				Type: ValueType{
					Code: TCDate,
				},
			},
		},
		"Bytes": {
			value: makeStringValue("eHh4eHg="),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_BYTES,
			},
			expected: Value{
				Data: []byte("xxxxx"),
				Type: ValueType{
					Code: TCBytes,
				},
			},
		},
		"ListInt": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("100"),
				makeStringValue("101"),
			)),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
			expected: Value{
				Data: makeTestWrappedArray(TCInt64, 100, 101),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCInt64,
					},
				},
			},
		},
		"ListString": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("xxx"),
				makeStringValue("yyy"),
			)),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_STRING,
				},
			},
			expected: Value{
				Data: makeTestWrappedArray(TCString, "xxx", "yyy"),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCString,
					},
				},
			},
		},
		"ListStringNull": {
			value: makeNullValue(),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_STRING,
				},
			},
			expected: Value{
				Data: []string(nil),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCString,
					},
				},
			},
		},
		"ListBool": {
			value: makeListValueAsValue(makeListValue(
				makeBoolValue(true),
				makeBoolValue(false),
			)),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_BOOL,
				},
			},
			expected: Value{
				Data: makeTestWrappedArray(TCBool, true, false),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCBool,
					},
				},
			},
		},
		"ListBoolNull": {
			value: makeNullValue(),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_BOOL,
				},
			},
			expected: Value{
				Data: []bool(nil),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCBool,
					},
				},
			},
		},
		"ListNumber": {
			value: makeListValueAsValue(makeListValue(
				makeNumberValue(0.123),
				makeNumberValue(1.123),
			)),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_FLOAT64,
				},
			},
			expected: Value{
				Data: makeTestWrappedArray(TCFloat64, 0.123, 1.123),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCFloat64,
					},
				},
			},
		},
		"ListNumberNull": {
			value: makeNullValue(),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_FLOAT64,
				},
			},
			expected: Value{
				Data: []float64(nil),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCFloat64,
					},
				},
			},
		},
		"ListTimestamp": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("2012-03-04T00:00:00.123456789Z"),
				makeStringValue("2012-03-04T00:00:00.000000000Z"),
			)),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_TIMESTAMP,
				},
			},
			expected: Value{
				Data: makeTestWrappedArray(TCString,
					"2012-03-04T00:00:00.123456789Z",
					"2012-03-04T00:00:00.000000000Z",
				),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCTimestamp,
					},
				},
			},
		},
		"ListTimestampNull": {
			value: makeNullValue(),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_TIMESTAMP,
				},
			},
			expected: Value{
				Data: []string(nil),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCTimestamp,
					},
				},
			},
		},
		"ListDate": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("2012-03-04"),
				makeStringValue("2012-03-05"),
			)),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_DATE,
				},
			},
			expected: Value{
				Data: makeTestWrappedArray(TCString, "2012-03-04", "2012-03-05"),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCDate,
					},
				},
			},
		},
		"ListDateNull": {
			value: makeNullValue(),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_DATE,
				},
			},
			expected: Value{
				Data: []string(nil),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCDate,
					},
				},
			},
		},
		"ListBytes": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("eHh4eHg="),
				makeStringValue("eXl5eXk="),
			)),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_BYTES,
				},
			},
			expected: Value{
				Data: makeTestWrappedArray(TCBytes, []byte("xxxxx"), []byte("yyyyy")),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCBytes,
					},
				},
			},
		},
		"ListBytesNull": {
			value: makeNullValue(),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_BYTES,
				},
			},
			expected: Value{
				Data: [][]byte(nil),
				Type: ValueType{
					Code: TCArray,
					ArrayType: &ValueType{
						Code: TCBytes,
					},
				},
			},
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			res, err := makeValueFromSpannerValue(tc.value, tc.typ)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.expected, res); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
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
		if err := ptypes.UnmarshalAny(op.GetResponse(), &db); err != nil {
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
		db, err := s.createDatabase(database)
		if err != nil {
			t.Fatalf("failed to create database: %s", err)
		}

		if _, err := s.DropDatabase(ctx, &adminv1pb.DropDatabaseRequest{
			Database: database,
		}); err != nil {
			t.Fatalf("failed to drop database: %s", err)
		}
		if _, ok := s.db[database]; ok {
			t.Error("failed to delete database from map")
		}
		if err := db.db.Ping(); err == nil {
			t.Error("db.db sould be closed")
		}

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
