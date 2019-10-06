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
	"testing"

	structpb "github.com/golang/protobuf/ptypes/struct"
	cmp "github.com/google/go-cmp/cmp"
	uuidpkg "github.com/google/uuid"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	} {
		if _, err := db.db.ExecContext(ctx, query); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	table := map[string]struct {
		sql      string
		types    map[string]*spannerpb.Type
		params   *structpb.Struct
		expected [][]*structpb.Value
	}{
		"Simple": {
			sql: `SELECT * FROM Simple`,
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
			expected: [][]*structpb.Value{
				{makeStringValue("100"), makeStringValue("xxx")},
				{makeStringValue("200"), makeStringValue("yyy")},
			},
		},

		"Simple_Unnest_Array": {
			sql: `SELECT * FROM Simple WHERE Id IN UNNEST([100, 200])`,
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
			expected: nil,
		},

		"CompositePrimaryKeys_Condition": {
			sql: `SELECT Id, PKey1, PKey2 FROM CompositePrimaryKeys WHERE PKey1 = "bbb" AND (PKey2 = 3 OR PKey2 = 4)`,
			expected: [][]*structpb.Value{
				{makeStringValue("3"), makeStringValue("bbb"), makeStringValue("3")},
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

			if diff := cmp.Diff(tc.expected, results); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}
