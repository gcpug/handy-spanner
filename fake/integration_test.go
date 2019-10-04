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
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	cmp "github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
)

type FullType struct {
	PKey            string              `spanner:"PKey" json:"PKey"`                       // PKey
	FTString        string              `spanner:"FTString" json:"FTString"`               // FTString
	FTStringNull    spanner.NullString  `spanner:"FTStringNull" json:"FTStringNull"`       // FTStringNull
	FTBool          bool                `spanner:"FTBool" json:"FTBool"`                   // FTBool
	FTBoolNull      spanner.NullBool    `spanner:"FTBoolNull" json:"FTBoolNull"`           // FTBoolNull
	FTBytes         []byte              `spanner:"FTBytes" json:"FTBytes"`                 // FTBytes
	FTBytesNull     []byte              `spanner:"FTBytesNull" json:"FTBytesNull"`         // FTBytesNull
	FTTimestamp     time.Time           `spanner:"FTTimestamp" json:"FTTimestamp"`         // FTTimestamp
	FTTimestampNull spanner.NullTime    `spanner:"FTTimestampNull" json:"FTTimestampNull"` // FTTimestampNull
	FTInt           int64               `spanner:"FTInt" json:"FTInt"`                     // FTInt
	FTIntNull       spanner.NullInt64   `spanner:"FTIntNull" json:"FTIntNull"`             // FTIntNull
	FTFloat         float64             `spanner:"FTFloat" json:"FTFloat"`                 // FTFloat
	FTFloatNull     spanner.NullFloat64 `spanner:"FTFloatNull" json:"FTFloatNull"`         // FTFloatNull
	FTDate          civil.Date          `spanner:"FTDate" json:"FTDate"`                   // FTDate
	FTDateNull      spanner.NullDate    `spanner:"FTDateNull" json:"FTDateNull"`           // FTDateNull
}

type Simple struct {
	ID    int64  `spanner:"Id"`
	Value string `spanner:"Value"`
}

func TestIntegration_ReadWrite(t *testing.T) {
	ctx := context.Background()
	dbName := "projects/fake/instances/fake/databases/fake"

	f, err := os.Open("./testdata/schema.sql")
	if err != nil {
		t.Fatalf("err %v", err)
	}

	srv, conn, err := Run()
	if err != nil {
		t.Fatalf("err %v", err)
	}
	defer srv.Stop()

	if err := srv.ParseAndApplyDDL(ctx, dbName, f); err != nil {
		t.Fatal(err)
	}

	client, err := spanner.NewClient(ctx, dbName, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Connecting to in-memory fake: %v", err)
	}

	now := time.Now()
	date := civil.DateOf(now)

	fullTypesKeys := []string{
		"PKey", "FTString", "FTStringNull",
		"FTBool", "FTBoolNull",
		"FTBytes", "FTBytesNull",
		"FTTimestamp", "FTTimestampNull",
		"FTInt", "FTIntNull",
		"FTFloat", "FTFloatNull",
		"FTDate", "FTDateNull",
	}

	table := []struct {
		expected FullType
	}{
		{
			expected: FullType{
				PKey:     "pkey1",
				FTString: "xxx1",
				FTStringNull: spanner.NullString{
					StringVal: "xxx1",
					Valid:     true,
				},
				FTBool: true,
				FTBoolNull: spanner.NullBool{
					Bool:  true,
					Valid: true,
				},
				FTBytes:     []byte("xxx1"),
				FTBytesNull: []byte("xxx2"),
				FTTimestamp: now,
				FTTimestampNull: spanner.NullTime{
					Time:  now,
					Valid: true,
				},
				FTInt: 101,
				FTIntNull: spanner.NullInt64{
					Int64: 101,
					Valid: true,
				},
				FTFloat: 0.123,
				FTFloatNull: spanner.NullFloat64{
					Float64: 0.123,
					Valid:   true,
				},
				FTDate: date,
				FTDateNull: spanner.NullDate{
					Date:  date,
					Valid: true,
				},
			},
		},
		{
			expected: FullType{
				PKey:            "pkey1",
				FTString:        "xxx1",
				FTStringNull:    spanner.NullString{},
				FTBool:          true,
				FTBoolNull:      spanner.NullBool{},
				FTBytes:         []byte("xxx1"),
				FTBytesNull:     []byte("xxx2"),
				FTTimestamp:     now,
				FTTimestampNull: spanner.NullTime{},
				FTInt:           101,
				FTIntNull:       spanner.NullInt64{},
				FTFloat:         0.123,
				FTFloatNull:     spanner.NullFloat64{},
				FTDate:          date,
				FTDateNull:      spanner.NullDate{},
			},
		},
	}

	for _, tc := range table {
		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("FullTypes", fullTypesKeys,
				[]interface{}{
					tc.expected.PKey,
					tc.expected.FTString,
					tc.expected.FTStringNull,
					tc.expected.FTBool, tc.expected.FTBoolNull,
					tc.expected.FTBytes, tc.expected.FTBytesNull,
					tc.expected.FTTimestamp, tc.expected.FTTimestampNull,
					tc.expected.FTInt, tc.expected.FTIntNull,
					tc.expected.FTFloat, tc.expected.FTFloatNull,
					tc.expected.FTDate, tc.expected.FTDateNull,
				}),
		})
		if err != nil {
			t.Fatalf("Applying mutations: %v", err)
		}

		var results []*FullType
		rows := client.Single().Read(ctx, "FullTypes", spanner.AllKeys(), fullTypesKeys)
		err = rows.Do(func(row *spanner.Row) error {
			var ft FullType
			if err := row.ToStruct(&ft); err != nil {
				return err
			}
			results = append(results, &ft)

			return nil
		})
		if err != nil {
			t.Fatalf("Iterating over all row read: %v", err)
		}

		if len(results) != 1 {
			t.Fatalf("rows should be 1 but got %v row", len(results))
		}

		if diff := cmp.Diff(&tc.expected, results[0]); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

		_, err = client.Apply(ctx, []*spanner.Mutation{
			spanner.Delete("FullTypes", spanner.AllKeys()),
		})
		if err != nil {
			t.Fatalf("delete failed: %v", err)
		}
	}
}

func TestIntegration_Read_KeySet(t *testing.T) {
	ctx := context.Background()
	dbName := "projects/fake/instances/fake/databases/fake"

	f, err := os.Open("./testdata/schema.sql")
	if err != nil {
		t.Fatalf("err %v", err)
	}

	srv, conn, err := Run()
	if err != nil {
		t.Fatalf("err %v", err)
	}
	defer srv.Stop()

	if err := srv.ParseAndApplyDDL(ctx, dbName, f); err != nil {
		t.Fatal(err)
	}

	client, err := spanner.NewClient(ctx, dbName, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Connecting to in-memory fake: %v", err)
	}

	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{100, "xxx0"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{101, "xxx1"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{102, "xxx2"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{200, "yyy"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{300, "zzz"},
		),
	})
	if err != nil {
		t.Fatalf("Applying mutations: %v", err)
	}

	table := map[string]struct {
		keyset   spanner.KeySet
		expected []*Simple
	}{
		"SingleKey": {
			keyset: spanner.Key([]interface{}{100}),
			expected: []*Simple{
				{ID: 100, Value: "xxx0"},
			},
		},
		"MultipleKeys": {
			keyset: spanner.KeySets(
				spanner.Key([]interface{}{100}),
				spanner.Key([]interface{}{101}),
				spanner.Key([]interface{}{102}),
			),
			expected: []*Simple{
				{ID: 100, Value: "xxx0"},
				{ID: 101, Value: "xxx1"},
				{ID: 102, Value: "xxx2"},
			},
		},
		"SingleRange_CloseClose": {
			keyset: spanner.KeyRange{
				Start: spanner.Key([]interface{}{100}),
				End:   spanner.Key([]interface{}{102}),
				Kind:  spanner.ClosedClosed,
			},
			expected: []*Simple{
				{ID: 100, Value: "xxx0"},
				{ID: 101, Value: "xxx1"},
				{ID: 102, Value: "xxx2"},
			},
		},
		"SingleRange_OpenClose": {
			keyset: spanner.KeyRange{
				Start: spanner.Key([]interface{}{100}),
				End:   spanner.Key([]interface{}{102}),
				Kind:  spanner.OpenClosed,
			},
			expected: []*Simple{
				{ID: 101, Value: "xxx1"},
				{ID: 102, Value: "xxx2"},
			},
		},
		"SingleRange_CloseOpen": {
			keyset: spanner.KeyRange{
				Start: spanner.Key([]interface{}{100}),
				End:   spanner.Key([]interface{}{102}),
				Kind:  spanner.ClosedOpen,
			},
			expected: []*Simple{
				{ID: 100, Value: "xxx0"},
				{ID: 101, Value: "xxx1"},
			},
		},
		"SingleRange_OpenOpen": {
			keyset: spanner.KeyRange{
				Start: spanner.Key([]interface{}{100}),
				End:   spanner.Key([]interface{}{102}),
				Kind:  spanner.OpenOpen,
			},
			expected: []*Simple{
				{ID: 101, Value: "xxx1"},
			},
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			var results []*Simple
			rows := client.Single().Read(ctx, "Simple", tc.keyset, []string{"Id", "Value"})
			err = rows.Do(func(row *spanner.Row) error {
				var s Simple

				if err := row.ToStruct(&s); err != nil {
					return err
				}
				results = append(results, &s)

				return nil
			})
			if err != nil {
				t.Fatalf("Iterating over all row read: %v", err)
			}

			if diff := cmp.Diff(tc.expected, results); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestIntegration_Query(t *testing.T) {
	ctx := context.Background()
	dbName := "projects/fake/instances/fake/databases/fake"

	f, err := os.Open("./testdata/schema.sql")
	if err != nil {
		t.Fatalf("err %v", err)
	}

	srv, conn, err := Run()
	if err != nil {
		t.Fatalf("err %v", err)
	}
	defer srv.Stop()

	if err := srv.ParseAndApplyDDL(ctx, dbName, f); err != nil {
		t.Fatal(err)
	}

	client, err := spanner.NewClient(ctx, dbName, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Connecting to in-memory fake: %v", err)
	}

	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{100, "xxx0"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{101, "xxx1"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{102, "xxx2"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{200, "yyy"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{300, "zzz"},
		),
	})
	if err != nil {
		t.Fatalf("Applying mutations: %v", err)
	}

	table := []struct {
		sql      string
		params   map[string]interface{}
		expected []*Simple
	}{
		{
			sql: "SELECT * FROM Simple",
			expected: []*Simple{
				{ID: 100, Value: "xxx0"},
				{ID: 101, Value: "xxx1"},
				{ID: 102, Value: "xxx2"},
				{ID: 200, Value: "yyy"},
				{ID: 300, Value: "zzz"},
			},
		},
		{
			sql: "SELECT Id, Value FROM Simple",
			expected: []*Simple{
				{ID: 100, Value: "xxx0"},
				{ID: 101, Value: "xxx1"},
				{ID: 102, Value: "xxx2"},
				{ID: 200, Value: "yyy"},
				{ID: 300, Value: "zzz"},
			},
		},
		{
			sql:    "SELECT * FROM Simple WHERE Id = @id",
			params: map[string]interface{}{"id": 101},
			expected: []*Simple{
				{ID: 101, Value: "xxx1"},
			},
		},
		{
			sql: "SELECT * FROM Simple WHERE Id IN UNNEST(@ids)",
			params: map[string]interface{}{
				"ids": []int64{101, 102},
			},
			expected: []*Simple{
				{ID: 101, Value: "xxx1"},
				{ID: 102, Value: "xxx2"},
			},
		},
		{
			sql: "SELECT * FROM Simple WHERE Id IN UNNEST(@ids)",
			params: map[string]interface{}{
				"ids": []int64{},
			},
			expected: nil,
		},
		{
			sql: "SELECT * FROM Simple WHERE Id IN UNNEST(@ids)",
			params: map[string]interface{}{
				"ids": []int64(nil),
			},
			expected: nil,
		},
		{
			sql: `SELECT a.* FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE a.Id = @id`,
			params: map[string]interface{}{
				"id": 200,
			},
			expected: []*Simple{
				{ID: 200, Value: "yyy"},
			},
		},
	}

	for _, tc := range table {
		stmt := spanner.NewStatement(tc.sql)
		stmt.Params = tc.params
		var results []*Simple
		rows := client.Single().Query(ctx, stmt)
		err = rows.Do(func(row *spanner.Row) error {
			var s Simple

			if err := row.ToStruct(&s); err != nil {
				return err
			}
			results = append(results, &s)

			return nil
		})
		if err != nil {
			t.Fatalf("Iterating over all row read: %v", err)
		}

		if diff := cmp.Diff(tc.expected, results); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}
	}
}

func TestIntegration_Query_Detail(t *testing.T) {
	ctx := context.Background()
	dbName := "projects/fake/instances/fake/databases/fake"

	f, err := os.Open("./testdata/schema.sql")
	if err != nil {
		t.Fatalf("err %v", err)
	}

	srv, conn, err := Run()
	if err != nil {
		t.Fatalf("err %v", err)
	}
	defer srv.Stop()

	if err := srv.ParseAndApplyDDL(ctx, dbName, f); err != nil {
		t.Fatal(err)
	}

	client, err := spanner.NewClient(ctx, dbName, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Connecting to in-memory fake: %v", err)
	}

	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{100, "xxx0"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{101, "xxx1"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{102, "xxx2"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{200, "yyy"},
		),
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{300, "zzz"},
		),
	})
	if err != nil {
		t.Fatalf("Applying mutations: %v", err)
	}

	table := []struct {
		sql      string
		params   map[string]interface{}
		names    []string
		columns  []interface{}
		expected [][]interface{}
	}{
		{
			sql:     "SELECT * FROM Simple",
			names:   []string{"Id", "Value"},
			columns: []interface{}{int64(0), ""},
			expected: [][]interface{}{
				{int64(100), string("xxx0")},
				{int64(101), string("xxx1")},
				{int64(102), string("xxx2")},
				{int64(200), string("yyy")},
				{int64(300), string("zzz")},
			},
		},
		{
			sql:     "SELECT COUNT(1) FROM Simple",
			names:   []string{""}, // TODO
			columns: []interface{}{int64(0)},
			expected: [][]interface{}{
				{int64(5)},
			},
		},
		{
			sql:     "SELECT COUNT(1) AS count FROM Simple",
			names:   []string{"count"},
			columns: []interface{}{int64(0)},
			expected: [][]interface{}{
				{int64(5)},
			},
		},
	}

	for _, tc := range table {
		stmt := spanner.NewStatement(tc.sql)
		stmt.Params = tc.params

		var result [][]interface{}
		rows := client.Single().Query(ctx, stmt)
		err := rows.Do(func(row *spanner.Row) error {
			if diff := cmp.Diff(tc.names, row.ColumnNames()); diff != "" {
				t.Fatalf("(-got, +want)\n%s", diff)
			}

			var data []interface{}
			for i := range tc.columns {
				typ := reflect.New(reflect.TypeOf(tc.columns[i]))
				if err := row.Column(i, typ.Interface()); err != nil {
					t.Fatalf("Column error: %v", err)
				}
				data = append(data, reflect.Indirect(typ).Interface())
			}
			result = append(result, data)

			return nil
		})
		if err != nil {
			t.Fatalf("Iterating over all row read: %v", err)
		}

		if diff := cmp.Diff(tc.expected, result); diff != "" {
			t.Fatalf("(-got, +want)\n%s", diff)
		}

	}
}
