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
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	admindatabasev1 "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	cmp "github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

var fullTypesKeys = []string{
	"PKey", "FTString", "FTStringNull",
	"FTBool", "FTBoolNull",
	"FTBytes", "FTBytesNull",
	"FTTimestamp", "FTTimestampNull",
	"FTInt", "FTIntNull",
	"FTFloat", "FTFloatNull",
	"FTDate", "FTDateNull",
}

type Simple struct {
	ID    int64  `spanner:"Id"`
	Value string `spanner:"Value"`
}

func prepareSimpleData(t *testing.T, ctx context.Context, client *spanner.Client) {
	_, err := client.Apply(ctx, []*spanner.Mutation{
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
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{400, "zzz"},
		),
	})
	if err != nil {
		t.Fatalf("Applying mutations: %v", err)
	}
}

func prepareFullTypeData(t *testing.T, ctx context.Context, client *spanner.Client, now time.Time) {
	_, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("FullTypes", fullTypesKeys,
			[]interface{}{
				"pkey0",
				"xxx0", spanner.NullString{},
				true, spanner.NullBool{},
				[]byte("xxx0"), []byte("xxx0"),
				now, spanner.NullTime{},
				100, spanner.NullInt64{},
				0.123, spanner.NullFloat64{},
				civil.DateOf(now), spanner.NullDate{},
			},
		),
		spanner.Insert("FullTypes", fullTypesKeys,
			[]interface{}{
				"pkey1",
				"xxx1", spanner.NullString{},
				true, spanner.NullBool{},
				[]byte("xxx1"), []byte("xxx1"),
				now.Add(time.Second), spanner.NullTime{},
				100, spanner.NullInt64{},
				0.123, spanner.NullFloat64{},
				civil.DateOf(now), spanner.NullDate{},
			},
		),
		spanner.Insert("FullTypes", fullTypesKeys,
			[]interface{}{
				"pkey2",
				"xxx2", spanner.NullString{},
				true, spanner.NullBool{},
				[]byte("xxx2"), []byte("xxx2"),
				now.Add(2 * time.Second), spanner.NullTime{},
				100, spanner.NullInt64{},
				0.123, spanner.NullFloat64{},
				civil.DateOf(now), spanner.NullDate{},
			},
		),
		spanner.Insert("FullTypes", fullTypesKeys,
			[]interface{}{
				"pkey3",
				"yyy3", spanner.NullString{},
				true, spanner.NullBool{},
				[]byte("yyy3"), []byte("yyy3"),
				now.Add(3 * time.Second), spanner.NullTime{},
				200, spanner.NullInt64{},
				0.123, spanner.NullFloat64{},
				civil.DateOf(now), spanner.NullDate{},
			},
		),
	})
	if err != nil {
		t.Fatalf("Applying mutations: %v", err)
	}
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

func TestIntegration_ReadWrite_AtomicCount(t *testing.T) {
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

	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Simple", []string{"Id", "Value"},
			[]interface{}{100, "100"},
		),
	}); err != nil {
		t.Fatalf("failed to apply: %v", err)
	}

	errCh := make(chan error, 10)
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(me int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
					var num int

					it := tx.Read(ctx, "Simple", spanner.Key([]interface{}{100}), []string{"Value"})
					err := it.Do(func(r *spanner.Row) error {
						var value string
						if err := r.Column(0, &value); err != nil {
							return err
						}
						num, _ = strconv.Atoi(value)
						return nil
					})
					if err != nil {
						return err
					}

					next := num + 1

					return tx.BufferWrite([]*spanner.Mutation{
						spanner.Update("Simple", []string{"Id", "Value"},
							[]interface{}{100, fmt.Sprint(next)},
						),
					})
				})
				if err != nil {
					errCh <- fmt.Errorf("Transaction failed: %v", err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	var errs []error
L:
	for {
		select {
		case err := <-errCh:
			errs = append(errs, err)
		default:
			break L
		}
	}

	if len(errs) != 0 {
		for _, err := range errs {
			t.Error(err)
		}
		t.FailNow()
	}

	var num int
	it := client.Single().Read(ctx, "Simple", spanner.Key([]interface{}{100}), []string{"Value"})
	err = it.Do(func(r *spanner.Row) error {
		var value string
		if err := r.Column(0, &value); err != nil {
			return err
		}
		num, _ = strconv.Atoi(value)
		return nil
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	expected := 200
	if num != expected {
		t.Fatalf("expect num to be %v, but got %v", expected, num)
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

	prepareSimpleData(t, ctx, client)

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
		"SingleMultiRange": {
			keyset: spanner.KeySets(
				spanner.KeyRange{
					Start: spanner.Key([]interface{}{100}),
					End:   spanner.Key([]interface{}{103}),
					Kind:  spanner.ClosedClosed,
				},
				spanner.KeyRange{
					Start: spanner.Key([]interface{}{200}),
					End:   spanner.Key([]interface{}{300}),
					Kind:  spanner.ClosedOpen,
				},
			),
			expected: []*Simple{
				{ID: 100, Value: "xxx0"},
				{ID: 101, Value: "xxx1"},
				{ID: 102, Value: "xxx2"},
				{ID: 200, Value: "yyy"},
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

func TestIntegration_Read_KeySet2(t *testing.T) {
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

	now := time.Now().UTC()
	prepareFullTypeData(t, ctx, client, now)

	table := map[string]struct {
		index    string
		keyset   spanner.KeySet
		expected []*FullType
	}{
		"TimestampAsc_AsPrefix": {
			index:  "FullTypesByIntTimestamp",
			keyset: spanner.Key{100}.AsPrefix(),
			expected: []*FullType{
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
				{PKey: "pkey2", FTInt: 100, FTTimestamp: now.Add(2 * time.Second)},
			},
		},
		"TimestamAsc_Range": {
			index: "FullTypesByIntTimestamp",
			keyset: &spanner.KeyRange{
				Start: spanner.Key{100, now},
				End:   spanner.Key{100},
				Kind:  spanner.ClosedOpen,
			},
			expected: []*FullType{
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
				{PKey: "pkey2", FTInt: 100, FTTimestamp: now.Add(2 * time.Second)},
			},
		},
		"TimestamAsc_Range2": {
			index: "FullTypesByIntTimestamp",
			keyset: &spanner.KeyRange{
				Start: spanner.Key{100, time.Time{}},
				End:   spanner.Key{100, now.Add(2 * time.Second)},
				Kind:  spanner.ClosedOpen,
			},
			expected: []*FullType{
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
			},
		},
		"TimestampDesc_AsPrefix": {
			index:  "FullTypesByIntTimestampReverse",
			keyset: spanner.Key{100}.AsPrefix(),
			expected: []*FullType{
				{PKey: "pkey2", FTInt: 100, FTTimestamp: now.Add(2 * time.Second)},
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
			},
		},
		"TimestampDesc_Range1": {
			index: "FullTypesByIntTimestampReverse",
			keyset: &spanner.KeyRange{
				Start: spanner.Key{100, now.Add(time.Second)},
				End:   spanner.Key{100},
				Kind:  spanner.ClosedOpen,
			},
			expected: []*FullType{
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
			},
		},
		"TimestampDesc_Range2": {
			index: "FullTypesByIntTimestampReverse",
			keyset: &spanner.KeyRange{
				Start: spanner.Key{100, now.Add(time.Second)},
				End:   spanner.Key{100, time.Time{}},
				Kind:  spanner.ClosedOpen,
			},
			expected: []*FullType{
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
			},
		},
		"TimestampDesc_Range3": {
			index: "FullTypesByIntTimestampReverse",
			keyset: &spanner.KeyRange{
				Start: spanner.Key{100, now.Add(time.Second)},
				End:   spanner.Key{100, time.Unix(1, 0)},
				Kind:  spanner.ClosedOpen,
			},
			expected: []*FullType{
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
			},
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			var results []*FullType
			rows := client.Single().ReadUsingIndex(ctx, "FullTypes", tc.index, tc.keyset, []string{"PKey", "FTInt", "FTTimestamp"})
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

			if diff := cmp.Diff(tc.expected, results); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestIntegration_ReadWrite_Update(t *testing.T) {
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

	read := func(rows *spanner.RowIterator) ([]*Simple, error) {
		var results []*Simple
		err = rows.Do(func(row *spanner.Row) error {
			var s Simple
			if err := row.ToStruct(&s); err != nil {
				return err
			}
			results = append(results, &s)

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("Iterating over all row read: %v", err)
		}
		return results, nil
	}

	expected1 := []*Simple{
		{ID: 100, Value: "xxx"},
		{ID: 101, Value: "yyy"},
	}

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		stmt := spanner.NewStatement(`INSERT INTO Simple (Id, Value) VALUES(@id1, @value1), (@id2, @value2)`)
		stmt.Params = map[string]interface{}{
			"id1":    100,
			"value1": "xxx",
			"id2":    101,
			"value2": "yyy",
		}
		if _, err := tx.Update(ctx, stmt); err != nil {
			return err
		}

		rows := tx.Read(ctx, "Simple", spanner.AllKeys(), []string{"Id", "Value"})
		results, err := read(rows)
		if err != nil {
			return err
		}

		if diff := cmp.Diff(expected1, results); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

		stmt2 := spanner.NewStatement(`UPDATE Simple SET Value = "200" WHERE Id = @id`)
		stmt2.Params = map[string]interface{}{
			"id": 100,
		}
		if _, err := tx.Update(ctx, stmt2); err != nil {
			return err
		}

		stmt3 := spanner.NewStatement(`DELETE FROM Simple WHERE Id = @id`)
		stmt3.Params = map[string]interface{}{
			"id": 101,
		}
		if _, err := tx.Update(ctx, stmt3); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("ReadWriteTransaction: %v", err)
	}

	expected2 := []*Simple{
		{ID: 100, Value: "200"},
	}

	var results []*Simple
	rows := client.Single().Read(ctx, "Simple", spanner.AllKeys(), []string{"Id", "Value"})
	results, err = read(rows)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if diff := cmp.Diff(expected2, results); diff != "" {
		t.Errorf("(-got, +want)\n%s", diff)
	}
}

func TestIntegration_ReadWrite_BatchUpdate(t *testing.T) {
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

	read := func(rows *spanner.RowIterator) ([]*Simple, error) {
		var results []*Simple
		err = rows.Do(func(row *spanner.Row) error {
			var s Simple
			if err := row.ToStruct(&s); err != nil {
				return err
			}
			results = append(results, &s)

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("Iterating over all row read: %v", err)
		}
		return results, nil
	}

	expected1 := []*Simple{
		{ID: 100, Value: "xxx"},
		{ID: 101, Value: "yyy2"},
	}

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		stmt := spanner.NewStatement(`INSERT INTO Simple (Id, Value) VALUES(@id1, @value1), (@id2, @value2)`)
		stmt.Params = map[string]interface{}{
			"id1":    100,
			"value1": "xxx",
			"id2":    101,
			"value2": "yyy",
		}
		stmt2 := spanner.NewStatement(`UPDATE Simple SET Value = "yyy2" WHERE Id = 101`)
		affectedRows, err := tx.BatchUpdate(ctx, []spanner.Statement{stmt, stmt2})
		if err != nil {
			return err
		}
		if diff := cmp.Diff([]int64{2, 1}, affectedRows); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

		rows := tx.Read(ctx, "Simple", spanner.AllKeys(), []string{"Id", "Value"})
		results, err := read(rows)
		if err != nil {
			return err
		}

		if diff := cmp.Diff(expected1, results); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

		stmt3 := spanner.NewStatement(`UPDATE Simple SET Value = "200" WHERE Id = @id`)
		stmt3.Params = map[string]interface{}{
			"id": 100,
		}
		stmt4 := spanner.NewStatement(`UPDAT`)
		affectedRows, err = tx.BatchUpdate(ctx, []spanner.Statement{stmt3, stmt4})
		if err == nil {
			t.Errorf("unexpected success for batch update")
		}

		st := status.Convert(err)
		r := regexp.MustCompile(`Statement 1: .* is not valid DML`)
		if !r.MatchString(st.Message()) {
			t.Errorf("unexpected error message: %v", st.Message())
		}
		if st.Code() != codes.InvalidArgument {
			t.Errorf("expect error code %v but got %v", codes.InvalidArgument, st.Code())
		}
		if diff := cmp.Diff([]int64{1}, affectedRows); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("ReadWriteTransaction: %v", err)
	}

	expected2 := []*Simple{
		{ID: 100, Value: "200"},
		{ID: 101, Value: "yyy2"},
	}

	var results []*Simple
	rows := client.Single().Read(ctx, "Simple", spanner.AllKeys(), []string{"Id", "Value"})
	results, err = read(rows)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if diff := cmp.Diff(expected2, results); diff != "" {
		t.Errorf("(-got, +want)\n%s", diff)
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

	prepareSimpleData(t, ctx, client)

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
				{ID: 400, Value: "zzz"},
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
				{ID: 400, Value: "zzz"},
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
		{
			sql: "SELECT DISTINCT Value FROM Simple",
			expected: []*Simple{
				{ID: 0, Value: "xxx0"},
				{ID: 0, Value: "xxx1"},
				{ID: 0, Value: "xxx2"},
				{ID: 0, Value: "yyy"},
				{ID: 0, Value: "zzz"},
			},
		},
	}

	for _, tc := range table {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

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

func TestIntegration_Query2(t *testing.T) {
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
	prepareFullTypeData(t, ctx, client, now)

	table := []struct {
		sql      string
		params   map[string]interface{}
		expected []*FullType
	}{
		{
			sql: `SELECT PKey, FTInt, FTTimestamp FROM FullTypes WHERE FTInt = 100`,
			expected: []*FullType{
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
				{PKey: "pkey2", FTInt: 100, FTTimestamp: now.Add(2 * time.Second)},
			},
		},
		{
			sql: `SELECT PKey, FTInt, FTTimestamp FROM FullTypes WHERE FTTimestamp BETWEEN @t1 AND @t2`,
			params: map[string]interface{}{
				"t1": now,
				"t2": now.Add(2 * time.Second),
			},
			expected: []*FullType{
				{PKey: "pkey0", FTInt: 100, FTTimestamp: now},
				{PKey: "pkey1", FTInt: 100, FTTimestamp: now.Add(time.Second)},
				{PKey: "pkey2", FTInt: 100, FTTimestamp: now.Add(2 * time.Second)},
			},
		},
	}

	for _, tc := range table {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		stmt := spanner.NewStatement(tc.sql)
		stmt.Params = tc.params
		var results []*FullType
		rows := client.Single().Query(ctx, stmt)
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

	prepareSimpleData(t, ctx, client)

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
				{int64(400), string("zzz")},
			},
		},
		{
			sql:     "SELECT COUNT(1) FROM Simple",
			names:   []string{""}, // TODO
			columns: []interface{}{int64(0)},
			expected: [][]interface{}{
				{int64(6)},
			},
		},
		{
			sql:     "SELECT COUNT(1) AS count FROM Simple",
			names:   []string{"count"},
			columns: []interface{}{int64(0)},
			expected: [][]interface{}{
				{int64(6)},
			},
		},
		{
			sql:   "SELECT 10, -10, 010, 0x10, 0X10",
			names: []string{"", "", "", "", ""},
			columns: []interface{}{
				int64(0), int64(0), int64(0), int64(0), int64(0),
			},
			expected: [][]interface{}{
				{int64(10), int64(-10), int64(8), int64(16), int64(16)},
			},
		},
		{
			sql:   "SELECT 1.1, .1, 123.456e-67, .1E4, 58., 4e2",
			names: []string{"", "", "", "", "", ""},
			columns: []interface{}{
				float64(0), float64(0), float64(0), float64(0), float64(0), float64(0),
			},
			expected: [][]interface{}{
				{float64(1.1), float64(.1), float64(123.456e-67), float64(.1e4), float64(58.), float64(4e2)},
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

func TestIntegration_CreateDatabase(t *testing.T) {
	ctx := context.Background()
	projectID, instanceID, databaseID := "fake", "fake", "fake"

	srv, conn, err := Run()
	if err != nil {
		t.Fatalf("err %v", err)
	}
	defer srv.Stop()

	adminclient, err := admindatabasev1.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("failed to connect fake spanner server: %v", err)
	}

	f, err := os.Open("./testdata/schema.sql")
	if err != nil {
		t.Fatalf("err %v", err)
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	var stmts []string
	for _, s := range strings.Split(string(b), ";") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		stmts = append(stmts, s)
	}

	op, err := adminclient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
		ExtraStatements: stmts,
	})
	if err != nil {
		t.Fatal(err)
	}

	db, err := op.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait err: %v", err)
	}
	if got, expect := db.GetName(), fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID); got != expect {
		t.Fatalf("expected %s but got %s", expect, db.GetName())
	}
}

func TestIntegration_UpdateDatbaseDdl(t *testing.T) {
	ctx := context.Background()
	dbName := "projects/fake/instances/fake/databases/fake"

	srv, conn, err := Run()
	if err != nil {
		t.Fatalf("err %v", err)
	}
	defer srv.Stop()

	adminclient, err := admindatabasev1.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("failed to connect fake spanner server: %v", err)
	}

	f, err := os.Open("./testdata/schema.sql")
	if err != nil {
		t.Fatalf("err %v", err)
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	var stmts []string
	for _, s := range strings.Split(string(b), ";") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		stmts = append(stmts, s)
	}

	op, err := adminclient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbName,
		Statements: stmts,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := op.Wait(ctx); err != nil {
		t.Fatalf("Wait err: %v", err)
	}
}

func TestIntegration_DropDatabase(t *testing.T) {
	ctx := context.Background()
	projectID, instanceID, databaseID := "fake", "fake", "fake"

	srv, conn, err := Run()
	if err != nil {
		t.Fatalf("err %v", err)
	}
	defer srv.Stop()

	adminclient, err := admindatabasev1.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("failed to connect fake spanner server: %v", err)
	}

	// prepare database
	f, err := os.Open("./testdata/schema.sql")
	if err != nil {
		t.Fatalf("err %v", err)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	var stmts []string
	for _, s := range strings.Split(string(b), ";") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		stmts = append(stmts, s)
	}
	if _, err := adminclient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
		ExtraStatements: stmts,
	}); err != nil {
		t.Fatal(err)
	}

	if err := adminclient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{
		Database: fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID),
	}); err != nil {
		t.Fatal(err)
	}
}
