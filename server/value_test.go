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
	"time"

	"database/sql"
	"github.com/MakeNowJust/memefish/pkg/ast"
	structpb "github.com/golang/protobuf/ptypes/struct"
	cmp "github.com/google/go-cmp/cmp"
	uuidpkg "github.com/google/uuid"
)

func TestDatabaseEncDec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	table := map[string]struct {
		value    *structpb.Value
		typ      ValueType
		expected interface{}
	}{
		"Bool": {
			value:    makeBoolValue(true),
			typ:      ValueType{Code: TCBool},
			expected: true,
		},
		"Int": {
			value:    makeStringValue("100"),
			typ:      ValueType{Code: TCInt64},
			expected: int64(100),
		},
		"Float": {
			value:    makeNumberValue(0.5),
			typ:      ValueType{Code: TCFloat64},
			expected: float64(0.5),
		},
		"Bytes": {
			value:    makeStringValue("xyz"),
			typ:      ValueType{Code: TCBytes},
			expected: []byte("xyz"),
		},
		"Timestamp": {
			value:    makeStringValue("2012-03-04T00:00:00.123456789Z"),
			typ:      ValueType{Code: TCTimestamp},
			expected: "2012-03-04T00:00:00.123456789Z",
		},
		"ArrayBool": {
			value: makeListValueAsValue(makeListValue(
				makeBoolValue(true),
				makeBoolValue(false),
			)),
			typ: ValueType{
				Code:      TCArray,
				ArrayType: &ValueType{Code: TCBool},
			},
			expected: []bool{
				true, false,
			},
		},
		"ArrayString": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("xxx"),
				makeStringValue("yyy"),
			)),
			typ: ValueType{
				Code:      TCArray,
				ArrayType: &ValueType{Code: TCString},
			},
			expected: []string{
				"xxx", "yyy",
			},
		},
		"ArrayInt": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("100"),
				makeStringValue("200"),
			)),
			typ: ValueType{
				Code:      TCArray,
				ArrayType: &ValueType{Code: TCInt64},
			},
			expected: []int64{
				int64(100), int64(200),
			},
		},
		"ArrayFloat": {
			value: makeListValueAsValue(makeListValue(
				makeNumberValue(0.1),
				makeNumberValue(0.2),
			)),
			typ: ValueType{
				Code:      TCArray,
				ArrayType: &ValueType{Code: TCFloat64},
			},
			expected: []float64{
				float64(0.1), float64(0.2),
			},
		},
		"ArrayBytes": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("xyz"),
				makeStringValue("xxx"),
			)),
			typ: ValueType{
				Code:      TCArray,
				ArrayType: &ValueType{Code: TCBytes},
			},
			expected: [][]byte{
				[]byte("xyz"), []byte("xxx"),
			},
		},
	}

	uuid := uuidpkg.New().String()
	db, err := sql.Open("sqlite3_spanner", fmt.Sprintf("file:%s.db?cache=shared&mode=memory", uuid))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "CREATE TABLE test (js JSON)"); err != nil {
		t.Fatal(err)
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if _, err := db.ExecContext(ctx, "DELETE FROM test"); err != nil {
					t.Fatalf("delete failed: %v", err)
				}
			}()

			column := Column{
				ast:        &ast.ColumnDef{Name: &ast.Ident{Name: "Id"}},
				valueType:  tc.typ,
				dbDataType: DBDTJson,
			}

			v, err := spannerValue2DatabaseValue(tc.value, column)
			if err != nil {
				t.Fatalf("spannerValue2DatabaseValue failed: %v", err)
			}

			if _, err := db.ExecContext(ctx, "INSERT INTO test VALUES(?)", v); err != nil {
				t.Fatalf("insert failed: %v", err)
			}

			r, err := db.QueryContext(ctx, "SELECT js FROM test")
			if err != nil {
				t.Fatalf("select failed: %v", err)
			}
			defer r.Close()

			item := createResultItemFromColumn(&column)
			iter := rows{rows: r, resultItems: []ResultItem{item}}

			result, next := iter.Next()
			if !next {
				t.Errorf("there should be only 1 row")
			}
			if diff := cmp.Diff(tc.expected, result[0]); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}

}
