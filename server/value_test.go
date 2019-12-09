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
	"testing"
	"time"

	"database/sql"
	"github.com/MakeNowJust/memefish/pkg/ast"
	structpb "github.com/golang/protobuf/ptypes/struct"
	cmp "github.com/google/go-cmp/cmp"
	uuidpkg "github.com/google/uuid"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
			value:    makeStringValue("eHh4"), // xxx
			typ:      ValueType{Code: TCBytes},
			expected: []byte("xxx"),
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
			expected: makeTestArray(TCBool, true, false),
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
			expected: makeTestArray(TCString, "xxx", "yyy"),
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
			expected: makeTestArray(TCInt64, 100, 200),
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
			expected: makeTestArray(TCFloat64, float64(0.1), float64(0.2)),
		},
		"ArrayBytes": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("eHl6"), // xyz
				makeStringValue("eHh4"), // xxx
			)),
			typ: ValueType{
				Code:      TCArray,
				ArrayType: &ValueType{Code: TCBytes},
			},
			expected: makeTestArray(TCBytes, []byte("xyz"), []byte("xxx")),
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
			iter := rows{rows: r, resultItems: []ResultItem{item}, transaction: &transaction{status: 1}}

			var rows [][]interface{}
			err = iter.Do(func(row []interface{}) error {
				rows = append(rows, row)
				return nil
			})
			if err != nil {
				t.Fatalf("unexpected error in iteration: %v", err)
			}

			if len(rows) != 1 {
				t.Errorf("there should be only 1 row")
			}
			if diff := cmp.Diff(tc.expected, rows[0][0]); diff != "" {
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
			res, err := makeValueFromSpannerValue("foo", tc.value, tc.typ)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.expected, res); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}

}

func TestMakeValueFromSpannerValue_Error(t *testing.T) {
	table := map[string]struct {
		value *structpb.Value
		typ   *spannerpb.Type
		msg   *regexp.Regexp
	}{
		"Int_InvalidValue": {
			value: makeStringValue("xxx"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected INT64`),
		},
		"Int_InvalidType": {
			value: makeBoolValue(true),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_INT64,
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected INT64`),
		},

		"Timestamp_InvalidValue": {
			value: makeStringValue("xxx"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_TIMESTAMP,
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected TIMESTAMP`),
		},
		"Timestamp_InvalidType": {
			value: makeBoolValue(true),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_TIMESTAMP,
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected TIMESTAMP`),
		},

		"Date_InvalidValue": {
			value: makeStringValue("xxx"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_DATE,
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected DATE`),
		},
		"Date_InvalidType": {
			value: makeBoolValue(true),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_DATE,
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected DATE`),
		},

		"Bytes_InvalidValue": {
			value: makeStringValue("x"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_BYTES,
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected BYTES`),
		},
		"Bytes_InvalidType": {
			value: makeBoolValue(true),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_BYTES,
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected BYTES`),
		},

		"Array_InvalidType": {
			value: makeStringValue("xxx"),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_STRING,
				},
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo: Expected ARRAY<STRING>`),
		},
		"Array_InvalidElementValue": {
			value: makeListValueAsValue(makeListValue(
				makeStringValue("100"),
				makeStringValue("xxx"),
			)),
			typ: &spannerpb.Type{
				Code: spannerpb.TypeCode_ARRAY,
				ArrayElementType: &spannerpb.Type{
					Code: spannerpb.TypeCode_INT64,
				},
			},
			msg: regexp.MustCompile(`^Invalid value for bind parameter foo\[1\]: Expected INT64`),
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			_, err := makeValueFromSpannerValue("foo", tc.value, tc.typ)
			if err == nil {
				t.Fatalf("unexpected success")
			}
			st := status.Convert(err)
			if st.Code() != codes.InvalidArgument {
				t.Errorf("expect code to be %v, but got %v", codes.InvalidArgument, st.Code())
			}
			if !tc.msg.MatchString(st.Message()) {
				t.Errorf("unexpected error message: \n %q\n expected:\n %q", st.Message(), tc.msg)
			}
		})
	}

}
