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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MakeNowJust/memefish/pkg/ast"
	"github.com/MakeNowJust/memefish/pkg/parser"
	"github.com/MakeNowJust/memefish/pkg/token"
	structpb "github.com/golang/protobuf/ptypes/struct"
	cmp "github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	allSchema    = []string{schemaSimple, schemaCompositePrimaryKeys, schemaFullTypes, schemaArrayTypes}
	schemaSimple = `CREATE TABLE Simple (
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id);
`
	schemaCompositePrimaryKeys = `CREATE TABLE CompositePrimaryKeys (
  Id INT64 NOT NULL,
  PKey1 STRING(32) NOT NULL,
  PKey2 INT64 NOT NULL,
  Error INT64 NOT NULL,
  X STRING(32) NOT NULL,
  Y STRING(32) NOT NULL,
  Z STRING(32) NOT NULL,
) PRIMARY KEY(PKey1, PKey2 DESC);
CREATE INDEX CompositePrimaryKeysByXY ON CompositePrimaryKeys(X, Y DESC) STORING (Z);
CREATE INDEX CompositePrimaryKeysByError ON CompositePrimaryKeys(Error);
`
	schemaFullTypes = `CREATE TABLE FullTypes (
  PKey STRING(32) NOT NULL,
  FTString STRING(32) NOT NULL,
  FTStringNull STRING(32),
  FTBool BOOL NOT NULL,
  FTBoolNull BOOL,
  FTBytes BYTES(32) NOT NULL,
  FTBytesNull BYTES(32),
  FTTimestamp TIMESTAMP NOT NULL,
  FTTimestampNull TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  FTInt INT64 NOT NULL,
  FTIntNull INT64,
  FTFloat FLOAT64 NOT NULL,
  FTFloatNull FLOAT64,
  FTDate DATE NOT NULL,
  FTDateNull DATE,
) PRIMARY KEY(PKey);
CREATE UNIQUE INDEX FullTypesByFTString ON FullTypes(FTString);
CREATE UNIQUE INDEX FullTypesByIntDate ON FullTypes(FTInt, FTDate);
CREATE INDEX FullTypesByIntTimestamp ON FullTypes(FTInt, FTTimestamp);
CREATE INDEX FullTypesByTimestamp ON FullTypes(FTTimestamp);
`
	schemaArrayTypes = `CREATE TABLE ArrayTypes (
  Id INT64 NOT NULL,
  ArrayString ARRAY<STRING(32)>,
  ArrayBool ARRAY<BOOL>,
  ArrayBytes ARRAY<BYTES(32)>,
  ArrayTimestamp ARRAY<TIMESTAMP>,
  ArrayInt ARRAY<INT64>,
  ArrayFloat ARRAY<FLOAT64>,
  ArrayDate ARRAY<DATE>,
) PRIMARY KEY(Id);
`
	compositePrimaryKeysKeys = []string{
		"Id", "PKey1", "PKey2", "Error", "X", "Y", "Z",
	}
	fullTypesKeys = []string{
		"PKey",
		"FTString",
		"FTStringNull",
		"FTBool",
		"FTBoolNull",
		"FTBytes",
		"FTBytesNull",
		"FTTimestamp",
		"FTTimestampNull",
		"FTInt",
		"FTIntNull",
		"FTFloat",
		"FTFloatNull",
		"FTDate",
		"FTDateNull",
	}
	arrayTypesKeys = []string{
		"Id",
		"ArrayString",
		"ArrayBool",
		"ArrayBytes",
		"ArrayTimestamp",
		"ArrayInt",
		"ArrayFloat",
		"ArrayDate",
	}
)

var initialData = []struct {
	table  string
	cols   []string
	values [][]*structpb.Value
}{
	{
		table: "Simple",
		cols:  []string{"Id", "Value"},
		values: [][]*structpb.Value{
			{
				makeStringValue("100"),
				makeStringValue("xxx"),
			},
		},
	},
	{
		table: "CompositePrimaryKeys",
		cols:  compositePrimaryKeysKeys,
		values: [][]*structpb.Value{
			{
				makeStringValue("1"),        // Id INT64 NOT NULL,
				makeStringValue("pkey1xxx"), // PKey1 STRING(32) NOT NULL,
				makeStringValue("100"),      // PKey2 INT64 NOT NULL,
				makeStringValue("2"),        // Error INT64 NOT NULL,
				makeStringValue("x1"),       // X STRING(32) NOT NULL,
				makeStringValue("y1"),       // Y STRING(32) NOT NULL,
				makeStringValue("z1"),       // Z STRING(32) NOT NULL,
			},
		},
	},
	{
		table: "FullTypes",
		cols:  fullTypesKeys,
		values: [][]*structpb.Value{
			{
				makeStringValue("xxx"),                            // PKey STRING(32) NOT NULL,
				makeStringValue("xxx"),                            // FTString STRING(32) NOT NULL,
				makeStringValue("xxx"),                            // FTStringNull STRING(32),
				makeBoolValue(true),                               // FTBool BOOL NOT NULL,
				makeBoolValue(true),                               // FTBoolNull BOOL,
				makeStringValue("xyz"),                            // FTBytes BYTES(32) NOT NULL,
				makeStringValue("xyz"),                            // FTBytesNull BYTES(32),
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestampNull TIMESTAMP,
				makeStringValue("100"),                            // FTInt INT64 NOT NULL,
				makeStringValue("100"),                            // FTIntNull INT64,
				makeNumberValue(0.5),                              // FTFloat FLOAT64 NOT NULL,
				makeNumberValue(0.5),                              // FTFloatNull FLOAT64,
				makeStringValue("2012-03-04"),                     // FTDate DATE NOT NULL,
				makeStringValue("2012-03-04"),                     // FTDateNull DATE,
			},
		},
	},
	{
		table: "FullTypes",
		cols:  fullTypesKeys,
		values: [][]*structpb.Value{
			{
				makeStringValue("yyy"),                            // PKey STRING(32) NOT NULL,
				makeStringValue("yyy"),                            // FTString STRING(32) NOT NULL,
				makeStringValue("yyy"),                            // FTStringNull STRING(32),
				makeBoolValue(true),                               // FTBool BOOL NOT NULL,
				makeBoolValue(true),                               // FTBoolNull BOOL,
				makeStringValue("xyz"),                            // FTBytes BYTES(32) NOT NULL,
				makeStringValue("xyz"),                            // FTBytesNull BYTES(32),
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestampNull TIMESTAMP,
				makeStringValue("101"),                            // FTInt INT64 NOT NULL,
				makeStringValue("101"),                            // FTIntNull INT64,
				makeNumberValue(0.5),                              // FTFloat FLOAT64 NOT NULL,
				makeNumberValue(0.5),                              // FTFloatNull FLOAT64,
				makeStringValue("2012-03-04"),                     // FTDate DATE NOT NULL,
				makeStringValue("2012-03-04"),                     // FTDateNull DATE,
			},
		},
	},
	{
		table: "ArrayTypes",
		cols:  arrayTypesKeys,
		values: [][]*structpb.Value{
			{
				makeStringValue("100"),
				makeListValueAsValue(makeListValue(
					makeStringValue("yyy"),
					makeStringValue("yyy"),
				)),
				makeListValueAsValue(makeListValue(
					makeBoolValue(true),
					makeBoolValue(true),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("xyz"),
					makeStringValue("xyz"),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("2012-03-04T12:34:56.123456789Z"),
					makeStringValue("2012-03-04T12:34:56.123456789Z"),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("101"),
					makeStringValue("101"),
				)),
				makeListValueAsValue(makeListValue(
					makeNumberValue(0.5),
					makeNumberValue(0.5),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("2012-03-04"),
					makeStringValue("2012-03-04"),
				)),
			},
		},
	},
}

func createInitialData(t *testing.T, ctx context.Context, db Database) {
	for _, d := range initialData {
		for _, values := range d.values {
			listValues := []*structpb.ListValue{
				{Values: values},
			}
			if err := db.Insert(ctx, d.table, d.cols, listValues); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}
	}

}

func makeNullValue() *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_NullValue{}}
}

func makeNumberValue(n float64) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: n}}
}

func makeStringValue(s string) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: s}}
}

func makeBoolValue(b bool) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: b}}
}

func makeStructValue(s *structpb.Struct) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: s}}
}

func makeListValue(vs ...*structpb.Value) *structpb.ListValue {
	return &structpb.ListValue{Values: vs}
}

func makeListValueAsValue(v *structpb.ListValue) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: v}}
}

func makeTestValue(v interface{}) Value {
	switch vv := v.(type) {
	case string:
		return Value{
			Data: v,
			Type: ValueType{Code: TCString},
		}
	case []string:
		return Value{
			Data: v,
			Type: ValueType{
				Code:      TCArray,
				ArrayType: &ValueType{Code: TCString},
			},
		}
	case int64:
		return Value{
			Data: v,
			Type: ValueType{Code: TCInt64},
		}
	case int:
		return Value{
			Data: int64(vv),
			Type: ValueType{Code: TCInt64},
		}
	case []int64:
		return Value{
			Data: v,
			Type: ValueType{
				Code:      TCArray,
				ArrayType: &ValueType{Code: TCInt64},
			},
		}
	default:
		panic(fmt.Sprintf("fix makeTestValue to be able to convert interface{} to Value: %T", v))
	}
}

func makeTestArray(code TypeCode, vs ...interface{}) interface{} {
	switch code {
	case TCBool:
		arr := make([]*bool, len(vs))
		for i := range vs {
			if vs[i] == nil {
				arr[i] = nil
			} else {
				s := new(bool)
				*s = vs[i].(bool)
				arr[i] = s
			}
		}
		return &ArrayBool{Data: arr}
	case TCString:
		arr := make([]*string, len(vs))
		for i := range vs {
			if vs[i] == nil {
				arr[i] = nil
			} else {
				s := new(string)
				*s = vs[i].(string)
				arr[i] = s
			}
		}
		return &ArrayString{Data: arr}
	case TCInt64:
		arr := make([]*int64, len(vs))
		for i := range vs {
			if vs[i] == nil {
				arr[i] = nil
			} else {
				s := new(int64)
				vv, ok := vs[i].(int64)
				if !ok {
					vv = int64(vs[i].(int))
				}
				*s = vv
				arr[i] = s
			}
		}
		return &ArrayInt64{Data: arr}
	case TCFloat64:
		arr := make([]*float64, len(vs))
		for i := range vs {
			if vs[i] == nil {
				arr[i] = nil
			} else {
				s := new(float64)
				*s = vs[i].(float64)
				arr[i] = s
			}
		}
		return &ArrayFloat64{Data: arr}
	case TCBytes:
		arr := make([][]byte, len(vs))
		for i := range vs {
			if vs[i] == nil {
				arr[i] = nil
			} else {
				arr[i] = vs[i].([]byte)
			}
		}
		return &ArrayBytes{Data: arr}
	default:
		panic(fmt.Sprintf("fix makeTestArray to be able to convert interface{}: %v", code))
	}
}

func parseDDL(t *testing.T, s string) []ast.DDL {
	var ddls []ast.DDL
	stmts := strings.Split(s, ";")
	for _, stmt := range stmts {
		stmt := strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		ddl, err := (&parser.Parser{
			Lexer: &parser.Lexer{
				File: &token.File{FilePath: "", Buffer: stmt},
			},
		}).ParseDDL()
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ddls = append(ddls, ddl)
	}

	return ddls
}

func TestApplyDDL(t *testing.T) {
	table := []struct {
		ddl string
	}{
		{
			ddl: schemaSimple,
		},
		{
			ddl: schemaCompositePrimaryKeys,
		},
		{
			ddl: schemaFullTypes,
		},
	}

	for _, tt := range table {
		ctx := context.Background()
		db := newDatabase()
		ddls := parseDDL(t, tt.ddl)
		for _, ddl := range ddls {
			db.ApplyDDL(ctx, ddl)
		}
	}
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	db := newDatabase()
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
		tbl   string
		idx   string
		cols  []string
		ks    *KeySet
		limit int64

		expected [][]interface{}
	}{
		"SimpleFull": {
			tbl:   "Simple",
			cols:  []string{"Id", "Value"},
			ks:    &KeySet{All: true},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"SimplePartialColumn": {
			tbl:   "Simple",
			cols:  []string{"Value"},
			ks:    &KeySet{All: true},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{"xxx"},
				[]interface{}{"yyy"},
				[]interface{}{"zzz"},
			},
		},
		"SimpleColumnOrder": {
			tbl:   "Simple",
			cols:  []string{"Value", "Id"},
			ks:    &KeySet{All: true},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{"xxx", int64(100)},
				[]interface{}{"yyy", int64(200)},
				[]interface{}{"zzz", int64(300)},
			},
		},
		"SimpleLimit": {
			tbl:   "Simple",
			cols:  []string{"Id", "Value"},
			ks:    &KeySet{All: true},
			limit: 2,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
			},
		},
		"SimpleDuplicateColumns": {
			tbl:   "Simple",
			cols:  []string{"Id", "Value", "Id", "Value"},
			ks:    &KeySet{All: true},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx", int64(100), "xxx"},
				[]interface{}{int64(200), "yyy", int64(200), "yyy"},
				[]interface{}{int64(300), "zzz", int64(300), "zzz"},
			},
		},

		// Simple KeySet
		"Simple_SingleKey": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("100")),
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_SingleKey_NotFound": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("1000")),
				},
			},
			limit:    100,
			expected: nil,
		},
		"Simple_MultiKeys": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("100")),
					makeListValue(makeStringValue("300")),
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_MultiKeys_PartialNotFound": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("100")),
					makeListValue(makeStringValue("300")),
					makeListValue(makeStringValue("1000")),
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_KeyRange": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("100")),
						end:         makeListValue(makeStringValue("300")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_KeyRange2": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("150")),
						end:         makeListValue(makeStringValue("250")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_KeyRange_OpenClose": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("100")),
						end:         makeListValue(makeStringValue("300")),
						startClosed: false,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_KeyRange_CloseOpen": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("100")),
						end:         makeListValue(makeStringValue("300")),
						startClosed: true,
						endClosed:   false,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_KeyRange_OpenOpen": {
			tbl:  "Simple",
			cols: []string{"Id", "Value"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("100")),
						end:         makeListValue(makeStringValue("300")),
						startClosed: false,
						endClosed:   false,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},

		// Composite Keys
		"CompositePrimaryKeys_Keys": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("aaa"), makeStringValue("1")),
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(1), "aaa", int64(1)},
			},
		},
		"CompositePrimaryKeys_Keys_2": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("aaa"), makeStringValue("100")),
					makeListValue(makeStringValue("xxx"), makeStringValue("2")),
					makeListValue(makeStringValue("ccc"), makeStringValue("3")),
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(4), "ccc", int64(3)},
			},
		},

		// Composite Keys KeyRange
		"CompositePrimaryKeys_KeySet": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb"), makeStringValue("3")),
						end:         makeListValue(makeStringValue("ccc"), makeStringValue("3")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(3), "bbb", int64(3)},
				[]interface{}{int64(4), "ccc", int64(3)},
			},
		},

		// Composite SecondaryIndex
		"CompositePrimaryKeys_Index": {
			tbl:   "CompositePrimaryKeys",
			idx:   "CompositePrimaryKeysByXY",
			cols:  []string{"PKey1", "PKey2", "X", "Y", "Z"},
			ks:    &KeySet{All: true},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{"bbb", int64(3), "x1", "y3", "z"},
				[]interface{}{"bbb", int64(2), "x1", "y2", "z"},
				[]interface{}{"aaa", int64(1), "x1", "y1", "z"},
				[]interface{}{"ccc", int64(4), "x2", "y5", "z"},
				[]interface{}{"ccc", int64(3), "x2", "y4", "z"},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			it, err := db.Read(ctx, tt.tbl, tt.idx, tt.cols, tt.ks, tt.limit)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			var rows [][]interface{}
			for {
				row, ok := it.Next()
				if !ok {
					break
				}

				rows = append(rows, row)
			}

			if diff := cmp.Diff(tt.expected, rows); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestReadError(t *testing.T) {
	ctx := context.Background()
	db := newDatabase()
	for _, s := range allSchema {
		ddls := parseDDL(t, s)
		for _, ddl := range ddls {
			db.ApplyDDL(ctx, ddl)
		}
	}

	table := map[string]struct {
		tbl   string
		idx   string
		cols  []string
		ks    *KeySet
		limit int64
		code  codes.Code
		msg   *regexp.Regexp
	}{
		"EmptyColumns": {
			tbl:   "Simple",
			idx:   "",
			cols:  []string{},
			ks:    &KeySet{All: true},
			limit: 100,
			code:  codes.InvalidArgument,
			msg:   regexp.MustCompile(`Invalid StreamingRead request`),
		},
		"EmptyTableName": {
			tbl:   "",
			idx:   "",
			cols:  []string{"Id"},
			ks:    &KeySet{All: true},
			limit: 100,
			code:  codes.InvalidArgument,
			msg:   regexp.MustCompile(`Invalid StreamingRead request`),
		},
		"EmptyKeySet": {
			tbl:   "Simple",
			idx:   "",
			cols:  []string{"Id"},
			ks:    nil,
			limit: 100,
			code:  codes.InvalidArgument,
			msg:   regexp.MustCompile(`Invalid StreamingRead request`),
		},

		"TableNotFound": {
			tbl:   "NotExistTable",
			idx:   "",
			cols:  []string{"Id"},
			ks:    &KeySet{All: true},
			limit: 100,
			code:  codes.NotFound,
			msg:   regexp.MustCompile(`Table not found`),
		},
		"IndexNotFound": {
			tbl:   "Simple",
			idx:   "NotExistIndex",
			cols:  []string{"Id", "Value"},
			ks:    &KeySet{All: true},
			limit: 100,
			code:  codes.NotFound,
			msg:   regexp.MustCompile(`Index not found on table`),
		},
		"ColumnNotFound": {
			tbl:   "Simple",
			idx:   "",
			cols:  []string{"Id", "Xyz"},
			ks:    &KeySet{All: true},
			limit: 100,
			code:  codes.NotFound,
			msg:   regexp.MustCompile(`Column not found`),
		},
		"ColumnNotFoundOnSecondaryIndex": {
			tbl:   "CompositePrimaryKeys",
			idx:   "CompositePrimaryKeysByXY",
			cols:  []string{"Id", "PKey1", "PKey2", "X", "Y", "Z"},
			ks:    &KeySet{All: true},
			limit: 100,
			code:  codes.Unimplemented, // real spanner returns Unimplemented
			msg:   regexp.MustCompile(`Reading non-indexed columns using an index is not supported. Consider adding Id to the index using a STORING clause`),
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			_, err := db.Read(ctx, tt.tbl, tt.idx, tt.cols, tt.ks, tt.limit)
			st := status.Convert(err)
			if st.Code() != tt.code {
				t.Errorf("expect code to be %v but got %v", tt.code, st.Code())
			}
			if !tt.msg.MatchString(st.Message()) {
				t.Errorf("unexpected error message: %v", st.Message())
			}
		})
	}
}

func TestRead_FullType_Range(t *testing.T) {
	ctx := context.Background()
	db := newDatabase()

	schema := []string{
		`CREATE TABLE FTString ( Id STRING(MAX) NOT NULL ) PRIMARY KEY (Id)`,
		`CREATE TABLE FTTimestamp ( Id TIMESTAMP NOT NULL ) PRIMARY KEY (Id)`,
	}
	for _, s := range schema {
		ddls := parseDDL(t, s)
		for _, ddl := range ddls {
			db.ApplyDDL(ctx, ddl)
		}
	}

	for _, query := range []string{
		`INSERT INTO FTString VALUES("aaa")`,
		`INSERT INTO FTString VALUES("aaaa")`,
		`INSERT INTO FTString VALUES("aaab")`,
		`INSERT INTO FTString VALUES("11")`,
		`INSERT INTO FTString VALUES("100")`,

		`INSERT INTO FTTimestamp VALUES("2012-03-04T12:34:56.123456789Z")`,
		`INSERT INTO FTTimestamp VALUES("2012-03-04T00:00:00.123456789Z")`,
		`INSERT INTO FTTimestamp VALUES("2012-03-04T00:00:00.999999999Z")`,
		`INSERT INTO FTTimestamp VALUES("2012-03-05T00:00:00.000000000Z")`,
	} {
		if _, err := db.db.ExecContext(ctx, query); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	table := map[string]struct {
		tbl         string
		cols        []string
		start       *structpb.ListValue
		end         *structpb.ListValue
		startClosed bool
		endClosed   bool

		expected [][]interface{}
	}{
		"FTString_1": {
			tbl:         "FTString",
			cols:        []string{"Id"},
			start:       makeListValue(makeStringValue("aaa")),
			end:         makeListValue(makeStringValue("aaz")),
			startClosed: true, endClosed: true,
			expected: [][]interface{}{
				[]interface{}{"aaa"},
				[]interface{}{"aaaa"},
				[]interface{}{"aaab"},
			},
		},
		"FTString_2": {
			tbl:         "FTString",
			cols:        []string{"Id"},
			start:       makeListValue(makeStringValue("10")),
			end:         makeListValue(makeStringValue("11")),
			startClosed: true, endClosed: true,
			expected: [][]interface{}{
				[]interface{}{"100"},
				[]interface{}{"11"},
			},
		},
		"FTTimestamp_1": {
			tbl:         "FTTimestamp",
			cols:        []string{"Id"},
			start:       makeListValue(makeStringValue("2012-03-04T00:00:00.000000000Z")),
			end:         makeListValue(makeStringValue("2012-03-05T00:00:00.000000000Z")),
			startClosed: true, endClosed: false,
			expected: [][]interface{}{

				[]interface{}{"2012-03-04T00:00:00.123456789Z"},
				[]interface{}{"2012-03-04T00:00:00.999999999Z"},
				[]interface{}{"2012-03-04T12:34:56.123456789Z"},
			},
		},
		"FTTimestamp_2": {
			tbl:         "FTTimestamp",
			cols:        []string{"Id"},
			start:       makeListValue(makeStringValue("2012-03-04T00:00:00.000000000Z")),
			end:         makeListValue(makeStringValue("2012-03-04T00:00:01.000000000Z")),
			startClosed: true, endClosed: false,
			expected: [][]interface{}{

				[]interface{}{"2012-03-04T00:00:00.123456789Z"},
				[]interface{}{"2012-03-04T00:00:00.999999999Z"},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ks := &KeySet{
				Ranges: []*KeyRange{
					{
						start:       tt.start,
						end:         tt.end,
						startClosed: tt.startClosed,
						endClosed:   tt.endClosed,
					},
				},
			}
			it, err := db.Read(ctx, tt.tbl, "", tt.cols, ks, 100)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			var rows [][]interface{}
			for {
				row, ok := it.Next()
				if !ok {
					break
				}

				rows = append(rows, row)
			}

			if diff := cmp.Diff(tt.expected, rows); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestQuery(t *testing.T) {
	ctx := context.Background()
	db := newDatabase()
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
		params   map[string]Value
		expected [][]interface{}
	}{
		"Simple_Star": {
			sql: "SELECT * FROM Simple",
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Identifer": {
			sql: "SELECT Value, Id FROM Simple",
			expected: [][]interface{}{
				[]interface{}{"xxx", int64(100)},
				[]interface{}{"yyy", int64(200)},
				[]interface{}{"zzz", int64(300)},
			},
		},
		"Simple_Alias": {
			sql: "SELECT Id a, Value b FROM Simple",
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_IntLiteral": {
			sql: "SELECT Id, Value FROM Simple WHERE Id = 200",
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_Where_IntLiteral2": {
			sql: "SELECT Id, Value FROM Simple WHERE Id >= 200",
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_IntLiteral3": {
			sql: "SELECT Id, Value FROM Simple WHERE Id = +200",
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_Where_IntLiteral4": {
			sql:      "SELECT Id, Value FROM Simple WHERE Id = -200",
			expected: nil,
		},
		"Simple_Where_StringLiteral": {
			sql: `SELECT Id, Value FROM Simple WHERE Value = "xxx"`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_Where_StringLiteral2": {
			sql: `SELECT Id, Value FROM Simple WHERE Value > "xxx"`,
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_Param": {
			sql: `SELECT Id, Value FROM Simple WHERE Id = @id`,
			params: map[string]Value{
				"id": makeTestValue(100),
			},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_Where_AND": {
			sql: `SELECT Id, Value FROM Simple WHERE Id > @id AND Value = @val`,
			params: map[string]Value{
				"id":  makeTestValue(100),
				"val": makeTestValue("zzz"),
			},
			expected: [][]interface{}{
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_Paren": {
			sql: `SELECT Id, Value FROM Simple WHERE Id >= @id AND (Value = @val1 OR Value = @val2)`,
			params: map[string]Value{
				"id":   makeTestValue(100),
				"val1": makeTestValue("zzz"),
				"val2": makeTestValue("xxx"),
			},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_LIKE": {
			sql:    `SELECT Id, Value FROM Simple WHERE Value LIKE "x%"`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_Where_NOT_LIKE": {
			sql:    `SELECT Id, Value FROM Simple WHERE Value NOT LIKE "x%"`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_IN": {
			sql:    `SELECT Id, Value FROM Simple WHERE Id IN (100, 300)`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_NOT_IN": {
			sql:    `SELECT Id, Value FROM Simple WHERE Id NOT IN (100, 300)`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_Where_IS_NULL": {
			sql:      `SELECT Id, Value FROM Simple WHERE Value IS NULL`,
			params:   map[string]Value{},
			expected: nil,
		},
		"Simple_Where_IS_NOT_NULL": {
			sql:    `SELECT Id, Value FROM Simple WHERE Value IS NOT NULL`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_IS_BOOL": {
			sql:      `SELECT Id, Value FROM Simple WHERE Id IS TRUE`,
			params:   map[string]Value{},
			expected: nil,
		},
		"Simple_Where_IS_NOT_BOOL": {
			sql:    `SELECT Id, Value FROM Simple WHERE Id IS NOT TRUE`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_BETWEEN": {
			sql:    `SELECT Id, Value FROM Simple WHERE Id BETWEEN 200 AND 300`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_Where_NOT_BETWEEN": {
			sql:    `SELECT Id, Value FROM Simple WHERE Id NOT BETWEEN 200 AND 300`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},

		"Simple_Where_STARTS_WITH": {
			sql:    `SELECT Id, Value FROM Simple WHERE STARTS_WITH(Value, "x")`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_Where_STARTS_WITH_PARAM": {
			sql: `SELECT Id, Value FROM Simple WHERE STARTS_WITH(Value, @x)`,
			params: map[string]Value{
				"x": makeTestValue("xx"),
			},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},

		"Simple_GROUP": {
			sql:    `SELECT Id, Value FROM Simple GROUP BY Value`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_GROUP_HAVING": {
			sql:    `SELECT Id, Value FROM Simple GROUP BY Value HAVING Value > "xxx"`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},

		"Simple_LIMIT": {
			sql:    `SELECT Id, Value FROM Simple LIMIT 1`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_LIMIT_Param": {
			sql: `SELECT Id, Value FROM Simple LIMIT @limit`,
			params: map[string]Value{
				"limit": makeTestValue(2),
			},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_LIMIT_OFFSET": {
			sql:    `SELECT Id, Value FROM Simple LIMIT 1 OFFSET 1`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_LIMIT_OFFSET_Param": {
			sql: `SELECT Id, Value FROM Simple LIMIT @limit OFFSET @offset`,
			params: map[string]Value{
				"limit":  makeTestValue(1),
				"offset": makeTestValue(1),
			},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_ORDER": {
			sql:    `SELECT Id, Value FROM Simple ORDER BY Id DESC`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(300), "zzz"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_ORDER2": {
			sql:    `SELECT Id, Value FROM Simple ORDER BY Id DESC, Value`,
			params: map[string]Value{},
			expected: [][]interface{}{
				[]interface{}{int64(300), "zzz"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(100), "xxx"},
			},
		},

		"Simple_UNNEST_Bind": {
			sql: `SELECT Id, Value FROM Simple WHERE Id IN UNNEST (@foo)`,
			params: map[string]Value{
				"foo": makeTestValue([]int64{100, 200}),
			},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_UNNEST_Bind2": {
			sql: `SELECT Id, Value FROM Simple WHERE Id IN UNNEST (@foo)`,
			params: map[string]Value{
				"foo": makeTestValue([]int64{}),
			},
			expected: nil,
		},
		"Simple_UNNEST_Bind3": {
			sql: `SELECT Id, Value FROM Simple WHERE Value IN UNNEST (@foo)`,
			params: map[string]Value{
				"foo": makeTestValue([]string{"yyy"}),
			},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},
		"Simple_UNNEST_Array": {
			sql: `SELECT Id, Value FROM Simple WHERE Id IN UNNEST ([100 ,200, 300])`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "yyy"},
				[]interface{}{int64(300), "zzz"},
			},
		},
		"Simple_UNNEST_Array2": {
			sql: `SELECT Id, Value FROM Simple WHERE Value IN UNNEST (["xxx"])`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_UNNEST_Array3": {
			sql: `SELECT Id, Value FROM Simple WHERE Value IN UNNEST (["xxx", 1])`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},

		"ArrayLiteral_Empty": {
			sql: `SELECT []`,
			expected: [][]interface{}{
				[]interface{}{makeTestArray(TCInt64)},
			},
		},
		"ArrayLiteral_IntLiteral": {
			sql: `SELECT [1, 2, 3]`,
			expected: [][]interface{}{
				[]interface{}{makeTestArray(TCInt64, 1, 2, 3)},
			},
		},
		"ArrayLiteral_Ident": {
			sql: `SELECT [1, Id] FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{makeTestArray(TCInt64, 1, 100)},
				[]interface{}{makeTestArray(TCInt64, 1, 200)},
				[]interface{}{makeTestArray(TCInt64, 1, 300)},
			},
		},
		"ArrayLiteral_Params": {
			sql: `SELECT ["xxx", @foo]`,
			params: map[string]Value{
				"foo": makeTestValue("yyy"),
			},
			expected: [][]interface{}{
				[]interface{}{makeTestArray(TCString, "xxx", "yyy")},
			},
		},
		// "ArrayLiteral_IntAndFloat": {
		// 	sql: `SELECT [100, 0.1]`,
		// 	expected: [][]interface{}{
		// 		[]interface{}{makeTestArray(TCFloat64, 0.1, 0.1)},
		// 	},
		// },

		"NoTable_IntLiteral": {
			sql: `SELECT 1`,
			expected: [][]interface{}{
				[]interface{}{int64(1)},
			},
		},
		"NoTable_StringLiteral": {
			sql: `SELECT "foo"`,
			expected: [][]interface{}{
				[]interface{}{"foo"},
			},
		},
		"NoTable_NullLiteral": {
			sql: `SELECT NULL`,
			expected: [][]interface{}{
				[]interface{}{nil},
			},
		},
		"NoTable_Params_Int": {
			sql: `SELECT @foo`,
			params: map[string]Value{
				"foo": makeTestValue(int64(100)),
			},
			expected: [][]interface{}{
				[]interface{}{int64(100)},
			},
		},
		"NoTable_Params_String": {
			sql: `SELECT @foo`,
			params: map[string]Value{
				"foo": makeTestValue("xxx"),
			},
			expected: [][]interface{}{
				[]interface{}{"xxx"},
			},
		},

		"From_Unnest_Lietral_String": {
			sql: `SELECT * FROM UNNEST (["xxx", "yyy"])`,
			expected: [][]interface{}{
				[]interface{}{"xxx"},
				[]interface{}{"yyy"},
			},
		},
		"From_Unnest_Literal_Int": {
			sql: `SELECT * FROM UNNEST ([1,2,3])`,
			expected: [][]interface{}{
				[]interface{}{int64(1)},
				[]interface{}{int64(2)},
				[]interface{}{int64(3)},
			},
		},
		"From_Unnest_Literal_Params": {
			sql: `SELECT * FROM UNNEST ([ @a, @b, @c])`,
			params: map[string]Value{
				"a": makeTestValue(1),
				"b": makeTestValue(2),
				"c": makeTestValue(3),
			},
			expected: [][]interface{}{
				[]interface{}{int64(1)},
				[]interface{}{int64(2)},
				[]interface{}{int64(3)},
			},
		},
		"From_Unnest_Literal_Params_Array": {
			sql: `SELECT * FROM UNNEST (@foo)`,
			params: map[string]Value{
				"foo": makeTestValue([]int64{3, 4}),
			},
			expected: [][]interface{}{
				[]interface{}{int64(3)},
				[]interface{}{int64(4)},
			},
		},
		"From_Unnest_Literal_Params_Array_WithOffset": {
			sql: `SELECT * FROM UNNEST (@foo) WITH OFFSET`,
			params: map[string]Value{
				"foo": makeTestValue([]int64{3, 4}),
			},
			expected: [][]interface{}{
				[]interface{}{int64(3), int64(0)},
				[]interface{}{int64(4), int64(1)},
			},
		},
		"From_Unnest_Literal_Params_Alias": {
			sql: `SELECT x FROM UNNEST (@foo) AS x`,
			params: map[string]Value{
				"foo": makeTestValue([]int64{3, 4}),
			},
			expected: [][]interface{}{
				[]interface{}{int64(3)},
				[]interface{}{int64(4)},
			},
		},
		"From_Unnest_Literal_Params_WithOffset_Alias": {
			sql: `SELECT x, y FROM UNNEST (@foo) AS x WITH OFFSET y`,
			params: map[string]Value{
				"foo": makeTestValue([]int64{3, 4}),
			},
			expected: [][]interface{}{
				[]interface{}{int64(3), int64(0)},
				[]interface{}{int64(4), int64(1)},
			},
		},
		"From_Unnest_Literal_Alias_Star": {
			sql: `SELECT * FROM UNNEST ([1,2,3]) AS xxx`,
			expected: [][]interface{}{
				[]interface{}{int64(1)},
				[]interface{}{int64(2)},
				[]interface{}{int64(3)},
			},
		},
		"From_Unnest_Literal_Alias_Star_WithOffset": {
			sql: `SELECT * FROM UNNEST ([1,2,3]) AS xxx WITH OFFSET AS yyy`,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(0)},
				[]interface{}{int64(2), int64(1)},
				[]interface{}{int64(3), int64(2)},
			},
		},
		"From_Unnest_Literal_Alias_Ident": {
			sql: `SELECT xxx FROM UNNEST ([1,2,3]) AS xxx`,
			expected: [][]interface{}{
				[]interface{}{int64(1)},
				[]interface{}{int64(2)},
				[]interface{}{int64(3)},
			},
		},
		"From_Unnest_Literal_Alias_WithOffset_Ident": {
			sql: `SELECT xxx, yyy FROM UNNEST ([1,2,3]) AS xxx WITH OFFSET AS yyy`,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(0)},
				[]interface{}{int64(2), int64(1)},
				[]interface{}{int64(3), int64(2)},
			},
		},

		"From_Join_CommaJoin": {
			sql: `SELECT * FROM Simple a, Simple b`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx", int64(100), "xxx"},
				[]interface{}{int64(100), "xxx", int64(200), "yyy"},
				[]interface{}{int64(100), "xxx", int64(300), "zzz"},
				[]interface{}{int64(200), "yyy", int64(100), "xxx"},
				[]interface{}{int64(200), "yyy", int64(200), "yyy"},
				[]interface{}{int64(200), "yyy", int64(300), "zzz"},
				[]interface{}{int64(300), "zzz", int64(100), "xxx"},
				[]interface{}{int64(300), "zzz", int64(200), "yyy"},
				[]interface{}{int64(300), "zzz", int64(300), "zzz"},
			},
		},
		"From_Join_ON": {
			sql: `SELECT a.* FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE b.Value = "xxx"`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"From_Join_ON1": {
			sql: `SELECT a.Id, a.Value FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE b.Value = "xxx"`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"From_Join_ON2": {
			sql: `SELECT a.* FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE a.Id = @id`,
			params: map[string]Value{
				"id": makeTestValue(200),
			},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy"},
			},
		},
		"From_Join_ON3": {
			sql: `SELECT * FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE a.Id = @id`,
			params: map[string]Value{
				"id": makeTestValue(200),
			},
			expected: [][]interface{}{
				[]interface{}{int64(200), "yyy", int64(200), "yyy"},
			},
		},
		"From_Join_USING": {
			sql: `SELECT a.* FROM Simple AS a JOIN Simple AS b USING (Id) WHERE b.Value = "xxx"`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"From_Join_Subquery_USING": {
			sql: `SELECT a.* FROM Simple AS a JOIN (SELECT Id FROM Simple) AS b USING (Id) WHERE a.Value = "xxx"`,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},

		"From_Join_Paren": {
			sql: `SELECT a.Id FROM (Simple AS a JOIN Simple AS b USING (Id))`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},

		"From_Subquery_Simple": {
			sql: `SELECT s.* FROM (SELECT 1) s`,
			expected: [][]interface{}{
				[]interface{}{int64(1)},
			},
		},
		"From_Subquery_Table": {
			sql: `SELECT s.* FROM (SELECT Id FROM Simple WHERE Value = "xxx") s`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
			},
		},

		"SubQuery_Scalar_Simple": {
			sql: `SELECT Id FROM Simple WHERE 1 = (SELECT 1)`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		"SubQuery_Scalar_Table": {
			sql: `SELECT Id FROM Simple WHERE Id = (SELECT Id From Simple WHERE Id = 100)`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
			},
		},
		"SubQuery_In": {
			sql: `SELECT Id FROM Simple WHERE Id IN (SELECT Id From Simple WHERE Id > 100)`,
			expected: [][]interface{}{
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		"SubQuery_NotIn": {
			sql: `SELECT Id FROM Simple WHERE Id NOT IN (SELECT Id From Simple WHERE Id > 100)`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
			},
		},
		"SubQuery_EXISTS": {
			sql: `SELECT Id FROM Simple WHERE EXISTS(SELECT 1, "xx")`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		"SubQuery_EXISTS_NoMatch": {
			sql:      `SELECT Id FROM Simple WHERE EXISTS(SELECT * FROM Simple WHERE Id = 1000)`,
			expected: nil,
		},
		"SubQuery_ColumnAlias": {
			sql: `SELECT Id, foo, bar FROM (SELECT Id, Id AS foo, Id bar FROM Simple)`,
			expected: [][]interface{}{
				[]interface{}{int64(100), int64(100), int64(100)},
				[]interface{}{int64(200), int64(200), int64(200)},
				[]interface{}{int64(300), int64(300), int64(300)},
			},
		},
		"SubQuery_TableAlias": {
			sql: `SELECT xx.Id FROM (SELECT Id FROM Simple) AS xx`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},

		"Compound_Union_Distinct": {
			sql: `SELECT Id FROM Simple UNION DISTINCT SELECT Id FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		"Compound_Union_All": {
			sql: `SELECT Id FROM Simple UNION ALL SELECT Id FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		"Compound_Union_All_SelectLimit": {
			sql: `SELECT Id FROM Simple UNION ALL (SELECT Id FROM Simple LIMIT 1)`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
				[]interface{}{int64(100)},
			},
		},
		"Compound_Union_All_UnionLimit": {
			sql: `SELECT Id FROM Simple UNION ALL (SELECT Id FROM Simple) LIMIT 5`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
			},
		},
		"Compound_Union_All_OrderBy": {
			sql: `SELECT Id FROM Simple UNION ALL (SELECT Id FROM Simple) ORDER BY Id`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
				[]interface{}{int64(300)},
			},
		},
		"Compound_Union_All_OrderBy_Limit": {
			sql: `SELECT Id FROM Simple UNION ALL (SELECT Id FROM Simple) ORDER BY Id LIMIT 3`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
			},
		},
		"Compound_Intersect_Distinct": {
			sql: `SELECT Id FROM Simple INTERSECT DISTINCT SELECT Id FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		"Compound_Intersect_Distinct2": {
			sql: `SELECT Id FROM Simple INTERSECT DISTINCT (SELECT Id FROM Simple WHERE Id IN (100, 300))`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(300)},
			},
		},
		"Compound_Intersect_Distinct_Limit": {
			sql: `SELECT Id FROM Simple INTERSECT DISTINCT (SELECT Id FROM Simple) LIMIT 2`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
			},
		},
		"Compound_Intersect_Distinct_OrderBy": {
			sql: `SELECT Id FROM Simple INTERSECT DISTINCT (SELECT Id FROM Simple) ORDER BY Id DESC`,
			expected: [][]interface{}{
				[]interface{}{int64(300)},
				[]interface{}{int64(200)},
				[]interface{}{int64(100)},
			},
		},
		"Compound_Except_Distinct": {
			sql:      `SELECT Id FROM Simple EXCEPT DISTINCT SELECT Id FROM Simple`,
			expected: nil,
		},
		"Compound_Except_Distinct2": {
			sql: `SELECT Id FROM Simple EXCEPT DISTINCT (SELECT Id FROM Simple WHERE Id IN (100, 300))`,
			expected: [][]interface{}{
				[]interface{}{int64(200)},
			},
		},
		"Compound_Except_Distinct_Limit": {
			sql: `SELECT Id FROM Simple EXCEPT DISTINCT (SELECT Id FROM Simple WHERE Id = 200) LIMIT 1`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
			},
		},
		"Compound_Except_Distinct_OrderBy": {
			sql: `SELECT Id FROM Simple EXCEPT DISTINCT (SELECT Id FROM Simple WHERE Id = 200) ORDER BY Id DESC`,
			expected: [][]interface{}{
				[]interface{}{int64(300)},
				[]interface{}{int64(100)},
			},
		},
		"Compound_Complex1": {
			sql: `SELECT Id FROM Simple UNION ALL SELECT Id+2 FROM Simple UNION ALL SELECT Id+1 FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
				[]interface{}{int64(102)},
				[]interface{}{int64(202)},
				[]interface{}{int64(302)},
				[]interface{}{int64(101)},
				[]interface{}{int64(201)},
				[]interface{}{int64(301)},
			},
		},
		"Compound_Complex2": {
			sql: `SELECT Id FROM Simple UNION ALL SELECT Id+1 FROM Simple UNION ALL SELECT Id+2 FROM Simple ORDER BY Id DESC`,
			expected: [][]interface{}{
				[]interface{}{int64(302)},
				[]interface{}{int64(301)},
				[]interface{}{int64(300)},
				[]interface{}{int64(202)},
				[]interface{}{int64(201)},
				[]interface{}{int64(200)},
				[]interface{}{int64(102)},
				[]interface{}{int64(101)},
				[]interface{}{int64(100)},
			},
		},
		"Compound_Complex3": {
			sql: `(SELECT Id FROM Simple UNION ALL SELECT Id+1 FROM Simple) INTERSECT DISTINCT SELECT Id FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		// TODO: INT64 and FLOAT64 are compatible value type
		// "Compound_MergeIntFloat": {
		// 	sql: `SELECT 1 UNION ALL SELECT 0.1`,
		// 	expected: [][]interface{}{
		// 		[]interface{}{float64(1)},
		// 		[]interface{}{float64(0.1)},
		// 	},
		// },
		// "Compound_MergeIntFloat2": {
		// 	sql: `SELECT 1 UNION DISTINCT SELECT 1.0`,
		// 	expected: [][]interface{}{
		// 		[]interface{}{float64(1.0)},
		// 	},
		// },

		"Arithmetic_Add": {
			sql: `SELECT 1 + 2`,
			expected: [][]interface{}{
				[]interface{}{int64(3)},
			},
		},
		"Arithmetic_Add_Float": {
			sql: `SELECT 1.5 + 2.5`,
			expected: [][]interface{}{
				[]interface{}{float64(4)},
			},
		},
		"Arithmetic_Sub": {
			sql: `SELECT 1 - 2`,
			expected: [][]interface{}{
				[]interface{}{int64(-1)},
			},
		},
		"Arithmetic_Sub_Float": {
			sql: `SELECT 1.5 - 2`,
			expected: [][]interface{}{
				[]interface{}{float64(-0.5)},
			},
		},
		"Arithmetic_Mult": {
			sql: `SELECT 2 * 2`,
			expected: [][]interface{}{
				[]interface{}{int64(4)},
			},
		},
		"Arithmetic_Mult_Float": {
			sql: `SELECT 2.5 * 2`,
			expected: [][]interface{}{
				[]interface{}{float64(5)},
			},
		},
		"Arithmetic_Div": {
			sql: `SELECT 3 / 2`,
			expected: [][]interface{}{
				[]interface{}{float64(1.5)},
			},
		},
		"Arithmetic_BitOr": {
			sql: `SELECT 0x11 | 0x04`,
			expected: [][]interface{}{
				[]interface{}{int64(0x15)},
			},
		},
		"Arithmetic_BitXor": {
			sql: `SELECT 0x11 ^ 0x01`,
			expected: [][]interface{}{
				[]interface{}{int64(0x10)},
			},
		},
		"Arithmetic_BitAnd": {
			sql: `SELECT 0x13 & 0x01`,
			expected: [][]interface{}{
				[]interface{}{int64(0x01)},
			},
		},
		"Arithmetic_BitLeftShift": {
			sql: `SELECT 0x3 << 3`,
			expected: [][]interface{}{
				[]interface{}{int64(24)},
			},
		},
		"Arithmetic_BitRightShift": {
			sql: `SELECT 0xf0 >> 2`,
			expected: [][]interface{}{
				[]interface{}{int64(60)},
			},
		},
		"Unary_Int": {
			sql: `SELECT - -1`,
			expected: [][]interface{}{
				[]interface{}{int64(1)},
			},
		},
		"Unary_Float": {
			sql: `SELECT - -0.1`,
			expected: [][]interface{}{
				[]interface{}{float64(0.1)},
			},
		},
		"Unary_BitNot": {
			sql: `SELECT ~ -3`,
			expected: [][]interface{}{
				[]interface{}{int64(2)},
			},
		},
		"Unary_Not": {
			sql: `SELECT NOT true`,
			expected: [][]interface{}{
				[]interface{}{false},
			},
		},

		"Function_Count": {
			sql: `SELECT COUNT(1) FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(3)},
			},
		},
		"Function_Count2": {
			sql: `SELECT COUNT(Id) FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(3)},
			},
		},
		"Function_Count3": {
			sql: `SELECT COUNT(Id) AS count FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(3)},
			},
		},
		"Function_Count4": {
			sql: `SELECT COUNT("x") FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(3)},
			},
		},
		"Function_Count5": {
			sql: `SELECT COUNT(NULL) FROM Simple`,
			expected: [][]interface{}{
				[]interface{}{int64(0)},
			},
		},
		"Function_Count_Param": {
			sql: `SELECT COUNT(@foo) FROM Simple`,
			params: map[string]Value{
				"foo": makeTestValue(200),
			},
			expected: [][]interface{}{
				[]interface{}{int64(3)},
			},
		},
		"Function_Sign": {
			sql: `SELECT SIGN(1)`,
			expected: [][]interface{}{
				[]interface{}{int64(1)},
			},
		},
		"Function_Sign2": {
			sql: `SELECT SIGN(-1)`,
			expected: [][]interface{}{
				[]interface{}{int64(-1)},
			},
		},
		"Function_Sign3": {
			sql: `SELECT SIGN(0)`,
			expected: [][]interface{}{
				[]interface{}{int64(0)},
			},
		},
		"Function_StartsWith": {
			sql: `SELECT STARTS_WITH("abc", "ab")`,
			expected: [][]interface{}{
				[]interface{}{true},
			},
		},
		"Function_StartsWith2": {
			sql: `SELECT STARTS_WITH("abc", "xx")`,
			expected: [][]interface{}{
				[]interface{}{false},
			},
		},
		"Function_StartsWith_Param": {
			sql: `SELECT STARTS_WITH(@a, @b)`,
			params: map[string]Value{
				"a": makeTestValue("xyz"),
				"b": makeTestValue("xy"),
			},
			expected: [][]interface{}{
				[]interface{}{true},
			},
		},
		"Function_Max_Int": {
			sql: `SELECT MAX(x) FROM UNNEST([100, 200, 300]) AS x`,
			expected: [][]interface{}{
				[]interface{}{int64(300)},
			},
		},
		"Function_Max_String": {
			sql: `SELECT MAX(x) FROM UNNEST(["xxx", "zz", "yy"]) AS x`,
			expected: [][]interface{}{
				[]interface{}{"zz"},
			},
		},
		"Function_Min_Int": {
			sql: `SELECT MIN(x) FROM UNNEST([100, 200, 300]) AS x`,
			expected: [][]interface{}{
				[]interface{}{int64(100)},
			},
		},
		"Function_Min_String": {
			sql: `SELECT MIN(x) FROM UNNEST(["xxx", "zz", "yy"]) AS x`,
			expected: [][]interface{}{
				[]interface{}{"xxx"},
			},
		},
		"Function_Avg": {
			sql: `SELECT AVG(x) as avg FROM UNNEST([100, 200, NULL, 300, 100]) AS x`,
			expected: [][]interface{}{
				[]interface{}{float64(175)},
			},
		},
		"Function_Avg_Distinct": {
			sql: `SELECT AVG(DISTINCT x) as avg FROM UNNEST([100, 200, NULL, 300, 100]) AS x`,
			expected: [][]interface{}{
				[]interface{}{float64(200)},
			},
		},
		"Function_Sum": {
			sql: `SELECT SUM(x) as avg FROM UNNEST([100, 200, 300]) AS x`,
			expected: [][]interface{}{
				[]interface{}{int64(600)},
			},
		},
		// TODO: SUM with float

		"Function_Concat": {
			sql: `SELECT CONCAT("xx", "yy")`,
			expected: [][]interface{}{
				[]interface{}{"xxyy"},
			},
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			stmt, err := (&parser.Parser{
				Lexer: &parser.Lexer{
					File: &token.File{FilePath: "", Buffer: tc.sql},
				},
			}).ParseQuery()
			if err != nil {
				t.Fatalf("failed to parse sql: %q %v", tc.sql, err)
			}

			it, err := db.Query(ctx, stmt, tc.params)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			var rows [][]interface{}
			for {
				row, ok := it.Next()
				if !ok {
					break
				}

				rows = append(rows, row)
			}

			if diff := cmp.Diff(tc.expected, rows); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestQueryError(t *testing.T) {
	ctx := context.Background()
	db := newDatabase()
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

	table := map[string]struct {
		sql    string
		params map[string]Value
		code   codes.Code
		msg    *regexp.Regexp
	}{
		"StarWithoutFromClause": {
			sql:  "SELECT *",
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`SELECT \* must have a FROM clause`),
		},
		"TableNotFound": {
			sql:  "SELECT * FROM xxx",
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Table not found: xxx`),
		},
		"IdentiferNotFound": {
			sql:  "SELECT x.* FROM Simple a",
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Unrecognized name: x`),
		},
		"IdentiferNotFound2": {
			sql:  "SELECT foo FROM Simple",
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Unrecognized name: foo`),
		},
		"IdentiferNotFound3": {
			sql:  "SELECT a.foo FROM Simple a",
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Name foo not found inside a`),
		},
		"IdentiferNotFoundWithoutFromClause": {
			sql:  "SELECT x",
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Unrecognized name: x`),
		},
		"PathIdentiferNotFoundWithoutFromClause": {
			sql:  "SELECT x.*",
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Unrecognized name: x`),
		},

		"DuplicateTableAlias": {
			sql: "SELECT 1 FROM Simple a, Simple a",
			// code: codes.InvalidArgument,
			code: codes.Unknown, // TODO
			msg:  regexp.MustCompile(`Duplicate table alias a in the same FROM clause`),
		},
		"AmbiguousColumn": {
			sql:  "SELECT Id FROM Simple a, Simple b",
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Column name Id is ambiguous`),
		},

		// TODO: memefish cannot parse this sql
		// "QueryParam_TableName": {
		// 	sql:  "SELECT 1 FROM @table",
		// 	code: codes.InvalidArgument,
		// 	 msg:  regexp.MustCompile(`Query parameters cannot be used in place of table names`),
		// },

		"ScalarQueryWithMultiColumns": {
			sql:  `SELECT 1 WHERE 1 = (SELECT 1, "abc")`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Scalar subquery cannot have more than one column unless using SELECT AS STRUCT to build STRUCT values`),
		},
		"SubqueryForInConditionWithMultiColumns": {
			sql:  `SELECT 1 WHERE 1 IN (SELECT 1, "abc")`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Subquery of type IN must have only one output column`),
		},

		// TODO: sqlite implicitly extract 1 row if subquery returns multi rows
		// "ScalarQueryReturnedMultiRows": {
		// 	sql:  "SELECT 1 FROM Simple WHERE 1 = (SELECT 1 FROM Simple)",
		// 	code: codes.InvalidArgument,
		// 	msg:  regexp.MustCompile(`A scalar subquery returned more than one row.`),
		// },

		"JoinUsingColumnNotExist": {
			sql:  `SELECT 1 FROM Simple a JOIN Simple b USING (foo)`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Column foo in USING clause not found on left side of join`),
		},
		"JoinUsingColumnNotExist_RightSide": {
			sql:  `SELECT 1 FROM Simple a JOIN CompositePrimaryKeys b USING (Value)`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Column Value in USING clause not found on right side of join`),
		},
		"JoinUsingColumnNotExist_Subquery": {
			sql:  `SELECT 1 FROM Simple a JOIN (SELECT Value FROM Simple) b USING (Id)`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Column Id in USING clause not found on right side of join`),
		},

		"CompoundColumnsNum": {
			sql:  `SELECT 1 UNION ALL SELECT 1, 2`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Queries in UNION ALL have mismatched column count; query 1 has 1 column, query 2 has 2 columns`),
		},
		"CompoundColumnsNum2": {
			sql:  `SELECT 1, 2 INTERSECT DISTINCT SELECT 1`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Queries in INTERSECT DISTINCT have mismatched column count; query 1 has 2 column, query 2 has 1 columns`),
		},
		"CompoundColumnsType": {
			sql:  `SELECT 1 EXCEPT DISTINCT SELECT "xx"`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Column 1 in EXCEPT DISTINCT has incompatible types: INT64, STRING`),
		},
		"CompoundColumnsType2": {
			sql:  `SELECT 1, 0.1 EXCEPT DISTINCT SELECT 1, true`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Column 2 in EXCEPT DISTINCT has incompatible types: FLOAT64, BOOL`),
		},

		"ArrayLiteral_IncompatibleElements": {
			sql:  `SELECT [100, "xxx"]`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`^Array elements of types {.*} do not have a common supertype`),
		},
	}

	for name, tc := range table {
		t.Run(name, func(t *testing.T) {
			stmt, err := (&parser.Parser{
				Lexer: &parser.Lexer{
					File: &token.File{FilePath: "", Buffer: tc.sql},
				},
			}).ParseQuery()
			if err != nil {
				t.Fatalf("failed to parse sql: %q %v", tc.sql, err)
			}

			_, err = db.Query(ctx, stmt, tc.params)
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

func TestInsertAndReplace(t *testing.T) {
	table := map[string]struct {
		tbl    string
		wcols  []string
		values []*structpb.Value
		cols   []string
		limit  int64

		expected [][]interface{}
	}{
		"Simple": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("xxx"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"SimpleMaxInt": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue(strconv.FormatInt(0x7FFFFFFFFFFFFFFF, 10)),
				makeStringValue("xxx"),
			},
			cols:  []string{"Id"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(0x7FFFFFFFFFFFFFFF)},
			},
		},
		"SimpleMinInt": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue(strconv.FormatInt(-0x7FFFFFFFFFFFFFFF, 10)),
				makeStringValue("xxx"),
			},
			cols:  []string{"Id"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(-0x7FFFFFFFFFFFFFFF)},
			},
		},
		"FullType": {
			tbl:   "FullTypes",
			wcols: fullTypesKeys,
			values: []*structpb.Value{
				makeStringValue("xxx"),                            // PKey STRING(32) NOT NULL,
				makeStringValue("xxx"),                            // FTString STRING(32) NOT NULL,
				makeStringValue("xxx"),                            // FTStringNull STRING(32),
				makeBoolValue(true),                               // FTBool BOOL NOT NULL,
				makeBoolValue(true),                               // FTBoolNull BOOL,
				makeStringValue("xyz"),                            // FTBytes BYTES(32) NOT NULL,
				makeStringValue("xyz"),                            // FTBytesNull BYTES(32),
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestampNull TIMESTAMP,
				makeStringValue("100"),                            // FTInt INT64 NOT NULL,
				makeStringValue("100"),                            // FTIntNull INT64,
				makeNumberValue(0.5),                              // FTFloat FLOAT64 NOT NULL,
				makeNumberValue(0.5),                              // FTFloatNull FLOAT64,
				makeStringValue("2012-03-04"),                     // FTDate DATE NOT NULL,
				makeStringValue("2012-03-04"),                     // FTDateNull DATE,
			},
			cols: []string{
				"PKey",
				"FTString", "FTStringNull",
				"FTBool", "FTBoolNull",
				"FTBytes", "FTBytesNull",
				"FTTimestamp", "FTTimestampNull",
				"FTInt", "FTIntNull",
				"FTFloat", "FTFloatNull",
				"FTDate", "FTDateNull",
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{"xxx",
					"xxx", "xxx",
					true, true,
					[]byte("xyz"), []byte("xyz"),
					"2012-03-04T12:34:56.123456789Z", "2012-03-04T12:34:56.123456789Z",
					int64(100), int64(100),
					float64(0.5), float64(0.5),
					"2012-03-04", "2012-03-04",
				},
			},
		},
		"FullType_WithNull": {
			tbl:   "FullTypes",
			wcols: fullTypesKeys,
			values: []*structpb.Value{
				makeStringValue("xxx"), // PKey STRING(32) NOT NULL,
				makeStringValue("xxx"), // FTString STRING(32) NOT NULL,
				makeNullValue(),        // FTStringNull STRING(32),
				makeBoolValue(true),    // FTBool BOOL NOT NULL,
				makeNullValue(),        // FTBoolNull BOOL,
				makeStringValue("xyz"), // FTBytes BYTES(32) NOT NULL,
				makeNullValue(),        // FTBytesNull BYTES(32),
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
				makeNullValue(),               // FTTimestampNull TIMESTAMP,
				makeStringValue("100"),        // FTInt INT64 NOT NULL,
				makeNullValue(),               // FTIntNull INT64,
				makeNumberValue(0.5),          // FTFloat FLOAT64 NOT NULL,
				makeNullValue(),               // FTFloatNull FLOAT64,
				makeStringValue("2012-03-04"), // FTDate DATE NOT NULL,
				makeNullValue(),               // FTDateNull DATE,
			},
			cols: []string{
				"PKey",
				"FTString", "FTStringNull",
				"FTBool", "FTBoolNull",
				"FTBytes", "FTBytesNull",
				"FTTimestamp", "FTTimestampNull",
				"FTInt", "FTIntNull",
				"FTFloat", "FTFloatNull",
				"FTDate", "FTDateNull",
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{"xxx",
					"xxx", nil,
					true, nil,
					[]byte("xyz"), nil,
					"2012-03-04T12:34:56.123456789Z", nil,
					int64(100), nil,
					float64(0.5), nil,
					"2012-03-04", nil,
				},
			},
		},
		"FullType_NoValuesForNull": {
			tbl: "FullTypes",
			wcols: []string{
				"PKey",
				"FTString",
				"FTBool",
				"FTBytes",
				"FTTimestamp",
				"FTInt",
				"FTFloat",
				"FTDate",
			},
			values: []*structpb.Value{
				makeStringValue("xxx"),                            // PKey STRING(32) NOT NULL,
				makeStringValue("xxx"),                            // FTString STRING(32) NOT NULL,
				makeBoolValue(true),                               // FTBool BOOL NOT NULL,
				makeStringValue("xyz"),                            // FTBytes BYTES(32) NOT NULL,
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
				makeStringValue("100"),                            // FTInt INT64 NOT NULL,
				makeNumberValue(0.5),                              // FTFloat FLOAT64 NOT NULL,
				makeStringValue("2012-03-04"),                     // FTDate DATE NOT NULL,
			},
			cols:  fullTypesKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{"xxx",
					"xxx", nil,
					true, nil,
					[]byte("xyz"), nil,
					"2012-03-04T12:34:56.123456789Z", nil,
					int64(100), nil,
					float64(0.5), nil,
					"2012-03-04", nil,
				},
			},
		},
		"ArrayTypes": {
			tbl:   "ArrayTypes",
			wcols: arrayTypesKeys,
			values: []*structpb.Value{
				makeStringValue("100"),
				makeListValueAsValue(makeListValue(
					makeStringValue("xxx"),
					makeStringValue("yyy"),
				)),
				makeListValueAsValue(makeListValue(
					makeBoolValue(true),
					makeBoolValue(false),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("xyz"),
					makeStringValue("zzz"),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("2012-03-04T12:34:56.123456789Z"),
					makeStringValue("2012-03-04T12:34:56.000000000Z"),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("100"),
					makeStringValue("101"),
				)),
				makeListValueAsValue(makeListValue(
					makeNumberValue(0.2),
					makeNumberValue(0.5),
				)),
				makeListValueAsValue(makeListValue(
					makeStringValue("2012-03-04"),
					makeStringValue("2012-03-05"),
				)),
			},
			cols:  arrayTypesKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100),
					makeTestArray(TCString, "xxx", "yyy"),
					makeTestArray(TCBool, true, false),
					makeTestArray(TCBytes, []byte("xyz"), []byte("zzz")),
					makeTestArray(TCString, "2012-03-04T12:34:56.123456789Z", "2012-03-04T12:34:56.000000000Z"),
					makeTestArray(TCInt64, int64(100), int64(101)),
					makeTestArray(TCFloat64, float64(0.2), float64(0.5)),
					makeTestArray(TCString, "2012-03-04", "2012-03-05"),
				},
			},
		},
	}

	for _, op := range []string{"INSERT", "REPLACE"} {
		t.Run(op, func(t *testing.T) {
			for name, tt := range table {
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					db := newDatabase()
					for _, s := range allSchema {
						ddls := parseDDL(t, s)
						for _, ddl := range ddls {
							db.ApplyDDL(ctx, ddl)
						}
					}

					listValues := []*structpb.ListValue{
						{Values: tt.values},
					}

					if op == "INSERT" {
						if err := db.Insert(ctx, tt.tbl, tt.wcols, listValues); err != nil {
							t.Fatalf("Insert failed: %v", err)
						}
					} else if op == "REPLACE" {
						if err := db.Replace(ctx, tt.tbl, tt.wcols, listValues); err != nil {
							t.Fatalf("Replace failed: %v", err)
						}
					}

					it, err := db.Read(ctx, tt.tbl, "", tt.cols, &KeySet{All: true}, tt.limit)
					if err != nil {
						t.Fatalf("Read failed: %v", err)
					}

					var rows [][]interface{}
					for {
						row, ok := it.Next()
						if !ok {
							break
						}

						rows = append(rows, row)
					}

					if diff := cmp.Diff(tt.expected, rows); diff != "" {
						t.Errorf("(-got, +want)\n%s", diff)
					}
				})
			}
		})
	}
}

func TestInsertOrRepace_CommitTimestamp(t *testing.T) {
	tbl := "FullTypes"
	wcols := []string{
		"PKey",
		"FTString",
		"FTBool",
		"FTBytes",
		"FTTimestamp",
		"FTTimestampNull",
		"FTInt",
		"FTFloat",
		"FTDate",
	}
	values := []*structpb.Value{
		makeStringValue("xxx"),                            // PKey STRING(32) NOT NULL,
		makeStringValue("xxx"),                            // FTString STRING(32) NOT NULL,
		makeBoolValue(true),                               // FTBool BOOL NOT NULL,
		makeStringValue("xyz"),                            // FTBytes BYTES(32) NOT NULL,
		makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
		makeStringValue("spanner.commit_timestamp()"),
		makeStringValue("100"),        // FTInt INT64 NOT NULL,
		makeNumberValue(0.5),          // FTFloat FLOAT64 NOT NULL,
		makeStringValue("2012-03-04"), // FTDate DATE NOT NULL,
	}
	cols := []string{"FTTimestamp", "FTTimestampNull"}
	limit := int64(100)

	for _, op := range []string{"INSERT", "REPLACE"} {
		ctx := context.Background()
		db := newDatabase()
		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		listValues := []*structpb.ListValue{
			{Values: values},
		}

		if op == "INSERT" {
			if err := db.Insert(ctx, tbl, wcols, listValues); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		} else if op == "REPLACE" {
			if err := db.Replace(ctx, tbl, wcols, listValues); err != nil {
				t.Fatalf("Replace failed: %v", err)
			}
		}

		it, err := db.Read(ctx, tbl, "", cols, &KeySet{All: true}, limit)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		row, ok := it.Next()
		if !ok {
			t.Fatalf("unexpected")
		}

		var a, b string
		a = row[0].(string)
		b = row[1].(string)
		if got := "2012-03-04T12:34:56.123456789Z"; a != got {
			t.Errorf("expect %v, but got %v", got, a)
		}

		timestamp, err := time.Parse(time.RFC3339Nano, b)
		if err != nil {
			t.Fatalf("unexpected format timestamp: %v", err)
		}

		d := time.Since(timestamp)
		if d >= 3*time.Millisecond {
			t.Fatalf("unexpected time: %v", d)
		}
	}
}

func TestReplace(t *testing.T) {
	table := map[string]struct {
		tbl    string
		wcols  []string
		values []*structpb.Value
		cols   []string
		limit  int64

		expected [][]interface{}
	}{
		"Simple_NothingChanged": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("yyy"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "yyy"},
			},
		},
		"Simple_ConflictUpdate": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("zzz"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "zzz"},
			},
		},
		"Simple_Insert": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("101"),
				makeStringValue("xxx"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(101), "xxx"},
			},
		},
		"Composite_NothingChanged": {
			tbl:   "CompositePrimaryKeys",
			wcols: compositePrimaryKeysKeys,
			values: []*structpb.Value{
				makeStringValue("1"),        // Id INT64 NOT NULL,
				makeStringValue("pkey1xxx"), // PKey1 STRING(32) NOT NULL,
				makeStringValue("100"),      // PKey2 INT64 NOT NULL,
				makeStringValue("2"),        // Error INT64 NOT NULL,
				makeStringValue("x1"),       // X STRING(32) NOT NULL,
				makeStringValue("y1"),       // Y STRING(32) NOT NULL,
				makeStringValue("z1"),       // Z STRING(32) NOT NULL,
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(2),
					"x1",
					"y1",
					"z1",
				},
			},
		},
		"Composite_NothingChanged_RandomOrder": {
			tbl:   "CompositePrimaryKeys",
			wcols: []string{"PKey2", "X", "PKey1", "Id", "Error", "Y", "Z"},
			values: []*structpb.Value{
				makeStringValue("100"),      // PKey2 INT64 NOT NULL,
				makeStringValue("x1"),       // X STRING(32) NOT NULL,
				makeStringValue("pkey1xxx"), // PKey1 STRING(32) NOT NULL,
				makeStringValue("1"),        // Id INT64 NOT NULL,
				makeStringValue("2"),        // Error INT64 NOT NULL,
				makeStringValue("y1"),       // Y STRING(32) NOT NULL,
				makeStringValue("z1"),       // Z STRING(32) NOT NULL,
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(2),
					"x1",
					"y1",
					"z1",
				},
			},
		},
		"Composite_ConflictUpdate_RandomOrder": {
			tbl:   "CompositePrimaryKeys",
			wcols: []string{"PKey2", "X", "PKey1", "Id", "Error", "Y", "Z"},
			values: []*structpb.Value{
				makeStringValue("100"),      // PKey2 INT64 NOT NULL,
				makeStringValue("x4"),       // X STRING(32) NOT NULL,
				makeStringValue("pkey1xxx"), // PKey1 STRING(32) NOT NULL,
				makeStringValue("1"),        // Id INT64 NOT NULL,
				makeStringValue("10000"),    // Error INT64 NOT NULL,
				makeStringValue("y4"),       // Y STRING(32) NOT NULL,
				makeStringValue("z4"),       // Z STRING(32) NOT NULL,
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(10000),
					"x4",
					"y4",
					"z4",
				},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := newDatabase()
			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			createInitialData(t, ctx, db)

			listValues := []*structpb.ListValue{
				{Values: tt.values},
			}
			if err := db.Replace(ctx, tt.tbl, tt.wcols, listValues); err != nil {
				t.Fatalf("Replace failed: %v", err)
			}

			it, err := db.Read(ctx, tt.tbl, "", tt.cols, &KeySet{All: true}, tt.limit)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			var rows [][]interface{}
			for {
				row, ok := it.Next()
				if !ok {
					break
				}

				rows = append(rows, row)
			}

			if diff := cmp.Diff(tt.expected, rows); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	table := map[string]struct {
		tbl    string
		wcols  []string
		values []*structpb.Value
		cols   []string
		limit  int64

		expected [][]interface{}
	}{
		"Simple": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("yyy"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "yyy"},
			},
		},
		"Simple_UpdateWithSameValues": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("xxx"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Simple_PKeyOnly": {
			tbl:   "Simple",
			wcols: []string{"Id"},
			values: []*structpb.Value{
				makeStringValue("100"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"Composite_InOrder": {
			tbl:   "CompositePrimaryKeys",
			wcols: []string{"PKey1", "PKey2", "X", "Y"},
			values: []*structpb.Value{
				makeStringValue("pkey1xxx"),
				makeStringValue("100"),
				makeStringValue("xxxxxxxxx"),
				makeStringValue("yyyyyyyyy"),
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(2),
					"xxxxxxxxx",
					"yyyyyyyyy",
					"z1",
				},
			},
		},
		"Composite_PKeyReverse": {
			tbl:   "CompositePrimaryKeys",
			wcols: []string{"PKey2", "PKey1", "X", "Y"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("pkey1xxx"),
				makeStringValue("xxxxxxxxx"),
				makeStringValue("yyyyyyyyy"),
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(2),
					"xxxxxxxxx",
					"yyyyyyyyy",
					"z1",
				},
			},
		},
		"Composite_PKeyRandom": {
			tbl:   "CompositePrimaryKeys",
			wcols: []string{"PKey2", "X", "PKey1", "Y"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("xxxxxxxxx"),
				makeStringValue("pkey1xxx"),
				makeStringValue("yyyyyyyyy"),
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(2),
					"xxxxxxxxx",
					"yyyyyyyyy",
					"z1",
				},
			},
		},
		"FullType_String": {
			tbl:   "FullTypes",
			wcols: []string{"PKey", "FTString"},
			values: []*structpb.Value{
				makeStringValue("xxx"),
				makeStringValue("pppp"),
			},
			cols:  fullTypesKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					"xxx",
					"pppp", "xxx",
					true, true,
					[]byte("xyz"), []byte("xyz"),
					"2012-03-04T12:34:56.123456789Z", "2012-03-04T12:34:56.123456789Z",
					int64(100), int64(100),
					float64(0.5), float64(0.5),
					"2012-03-04", "2012-03-04",
				},
				[]interface{}{
					"yyy",
					"yyy", "yyy",
					true, true,
					[]byte("xyz"), []byte("xyz"),
					"2012-03-04T12:34:56.123456789Z", "2012-03-04T12:34:56.123456789Z",
					int64(101), int64(101),
					float64(0.5), float64(0.5),
					"2012-03-04", "2012-03-04",
				},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := newDatabase()
			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			createInitialData(t, ctx, db)

			listValues := []*structpb.ListValue{
				{Values: tt.values},
			}
			if err := db.Update(ctx, tt.tbl, tt.wcols, listValues); err != nil {
				t.Fatalf("Update failed: %v", err)
			}

			it, err := db.Read(ctx, tt.tbl, "", tt.cols, &KeySet{All: true}, tt.limit)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			var rows [][]interface{}
			for {
				row, ok := it.Next()
				if !ok {
					break
				}

				rows = append(rows, row)
			}

			if diff := cmp.Diff(tt.expected, rows); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestInsertOrUpdate(t *testing.T) {
	table := map[string]struct {
		tbl    string
		wcols  []string
		values []*structpb.Value
		cols   []string
		limit  int64

		expected [][]interface{}
	}{
		"Simple_NothingChanged": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("yyy"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "yyy"},
			},
		},
		"Simple_ConflictUpdate": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("zzz"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "zzz"},
			},
		},
		"Simple_Insert": {
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("101"),
				makeStringValue("xxx"),
			},
			cols:  []string{"Id", "Value"},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(101), "xxx"},
			},
		},
		"Composite_NothingChanged": {
			tbl:   "CompositePrimaryKeys",
			wcols: compositePrimaryKeysKeys,
			values: []*structpb.Value{
				makeStringValue("1"),        // Id INT64 NOT NULL,
				makeStringValue("pkey1xxx"), // PKey1 STRING(32) NOT NULL,
				makeStringValue("100"),      // PKey2 INT64 NOT NULL,
				makeStringValue("2"),        // Error INT64 NOT NULL,
				makeStringValue("x1"),       // X STRING(32) NOT NULL,
				makeStringValue("y1"),       // Y STRING(32) NOT NULL,
				makeStringValue("z1"),       // Z STRING(32) NOT NULL,
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(2),
					"x1",
					"y1",
					"z1",
				},
			},
		},
		"Composite_NothingChanged_RandomOrder": {
			tbl:   "CompositePrimaryKeys",
			wcols: []string{"PKey2", "X", "PKey1", "Id", "Error", "Y", "Z"},
			values: []*structpb.Value{
				makeStringValue("100"),      // PKey2 INT64 NOT NULL,
				makeStringValue("x1"),       // X STRING(32) NOT NULL,
				makeStringValue("pkey1xxx"), // PKey1 STRING(32) NOT NULL,
				makeStringValue("1"),        // Id INT64 NOT NULL,
				makeStringValue("2"),        // Error INT64 NOT NULL,
				makeStringValue("y1"),       // Y STRING(32) NOT NULL,
				makeStringValue("z1"),       // Z STRING(32) NOT NULL,
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(2),
					"x1",
					"y1",
					"z1",
				},
			},
		},
		"Composite_ConflictUpdate_RandomOrder": {
			tbl:   "CompositePrimaryKeys",
			wcols: []string{"PKey2", "X", "PKey1", "Id", "Error", "Y", "Z"},
			values: []*structpb.Value{
				makeStringValue("100"),      // PKey2 INT64 NOT NULL,
				makeStringValue("x4"),       // X STRING(32) NOT NULL,
				makeStringValue("pkey1xxx"), // PKey1 STRING(32) NOT NULL,
				makeStringValue("1"),        // Id INT64 NOT NULL,
				makeStringValue("10000"),    // Error INT64 NOT NULL,
				makeStringValue("y4"),       // Y STRING(32) NOT NULL,
				makeStringValue("z4"),       // Z STRING(32) NOT NULL,
			},
			cols:  compositePrimaryKeysKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{
					int64(1),
					"pkey1xxx",
					int64(100),
					int64(10000),
					"x4",
					"y4",
					"z4",
				},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := newDatabase()
			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			createInitialData(t, ctx, db)

			listValues := []*structpb.ListValue{
				{Values: tt.values},
			}
			if err := db.InsertOrUpdate(ctx, tt.tbl, tt.wcols, listValues); err != nil {
				t.Fatalf("InsertOrUpdate failed: %v", err)
			}

			it, err := db.Read(ctx, tt.tbl, "", tt.cols, &KeySet{All: true}, tt.limit)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			var rows [][]interface{}
			for {
				row, ok := it.Next()
				if !ok {
					break
				}

				rows = append(rows, row)
			}

			if diff := cmp.Diff(tt.expected, rows); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	table := map[string]struct {
		tbl  string
		ks   *KeySet
		cols []string

		expected [][]interface{}
	}{
		"Simple_All": {
			tbl:      "Simple",
			ks:       &KeySet{All: true},
			cols:     []string{"Id"},
			expected: nil,
		},
		"Simple_Keys_Single": {
			tbl: "Simple",
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("100")),
				},
			},
			cols: []string{"Id"},
			expected: [][]interface{}{
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		"Simple_Keys_MultiKeys": {
			tbl: "Simple",
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("100")),
					makeListValue(makeStringValue("300")),
				},
			},
			cols: []string{"Id"},
			expected: [][]interface{}{
				[]interface{}{int64(200)},
			},
		},
		"Simple_Keys_NotExist": {
			tbl: "Simple",
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("1000")),
				},
			},
			cols: []string{"Id"},
			expected: [][]interface{}{
				[]interface{}{int64(100)},
				[]interface{}{int64(200)},
				[]interface{}{int64(300)},
			},
		},
		"Simple_KeyRange": {
			tbl: "Simple",
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("100")),
						end:         makeListValue(makeStringValue("200")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			cols: []string{"Id"},
			expected: [][]interface{}{
				[]interface{}{int64(300)},
			},
		},
		"CompositePrimaryKeys_Keys": {
			tbl: "CompositePrimaryKeys",
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("bbb"), makeStringValue("1")),
					makeListValue(makeStringValue("ccc"), makeStringValue("4")),
				},
			},
			cols: []string{"PKey1", "PKey2"},
			expected: [][]interface{}{
				[]interface{}{"aaa", int64(1)},
				[]interface{}{"bbb", int64(3)},
				[]interface{}{"bbb", int64(2)},
				[]interface{}{"ccc", int64(3)},
			},
		},
		"CompositePrimaryKeys_KeyRange": {
			tbl: "CompositePrimaryKeys",
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb"), makeStringValue("1")),
						end:         makeListValue(makeStringValue("bbb"), makeStringValue("4")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			cols: []string{"PKey1", "PKey2"},
			expected: [][]interface{}{
				[]interface{}{"aaa", int64(1)},
				[]interface{}{"ccc", int64(4)},
				[]interface{}{"ccc", int64(3)},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := newDatabase()
			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					if err := db.ApplyDDL(ctx, ddl); err != nil {
						t.Fatal(err)
					}
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

			if err := db.Delete(ctx, tt.tbl, tt.ks); err != nil {
				t.Fatalf("Update failed: %v", err)
			}

			it, err := db.Read(ctx, tt.tbl, "", tt.cols, &KeySet{All: true}, 100)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			var rows [][]interface{}
			for {
				row, ok := it.Next()
				if !ok {
					break
				}

				rows = append(rows, row)
			}

			if diff := cmp.Diff(tt.expected, rows); diff != "" {
				t.Errorf("(-got, +want)\n%s", diff)
			}
		})
	}
}

func TestMutationError(t *testing.T) {
	table := map[string]struct {
		op     []string
		tbl    string
		wcols  []string
		values []*structpb.Value
		code   codes.Code
		msg    *regexp.Regexp
	}{
		"TableNotfound": {
			op:     []string{"UPDATE", "INSERT", "UPSERT", "REPLACE"},
			tbl:    "XXX",
			wcols:  []string{},
			values: []*structpb.Value{},
			code:   codes.NotFound,
			msg:    regexp.MustCompile(`Table not found`),
		},
		"ColumnNotFound": {
			op:    []string{"UPDATE", "INSERT", "UPSERT", "REPLACE"},
			tbl:   "Simple",
			wcols: []string{"Id", "Value", "XXX"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("yyy"),
				makeStringValue("xxx"),
			},
			code: codes.NotFound,
			msg:  regexp.MustCompile(`Column not found`),
		},
		"MultipleValues": {
			op:    []string{"UPDATE", "INSERT", "UPSERT", "REPLACE"},
			tbl:   "Simple",
			wcols: []string{"Id", "Value", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("yyy"),
				makeStringValue("yyy"),
			},
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Multiple values for column Value`),
		},
		"NoPrimaryKeys": {
			op:    []string{"UPDATE", "INSERT", "UPSERT", "REPLACE"},
			tbl:   "CompositePrimaryKeys",
			wcols: []string{"PKey1", "Id", "Error", "X", "Y", "Z"},
			values: []*structpb.Value{
				makeStringValue("yyy"),
				makeStringValue("100"),
				makeStringValue("1"),
				makeStringValue("x"),
				makeStringValue("y"),
				makeStringValue("z"),
			},
			code: codes.FailedPrecondition,
			msg:  regexp.MustCompile(`PKey2 must not be NULL in table CompositePrimaryKeys`),
		},
		"ValuesMisMatch": {
			op:    []string{"UPDATE", "INSERT", "UPSERT", "REPLACE"},
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("yyy"),
				makeStringValue("yyy"),
			},
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Mutation has mismatched number of columns and values.`),
		},
		"RowNotFound": {
			op:    []string{"UPDATE"},
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100000"),
				makeStringValue("yyy"),
			},
			code: codes.NotFound,
			msg:  regexp.MustCompile(`Row \[.*\] in table Simple is missing. Row cannot be updated.`),
		},
		"ValuesNotSpecifiedForPKey": {
			op:    []string{"INSERT", "REPLACE"},
			tbl:   "Simple",
			wcols: []string{"Value"},
			values: []*structpb.Value{
				makeStringValue("yyy"),
			},
			code: codes.FailedPrecondition,
			msg:  regexp.MustCompile(`Id must not be NULL in table Simple.`),
		},
		"NullSpecifiedForPKey": {
			op:    []string{"INSERT", "REPLACE"},
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeNullValue(),
				makeStringValue("yyy"),
			},
			code: codes.FailedPrecondition,
			msg:  regexp.MustCompile(`Id must not be NULL in table Simple.`),
		},
		"ValuesNotSpecifiedForNonNullable": {
			op:    []string{"INSERT", "REPLACE"},
			tbl:   "Simple",
			wcols: []string{"Id"},
			values: []*structpb.Value{
				makeStringValue("100"),
			},
			code: codes.FailedPrecondition,
			msg:  regexp.MustCompile(`A new row in table Simple does not specify a non-null value for these NOT NULL columns: Value`),
		},
		"NullSpecifiedForNonNullable": {
			op:    []string{"INSERT", "REPLACE"},
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeNullValue(),
			},
			code: codes.FailedPrecondition,
			msg:  regexp.MustCompile(`Value must not be NULL in table Simple.`),
		},

		"PrimaryKeyViolation": {
			op:    []string{"INSERT"},
			tbl:   "Simple",
			wcols: []string{"Id", "Value"},
			values: []*structpb.Value{
				makeStringValue("100"),
				makeStringValue("yyyy"),
			},
			code: codes.AlreadyExists,
			msg:  regexp.MustCompile(`Row \[.*\] in table Simple already exists`),
		},

		"FullType": {
			op:    []string{"INSERT"},
			tbl:   "FullTypes",
			wcols: fullTypesKeys,
			values: []*structpb.Value{
				makeStringValue("zzz"),                            // PKey STRING(32) NOT NULL,
				makeStringValue("xxx"),                            // FTString STRING(32) NOT NULL,
				makeStringValue("xxx"),                            // FTStringNull STRING(32),
				makeBoolValue(true),                               // FTBool BOOL NOT NULL,
				makeBoolValue(true),                               // FTBoolNull BOOL,
				makeStringValue("xyz"),                            // FTBytes BYTES(32) NOT NULL,
				makeStringValue("xyz"),                            // FTBytesNull BYTES(32),
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestampNull TIMESTAMP,
				makeStringValue("100"),                            // FTInt INT64 NOT NULL,
				makeStringValue("100"),                            // FTIntNull INT64,
				makeNumberValue(0.5),                              // FTFloat FLOAT64 NOT NULL,
				makeNumberValue(0.5),                              // FTFloatNull FLOAT64,
				makeStringValue("2012-03-04"),                     // FTDate DATE NOT NULL,
				makeStringValue("2012-03-04"),                     // FTDateNull DATE,
			},
			code: codes.AlreadyExists,
			msg:  regexp.MustCompile(`Unique index violation at index key \[.*\]. It conflicts with row \[.*\] in table FullTypes`),
		},
		// A new record coflicts only secondary index but it does not replace
		// "FullTypeConflictsSecondaryIndex": {
		// 	op:    []string{"REPLACE", "UPSERT"},
		// 	tbl:   "FullTypes",
		// 	wcols: fullTypesKeys,
		// 	values: []*structpb.Value{
		// 		makeStringValue("zzzzzzzz"),                       // PKey STRING(32) NOT NULL,
		// 		makeStringValue("xxx"),                            // FTString STRING(32) NOT NULL,
		// 		makeStringValue("xxx"),                            // FTStringNull STRING(32),
		// 		makeBoolValue(true),                               // FTBool BOOL NOT NULL,
		// 		makeBoolValue(true),                               // FTBoolNull BOOL,
		// 		makeStringValue("xyz"),                            // FTBytes BYTES(32) NOT NULL,
		// 		makeStringValue("xyz"),                            // FTBytesNull BYTES(32),
		// 		makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
		// 		makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestampNull TIMESTAMP,
		// 		makeStringValue("300"),                            // FTInt INT64 NOT NULL,
		// 		makeStringValue("300"),                            // FTIntNull INT64,
		// 		makeNumberValue(0.5),                              // FTFloat FLOAT64 NOT NULL,
		// 		makeNumberValue(0.5),                              // FTFloatNull FLOAT64,
		// 		makeStringValue("2012-03-04"),                     // FTDate DATE NOT NULL,
		// 		makeStringValue("2012-03-04"),                     // FTDateNull DATE,
		// 	},
		// 	code: codes.AlreadyExists,
		// 	msg:  regexp.MustCompile(`Unique index violation at index key \[.*\]. It conflicts with row \[.*\] in table FullTypes`),
		// },

		"FullType_CommitTimestamp": {
			op:    []string{"INSERT"},
			tbl:   "FullTypes",
			wcols: fullTypesKeys,
			values: []*structpb.Value{
				makeStringValue("zzz"),                            // PKey STRING(32) NOT NULL,
				makeStringValue("zzz"),                            // FTString STRING(32) NOT NULL,
				makeStringValue("zzz"),                            // FTStringNull STRING(32),
				makeBoolValue(true),                               // FTBool BOOL NOT NULL,
				makeBoolValue(true),                               // FTBoolNull BOOL,
				makeStringValue("xyz"),                            // FTBytes BYTES(32) NOT NULL,
				makeStringValue("xyz"),                            // FTBytesNull BYTES(32),
				makeStringValue("spanner.commit_timestamp()"),     // FTTimestamp TIMESTAMP NOT NULL,
				makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestampNull TIMESTAMP,
				makeStringValue("999"),                            // FTInt INT64 NOT NULL,
				makeStringValue("100"),                            // FTIntNull INT64,
				makeNumberValue(0.5),                              // FTFloat FLOAT64 NOT NULL,
				makeNumberValue(0.5),                              // FTFloatNull FLOAT64,
				makeStringValue("2012-03-04"),                     // FTDate DATE NOT NULL,
				makeStringValue("2012-03-04"),                     // FTDateNull DATE,
			},
			code: codes.InvalidArgument, // TODO:  FailedPrecondition
			msg:  regexp.MustCompile(`Cannot write commit timestamp because the allow_commit_timestamp column option is not set to true for column`),
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := newDatabase()
			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			createInitialData(t, ctx, db)

			for _, op := range tt.op {
				t.Run(op, func(t *testing.T) {
					listValues := []*structpb.ListValue{
						{Values: tt.values},
					}

					var err error
					switch op {
					case "INSERT":
						err = db.Insert(ctx, tt.tbl, tt.wcols, listValues)
					case "UPDATE":
						err = db.Update(ctx, tt.tbl, tt.wcols, listValues)
					case "UPSERT":
						err = db.InsertOrUpdate(ctx, tt.tbl, tt.wcols, listValues)
					case "REPLACE":
						err = db.Replace(ctx, tt.tbl, tt.wcols, listValues)
					default:
						t.Fatalf("unexpected op: %v", op)
					}
					st := status.Convert(err)
					if st.Code() != tt.code {
						t.Errorf("expect code to be %v but got %v", tt.code, st.Code())
					}
					if !tt.msg.MatchString(st.Message()) {
						t.Errorf("unexpected error message: %v", st.Message())
					}
				})
			}
		})
	}
}
