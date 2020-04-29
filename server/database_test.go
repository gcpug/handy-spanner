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
	allSchema    = []string{schemaSimple, schemaInterleaved, schemaInterleavedCascade, schemaInterleavedNoAction, schemaCompositePrimaryKeys, schemaFullTypes, schemaArrayTypes, schemaJoinA, schemaJoinB, schemaFromTable}
	schemaSimple = `CREATE TABLE Simple (
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id);
`
	schemaInterleaved = `CREATE TABLE ParentTable (
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id);

CREATE TABLE Interleaved (
  InterleavedId INT64 NOT NULL,
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id, InterleavedId),
INTERLEAVE IN PARENT ParentTable;
CREATE INDEX InterleavedKey ON Interleaved(Id, Value), INTERLEAVE IN ParentTable 
`
	schemaInterleavedCascade = `CREATE TABLE ParentTableCascade (
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id);

CREATE TABLE InterleavedCascade (
  InterleavedId INT64 NOT NULL,
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id, InterleavedId),
INTERLEAVE IN PARENT ParentTableCascade ON DELETE CASCADE;
`
	schemaInterleavedNoAction = `CREATE TABLE ParentTableNoAction (
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id);

CREATE TABLE InterleavedNoAction (
  InterleavedId INT64 NOT NULL,
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id, InterleavedId),
INTERLEAVE IN PARENT ParentTableNoAction ON DELETE NO ACTION;
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
	schemaJoinA = `CREATE TABLE JoinA (
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id);
`
	schemaJoinB = `CREATE TABLE JoinB (
  Id INT64 NOT NULL,
  Value STRING(MAX) NOT NULL,
) PRIMARY KEY(Id);
`
	schemaFromTable = "CREATE TABLE `From` (" +
		"`ALL` INT64 NOT NULL," +
		"`CAST` INT64 NOT NULL, " +
		"`JOIN` INT64 NOT NULL, " +
		") PRIMARY KEY(`ALL`); \n" +
		"CREATE INDEX `ALL` ON `From`(`ALL`);"
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
	fromTableKeys = []string{"ALL", "CAST", "JOIN"}
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
				makeStringValue("eHl6"),                           // FTBytes BYTES(32) NOT NULL,
				makeStringValue("eHl6"),                           // FTBytesNull BYTES(32),
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
				makeStringValue("eHl6"),                           // FTBytes BYTES(32) NOT NULL,
				makeStringValue("eHl6"),                           // FTBytesNull BYTES(32),
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
					makeStringValue("eHl6"),
					makeStringValue("eHl6"),
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
	{
		table: "From",
		cols:  fromTableKeys,
		values: [][]*structpb.Value{
			{
				makeStringValue("1"),
				makeStringValue("1"),
				makeStringValue("1"),
			},
		},
	},
}

func testRunInTransaction(t *testing.T, ses *session, fn func(*transaction)) {
	t.Helper()

	tx, err := ses.createTransaction(txReadWrite, false)
	if err != nil {
		t.Fatalf("createTransaction failed: %v", err)
	}
	defer tx.Done(TransactionCommited)

	fn(tx)

	if err := ses.database.Commit(tx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func createInitialData(t *testing.T, ctx context.Context, db Database, tx *transaction) {
	t.Helper()

	for _, d := range initialData {
		for _, values := range d.values {
			listValues := []*structpb.ListValue{
				{Values: values},
			}
			if err := db.Insert(ctx, tx, d.table, d.cols, listValues); err != nil {
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

func makeStructValue(v map[string]*structpb.Value) *structpb.Value {
	return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
		Fields: v,
	}}}
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
	case float64:
		return Value{
			Data: v,
			Type: ValueType{Code: TCFloat64},
		}

	default:
		panic(fmt.Sprintf("fix makeTestValue to be able to convert interface{} to Value: %T", v))
	}
}

func makeTestWrappedArray(code TypeCode, vs ...interface{}) interface{} {
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
		return &ArrayValueEncoder{Values: arr}
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
		return &ArrayValueEncoder{Values: arr}
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
		return &ArrayValueEncoder{Values: arr}
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
		return &ArrayValueEncoder{Values: arr}
	case TCBytes:
		arr := make([][]byte, len(vs))
		for i := range vs {
			if vs[i] == nil {
				arr[i] = nil
			} else {
				arr[i] = vs[i].([]byte)
			}
		}
		return &ArrayValueEncoder{Values: arr}
	default:
		panic(fmt.Sprintf("fix makeTestArray to be able to convert interface{}: %v", code))
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
		return arr
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
		return arr
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
		return arr
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
		return arr
	case TCBytes:
		arr := make([][]byte, len(vs))
		for i := range vs {
			if vs[i] == nil {
				arr[i] = nil
			} else {
				arr[i] = vs[i].([]byte)
			}
		}
		return arr
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

func TestClose(t *testing.T) {
	db := newDatabase()
	err := db.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
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

		"INSERT INTO `From` VALUES(1, 1, 1)",
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
		"Simple_KeyRange_OpenClose2": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb"), makeStringValue("3")),
						end:         makeListValue(makeStringValue("ccc"), makeStringValue("3")),
						startClosed: false,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(4), "ccc", int64(3)},
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
		"Simple_KeyRange_LessPrimaryKey": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb"), makeStringValue("2")),
						end:         makeListValue(makeStringValue("bbb")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(3), "bbb", int64(3)},
				[]interface{}{int64(2), "bbb", int64(2)},
			},
		},
		"Simple_KeyRange_LessPrimaryKey2": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb")),
						end:         makeListValue(makeStringValue("bbb"), makeStringValue("3")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(3), "bbb", int64(3)},
				[]interface{}{int64(2), "bbb", int64(2)},
			},
		},
		"Simple_KeyRange_LessPrimaryKey3": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb")),
						end:         makeListValue(makeStringValue("bbb")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			expected: [][]interface{}{
				[]interface{}{int64(3), "bbb", int64(3)},
				[]interface{}{int64(2), "bbb", int64(2)},
			},
		},
		"Simple_KeyRange_LessPrimaryKeyOpenClosed": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb"), makeStringValue("2")),
						end:         makeListValue(makeStringValue("bbb")),
						startClosed: false,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(3), "bbb", int64(3)},
			},
		},
		"Simple_KeyRange_LessPrimaryKeyClosedOpen": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb")),
						end:         makeListValue(makeStringValue("bbb"), makeStringValue("3")),
						startClosed: true,
						endClosed:   false,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(2), "bbb", int64(2)},
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
		"CompositePrimaryKeys_Keys_Multi": {
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

		// Composite Ranges
		"CompositePrimaryKeys_Ranges": {
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
		"CompositePrimaryKeys_Ranges_Multi": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb"), makeStringValue("2")),
						end:         makeListValue(makeStringValue("bbb"), makeStringValue("3")),
						startClosed: true,
						endClosed:   true,
					},
					{
						start:       makeListValue(makeStringValue("ccc"), makeStringValue("3")),
						end:         makeListValue(makeStringValue("ccc"), makeStringValue("4")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(3), "bbb", int64(3)},
				[]interface{}{int64(2), "bbb", int64(2)},
				[]interface{}{int64(5), "ccc", int64(4)},
				[]interface{}{int64(4), "ccc", int64(3)},
			},
		},

		// Composite Keys and Ranges
		"CompositePrimaryKeys_KeysRanges": {
			tbl:  "CompositePrimaryKeys",
			cols: []string{"Id", "PKey1", "PKey2"},
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("aaa"), makeStringValue("1")),
				},
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("bbb"), makeStringValue("2")),
						end:         makeListValue(makeStringValue("bbb"), makeStringValue("3")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(1), "aaa", int64(1)},
				[]interface{}{int64(3), "bbb", int64(3)},
				[]interface{}{int64(2), "bbb", int64(2)},
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

		// Escape Keywork
		"Keyword_All": {
			tbl:   "From",
			idx:   "",
			cols:  fromTableKeys,
			ks:    &KeySet{All: true},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(1), int64(1)},
			},
		},
		"Keyword_Keys": {
			tbl:  "From",
			idx:  "",
			cols: fromTableKeys,
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("1")),
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(1), int64(1)},
			},
		},
		"Keyword_Ranges": {
			tbl:  "From",
			idx:  "",
			cols: fromTableKeys,
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("1")),
						end:         makeListValue(makeStringValue("1")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(1), int64(1)},
			},
		},
		"Keyword_Index": {
			tbl:   "From",
			idx:   "ALL",
			cols:  []string{"ALL"},
			ks:    &KeySet{All: true},
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(1)},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ses := newSession(db, "foo")
			testRunInTransaction(t, ses, func(tx *transaction) {
				it, err := db.Read(ctx, tx, tt.tbl, tt.idx, tt.cols, tt.ks, tt.limit)
				if err != nil {
					t.Fatalf("Read failed: %v", err)
				}

				var rows [][]interface{}
				err = it.Do(func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})
				if err != nil {
					t.Fatalf("unexpected error in iteration: %v", err)
				}

				if diff := cmp.Diff(tt.expected, rows); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			})
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ses := newSession(db, "foo")
			testRunInTransaction(t, ses, func(tx *transaction) {
				_, err := db.Read(ctx, tx, tt.tbl, tt.idx, tt.cols, tt.ks, tt.limit)
				st := status.Convert(err)
				if st.Code() != tt.code {
					t.Errorf("expect code to be %v but got %v", tt.code, st.Code())
				}
				if !tt.msg.MatchString(st.Message()) {
					t.Errorf("unexpected error message: %v", st.Message())
				}
			})
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ses := newSession(db, "foo")
			testRunInTransaction(t, ses, func(tx *transaction) {
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
				it, err := db.Read(ctx, tx, tt.tbl, "", tt.cols, ks, 100)
				if err != nil {
					t.Fatalf("Read failed: %v", err)
				}

				var rows [][]interface{}
				err = it.Do(func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})
				if err != nil {
					t.Fatalf("unexpected error in iteration: %v", err)
				}

				if diff := cmp.Diff(tt.expected, rows); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			})
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
				makeStringValue("eHl6"),                           // FTBytes BYTES(32) NOT NULL,
				makeStringValue("eHl6"),                           // FTBytesNull BYTES(32),
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
				makeStringValue("xxx"),  // PKey STRING(32) NOT NULL,
				makeStringValue("xxx"),  // FTString STRING(32) NOT NULL,
				makeNullValue(),         // FTStringNull STRING(32),
				makeBoolValue(true),     // FTBool BOOL NOT NULL,
				makeNullValue(),         // FTBoolNull BOOL,
				makeStringValue("eHl6"), // FTBytes BYTES(32) NOT NULL,
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
				makeStringValue("eHl6"),                           // FTBytes BYTES(32) NOT NULL,
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
					makeStringValue("eHl6"),
					makeStringValue("enp6"),
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
		"Keyword": {
			tbl:   "From",
			wcols: fromTableKeys,
			values: []*structpb.Value{
				makeStringValue("2"),
				makeStringValue("2"),
				makeStringValue("2"),
			},
			cols:  fromTableKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(2), int64(2), int64(2)},
			},
		},
	}

	for _, op := range []string{"INSERT", "REPLACE"} {
		t.Run(op, func(t *testing.T) {
			for name, tt := range table {
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()

					db := newDatabase()
					ses := newSession(db, "foo")

					for _, s := range allSchema {
						ddls := parseDDL(t, s)
						for _, ddl := range ddls {
							db.ApplyDDL(ctx, ddl)
						}
					}

					testRunInTransaction(t, ses, func(tx *transaction) {
						listValues := []*structpb.ListValue{
							{Values: tt.values},
						}
						if op == "INSERT" {
							if err := db.Insert(ctx, tx, tt.tbl, tt.wcols, listValues); err != nil {
								t.Fatalf("Insert failed: %v", err)
							}
						} else if op == "REPLACE" {
							if err := db.Replace(ctx, tx, tt.tbl, tt.wcols, listValues); err != nil {
								t.Fatalf("Replace failed: %v", err)
							}
						}
					})

					testRunInTransaction(t, ses, func(tx *transaction) {
						it, err := db.Read(ctx, tx, tt.tbl, "", tt.cols, &KeySet{All: true}, tt.limit)
						if err != nil {
							t.Fatalf("Read failed: %v", err)
						}

						var rows [][]interface{}
						err = it.Do(func(row []interface{}) error {
							rows = append(rows, row)
							return nil
						})
						if err != nil {
							t.Fatalf("unexpected error in iteration: %v", err)
						}

						if diff := cmp.Diff(tt.expected, rows); diff != "" {
							t.Errorf("(-got, +want)\n%s", diff)
						}
					})
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
		makeStringValue("eHl6"),                           // FTBytes BYTES(32) NOT NULL,
		makeStringValue("2012-03-04T12:34:56.123456789Z"), // FTTimestamp TIMESTAMP NOT NULL,
		makeStringValue("spanner.commit_timestamp()"),
		makeStringValue("100"),        // FTInt INT64 NOT NULL,
		makeNumberValue(0.5),          // FTFloat FLOAT64 NOT NULL,
		makeStringValue("2012-03-04"), // FTDate DATE NOT NULL,
	}
	cols := []string{"FTTimestamp", "FTTimestampNull"}
	limit := int64(100)

	for _, op := range []string{"INSERT", "REPLACE"} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		db := newDatabase()
		ses := newSession(db, "foo")

		for _, s := range allSchema {
			ddls := parseDDL(t, s)
			for _, ddl := range ddls {
				db.ApplyDDL(ctx, ddl)
			}
		}

		testRunInTransaction(t, ses, func(tx *transaction) {
			listValues := []*structpb.ListValue{
				{Values: values},
			}
			if op == "INSERT" {
				if err := db.Insert(ctx, tx, tbl, wcols, listValues); err != nil {
					t.Fatalf("Insert failed: %v", err)
				}
			} else if op == "REPLACE" {
				if err := db.Replace(ctx, tx, tbl, wcols, listValues); err != nil {
					t.Fatalf("Replace failed: %v", err)
				}
			}
		})

		testRunInTransaction(t, ses, func(tx *transaction) {
			it, err := db.Read(ctx, tx, tbl, "", cols, &KeySet{All: true}, limit)
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			var rows [][]interface{}
			err = it.Do(func(row []interface{}) error {
				rows = append(rows, row)
				return nil
			})
			if err != nil {
				t.Fatalf("unexpected error in iteration: %v", err)
			}

			if len(rows) != 1 {
				t.Fatalf("unexpected numbers of rows: %v", len(rows))

			}
			row := rows[0]

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
			if d >= 100*time.Millisecond {
				t.Fatalf("unexpected time: %v", d)
			}
		})
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
		"Keyword": {
			tbl:   "From",
			wcols: fromTableKeys,
			values: []*structpb.Value{
				makeStringValue("1"),
				makeStringValue("2"),
				makeStringValue("3"),
			},
			cols:  fromTableKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(2), int64(3)},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			db := newDatabase()
			ses := newSession(db, "foo")

			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				createInitialData(t, ctx, db, tx)
			})

			testRunInTransaction(t, ses, func(tx *transaction) {
				listValues := []*structpb.ListValue{
					{Values: tt.values},
				}
				if err := db.Replace(ctx, tx, tt.tbl, tt.wcols, listValues); err != nil {
					t.Fatalf("Replace failed: %v", err)
				}
			})

			testRunInTransaction(t, ses, func(tx *transaction) {
				it, err := db.Read(ctx, tx, tt.tbl, "", tt.cols, &KeySet{All: true}, tt.limit)
				if err != nil {
					t.Fatalf("Read failed: %v", err)
				}

				var rows [][]interface{}
				err = it.Do(func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})
				if err != nil {
					t.Fatalf("unexpected error in iteration: %v", err)
				}

				if diff := cmp.Diff(tt.expected, rows); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			})
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
		"Keyword": {
			tbl:   "From",
			wcols: fromTableKeys,
			values: []*structpb.Value{
				makeStringValue("1"),
				makeStringValue("2"),
				makeStringValue("3"),
			},
			cols:  fromTableKeys,
			limit: 100,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(2), int64(3)},
			},
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			db := newDatabase()
			ses := newSession(db, "foo")

			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				createInitialData(t, ctx, db, tx)
			})

			testRunInTransaction(t, ses, func(tx *transaction) {
				listValues := []*structpb.ListValue{
					{Values: tt.values},
				}
				if err := db.Update(ctx, tx, tt.tbl, tt.wcols, listValues); err != nil {
					t.Fatalf("Update failed: %v", err)
				}
			})

			testRunInTransaction(t, ses, func(tx *transaction) {
				it, err := db.Read(ctx, tx, tt.tbl, "", tt.cols, &KeySet{All: true}, tt.limit)
				if err != nil {
					t.Fatalf("Read failed: %v", err)
				}

				var rows [][]interface{}
				err = it.Do(func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})
				if err != nil {
					t.Fatalf("unexpected error in iteration: %v", err)
				}

				if diff := cmp.Diff(tt.expected, rows); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			})
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			db := newDatabase()
			ses := newSession(db, name)

			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				createInitialData(t, ctx, db, tx)
			})

			testRunInTransaction(t, ses, func(tx *transaction) {
				listValues := []*structpb.ListValue{
					{Values: tt.values},
				}
				if err := db.InsertOrUpdate(ctx, tx, tt.tbl, tt.wcols, listValues); err != nil {
					t.Fatalf("InsertOrUpdate failed: %v", err)
				}
			})

			testRunInTransaction(t, ses, func(tx *transaction) {
				it, err := db.Read(ctx, tx, tt.tbl, "", tt.cols, &KeySet{All: true}, tt.limit)
				if err != nil {
					t.Fatalf("Read failed: %v", err)
				}

				var rows [][]interface{}
				err = it.Do(func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})
				if err != nil {
					t.Fatalf("unexpected error in iteration: %v", err)
				}

				if diff := cmp.Diff(tt.expected, rows); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			})
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

		// Escape keywords
		"Keyword_All": {
			tbl:      "From",
			ks:       &KeySet{All: true},
			cols:     []string{"ALL"},
			expected: nil,
		},
		"Keyword_Keys": {
			tbl: "From",
			ks: &KeySet{
				Keys: []*structpb.ListValue{
					makeListValue(makeStringValue("1")),
				},
			},
			cols:     []string{"ALL"},
			expected: nil,
		},
		"Keyword_Ranges": {
			tbl: "From",
			ks: &KeySet{
				Ranges: []*KeyRange{
					{
						start:       makeListValue(makeStringValue("1")),
						end:         makeListValue(makeStringValue("1")),
						startClosed: true,
						endClosed:   true,
					},
				},
			},
			cols:     []string{"ALL"},
			expected: nil,
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			db := newDatabase()
			ses := newSession(db, name)

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

				"INSERT INTO `From` VALUES(1, 1, 1)",
			} {
				if _, err := db.db.ExecContext(ctx, query); err != nil {
					t.Fatalf("Insert failed: %v", err)
				}
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				if err := db.Delete(ctx, tx, tt.tbl, tt.ks); err != nil {
					t.Fatalf("Delete failed: %v", err)
				}
			})

			testRunInTransaction(t, ses, func(tx *transaction) {
				it, err := db.Read(ctx, tx, tt.tbl, "", tt.cols, &KeySet{All: true}, 100)
				if err != nil {
					t.Fatalf("Read failed: %v", err)
				}

				var rows [][]interface{}
				err = it.Do(func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})
				if err != nil {
					t.Fatalf("unexpected error in iteration: %v", err)
				}

				if diff := cmp.Diff(tt.expected, rows); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			})
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
				makeStringValue("eHl6"),                           // FTBytes BYTES(32) NOT NULL,
				makeStringValue("eHl6"),                           // FTBytesNull BYTES(32),
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
				makeStringValue("eHl6"),                           // FTBytes BYTES(32) NOT NULL,
				makeStringValue("eHl6"),                           // FTBytesNull BYTES(32),
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
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			db := newDatabase()
			ses := newSession(db, "foo")

			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				createInitialData(t, ctx, db, tx)
			})

			for _, op := range tt.op {
				t.Run(op, func(t *testing.T) {
					listValues := []*structpb.ListValue{
						{Values: tt.values},
					}

					testRunInTransaction(t, ses, func(tx *transaction) {
						var err error
						switch op {
						case "INSERT":
							err = db.Insert(ctx, tx, tt.tbl, tt.wcols, listValues)
						case "UPDATE":
							err = db.Update(ctx, tx, tt.tbl, tt.wcols, listValues)
						case "UPSERT":
							err = db.InsertOrUpdate(ctx, tx, tt.tbl, tt.wcols, listValues)
						case "REPLACE":
							err = db.Replace(ctx, tx, tt.tbl, tt.wcols, listValues)
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
				})
			}
		})
	}
}

func TestExecute(t *testing.T) {
	table := map[string]struct {
		sql    string
		params map[string]Value

		code          codes.Code
		msg           *regexp.Regexp
		expectedCount int64
		table         string
		cols          []string
		expected      [][]interface{}
	}{
		"Simple_Update": {
			sql:           `UPDATE Simple SET Value = "zzz" WHERE Id = 100`,
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "zzz"},
			},
		},
		// TODO
		// "Simple_Update_Alias": {
		// 	sql:           `UPDATE Simple AS s SET s.Value = "zzz" WHERE s.Id = 100`,
		// 	expectedCount: 1,
		// 	table:         "Simple",
		// 	cols:          []string{"Id", "Value"},
		// 	expected: [][]interface{}{
		// 		[]interface{}{int64(100), "zzz"},
		// 	},
		// },
		"Simple_Update_ParamInWhere": {
			sql: `UPDATE Simple SET Value = "zzz" WHERE Id = @id`,
			params: map[string]Value{
				"id": makeTestValue(100),
			},
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "zzz"},
			},
		},
		"Simple_Update_ParamInValue": {
			sql: `UPDATE Simple SET Value = @value WHERE Id = 100`,
			params: map[string]Value{
				"value": makeTestValue("zzz"),
			},
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "zzz"},
			},
		},
		"Simple_Update_MultipleItems": {
			sql:  `UPDATE Simple SET Value = "zzz", Value = "zzz" WHERE Id = 100`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Update item Value assigned more than once`),
		},
		"Simple_Update_MultipleItems2": {
			sql:  `UPDATE Simple AS s SET s.Value = "zzz", Value = "zzz" WHERE Id = 100`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Update item Value assigned more than once`),
		},
		"Simple_Update_MultipleItems3": {
			sql:  `UPDATE Simple AS s SET Value = "zzz", s.Value = "zzz" WHERE Id = 100`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Update item s.Value assigned more than once`),
		},
		"Simple_Update_MultipleItems4": {
			sql:  `UPDATE Simple AS s SET s.Value = "zzz", s.Value = "zzz" WHERE Id = 100`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Update item s.Value assigned more than once`),
		},
		"Simple_Update_MultipleItems5": {
			sql:  `UPDATE Simple SET Simple.Value = "zzz", Value = "zzz" WHERE Id = 100`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Update item Value assigned more than once`),
		},
		"Simple_Update_ColumnNotFound": {
			sql:  `UPDATE Simple SET Valu = "zzz" WHERE Id = 100`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Unrecognized name: Valu`),
		},
		"Simple_Update_PathNotFound": {
			sql:  `UPDATE Simple AS s SET s.Valu = "zzz" WHERE Id = 100`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Name Valu not found inside s`),
		},
		"EscapeKeyword_Update": {
			sql:           "UPDATE `From` SET `CAST` = 2 WHERE `ALL` = 1",
			expectedCount: 1,
			table:         "From",
			cols:          fromTableKeys,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(2), int64(1)},
			},
		},
		// TODO
		// "EscapeKeyword_Update_Alias": {
		// 	sql:           "UPDATE `From` AS `AND` SET `AND`.`CAST` = 2 WHERE `ALL` = 1",
		// 	expectedCount: 1,
		// 	table:         "From",
		// 	cols:          fromTableKeys,
		// 	expected: [][]interface{}{
		// 		[]interface{}{int64(1), int64(2), int64(1)},
		// 	},
		// },
		"Simple_Insert": {
			sql:           `INSERT INTO Simple (Id, Value) VALUES(101, "yyy")`,
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(101), "yyy"},
			},
		},
		"Simple_Insert_Param": {
			sql: `INSERT INTO Simple (Id, Value) VALUES(@a, @b)`,
			params: map[string]Value{
				"a": makeTestValue("1000"),
				"b": makeTestValue("bbbb"),
			},
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(1000), "bbbb"},
			},
		},
		"Simple_InsertMulti": {
			sql:           `INSERT INTO Simple (Id, Value) VALUES(101, "yyy"), (102, "zzz")`,
			expectedCount: 2,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(101), "yyy"},
				[]interface{}{int64(102), "zzz"},
			},
		},
		"Simple_Insert_Query": {
			sql:           `INSERT INTO Simple (Id, Value) SELECT FTInt+1, PKey FROM FullTypes`,
			expectedCount: 2,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(101), "xxx"},
				[]interface{}{int64(102), "yyy"},
			},
		},
		"Simple_Insert_Subquery": {
			sql:           `INSERT INTO Simple (Id, Value) VALUES(200, (SELECT PKey FROM FullTypes WHERE PKey = "xxx"))`,
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
				[]interface{}{int64(200), "xxx"},
			},
		},
		// "Simple_Insert_Unnest": {
		// 	sql: `INSERT INTO Simple (Id, Value) SELECT * FROM UNNEST ([(200, "y"), (300, "z")])`,

		// 	expectedCount: 2,
		// 	table:         "Simple",
		// 	cols:          []string{"Id", "Value"},
		// 	expected: [][]interface{}{
		// 		[]interface{}{int64(100), "xxx"},
		// 		[]interface{}{int64(200), "y"},
		// 		[]interface{}{int64(300), "z"},
		// 	},
		// },
		"Simple_Insert_NonNullValue": {
			sql:  `INSERT INTO Simple (Id) VALUES(101)`,
			code: codes.FailedPrecondition,
			msg:  regexp.MustCompile(`A new row in table Simple does not specify a non-null value for these NOT NULL columns: Value`),
		},
		"Simple_Insert_MultipleKeys": {
			sql:  `INSERT INTO Simple (Id, Id, Value) VALUES(101, 102, "x")`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`INSERT has columns with duplicate name: Id`),
		},
		"Simple_Insert_WrongColumnCount": {
			sql:  `INSERT INTO Simple (Id, Value) VALUES(101, 102, "x")`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Inserted row has wrong column count; Has 3, expected 2`),
		},
		"Simple_Insert_ColumnNotFound": {
			sql:  `INSERT INTO Simple (Id, Valueee) VALUES(101, "x")`,
			code: codes.InvalidArgument,
			msg:  regexp.MustCompile(`Column Valueee is not present in table Simple`),
		},
		"EscapeKeyword_Insert": {
			sql:           "INSERT INTO `From` (`ALL`, `CAST`, `JOIN`) VALUES(2, 2, 2)",
			expectedCount: 1,
			table:         "From",
			cols:          fromTableKeys,
			expected: [][]interface{}{
				[]interface{}{int64(1), int64(1), int64(1)},
				[]interface{}{int64(2), int64(2), int64(2)},
			},
		},
		"Simple_Delete": {
			sql:           `DELETE FROM Simple WHERE Id = 100`,
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected:      nil,
		},
		"Simple_Delete_SubQuery": {
			sql:           `DELETE FROM Simple WHERE Id IN (SELECT Id FROM Simple)`,
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected:      nil,
		},
		"Simple_Delete_Param": {
			sql: `DELETE FROM Simple WHERE Id = @id`,
			params: map[string]Value{
				"id": makeTestValue(100),
			},
			expectedCount: 1,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected:      nil,
		},
		"Simple_Delete_NotExist": {
			sql:           `DELETE FROM Simple WHERE Id = 10000`,
			expectedCount: 0,
			table:         "Simple",
			cols:          []string{"Id", "Value"},
			expected: [][]interface{}{
				[]interface{}{int64(100), "xxx"},
			},
		},
		"EscapeKeyword_Delete": {
			sql:           "DELETE FROM `From` WHERE `ALL` = 1",
			expectedCount: 1,
			table:         "From",
			cols:          fromTableKeys,
			expected:      nil,
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			stmt, err := (&parser.Parser{
				Lexer: &parser.Lexer{
					File: &token.File{FilePath: "", Buffer: tt.sql},
				},
			}).ParseDML()
			if err != nil {
				t.Fatalf("failed to parse DML: %q %v", tt.sql, err)
			}

			db := newDatabase()
			ses := newSession(db, "foo")

			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				createInitialData(t, ctx, db, tx)
			})

			testRunInTransaction(t, ses, func(tx *transaction) {
				r, err := db.Execute(ctx, tx, stmt, tt.params)
				if tt.code == codes.OK {
					if err != nil {
						t.Fatalf("Execute failed: %v", err)
					}
					if r != tt.expectedCount {
						t.Fatalf("expect count to be %v, but got %v", tt.expectedCount, r)
					}
				} else {
					st := status.Convert(err)
					if st.Code() != tt.code {
						t.Errorf("expect code to be %v but got %v", tt.code, st.Code())
					}
					if !tt.msg.MatchString(st.Message()) {
						t.Errorf("unexpected error message: \n %q\n expected:\n %q", st.Message(), tt.msg)
					}
				}
			})

			if tt.code != codes.OK {
				return
			}

			// check database values
			testRunInTransaction(t, ses, func(tx *transaction) {
				it, err := db.Read(ctx, tx, tt.table, "", tt.cols, &KeySet{All: true}, 100)
				if err != nil {
					t.Fatalf("Read failed: %v", err)
				}

				var rows [][]interface{}
				err = it.Do(func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})
				if err != nil {
					t.Fatalf("unexpected error in iteration: %v", err)
				}

				if diff := cmp.Diff(tt.expected, rows); diff != "" {
					t.Errorf("(-got, +want)\n%s", diff)
				}
			})
		})
	}
}

func TestInterleaveInsert(t *testing.T) {
	type tableConfig struct {
		tbl      string
		wcols    []string
		values   []*structpb.Value
		cols     []string
		limit    int64
		expected [][]interface{}
	}

	table := map[string]struct {
		child        *tableConfig
		parent       *tableConfig
		expectsError bool
	}{
		"InsertWithoutParent": {
			child: &tableConfig{
				tbl:   "Interleaved",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols:     []string{"InterleavedId", "Id", "Value"},
				limit:    100,
				expected: nil,
			},
			expectsError: true,
		},
		"InsertWithParent": {
			parent: &tableConfig{
				tbl:   "ParentTable",
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
			child: &tableConfig{
				tbl:   "Interleaved",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols:  []string{"InterleavedId", "Id", "Value"},
				limit: 100,
				expected: [][]interface{}{
					[]interface{}{int64(100), int64(100), "xxx"},
				},
			},
			expectsError: false,
		},
		"InsertWithoutParent(Cascade)": {
			child: &tableConfig{
				tbl:   "Interleaved",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols:     []string{"InterleavedId", "Id", "Value"},
				limit:    100,
				expected: nil,
			},
			expectsError: true,
		},
		"InsertWithParent(Cascade)": {
			parent: &tableConfig{
				tbl:   "ParentTable",
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
			child: &tableConfig{
				tbl:   "Interleaved",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols:  []string{"InterleavedId", "Id", "Value"},
				limit: 100,
				expected: [][]interface{}{
					[]interface{}{int64(100), int64(100), "xxx"},
				},
			},
			expectsError: false,
		},
		"InsertWithoutParent(NoAction)": {
			child: &tableConfig{
				tbl:   "Interleaved",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols:     []string{"InterleavedId", "Id", "Value"},
				limit:    100,
				expected: nil,
			},
			expectsError: true,
		},
		"InsertWithParent(NoAction)": {
			parent: &tableConfig{
				tbl:   "ParentTable",
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
			child: &tableConfig{
				tbl:   "Interleaved",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols:  []string{"InterleavedId", "Id", "Value"},
				limit: 100,
				expected: [][]interface{}{
					[]interface{}{int64(100), int64(100), "xxx"},
				},
			},
			expectsError: false,
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := newDatabase()
			ses := newSession(db, "foo")
			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			if tt.parent != nil {
				testRunInTransaction(t, ses, func(tx *transaction) {
					testInsertInterleaveHelper(t, ctx, db, tx, tt.parent.tbl, tt.parent.cols, tt.parent.wcols, tt.parent.values, tt.parent.limit, tt.parent.expected)
				})
			}

			if tt.expectsError {
				listValues := []*structpb.ListValue{
					{Values: tt.child.values},
				}
				testRunInTransaction(t, ses, func(tx *transaction) {
					if err := db.Insert(ctx, tx, tt.child.tbl, tt.child.wcols, listValues); err == nil {
						t.Fatalf("Insert succeeded even though it should fail: %v", err)
					}
				})
				return
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				testInsertInterleaveHelper(t, ctx, db, tx, tt.child.tbl, tt.child.cols, tt.child.wcols, tt.child.values, tt.child.limit, tt.child.expected)
			})
		})
	}
}

func testInsertInterleaveHelper(
	t *testing.T,
	ctx context.Context,
	db *database,
	tx *transaction,
	tbl string,
	cols []string,
	wcols []string,
	values []*structpb.Value,
	limit int64,
	expected [][]interface{},
) {
	listValues := []*structpb.ListValue{
		{Values: values},
	}

	if err := db.Insert(ctx, tx, tbl, wcols, listValues); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	it, err := db.Read(ctx, tx, tbl, "", cols, &KeySet{All: true}, limit)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	var rows [][]interface{}
	err = it.Do(func(row []interface{}) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error in iteration: %v", err)
	}

	if diff := cmp.Diff(expected, rows); diff != "" {
		t.Errorf("(-got, +want)\n%s", diff)
	}
}

func TestInterleaveDeleteParent(t *testing.T) {
	type tableConfig struct {
		tbl    string
		wcols  []string
		values []*structpb.Value
		cols   []string
		ks     *KeySet
	}

	table := map[string]struct {
		child              *tableConfig
		parent             *tableConfig
		expectsDeleteError bool
	}{
		"DeleteDefault": {
			parent: &tableConfig{
				tbl:   "ParentTable",
				wcols: []string{"Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols: []string{"Id", "Value"},
				ks: &KeySet{
					Keys: []*structpb.ListValue{
						makeListValue(makeStringValue("100")),
					},
				},
			},
			child: &tableConfig{
				tbl:   "Interleaved",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols: []string{"InterleavedId", "Id", "Value"},
				ks: &KeySet{
					Keys: []*structpb.ListValue{
						makeListValue(
							makeStringValue("100"),
							makeStringValue("100"),
						),
					},
				},
			},
			expectsDeleteError: true,
		},
		"DeleteCascade": {
			parent: &tableConfig{
				tbl:   "ParentTableCascade",
				wcols: []string{"Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols: []string{"Id", "Value"},
				ks: &KeySet{
					Keys: []*structpb.ListValue{
						makeListValue(makeStringValue("100")),
					},
				},
			},
			child: &tableConfig{
				tbl:   "InterleavedCascade",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols: []string{"InterleavedId", "Id", "Value"},
				ks: &KeySet{
					Keys: []*structpb.ListValue{
						makeListValue(
							makeStringValue("100"),
							makeStringValue("100"),
						),
					},
				},
			},
			expectsDeleteError: false,
		},
		"DeleteNoAction": {
			parent: &tableConfig{
				tbl:   "ParentTableNoAction",
				wcols: []string{"Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols: []string{"Id", "Value"},
				ks: &KeySet{
					Keys: []*structpb.ListValue{
						makeListValue(makeStringValue("100")),
					},
				},
			},
			child: &tableConfig{
				tbl:   "InterleavedNoAction",
				wcols: []string{"InterleavedId", "Id", "Value"},
				values: []*structpb.Value{
					makeStringValue("100"),
					makeStringValue("100"),
					makeStringValue("xxx"),
				},
				cols: []string{"InterleavedId", "Id", "Value"},
				ks: &KeySet{
					Keys: []*structpb.ListValue{
						makeListValue(
							makeStringValue("100"),
							makeStringValue("100"),
						),
					},
				},
			},
			expectsDeleteError: true,
		},
	}

	for name, tt := range table {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := newDatabase()
			ses := newSession(db, "foo")
			for _, s := range allSchema {
				ddls := parseDDL(t, s)
				for _, ddl := range ddls {
					db.ApplyDDL(ctx, ddl)
				}
			}

			// Insert data

			parentListValues := []*structpb.ListValue{
				{Values: tt.parent.values},
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				if err := db.Insert(ctx, tx, tt.parent.tbl, tt.parent.wcols, parentListValues); err != nil {
					t.Fatalf("Insert failed: %v", err)
				}
			})

			listValues := []*structpb.ListValue{
				{Values: tt.child.values},
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				if err := db.Insert(ctx, tx, tt.child.tbl, tt.child.wcols, listValues); err != nil {
					t.Fatalf("Insert failed: %v", err)
				}
			})

			// Delete parent entry

			if tt.expectsDeleteError {
				testRunInTransaction(t, ses, func(tx *transaction) {
					if err := db.Delete(ctx, tx, tt.parent.tbl, tt.parent.ks); err == nil {
						t.Fatalf("Delete parent succeeded even though it should fail: %v", err)
					}
				})
				return
			}

			testRunInTransaction(t, ses, func(tx *transaction) {
				if err := db.Delete(ctx, tx, tt.parent.tbl, tt.parent.ks); err != nil {
					t.Fatalf("Delete parent failed: %v", err)
				}
			})

			// Try to read child entry

			testRunInTransaction(t, ses, func(tx *transaction) {
				it, err := db.Read(ctx, tx, tt.child.tbl, "", tt.child.cols, tt.child.ks, 1)
				if err != nil {
					t.Fatalf("Read failed: %v", err)
				}

				var rows [][]interface{}
				err = it.Do(func(row []interface{}) error {
					rows = append(rows, row)
					return nil
				})

				if err != nil {
					t.Fatalf("unexpected error in iteration: %v", err)
				}

				if rows != nil {
					t.Fatalf("Child did not get deleted with parent")
				}
			})
		})
	}
}

func TestInformationSchema(t *testing.T) {
	ctx := context.Background()
	db := newDatabase()
	for _, s := range allSchema {
		ddls := parseDDL(t, s)
		for _, ddl := range ddls {
			db.ApplyDDL(ctx, ddl)
		}
	}

	table := []struct {
		name     string
		sql      string
		params   map[string]Value
		expected [][]interface{}
		names    []string
		code     codes.Code
		msg      *regexp.Regexp
	}{
		{
			name:  "Schemata1",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE EFFECTIVE_TIMESTAMP IS NULL`,
			names: []string{"CATALOG_NAME", "SCHEMA_NAME", "EFFECTIVE_TIMESTAMP"},
			expected: [][]interface{}{
				[]interface{}{"", "INFORMATION_SCHEMA", nil},
				[]interface{}{"", "SPANNER_SYS", nil},
			},
		},
		{
			name:  "Schemata2",
			sql:   `SELECT CATALOG_NAME, SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE EFFECTIVE_TIMESTAMP IS NOT NULL`,
			names: []string{"CATALOG_NAME", "SCHEMA_NAME"},
			expected: [][]interface{}{
				[]interface{}{"", ""},
			},
		},
		{
			name:  "Tables_Reserved",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA != "" ORDER BY TABLE_SCHEMA, TABLE_NAME`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PARENT_TABLE_NAME", "ON_DELETE_ACTION", "SPANNER_STATE"},
			expected: [][]interface{}{
				[]interface{}{"", "INFORMATION_SCHEMA", "COLUMNS", nil, nil, nil},
				[]interface{}{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", nil, nil, nil},
				// []interface{}{"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", nil, nil, nil},
				// []interface{}{"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", nil, nil, nil},
				[]interface{}{"", "INFORMATION_SCHEMA", "INDEXES", nil, nil, nil},
				[]interface{}{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", nil, nil, nil},
				// []interface{}{"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", nil, nil, nil},
				// []interface{}{"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", nil, nil, nil},
				[]interface{}{"", "INFORMATION_SCHEMA", "SCHEMATA", nil, nil, nil},
				// []interface{}{"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", nil, nil, nil},
				[]interface{}{"", "INFORMATION_SCHEMA", "TABLES", nil, nil, nil},
				// []interface{}{"", "SPANNER_SYS", "QUERY_STATS_TOP_10MINUTE", nil, nil, nil},
				// []interface{}{"", "SPANNER_SYS", "QUERY_STATS_TOP_HOUR", nil, nil, nil},
				// []interface{}{"", "SPANNER_SYS", "QUERY_STATS_TOP_MINUTE", nil, nil, nil},
				// []interface{}{"", "SPANNER_SYS", "QUERY_STATS_TOTAL_10MINUTE", nil, nil, nil},
				// []interface{}{"", "SPANNER_SYS", "QUERY_STATS_TOTAL_HOUR", nil, nil, nil},
				// []interface{}{"", "SPANNER_SYS", "QUERY_STATS_TOTAL_MINUTE", nil, nil, nil},
			},
		},
		{
			name:  "Tables_NonReserved",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "" ORDER BY TABLE_NAME`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PARENT_TABLE_NAME", "ON_DELETE_ACTION", "SPANNER_STATE"},
			expected: [][]interface{}{
				[]interface{}{"", "", "ArrayTypes", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "CompositePrimaryKeys", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "From", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "FullTypes", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "Interleaved", "ParentTable", nil, "COMMITTED"},
				[]interface{}{"", "", "InterleavedCascade", "ParentTableCascade", "CASCADE", "COMMITTED"},
				[]interface{}{"", "", "InterleavedNoAction", "ParentTableNoAction", "NO ACTION", "COMMITTED"},
				[]interface{}{"", "", "JoinA", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "JoinB", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "ParentTable", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "ParentTableCascade", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "ParentTableNoAction", nil, nil, "COMMITTED"},
				[]interface{}{"", "", "Simple", nil, nil, "COMMITTED"},
			},
		},
		{
			name:  "Columns_Reserved",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA != "" ORDER BY TABLE_NAME, ORDINAL_POSITION`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "DATA_TYPE", "IS_NULLABLE", "SPANNER_TYPE"},
			expected: [][]interface{}{
				{"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "TABLE_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_NAME", int64(4), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "ORDINAL_POSITION", int64(5), nil, nil, "NO", "INT64"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "COLUMN_DEFAULT", int64(6), nil, nil, "YES", "BYTES(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "DATA_TYPE", int64(7), nil, nil, "YES", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "IS_NULLABLE", int64(8), nil, nil, "YES", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "SPANNER_TYPE", int64(9), nil, nil, "YES", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "TABLE_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "COLUMN_NAME", int64(4), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_NAME", int64(5), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_TYPE", int64(6), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "OPTION_VALUE", int64(7), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "TABLE_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "COLUMN_NAME", int64(4), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_CATALOG", int64(5), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_SCHEMA", int64(6), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "CONSTRAINT_NAME", int64(7), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "TABLE_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_CATALOG", int64(4), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_SCHEMA", int64(5), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_NAME", int64(6), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "TABLE_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_NAME", int64(4), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_TYPE", int64(5), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "PARENT_TABLE_NAME", int64(6), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "IS_UNIQUE", int64(7), nil, nil, "NO", "BOOL"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "IS_NULL_FILTERED", int64(8), nil, nil, "NO", "BOOL"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "INDEX_STATE", int64(9), nil, nil, "YES", "STRING(100)"}, // TODO
				{"", "INFORMATION_SCHEMA", "INDEXES", "SPANNER_IS_MANAGED", int64(10), nil, nil, "NO", "BOOL"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "TABLE_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_NAME", int64(4), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "INDEX_TYPE", int64(5), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_NAME", int64(6), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "ORDINAL_POSITION", int64(7), nil, nil, "YES", "INT64"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "COLUMN_ORDERING", int64(8), nil, nil, "YES", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "IS_NULLABLE", int64(9), nil, nil, "YES", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "SPANNER_TYPE", int64(10), nil, nil, "YES", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "CONSTRAINT_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_CATALOG", int64(4), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_SCHEMA", int64(5), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "TABLE_NAME", int64(6), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "COLUMN_NAME", int64(7), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "ORDINAL_POSITION", int64(8), nil, nil, "NO", "INT64"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "POSITION_IN_UNIQUE_CONSTRAINT", int64(9), nil, nil, "YES", "INT64"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_CATALOG", int64(4), nil, nil, "YES", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_SCHEMA", int64(5), nil, nil, "YES", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UNIQUE_CONSTRAINT_NAME", int64(6), nil, nil, "YES", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "MATCH_OPTION", int64(7), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "UPDATE_RULE", int64(8), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "DELETE_RULE", int64(9), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "SPANNER_STATE", int64(10), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "SCHEMATA", "CATALOG_NAME", int64(1), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "SCHEMATA", "SCHEMA_NAME", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "SCHEMATA", "EFFECTIVE_TIMESTAMP", int64(3), nil, nil, "YES", "INT64"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_CATALOG", int64(4), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_SCHEMA", int64(5), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "TABLE_NAME", int64(6), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "CONSTRAINT_TYPE", int64(7), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "IS_DEFERRABLE", int64(8), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "INITIALLY_DEFERRED", int64(9), nil, nil, "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "ENFORCED", int64(10), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "TABLE_CATALOG", int64(1), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "TABLE_SCHEMA", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "TABLE_NAME", int64(3), nil, nil, "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "PARENT_TABLE_NAME", int64(4), nil, nil, "YES", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "ON_DELETE_ACTION", int64(5), nil, nil, "YES", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "SPANNER_STATE", int64(6), nil, nil, "YES", "STRING(MAX)"},
			},
		},
		{
			name:  "Columns_NonReserved",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = "" ORDER BY TABLE_NAME, ORDINAL_POSITION`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "DATA_TYPE", "IS_NULLABLE", "SPANNER_TYPE"},
			expected: [][]interface{}{
				{"", "", "ArrayTypes", "Id", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "ArrayTypes", "ArrayString", int64(2), nil, nil, "YES", "ARRAY<STRING(32)>"},
				{"", "", "ArrayTypes", "ArrayBool", int64(3), nil, nil, "YES", "ARRAY<BOOL>"},
				{"", "", "ArrayTypes", "ArrayBytes", int64(4), nil, nil, "YES", "ARRAY<BYTES(32)>"},
				{"", "", "ArrayTypes", "ArrayTimestamp", int64(5), nil, nil, "YES", "ARRAY<TIMESTAMP>"},
				{"", "", "ArrayTypes", "ArrayInt", int64(6), nil, nil, "YES", "ARRAY<INT64>"},
				{"", "", "ArrayTypes", "ArrayFloat", int64(7), nil, nil, "YES", "ARRAY<FLOAT64>"},
				{"", "", "ArrayTypes", "ArrayDate", int64(8), nil, nil, "YES", "ARRAY<DATE>"},
				{"", "", "CompositePrimaryKeys", "Id", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "CompositePrimaryKeys", "PKey1", int64(2), nil, nil, "NO", "STRING(32)"},
				{"", "", "CompositePrimaryKeys", "PKey2", int64(3), nil, nil, "NO", "INT64"},
				{"", "", "CompositePrimaryKeys", "Error", int64(4), nil, nil, "NO", "INT64"},
				{"", "", "CompositePrimaryKeys", "X", int64(5), nil, nil, "NO", "STRING(32)"},
				{"", "", "CompositePrimaryKeys", "Y", int64(6), nil, nil, "NO", "STRING(32)"},
				{"", "", "CompositePrimaryKeys", "Z", int64(7), nil, nil, "NO", "STRING(32)"},
				{"", "", "From", "ALL", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "From", "CAST", int64(2), nil, nil, "NO", "INT64"},
				{"", "", "From", "JOIN", int64(3), nil, nil, "NO", "INT64"},
				{"", "", "FullTypes", "PKey", int64(1), nil, nil, "NO", "STRING(32)"},
				{"", "", "FullTypes", "FTString", int64(2), nil, nil, "NO", "STRING(32)"},
				{"", "", "FullTypes", "FTStringNull", int64(3), nil, nil, "YES", "STRING(32)"},
				{"", "", "FullTypes", "FTBool", int64(4), nil, nil, "NO", "BOOL"},
				{"", "", "FullTypes", "FTBoolNull", int64(5), nil, nil, "YES", "BOOL"},
				{"", "", "FullTypes", "FTBytes", int64(6), nil, nil, "NO", "BYTES(32)"},
				{"", "", "FullTypes", "FTBytesNull", int64(7), nil, nil, "YES", "BYTES(32)"},
				{"", "", "FullTypes", "FTTimestamp", int64(8), nil, nil, "NO", "TIMESTAMP"},
				{"", "", "FullTypes", "FTTimestampNull", int64(9), nil, nil, "YES", "TIMESTAMP"},
				{"", "", "FullTypes", "FTInt", int64(10), nil, nil, "NO", "INT64"},
				{"", "", "FullTypes", "FTIntNull", int64(11), nil, nil, "YES", "INT64"},
				{"", "", "FullTypes", "FTFloat", int64(12), nil, nil, "NO", "FLOAT64"},
				{"", "", "FullTypes", "FTFloatNull", int64(13), nil, nil, "YES", "FLOAT64"},
				{"", "", "FullTypes", "FTDate", int64(14), nil, nil, "NO", "DATE"},
				{"", "", "FullTypes", "FTDateNull", int64(15), nil, nil, "YES", "DATE"},
				{"", "", "Interleaved", "InterleavedId", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "Interleaved", "Id", int64(2), nil, nil, "NO", "INT64"},
				{"", "", "Interleaved", "Value", int64(3), nil, nil, "NO", "STRING(MAX)"},
				{"", "", "InterleavedCascade", "InterleavedId", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "InterleavedCascade", "Id", int64(2), nil, nil, "NO", "INT64"},
				{"", "", "InterleavedCascade", "Value", int64(3), nil, nil, "NO", "STRING(MAX)"},
				{"", "", "InterleavedNoAction", "InterleavedId", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "InterleavedNoAction", "Id", int64(2), nil, nil, "NO", "INT64"},
				{"", "", "InterleavedNoAction", "Value", int64(3), nil, nil, "NO", "STRING(MAX)"},
				{"", "", "JoinA", "Id", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "JoinA", "Value", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "", "JoinB", "Id", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "JoinB", "Value", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "", "ParentTable", "Id", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "ParentTable", "Value", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "", "ParentTableCascade", "Id", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "ParentTableCascade", "Value", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "", "ParentTableNoAction", "Id", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "ParentTableNoAction", "Value", int64(2), nil, nil, "NO", "STRING(MAX)"},
				{"", "", "Simple", "Id", int64(1), nil, nil, "NO", "INT64"},
				{"", "", "Simple", "Value", int64(2), nil, nil, "NO", "STRING(MAX)"},
			},
		},
		{
			name:  "ColumnOptions",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.COLUMN_OPTIONS ORDER BY TABLE_NAME`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "OPTION_NAME", "OPTION_TYPE", "OPTION_VALUE"},
			expected: [][]interface{}{
				{"", "", "FullTypes", "FTTimestampNull", "allow_commit_timestamp", "BOOL", "TRUE"},
			},
		},
		{
			name:  "Indexes_ReservedTables",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA != "" ORDER BY TABLE_NAME`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "INDEX_TYPE", "PARENT_TABLE_NAME", "IS_UNIQUE", "IS_NULL_FILTERED", "INDEX_STATE", "SPANNER_IS_MANAGED"},
			expected: [][]interface{}{
				{"", "INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
			},
		},
		{
			name:  "Indexes_NonReservedTables_PrimaryKey",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = "" AND INDEX_TYPE = "PRIMARY_KEY" ORDER BY TABLE_NAME`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "INDEX_TYPE", "PARENT_TABLE_NAME", "IS_UNIQUE", "IS_NULL_FILTERED", "INDEX_STATE", "SPANNER_IS_MANAGED"},
			expected: [][]interface{}{
				{"", "", "ArrayTypes", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "CompositePrimaryKeys", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "From", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "FullTypes", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "Interleaved", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "InterleavedCascade", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "InterleavedNoAction", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "JoinA", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "JoinB", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "ParentTable", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "ParentTableCascade", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "ParentTableNoAction", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
				{"", "", "Simple", "PRIMARY_KEY", "PRIMARY_KEY", "", true, false, nil, false},
			},
		},
		{
			name:  "Indexes_NonReservedTables_Others",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = "" AND INDEX_TYPE != "PRIMARY_KEY" ORDER BY TABLE_NAME, INDEX_NAME`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "INDEX_TYPE", "PARENT_TABLE_NAME", "IS_UNIQUE", "IS_NULL_FILTERED", "INDEX_STATE", "SPANNER_IS_MANAGED"},
			expected: [][]interface{}{
				{"", "", "CompositePrimaryKeys", "CompositePrimaryKeysByError", "INDEX", "", false, false, "READ_WRITE", false},
				{"", "", "CompositePrimaryKeys", "CompositePrimaryKeysByXY", "INDEX", "", false, false, "READ_WRITE", false},
				{"", "", "From", "ALL", "INDEX", "", false, false, "READ_WRITE", false},
				{"", "", "FullTypes", "FullTypesByFTString", "INDEX", "", true, false, "READ_WRITE", false},
				{"", "", "FullTypes", "FullTypesByIntDate", "INDEX", "", true, false, "READ_WRITE", false},
				{"", "", "FullTypes", "FullTypesByIntTimestamp", "INDEX", "", false, false, "READ_WRITE", false},
				{"", "", "FullTypes", "FullTypesByTimestamp", "INDEX", "", false, false, "READ_WRITE", false},
				{"", "", "Interleaved", "InterleavedKey", "INDEX", "ParentTable", false, false, "READ_WRITE", false},
			},
		},
		{
			name:  "IndexColumns_ReservedTables",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_SCHEMA != "" ORDER BY TABLE_NAME, INDEX_NAME, ORDINAL_POSITION`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "INDEX_TYPE", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_ORDERING", "IS_NULLABLE", "SPANNER_TYPE"},
			expected: [][]interface{}{
				{"", "INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", int64(4), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", int64(4), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "COLUMN_OPTIONS", "PRIMARY_KEY", "PRIMARY_KEY", "OPTION_NAME", int64(5), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", int64(4), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", int64(4), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", int64(5), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "CONSTRAINT_TABLE_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", int64(6), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "INDEX_NAME", int64(4), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEXES", "PRIMARY_KEY", "PRIMARY_KEY", "INDEX_TYPE", int64(5), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "INDEX_NAME", int64(4), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "INDEX_TYPE", int64(5), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "INDEX_COLUMNS", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", int64(6), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "KEY_COLUMN_USAGE", "PRIMARY_KEY", "PRIMARY_KEY", "COLUMN_NAME", int64(4), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "REFERENTIAL_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY", "PRIMARY_KEY", "CATALOG_NAME", int64(1), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "SCHEMATA", "PRIMARY_KEY", "PRIMARY_KEY", "SCHEMA_NAME", int64(2), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				// {"", "INFORMATION_SCHEMA", "TABLE_CONSTRAINTS", "PRIMARY_KEY", "PRIMARY_KEY", "CONSTRAINT_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_CATALOG", int64(1), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_SCHEMA", int64(2), "ASC", "NO", "STRING(MAX)"},
				{"", "INFORMATION_SCHEMA", "TABLES", "PRIMARY_KEY", "PRIMARY_KEY", "TABLE_NAME", int64(3), "ASC", "NO", "STRING(MAX)"},
			},
		},
		{
			name:  "IndexColumns_NonReservedTables",
			sql:   `SELECT * FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_SCHEMA = "" ORDER BY TABLE_NAME, INDEX_NAME, ORDINAL_POSITION`,
			names: []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "INDEX_TYPE", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_ORDERING", "IS_NULLABLE", "SPANNER_TYPE"},
			expected: [][]interface{}{
				{"", "", "ArrayTypes", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},

				{"", "", "CompositePrimaryKeys", "CompositePrimaryKeysByError", "INDEX", "Error", int64(1), "ASC", "NO", "INT64"},
				{"", "", "CompositePrimaryKeys", "CompositePrimaryKeysByXY", "INDEX", "Z", nil, nil, "NO", "STRING(32)"},
				{"", "", "CompositePrimaryKeys", "CompositePrimaryKeysByXY", "INDEX", "X", int64(1), "ASC", "NO", "STRING(32)"},
				{"", "", "CompositePrimaryKeys", "CompositePrimaryKeysByXY", "INDEX", "Y", int64(2), "DESC", "NO", "STRING(32)"},
				{"", "", "CompositePrimaryKeys", "PRIMARY_KEY", "PRIMARY_KEY", "PKey1", int64(1), "ASC", "NO", "STRING(32)"},
				{"", "", "CompositePrimaryKeys", "PRIMARY_KEY", "PRIMARY_KEY", "PKey2", int64(2), "DESC", "NO", "INT64"},
				{"", "", "From", "ALL", "INDEX", "ALL", int64(1), "ASC", "NO", "INT64"},
				{"", "", "From", "PRIMARY_KEY", "PRIMARY_KEY", "ALL", int64(1), "ASC", "NO", "INT64"},
				{"", "", "FullTypes", "FullTypesByFTString", "INDEX", "FTString", int64(1), "ASC", "NO", "STRING(32)"},
				{"", "", "FullTypes", "FullTypesByIntDate", "INDEX", "FTInt", int64(1), "ASC", "NO", "INT64"},
				{"", "", "FullTypes", "FullTypesByIntDate", "INDEX", "FTDate", int64(2), "ASC", "NO", "DATE"},
				{"", "", "FullTypes", "FullTypesByIntTimestamp", "INDEX", "FTInt", int64(1), "ASC", "NO", "INT64"},
				{"", "", "FullTypes", "FullTypesByIntTimestamp", "INDEX", "FTTimestamp", int64(2), "ASC", "NO", "TIMESTAMP"},
				{"", "", "FullTypes", "FullTypesByTimestamp", "INDEX", "FTTimestamp", int64(1), "ASC", "NO", "TIMESTAMP"},
				{"", "", "FullTypes", "PRIMARY_KEY", "PRIMARY_KEY", "PKey", int64(1), "ASC", "NO", "STRING(32)"},
				{"", "", "Interleaved", "InterleavedKey", "INDEX", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "Interleaved", "InterleavedKey", "INDEX", "Value", int64(2), "ASC", "NO", "STRING(MAX)"},
				{"", "", "Interleaved", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "Interleaved", "PRIMARY_KEY", "PRIMARY_KEY", "InterleavedId", int64(2), "ASC", "NO", "INT64"},
				{"", "", "InterleavedCascade", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "InterleavedCascade", "PRIMARY_KEY", "PRIMARY_KEY", "InterleavedId", int64(2), "ASC", "NO", "INT64"},
				{"", "", "InterleavedNoAction", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "InterleavedNoAction", "PRIMARY_KEY", "PRIMARY_KEY", "InterleavedId", int64(2), "ASC", "NO", "INT64"},
				{"", "", "JoinA", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "JoinB", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "ParentTable", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "ParentTableCascade", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "ParentTableNoAction", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
				{"", "", "Simple", "PRIMARY_KEY", "PRIMARY_KEY", "Id", int64(1), "ASC", "NO", "INT64"},
			},
		},
		{
			name: "IndexColumns_WithAlias",
			sql:  `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS a WHERE a.TABLE_NAME = "Simple"`,
			expected: [][]interface{}{
				[]interface{}{"Id"},
			},
		},
		{
			name: "IndexColumns_WithOmittedSchema",
			sql:  `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE INDEX_COLUMNS.TABLE_NAME = "Simple"`,
			expected: [][]interface{}{
				[]interface{}{"Id"},
			},
		},
		{
			name: "IndexColumns_WithJoin",
			sql:  `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS LEFT JOIN INFORMATION_SCHEMA.INDEXES ON INDEX_COLUMNS.TABLE_NAME = INDEXES.TABLE_NAME WHERE INDEX_COLUMNS.TABLE_NAME = "Simple"`,
			expected: [][]interface{}{
				[]interface{}{"Id"},
			},
		},
		{
			name: "IndexColumns_WithUsingJoin",
			sql:  `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS LEFT JOIN INFORMATION_SCHEMA.INDEXES USING(TABLE_NAME) WHERE INDEX_COLUMNS.TABLE_NAME = "Simple"`,
			expected: [][]interface{}{
				[]interface{}{"Id"},
			},
		},
	}

	for _, tc := range table {
		t.Run(tc.name, func(t *testing.T) {
			ses := newSession(db, "foo")
			testRunInTransaction(t, ses, func(tx *transaction) {
				stmt, err := (&parser.Parser{
					Lexer: &parser.Lexer{
						File: &token.File{FilePath: "", Buffer: tc.sql},
					},
				}).ParseQuery()
				if err != nil {
					t.Fatalf("failed to parse sql: %q %v", tc.sql, err)
				}

				// The test case expects OK, it checks respons values.
				// otherwise it checks the error code and the error message.
				if tc.code == codes.OK {
					it, err := db.Query(ctx, tx, stmt, tc.params)
					if err != nil {
						t.Fatalf("Query failed: %v", err)
					}

					var rows [][]interface{}
					err = it.Do(func(row []interface{}) error {
						rows = append(rows, row)
						return nil
					})
					if err != nil {
						t.Fatalf("unexpected error in iteration: %v", err)
					}

					if diff := cmp.Diff(tc.expected, rows); diff != "" {
						t.Errorf("(-got, +want)\n%s", diff)
					}

					// TODO: add names to all test cases. now this is optional check
					if tc.names != nil {
						var gotnames []string
						for _, item := range it.ResultSet() {
							gotnames = append(gotnames, item.Name)
						}

						if diff := cmp.Diff(tc.names, gotnames); diff != "" {
							t.Errorf("(-got, +want)\n%s", diff)
						}
					}
				} else {
					it, err := db.Query(ctx, tx, stmt, tc.params)
					if err == nil {
						err = it.Do(func([]interface{}) error {
							return nil
						})
					}
					st := status.Convert(err)
					if st.Code() != tc.code {
						t.Errorf("expect code to be %v but got %v", tc.code, st.Code())
					}
					if !tc.msg.MatchString(st.Message()) {
						t.Errorf("unexpected error message: \n %q\n expected:\n %q", st.Message(), tc.msg)
					}
				}
			})
		})
	}
}
