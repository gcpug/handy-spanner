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
	"math"
	"regexp"
	"testing"

	"github.com/MakeNowJust/memefish/pkg/parser"
	"github.com/MakeNowJust/memefish/pkg/token"
	cmp "github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

	table := map[string][]struct {
		name     string
		sql      string
		params   map[string]Value
		expected [][]interface{}
		names    []string
		code     codes.Code
		msg      *regexp.Regexp
	}{
		"Simple": {
			{
				name: "NoTable_IntLiteral",
				sql:  `SELECT 1`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},
			{
				name: "NoTable_StringLiteral",
				sql:  `SELECT "foo"`,
				expected: [][]interface{}{
					[]interface{}{"foo"},
				},
			},
			{
				name: "NoTable_NullLiteral",
				sql:  `SELECT NULL`,
				expected: [][]interface{}{
					[]interface{}{nil},
				},
			},
			{
				name: "NoTable_Params_Int",
				sql:  `SELECT @foo`,
				params: map[string]Value{
					"foo": makeTestValue(int64(100)),
				},
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "NoTable_Params_String",
				sql:  `SELECT @foo`,
				params: map[string]Value{
					"foo": makeTestValue("xxx"),
				},
				expected: [][]interface{}{
					[]interface{}{"xxx"},
				},
			},
			{
				name: "Simple_Star",
				sql:  "SELECT * FROM Simple",
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name: "TableNotFound",
				sql:  "SELECT * FROM xxx",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`Table not found: xxx`),
			},
			// TODO: memefish cannot parse this sql
			// {
			// 	name: "QueryParam_TableName",
			// 	sql:  "SELECT 1 FROM @table",
			// 	code: codes.InvalidArgument,
			// 	msg:  regexp.MustCompile(`Query parameters cannot be used in place of table names`),
			// },
			{
				name: "Star_WithoutFromClause",
				sql:  "SELECT *",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`SELECT \* must have a FROM clause`),
			},
			{
				name: "Simple_Identifer",
				sql:  "SELECT Value, Id FROM Simple",
				expected: [][]interface{}{
					[]interface{}{"xxx", int64(100)},
					[]interface{}{"yyy", int64(200)},
					[]interface{}{"zzz", int64(300)},
				},
			},
			{
				name: "Identifer_NotFound",
				sql:  "SELECT x.* FROM Simple a",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`Unrecognized name: x`),
			},
			{
				name: "Identifer_NotFound2",
				sql:  "SELECT foo FROM Simple",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`Unrecognized name: foo`),
			},
			{
				name: "Identifer_NotFound3",
				sql:  "SELECT a.foo FROM Simple a",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`Name foo not found inside a`),
			},
			{
				name: "Identifer_NotFound_WithoutFromClause",
				sql:  "SELECT x",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`Unrecognized name: x`),
			},
			{
				name: "PathIdentifer_InvalidFieldAccess",
				sql:  "SELECT a.Id.zzz FROM Simple a",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`Cannot access field zzz on a value with type INT64`),
			},
			{
				name: "PathIdentifer_NotFound_WithoutFromClause",
				sql:  "SELECT x.*",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`Unrecognized name: x`),
			},
			{
				name: "Simple_Alias",
				sql:  "SELECT Id a, Value b FROM Simple",
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name: "Alias_DuplicateTableAlias",
				sql:  "SELECT 1 FROM Simple a, Simple a",
				// code: codes.InvalidArgument,
				code: codes.Unknown, // TODO
				msg:  regexp.MustCompile(`Duplicate table alias a in the same FROM clause`),
			},
			{
				name: "Alias_AmbiguousColumn",
				sql:  "SELECT Id FROM Simple a, Simple b",
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`Column name Id is ambiguous`),
			},
		},
		"SimpleWhere": {
			{
				name: "Simple_Where_IntLiteral",
				sql:  "SELECT Id, Value FROM Simple WHERE Id = 200",
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name: "Simple_Where_IntLiteral2",
				sql:  "SELECT Id, Value FROM Simple WHERE Id >= 200",
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name: "Simple_Where_IntLiteral3",
				sql:  "SELECT Id, Value FROM Simple WHERE Id = +200",
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name:     "Simple_Where_IntLiteral4",
				sql:      "SELECT Id, Value FROM Simple WHERE Id = -200",
				expected: nil,
			},
			{
				name: "Simple_Where_StringLiteral",
				sql:  `SELECT Id, Value FROM Simple WHERE Value = "xxx"`,
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name: "Simple_Where_StringLiteral2",
				sql:  `SELECT Id, Value FROM Simple WHERE Value > "xxx"`,
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name: "Simple_Where_Param",
				sql:  `SELECT Id, Value FROM Simple WHERE Id = @id`,
				params: map[string]Value{
					"id": makeTestValue(100),
				},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name: "Simple_Where_AND",
				sql:  `SELECT Id, Value FROM Simple WHERE Id > @id AND Value = @val`,
				params: map[string]Value{
					"id":  makeTestValue(100),
					"val": makeTestValue("zzz"),
				},
				expected: [][]interface{}{
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name: "Simple_Where_Paren",
				sql:  `SELECT Id, Value FROM Simple WHERE Id >= @id AND (Value = @val1 OR Value = @val2)`,
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
			{
				name:   "Simple_Where_LIKE",
				sql:    `SELECT Id, Value FROM Simple WHERE Value LIKE "x%"`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name:   "Simple_Where_NOT_LIKE",
				sql:    `SELECT Id, Value FROM Simple WHERE Value NOT LIKE "x%"`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name:   "Simple_Where_IN",
				sql:    `SELECT Id, Value FROM Simple WHERE Id IN (100, 300)`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name:   "Simple_Where_NOT_IN",
				sql:    `SELECT Id, Value FROM Simple WHERE Id NOT IN (100, 300)`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name:     "Simple_Where_IS_NULL",
				sql:      `SELECT Id, Value FROM Simple WHERE Value IS NULL`,
				params:   map[string]Value{},
				expected: nil,
			},
			{
				name:   "Simple_Where_IS_NOT_NULL",
				sql:    `SELECT Id, Value FROM Simple WHERE Value IS NOT NULL`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name:     "Simple_Where_IS_BOOL",
				sql:      `SELECT Id, Value FROM Simple WHERE Id IS TRUE`,
				params:   map[string]Value{},
				expected: nil,
			},
			{
				name:   "Simple_Where_IS_NOT_BOOL",
				sql:    `SELECT Id, Value FROM Simple WHERE Id IS NOT TRUE`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name:   "Simple_Where_BETWEEN",
				sql:    `SELECT Id, Value FROM Simple WHERE Id BETWEEN 200 AND 300`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name:   "Simple_Where_NOT_BETWEEN",
				sql:    `SELECT Id, Value FROM Simple WHERE Id NOT BETWEEN 200 AND 300`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},

			{
				name:   "Simple_Where_STARTS_WITH",
				sql:    `SELECT Id, Value FROM Simple WHERE STARTS_WITH(Value, "x")`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name: "Simple_Where_STARTS_WITH_PARAM",
				sql:  `SELECT Id, Value FROM Simple WHERE STARTS_WITH(Value, @x)`,
				params: map[string]Value{
					"x": makeTestValue("xx"),
				},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
		},

		"SimpleGroup": {
			{
				name:   "Simple_GROUP",
				sql:    `SELECT Id, Value FROM Simple GROUP BY Value`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name:   "Simple_GROUP_HAVING",
				sql:    `SELECT Id, Value FROM Simple GROUP BY Value HAVING Value > "xxx"`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
		},
		"LimitOffset": {
			{
				name:   "Simple_LIMIT",
				sql:    `SELECT Id, Value FROM Simple LIMIT 1`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name: "Simple_LIMIT_Param",
				sql:  `SELECT Id, Value FROM Simple LIMIT @limit`,
				params: map[string]Value{
					"limit": makeTestValue(2),
				},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name:   "Simple_LIMIT_OFFSET",
				sql:    `SELECT Id, Value FROM Simple LIMIT 1 OFFSET 1`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name: "Simple_LIMIT_OFFSET_Param",
				sql:  `SELECT Id, Value FROM Simple LIMIT @limit OFFSET @offset`,
				params: map[string]Value{
					"limit":  makeTestValue(1),
					"offset": makeTestValue(1),
				},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name: "Limit_InvalidType",
				sql:  `SELECT 1 FROM Simple LIMIT @foo`,
				params: map[string]Value{
					"foo": makeTestValue("xx"),
				},
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^LIMIT expects an integer literal or parameter`),
			},
			{
				name: "Offset_InvalidType",
				sql:  `SELECT 1 FROM Simple LIMIT 1 OFFSET @foo`,
				params: map[string]Value{
					"foo": makeTestValue("xx"),
				},
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^OFFSET expects an integer literal or parameter`),
			},
		},

		"OrderBy": {
			{
				name:   "Simple_ORDER",
				sql:    `SELECT Id, Value FROM Simple ORDER BY Id DESC`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(300), "zzz"},
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name:   "Simple_ORDER2",
				sql:    `SELECT Id, Value FROM Simple ORDER BY Id DESC, Value`,
				params: map[string]Value{},
				expected: [][]interface{}{
					[]interface{}{int64(300), "zzz"},
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(100), "xxx"},
				},
			},
		},

		"Array": {
			{
				name: "ArrayIndex_OFFSET1",
				sql:  `SELECT [1, 2, 3][OFFSET(0)]`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},
			{
				name: "ArrayIndex_OFFSET2",
				sql:  `SELECT [1, 2, 3][OFFSET(2)]`,
				expected: [][]interface{}{
					[]interface{}{int64(3)},
				},
			},
			{
				name: "ArrayIndex_OFFSET_Param",
				sql:  `SELECT [1, 2, 3][OFFSET(@foo)]`,
				params: map[string]Value{
					"foo": makeTestValue(1),
				},
				expected: [][]interface{}{
					[]interface{}{int64(2)},
				},
			},
			{
				name: "ArrayIndex_ORDINAL1",
				sql:  `SELECT [1, 2, 3][ORDINAL(1)]`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},
			{
				name: "ArrayIndex_ORDINAL3",
				sql:  `SELECT [1, 2, 3][ORDINAL(3)]`,
				expected: [][]interface{}{
					[]interface{}{int64(3)},
				},
			},
			{
				name: "ArrayIndex_ORDINAL_Param",
				sql:  `SELECT [1, 2, 3][ORDINAL(@foo)]`,
				params: map[string]Value{
					"foo": makeTestValue(1),
				},
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},

			{
				name: "ArrayLiteral_Empty",
				sql:  `SELECT []`,
				expected: [][]interface{}{
					[]interface{}{makeTestArray(TCInt64)},
				},
			},
			{
				name: "ArrayLiteral_IntLiteral",
				sql:  `SELECT [1, 2, 3]`,
				expected: [][]interface{}{
					[]interface{}{makeTestArray(TCInt64, 1, 2, 3)},
				},
			},
			{
				name: "ArrayLiteral_Ident",
				sql:  `SELECT [1, Id] FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{makeTestArray(TCInt64, 1, 100)},
					[]interface{}{makeTestArray(TCInt64, 1, 200)},
					[]interface{}{makeTestArray(TCInt64, 1, 300)},
				},
			},
			{
				name: "ArrayLiteral_Params",
				sql:  `SELECT ["xxx", @foo]`,
				params: map[string]Value{
					"foo": makeTestValue("yyy"),
				},
				expected: [][]interface{}{
					[]interface{}{makeTestArray(TCString, "xxx", "yyy")},
				},
			},
			{
				name: "ArrayLiteral_IncompatibleElements",
				sql:  `SELECT [100, "xxx"]`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Array elements of types {.*} do not have a common supertype`),
			},
			{
				name: "ArrayLiteral_Unnest_ImcompatibleElements",
				sql:  `SELECT * FROM UNNEST (["xxx", 1])`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Array elements of types {.*} do not have a common supertype`),
			},
			{
				name: "ArrayLiteral_Unnest_Bool",
				sql:  `SELECT 1 FROM UNNEST (true)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Values referenced in UNNEST must be arrays. UNNEST contains expression of type BOOL`),
			},
			{
				name: "ArrayLiteral_In_Unnest_Bool",
				sql:  `SELECT 1 FROM Simple WHERE 1 IN UNNEST (true)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Second argument of IN UNNEST must be an array but was BOOL`),
			},
			// {
			// 	name: "ArrayLiteral_IntAndFloat",
			// 	sql: `SELECT [100, 0.1]`,
			// 	expected: [][]interface{}{
			// 		[]interface{}{makeTestArray(TCFloat64, 0.1, 0.1)},
			// 	},
			// },
			{
				name: "NestedArray",
				sql:  `SELECT ARRAY(SELECT [1,2,3])`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Cannot use array subquery with column of type ARRAY<INT64> because nested arrays are not supported`),
			},
		},
		"Timestamp": {
			{
				name: "TimestampLiteral_Date",
				sql:  `SELECT TIMESTAMP "1999-01-02"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T08:00:00Z"},
				},
			},
			{
				name: "TimestampLiteral_Date2",
				sql:  `SELECT TIMESTAMP "1999-1-2"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T08:00:00Z"},
				},
			},
			{
				name: "TimestampLiteral_WithoutNano",
				sql:  `SELECT TIMESTAMP "1999-01-02 12:34:56"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T20:34:56Z"},
				},
			},
			{
				name: "TimestampLiteral_WithoutNano2",
				sql:  `SELECT TIMESTAMP "1999-01-02 1:2:3"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T09:02:03Z"},
				},
			},
			{
				name: "TimestampLiteral_WithoutNano_T",
				sql:  `SELECT TIMESTAMP "1999-01-02T12:34:56"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T20:34:56Z"},
				},
			},
			{
				name: "TimestampLiteral_WithNano",
				sql:  `SELECT TIMESTAMP "1999-01-02 1:2:3.123456789"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T09:02:03.123456789Z"},
				},
			},
			{
				name: "TimestampLiteral_WithNano2",
				sql:  `SELECT TIMESTAMP "1999-01-02 01:02:03.123456"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T09:02:03.123456Z"},
				},
			},
			{
				name: "TimestampLiteral_WithNano3",
				sql:  `SELECT TIMESTAMP "1999-01-02 01:02:03.12"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T09:02:03.12Z"},
				},
			},
			{
				name: "TimestampLiteral_WithNano_T",
				sql:  `SELECT TIMESTAMP "1999-01-02T1:2:3.123456789"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T09:02:03.123456789Z"},
				},
			},
			{
				name: "TimestampLiteral_Timezone",
				sql:  `SELECT TIMESTAMP "1999-01-02 12:02:03Z"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T12:02:03Z"},
				},
			},
			{
				name: "TimestampLiteral_Timezone2",
				sql:  `SELECT TIMESTAMP "1999-01-02 12:02:03+01"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T11:02:03Z"},
				},
			},
			{
				name: "TimestampLiteral_Timezone3",
				sql:  `SELECT TIMESTAMP "1999-01-02 12:02:03-01"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T13:02:03Z"},
				},
			},
			{
				name: "TimestampLiteral_Timezone4",
				sql:  `SELECT TIMESTAMP "1999-01-02 12:02:03+01:30"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T10:32:03Z"},
				},
			},
			{
				name: "TimestampLiteral_Timezone_T",
				sql:  `SELECT TIMESTAMP "1999-01-02T12:02:03Z"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T12:02:03Z"},
				},
			},
			{
				name: "TimestampLiteral_WithNano_Timezone",
				sql:  `SELECT TIMESTAMP "1999-01-02 12:02:03.123456789Z"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T12:02:03.123456789Z"},
				},
			},
			{
				name: "TimestampLiteral_WithNano_Timezone2",
				sql:  `SELECT TIMESTAMP "1999-01-02 12:02:03.123456789+03"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T09:02:03.123456789Z"},
				},
			},
			{
				name: "TimestampLiteral_WithNano_Timezone_T",
				sql:  `SELECT TIMESTAMP "1999-01-02T12:02:03.123456789Z"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02T12:02:03.123456789Z"},
				},
			},
			{
				name: "DateLiteral",
				sql:  `SELECT DATE "1999-01-02"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02"},
				},
			},
			{
				name: "DateLiteral2",
				sql:  `SELECT DATE "1999-1-2"`,
				expected: [][]interface{}{
					[]interface{}{"1999-01-02"},
				},
			},
			{
				name: "BytesLiteral",
				sql:  `SELECT B'xyz'`,
				expected: [][]interface{}{
					[]interface{}{[]byte("xyz")},
				},
			},
		},

		"InUnnest": {
			{
				name: "Bind",
				sql:  `SELECT Id, Value FROM Simple WHERE Id IN UNNEST (@foo)`,
				params: map[string]Value{
					"foo": makeTestValue([]int64{100, 200}),
				},
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name: "Bind2",
				sql:  `SELECT Id, Value FROM Simple WHERE Id IN UNNEST (@foo)`,
				params: map[string]Value{
					"foo": makeTestValue([]int64{}),
				},
				expected: nil,
			},
			{
				name: "Bind3",
				sql:  `SELECT Id, Value FROM Simple WHERE Value IN UNNEST (@foo)`,
				params: map[string]Value{
					"foo": makeTestValue([]string{"yyy"}),
				},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name: "ArrayLiteral",
				sql:  `SELECT Id, Value FROM Simple WHERE Id IN UNNEST ([100 ,200, 300])`,
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
					[]interface{}{int64(200), "yyy"},
					[]interface{}{int64(300), "zzz"},
				},
			},
			{
				name: "ArrayLiteral2",
				sql:  `SELECT Id, Value FROM Simple WHERE Value IN UNNEST (["xxx"])`,
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name: "Ident",
				sql:  `SELECT Id FROM ArrayTypes WHERE 1 IN UNNEST (ArrayInt)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name:     "Ident2",
				sql:      `SELECT Id FROM ArrayTypes WHERE 1 NOT IN UNNEST (ArrayInt)`,
				expected: nil,
			},
		},
		"FromUnnest": {
			{
				name: "Lietral_String",
				sql:  `SELECT * FROM UNNEST (["xxx", "yyy"])`,
				expected: [][]interface{}{
					[]interface{}{"xxx"},
					[]interface{}{"yyy"},
				},
			},
			{
				name: "Literal_Int",
				sql:  `SELECT * FROM UNNEST ([1,2,3])`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
					[]interface{}{int64(2)},
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Literal_Params",
				sql:  `SELECT * FROM UNNEST ([ @a, @b, @c])`,
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
			{
				name: "Literal_Params_Array",
				sql:  `SELECT * FROM UNNEST (@foo)`,
				params: map[string]Value{
					"foo": makeTestValue([]int64{3, 4}),
				},
				expected: [][]interface{}{
					[]interface{}{int64(3)},
					[]interface{}{int64(4)},
				},
			},
			{
				name: "Literal_Params_Array_WithOffset",
				sql:  `SELECT * FROM UNNEST (@foo) WITH OFFSET`,
				params: map[string]Value{
					"foo": makeTestValue([]int64{3, 4}),
				},
				expected: [][]interface{}{
					[]interface{}{int64(3), int64(0)},
					[]interface{}{int64(4), int64(1)},
				},
			},
			{
				name: "Literal_Params_Alias",
				sql:  `SELECT x FROM UNNEST (@foo) AS x`,
				params: map[string]Value{
					"foo": makeTestValue([]int64{3, 4}),
				},
				expected: [][]interface{}{
					[]interface{}{int64(3)},
					[]interface{}{int64(4)},
				},
			},
			{
				name: "Literal_Params_WithOffset_Alias",
				sql:  `SELECT x, y FROM UNNEST (@foo) AS x WITH OFFSET y`,
				params: map[string]Value{
					"foo": makeTestValue([]int64{3, 4}),
				},
				expected: [][]interface{}{
					[]interface{}{int64(3), int64(0)},
					[]interface{}{int64(4), int64(1)},
				},
			},
			{
				name: "Literal_Alias_Star",
				sql:  `SELECT * FROM UNNEST ([1,2,3]) AS xxx`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
					[]interface{}{int64(2)},
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Literal_Alias_Star_WithOffset",
				sql:  `SELECT * FROM UNNEST ([1,2,3]) AS xxx WITH OFFSET AS yyy`,
				expected: [][]interface{}{
					[]interface{}{int64(1), int64(0)},
					[]interface{}{int64(2), int64(1)},
					[]interface{}{int64(3), int64(2)},
				},
			},
			{
				name: "Literal_Alias_Ident",
				sql:  `SELECT xxx FROM UNNEST ([1,2,3]) AS xxx`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
					[]interface{}{int64(2)},
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Literal_Alias_WithOffset_Ident",
				sql:  `SELECT xxx, yyy FROM UNNEST ([1,2,3]) AS xxx WITH OFFSET AS yyy`,
				expected: [][]interface{}{
					[]interface{}{int64(1), int64(0)},
					[]interface{}{int64(2), int64(1)},
					[]interface{}{int64(3), int64(2)},
				},
			},
			// TODO: To be able to use Ident and Path in FROM clause
			// {
			// 	name: "Join",
			// 	sql:  `SELECT Id, flatten FROM ArrayTypes, UNNEST (ArrayTypes.ArrayString) AS flatten`,
			// 	expected: [][]interface{}{
			// 		[]interface{}{int64(1), int64(0)},
			// 		[]interface{}{int64(1), int64(1)},
			// 	},
			// },
			// {
			// 	name: "Join2",
			// 	sql:  `SELECT Id, flatten FROM ArrayTypes, UNNEST (ArrayString) AS flatten`,
			// 	expected: [][]interface{}{
			// 		[]interface{}{int64(1), int64(0)},
			// 		[]interface{}{int64(1), int64(1)},
			// 	},
			// },
			// {
			// 	name: "Join3",
			// 	sql:  `SELECT a.Id, flatten FROM ArrayTypes a, UNNEST (a.ArrayString) AS flatten`,
			// 	expected: [][]interface{}{
			// 		[]interface{}{int64(1), int64(0)},
			// 		[]interface{}{int64(1), int64(1)},
			// 	},
			// },
		},

		"Struct": {
			{
				name: "StructLiteral",
				sql:  `SELECT ARRAY(SELECT (1,"xx") x)`,
				expected: [][]interface{}{
					[]interface{}{ArrayStruct{
						Values: []*StructValue{
							{
								Keys:   []string{"", ""},
								Values: []interface{}{float64(1), string("xx")},
							},
						},
					},
					},
				},
			},
			{
				name: "StructLiteral_WithField",
				sql:  `SELECT ARRAY(SELECT STRUCT<Id int64, Value string>(1,"xx") x)`,
				expected: [][]interface{}{
					[]interface{}{ArrayStruct{
						Values: []*StructValue{
							{
								Keys:   []string{"Id", "Value"},
								Values: []interface{}{float64(1), string("xx")},
							},
						},
					},
					},
				},
			},
			{
				name: "StructLiteral_WithField_WithoutName",
				sql:  `SELECT ARRAY(SELECT STRUCT<int64, Value string>(1,"xx") x)`,
				expected: [][]interface{}{
					[]interface{}{ArrayStruct{
						Values: []*StructValue{
							{
								Keys:   []string{"", "Value"},
								Values: []interface{}{float64(1), string("xx")},
							},
						},
					},
					},
				},
			},
			// Parse error
			// {
			// 	name: "StructLiteral_WithField_WithoutType",
			// 	sql:  `SELECT ARRAY(SELECT STRUCT<Id, Value string>(1,"xx") x)`,
			// },
			{
				name: "StructLiteral_InvalidFieldSize",
				sql:  `SELECT ARRAY(SELECT STRUCT<Id INT64>(1,"xx") x)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^STRUCT type has 1 fields but constructor call has 2 fields`),
			},
			{
				name: "StructLiteral_InvalidFieldSize",
				sql:  `SELECT ARRAY(SELECT STRUCT<>(1,"xx") x)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^STRUCT type has 0 fields but constructor call has 2 fields`),
			},
			{
				name: "StructLiteral_IncompatibleType",
				sql:  `SELECT ARRAY(SELECT STRUCT<Id INT64, Value INT64>(1,"xx") x)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Struct field 2 has type literal STRING which does not coerce to INT64`),
			},
			{
				name: "PathIdentifier_StructField_BeginFromTableAlias",
				sql:  `SELECT y.x.Id, y.x.Value FROM (SELECT STRUCT<Id int64, Value string>(1, "xx") x) y`,
				expected: [][]interface{}{
					[]interface{}{int64(1), string("xx")},
				},
			},
			{
				name: "PathIdentifier_StructField_BeginFromStructColumnAlias",
				sql:  `SELECT x.Id, x.Value FROM (SELECT STRUCT<Id int64, Value string>(1, "xx") x)`,
				expected: [][]interface{}{
					[]interface{}{int64(1), string("xx")},
				},
			},
			{
				name: "PathIdentifier_StructField_Subquery_NotFound",
				sql:  `SELECT y.x.zzz FROM (SELECT STRUCT<Id int64, Value string>(1, "xx") x) y`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Field name zzz does not exist in STRUCT<Id INT64, Value STRING>`),
			},
			{
				name: "Selector_ArrayOfStruct",
				sql:  `SELECT z.y[OFFSET(0)].Id FROM (SELECT ARRAY(SELECT STRUCT<Id int64, Value string>(1,"xx") x) y) z`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},
			{
				name: "DotStar_Ident_StructField",
				sql:  `SELECT x.* FROM (SELECT STRUCT<Id int64, Value string>(1, "xx") x)`,
				expected: [][]interface{}{
					[]interface{}{int64(1), string("xx")},
				},
			},
			{
				name: "DotStar_Path_StructField",
				sql:  `SELECT y.x.* FROM (SELECT STRUCT<Id int64, Value string>(1, "xx") x) y`,
				expected: [][]interface{}{
					[]interface{}{int64(1), string("xx")},
				},
			},
		},

		"FromJoin": {
			{
				name: "From_Join_CommaJoin",
				sql:  `SELECT * FROM Simple a, Simple b`,
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
			{
				name: "From_Join_ON",
				sql:  `SELECT a.* FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE b.Value = "xxx"`,
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name: "From_Join_ON1",
				sql:  `SELECT a.Id, a.Value FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE b.Value = "xxx"`,
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name: "From_Join_ON2",
				sql:  `SELECT a.* FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE a.Id = @id`,
				params: map[string]Value{
					"id": makeTestValue(200),
				},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy"},
				},
			},
			{
				name: "From_Join_ON3",
				sql:  `SELECT * FROM Simple AS a JOIN Simple AS b ON a.Id = b.Id WHERE a.Id = @id`,
				params: map[string]Value{
					"id": makeTestValue(200),
				},
				expected: [][]interface{}{
					[]interface{}{int64(200), "yyy", int64(200), "yyy"},
				},
			},
			{
				name: "From_Join_USING",
				sql:  `SELECT a.* FROM Simple AS a JOIN Simple AS b USING (Id) WHERE b.Value = "xxx"`,
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},
			{
				name: "From_Join_Using_ColumnNotExist",
				sql:  `SELECT 1 FROM Simple a JOIN Simple b USING (foo)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Column foo in USING clause not found on left side of join`),
			},
			{
				name: "From_Join_Using_ColumnNotExist_RightSide",
				sql:  `SELECT 1 FROM Simple a JOIN CompositePrimaryKeys b USING (Value)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Column Value in USING clause not found on right side of join`),
			},
			{
				name: "From_Join_Using_ColumnNotExist_Subquery",
				sql:  `SELECT 1 FROM Simple a JOIN (SELECT Value FROM Simple) b USING (Id)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Column Id in USING clause not found on right side of join`),
			},
			{
				name: "From_Join_Subquery_USING",
				sql:  `SELECT a.* FROM Simple AS a JOIN (SELECT Id FROM Simple) AS b USING (Id) WHERE a.Value = "xxx"`,
				expected: [][]interface{}{
					[]interface{}{int64(100), "xxx"},
				},
			},

			{
				name: "From_Join_Paren",
				sql:  `SELECT a.Id FROM (Simple AS a JOIN Simple AS b USING (Id))`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},

			{
				name: "From_Subquery_Simple",
				sql:  `SELECT s.* FROM (SELECT 1) s`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},
			{
				name: "From_Subquery_Table",
				sql:  `SELECT s.* FROM (SELECT Id FROM Simple WHERE Value = "xxx") s`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
		},

		"Subquery": {
			{
				name: "SubQuery_Scalar_Simple",
				sql:  `SELECT Id FROM Simple WHERE 1 = (SELECT 1)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},
			{
				name: "Scalar_WithMultiColumns",
				sql:  `SELECT 1 WHERE 1 = (SELECT 1, "abc")`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Scalar subquery cannot have more than one column unless using SELECT AS STRUCT to build STRUCT values`),
			},
			// TODO: sqlite implicitly extract 1 row if subquery returns multi rows
			// {
			// 	name: "Scalar_ReturnedMultiRows",
			// 	sql:  "SELECT 1 FROM Simple WHERE 1 = (SELECT 1 FROM Simple)",
			// 	code: codes.InvalidArgument,
			// 	msg:  regexp.MustCompile(`A scalar subquery returned more than one row.`),
			// },
			{
				name: "SubQuery_Scalar_Table",
				sql:  `SELECT Id FROM Simple WHERE Id = (SELECT Id From Simple WHERE Id = 100)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "SubQuery_In",
				sql:  `SELECT Id FROM Simple WHERE Id IN (SELECT Id From Simple WHERE Id > 100)`,
				expected: [][]interface{}{
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},
			{
				name: "SubQuery_NotIn",
				sql:  `SELECT Id FROM Simple WHERE Id NOT IN (SELECT Id From Simple WHERE Id > 100)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Subquery_In_WithMultiColumns",
				sql:  `SELECT 1 WHERE 1 IN (SELECT 1, "abc")`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Subquery of type IN must have only one output column`),
			},
			{
				name: "SubQuery_EXISTS",
				sql:  `SELECT Id FROM Simple WHERE EXISTS(SELECT 1, "xx")`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},
			{
				name:     "SubQuery_EXISTS_NoMatch",
				sql:      `SELECT Id FROM Simple WHERE EXISTS(SELECT * FROM Simple WHERE Id = 1000)`,
				expected: nil,
			},
			{
				name: "SubQuery_Array",
				sql:  `SELECT * FROM UNNEST(ARRAY(SELECT 100))`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name:  "SubQuery_ColumnAlias",
				sql:   `SELECT Id, foo, bar FROM (SELECT Id, Id AS foo, Id bar FROM Simple)`,
				names: []string{"Id", "foo", "bar"},
				expected: [][]interface{}{
					[]interface{}{int64(100), int64(100), int64(100)},
					[]interface{}{int64(200), int64(200), int64(200)},
					[]interface{}{int64(300), int64(300), int64(300)},
				},
			},
			{
				name:  "SubQuery_ColumnAlias2",
				sql:   `SELECT Id +1, foo +1, bar +1 FROM (SELECT Id, Id AS foo, Id bar FROM Simple)`,
				names: []string{"", "", ""},
				expected: [][]interface{}{
					[]interface{}{int64(101), int64(101), int64(101)},
					[]interface{}{int64(201), int64(201), int64(201)},
					[]interface{}{int64(301), int64(301), int64(301)},
				},
			},
			{
				name:  "SubQuery_ColumnAlias_AsAlias",
				sql:   `SELECT x AS y FROM (SELECT Id x FROM Simple)`,
				names: []string{"y"},
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},
			{
				name: "SubQuery_TableAlias",
				sql:  `SELECT xx.Id FROM (SELECT Id FROM Simple) AS xx`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},

			{
				name: "Compound_Union_Distinct",
				sql:  `SELECT Id FROM Simple UNION DISTINCT SELECT Id FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},
			{
				name: "Compound_Union_All",
				sql:  `SELECT Id FROM Simple UNION ALL SELECT Id FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},
			{
				name: "Compound_Union_All_SelectLimit",
				sql:  `SELECT Id FROM Simple UNION ALL (SELECT Id FROM Simple LIMIT 1)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Compound_Union_All_UnionLimit",
				sql:  `SELECT Id FROM Simple UNION ALL (SELECT Id FROM Simple) LIMIT 5`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
				},
			},
			{
				name: "Compound_Union_All_OrderBy",
				sql:  `SELECT Id FROM Simple UNION ALL (SELECT Id FROM Simple) ORDER BY Id`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
					[]interface{}{int64(300)},
				},
			},
			{
				name: "Compound_Union_All_OrderBy_Limit",
				sql:  `SELECT Id FROM Simple UNION ALL (SELECT Id FROM Simple) ORDER BY Id LIMIT 3`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
				},
			},
			{
				name: "Compound_Intersect_Distinct",
				sql:  `SELECT Id FROM Simple INTERSECT DISTINCT SELECT Id FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},
			{
				name: "Compound_Intersect_Distinct2",
				sql:  `SELECT Id FROM Simple INTERSECT DISTINCT (SELECT Id FROM Simple WHERE Id IN (100, 300))`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(300)},
				},
			},
			{
				name: "Compound_Intersect_Distinct_Limit",
				sql:  `SELECT Id FROM Simple INTERSECT DISTINCT (SELECT Id FROM Simple) LIMIT 2`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
				},
			},
			{
				name: "Compound_Intersect_Distinct_OrderBy",
				sql:  `SELECT Id FROM Simple INTERSECT DISTINCT (SELECT Id FROM Simple) ORDER BY Id DESC`,
				expected: [][]interface{}{
					[]interface{}{int64(300)},
					[]interface{}{int64(200)},
					[]interface{}{int64(100)},
				},
			},
			{
				name:     "Compound_Except_Distinct",
				sql:      `SELECT Id FROM Simple EXCEPT DISTINCT SELECT Id FROM Simple`,
				expected: nil,
			},
			{
				name: "Compound_Except_Distinct2",
				sql:  `SELECT Id FROM Simple EXCEPT DISTINCT (SELECT Id FROM Simple WHERE Id IN (100, 300))`,
				expected: [][]interface{}{
					[]interface{}{int64(200)},
				},
			},
			{
				name: "Compound_Except_Distinct_Limit",
				sql:  `SELECT Id FROM Simple EXCEPT DISTINCT (SELECT Id FROM Simple WHERE Id = 200) LIMIT 1`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Compound_Except_Distinct_OrderBy",
				sql:  `SELECT Id FROM Simple EXCEPT DISTINCT (SELECT Id FROM Simple WHERE Id = 200) ORDER BY Id DESC`,
				expected: [][]interface{}{
					[]interface{}{int64(300)},
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Compound_Complex1",
				sql:  `SELECT Id FROM Simple UNION ALL SELECT Id+2 FROM Simple UNION ALL SELECT Id+1 FROM Simple`,
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
			{
				name: "Compound_Complex2",
				sql:  `SELECT Id FROM Simple UNION ALL SELECT Id+1 FROM Simple UNION ALL SELECT Id+2 FROM Simple ORDER BY Id DESC`,
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
			{
				name: "Compound_Complex3",
				sql:  `(SELECT Id FROM Simple UNION ALL SELECT Id+1 FROM Simple) INTERSECT DISTINCT SELECT Id FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
					[]interface{}{int64(200)},
					[]interface{}{int64(300)},
				},
			},
			// TODO: INT64 and FLOAT64 are compatible value type
			// {
			// 	name: "Compound_MergeIntFloat",
			// 	sql:  `SELECT 1 UNION ALL SELECT 0.1`,
			// 	expected: [][]interface{}{
			// 		[]interface{}{float64(1)},
			// 		[]interface{}{float64(0.1)},
			// 	},
			// },
			// {
			// 	name: "Compound_MergeIntFloat2",
			// 	sql:  `SELECT 1 UNION DISTINCT SELECT 1.0`,
			// 	expected: [][]interface{}{
			// 		[]interface{}{float64(1.0)},
			// 	},
			// },
			{
				name: "Compound_ColumnsNum",
				sql:  `SELECT 1 UNION ALL SELECT 1, 2`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Queries in UNION ALL have mismatched column count; query 1 has 1 column, query 2 has 2 columns`),
			},
			{
				name: "Compound_ColumnsNum2",
				sql:  `SELECT 1, 2 INTERSECT DISTINCT SELECT 1`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Queries in INTERSECT DISTINCT have mismatched column count; query 1 has 2 column, query 2 has 1 columns`),
			},
			{
				name: "Compound_ColumnsType",
				sql:  `SELECT 1 EXCEPT DISTINCT SELECT "xx"`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Column 1 in EXCEPT DISTINCT has incompatible types: INT64, STRING`),
			},
			{
				name: "Compound_ColumnsType2",
				sql:  `SELECT 1, 0.1 EXCEPT DISTINCT SELECT 1, true`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^Column 2 in EXCEPT DISTINCT has incompatible types: FLOAT64, BOOL`),
			},
		},

		"Arithmetic": {
			{
				name: "Arithmetic_Add",
				sql:  `SELECT 1 + 2`,
				expected: [][]interface{}{
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Arithmetic_Add_Float",
				sql:  `SELECT 1.5 + 2.5`,
				expected: [][]interface{}{
					[]interface{}{float64(4)},
				},
			},
			{
				name: "Arithmetic_Sub",
				sql:  `SELECT 1 - 2`,
				expected: [][]interface{}{
					[]interface{}{int64(-1)},
				},
			},
			{
				name: "Arithmetic_Sub_Float",
				sql:  `SELECT 1.5 - 2`,
				expected: [][]interface{}{
					[]interface{}{float64(-0.5)},
				},
			},
			{
				name: "Arithmetic_Mult",
				sql:  `SELECT 2 * 2`,
				expected: [][]interface{}{
					[]interface{}{int64(4)},
				},
			},
			{
				name: "Arithmetic_Mult_Float",
				sql:  `SELECT 2.5 * 2`,
				expected: [][]interface{}{
					[]interface{}{float64(5)},
				},
			},
			{
				name: "Arithmetic_Div",
				sql:  `SELECT 3 / 2`,
				expected: [][]interface{}{
					[]interface{}{float64(1.5)},
				},
			},
			{
				name: "Arithmetic_BitOr",
				sql:  `SELECT 0x11 | 0x04`,
				expected: [][]interface{}{
					[]interface{}{int64(0x15)},
				},
			},
			{
				name: "Arithmetic_BitXor",
				sql:  `SELECT 0x11 ^ 0x01`,
				expected: [][]interface{}{
					[]interface{}{int64(0x10)},
				},
			},
			{
				name: "Arithmetic_BitAnd",
				sql:  `SELECT 0x13 & 0x01`,
				expected: [][]interface{}{
					[]interface{}{int64(0x01)},
				},
			},
			{
				name: "Arithmetic_BitLeftShift",
				sql:  `SELECT 0x3 << 3`,
				expected: [][]interface{}{
					[]interface{}{int64(24)},
				},
			},
			{
				name: "Arithmetic_BitRightShift",
				sql:  `SELECT 0xf0 >> 2`,
				expected: [][]interface{}{
					[]interface{}{int64(60)},
				},
			},
			{
				name: "Unary_Int",
				sql:  `SELECT - -1`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},
			{
				name: "Unary_Float",
				sql:  `SELECT - -0.1`,
				expected: [][]interface{}{
					[]interface{}{float64(0.1)},
				},
			},
			{
				name: "Unary_BitNot",
				sql:  `SELECT ~ -3`,
				expected: [][]interface{}{
					[]interface{}{int64(2)},
				},
			},
			{
				name: "Unary_Not",
				sql:  `SELECT NOT true`,
				expected: [][]interface{}{
					[]interface{}{false},
				},
			},
		},

		"Cast": {
			{
				name: "Cast_Int64_String",
				sql:  `SELECT CAST(100 AS STRING)`,
				expected: [][]interface{}{
					[]interface{}{"100"},
				},
			},
			{
				name: "Cast_Int64_String_Neg",
				sql:  `SELECT CAST(-100 AS STRING)`,
				expected: [][]interface{}{
					[]interface{}{"-100"},
				},
			},
			{
				name: "Cast_Int64_Bool_True",
				sql:  `SELECT CAST(100 AS BOOL)`,
				expected: [][]interface{}{
					[]interface{}{true},
				},
			},
			{
				name: "Cast_Int64_Bool_True2",
				sql:  `SELECT CAST(-100 AS BOOL)`,
				expected: [][]interface{}{
					[]interface{}{true},
				},
			},
			{
				name: "Cast_Int64_Bool_False",
				sql:  `SELECT CAST(0 AS BOOL)`,
				expected: [][]interface{}{
					[]interface{}{false},
				},
			},
			{
				name: "Cast_Int64_Float64_1",
				sql:  `SELECT CAST(0 AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{float64(0)},
				},
			},
			{
				name: "Cast_Int64_Float64_2",
				sql:  `SELECT CAST(-1 AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{float64(-1.0)},
				},
			},

			{
				name: "Cast_Float64_String_Big_Small",
				sql:  `SELECT CAST(@foo AS STRING)`,
				params: map[string]Value{
					"foo": makeTestValue(2.3),
				},
				expected: [][]interface{}{
					[]interface{}{"2.3"},
				},
			},
			{
				name: "Cast_Float64_String_Big_Small_neg",
				sql:  `SELECT CAST(@foo AS STRING)`,
				params: map[string]Value{
					"foo": makeTestValue(-2.3),
				},
				expected: [][]interface{}{
					[]interface{}{"-2.3"},
				},
			},
			{
				name: "Cast_Float64_String_Big",
				sql:  `SELECT CAST(@foo AS STRING)`,
				params: map[string]Value{
					"foo": makeTestValue(383260575764816448.0),
				},
				expected: [][]interface{}{
					[]interface{}{"3.8326057576481645e+17"},
				},
			},
			{
				name: "Cast_Float64_String_Inf",
				sql:  `SELECT CAST(@foo AS STRING)`,
				params: map[string]Value{
					"foo": makeTestValue(math.Inf(0)),
				},
				expected: [][]interface{}{
					[]interface{}{"inf"},
				},
			},
			{
				name: "Cast_Float64_String_Inf_Neg",
				sql:  `SELECT CAST(@foo AS STRING)`,
				params: map[string]Value{
					"foo": makeTestValue(math.Inf(-1)),
				},
				expected: [][]interface{}{
					[]interface{}{"-inf"},
				},
			},
			// TODO
			// {
			// 	name: "Cast_Float64_String_NaN",
			// 	sql: `SELECT CAST(@foo AS STRING)`,
			// 	params: map[string]Value{
			// 		"foo": makeTestValue(math.NaN()),
			// 	},
			// 	expected: [][]interface{}{
			// 		[]interface{}{"nan"},
			// 	},
			// },
			{
				name: "Cast_Float64_Int64_Big_Small",
				sql:  `SELECT CAST(@foo AS INT64)`,
				params: map[string]Value{
					"foo": makeTestValue(2.3),
				},
				expected: [][]interface{}{
					[]interface{}{int64(2)},
				},
			},
			{
				name: "Cast_Float64_Int64_Big_Small_neg",
				sql:  `SELECT CAST(@foo AS INT64)`,
				params: map[string]Value{
					"foo": makeTestValue(-2.3),
				},
				expected: [][]interface{}{
					[]interface{}{int64(-2)},
				},
			},
			{
				name: "Cast_Float64_Int64_Big",
				sql:  `SELECT CAST(@foo AS INT64)`,
				params: map[string]Value{
					"foo": makeTestValue(383260575764816448.0),
				},
				expected: [][]interface{}{
					[]interface{}{int64(383260575764816448)},
				},
			},
			{
				name: "Cast_Float64_Int64_Round_Pos",
				sql:  `SELECT CAST(@foo AS INT64)`,
				params: map[string]Value{
					"foo": makeTestValue(1.5),
				},
				expected: [][]interface{}{
					[]interface{}{int64(2)},
				},
			},
			{
				name: "Cast_Float64_Int64_Round_Neg",
				sql:  `SELECT CAST(@foo AS INT64)`,
				params: map[string]Value{
					"foo": makeTestValue(-0.5),
				},
				expected: [][]interface{}{
					[]interface{}{int64(-1)},
				},
			},
			{
				name: "Cast_Float64_Int64_Inf",
				sql:  `SELECT CAST(@foo AS INT64)`,
				params: map[string]Value{
					"foo": makeTestValue(math.Inf(0)),
				},
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Illegal conversion of non-finite floating point number to an integer: inf`),
			},
			{
				name: "Cast_Float64_Int64_Inf_Neg",
				sql:  `SELECT CAST(@foo AS INT64)`,
				params: map[string]Value{
					"foo": makeTestValue(math.Inf(-1)),
				},
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Illegal conversion of non-finite floating point number to an integer: -inf`),
			},
			// TODO
			// {
			// 	name: "Cast_Float64_Int64_NaN",
			// 	sql:  `SELECT CAST(@foo AS INT64)`,
			// 	params: map[string]Value{
			// 		"foo": makeTestValue(math.NaN()),
			// 	},
			// 	code: codes.OutOfRange,
			// 	msg:  regexp.MustCompile(`^Illegal conversion of non-finite floating point number to an integer: nan`),
			// },

			{
				name: "Cast_Bool_String_True",
				sql:  `SELECT CAST(true AS STRING)`,
				expected: [][]interface{}{
					[]interface{}{"TRUE"},
				},
			},
			{
				name: "Cast_Bool_String_False",
				sql:  `SELECT CAST(false AS STRING)`,
				expected: [][]interface{}{
					[]interface{}{"FALSE"},
				},
			},
			{
				name: "Cast_Bool_Int64_True",
				sql:  `SELECT CAST(true AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},
			{
				name: "Cast_Bool_Int64_False",
				sql:  `SELECT CAST(false AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(0)},
				},
			},
			{
				name: "Cast_String_Bool_True",
				sql:  `SELECT CAST("TRUE" AS BOOL)`,
				expected: [][]interface{}{
					[]interface{}{true},
				},
			},
			{
				name: "Cast_String_Bool_True2",
				sql:  `SELECT CAST("TrUe" AS BOOL)`,
				expected: [][]interface{}{
					[]interface{}{true},
				},
			},
			{
				name: "Cast_String_Bool_False",
				sql:  `SELECT CAST("FALSE" AS BOOL)`,
				expected: [][]interface{}{
					[]interface{}{false},
				},
			},
			{
				name: "Cast_String_Bool_False2",
				sql:  `SELECT CAST("faLsE" AS BOOL)`,
				expected: [][]interface{}{
					[]interface{}{false},
				},
			},
			{
				name: "Cast_String_Bool_Invalid",
				sql:  `SELECT CAST("xx" AS BOOL)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Bad bool value: xx`),
			},
			{
				name: "Cast_String_Int64_Base10_1",
				sql:  `SELECT CAST("100" AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Cast_String_Int64_Base10_2",
				sql:  `SELECT CAST("-100" AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(-100)},
				},
			},
			{
				name: "Cast_String_Int64_Base10_3",
				sql:  `SELECT CAST("+100" AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Cast_String_Int64_Base10_4",
				sql:  `SELECT CAST("0100" AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Cast_String_Int64_Base10_5",
				sql:  `SELECT CAST("00100" AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Cast_String_Int64_Base16_1",
				sql:  `SELECT CAST("0x100" AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(256)},
				},
			},
			{
				name: "Cast_String_Int64_Base16_2",
				sql:  `SELECT CAST("-0x100" AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(-256)},
				},
			},
			{
				name: "Cast_String_Int64_Base16_3",
				sql:  `SELECT CAST("0xABC" AS INT64)`,
				expected: [][]interface{}{
					[]interface{}{int64(2748)},
				},
			},
			{
				name: "Cast_String_Int64_Invalid1",
				sql:  `SELECT CAST("- 100" AS INT64)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Bad int64 value: - 100`),
			},
			{
				name: "Cast_String_Int64_Invalid2",
				sql:  `SELECT CAST("00x100" AS INT64)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Bad int64 value: 00x100`),
			},
			{
				name: "Cast_String_Float64_1",
				sql:  `SELECT CAST("123.456e-67" AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{float64(1.23456e-65)},
				},
			},
			{
				name: "Cast_String_Float64_2",
				sql:  `SELECT CAST(".1E4" AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{float64(1000)},
				},
			},
			{
				name: "Cast_String_Float64_3",
				sql:  `SELECT CAST("58." AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{float64(58)},
				},
			},
			{
				name: "Cast_String_Float64_4",
				sql:  `SELECT CAST("4e2" AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{float64(400)},
				},
			},
			// {
			// 	name: "Cast_String_Float64_Nan1",
			// 	sql: `SELECT CAST("NaN" AS FLOAT64)`,
			// 	expected: [][]interface{}{
			// 		[]interface{}{math.NaN()},
			// 	},
			// },
			// {
			// 	name: "Cast_String_Float64_Nan2",
			// 	sql: `SELECT CAST("nan" AS FLOAT64)`,
			// 	expected: [][]interface{}{
			// 		[]interface{}{math.NaN()},
			// 	},
			// },
			{
				name: "Cast_String_Float64_Inf1",
				sql:  `SELECT CAST("inf" AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{math.Inf(0)},
				},
			},
			{
				name: "Cast_String_Float64_Inf2",
				sql:  `SELECT CAST("+inf" AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{math.Inf(0)},
				},
			},
			{
				name: "Cast_String_Float64_Inf3",
				sql:  `SELECT CAST("-inf" AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{math.Inf(-1)},
				},
			},
			{
				name: "Cast_String_Float64_Inf4",
				sql:  `SELECT CAST("-Inf" AS FLOAT64)`,
				expected: [][]interface{}{
					[]interface{}{math.Inf(-1)},
				},
			},
			{
				name: "Cast_String_Float64_Invalid",
				sql:  `SELECT CAST("xx" AS FLOAT64)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Bad double value: xx`),
			},
		},

		"Function": {
			{
				name: "Function_Count",
				sql:  `SELECT COUNT(1) FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Function_Count2",
				sql:  `SELECT COUNT(Id) FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Function_Count3",
				sql:  `SELECT COUNT(Id) AS count FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Function_Count4",
				sql:  `SELECT COUNT("x") FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Function_Count5",
				sql:  `SELECT COUNT(NULL) FROM Simple`,
				expected: [][]interface{}{
					[]interface{}{int64(0)},
				},
			},
			{
				name: "Function_Count_Param",
				sql:  `SELECT COUNT(@foo) FROM Simple`,
				params: map[string]Value{
					"foo": makeTestValue(200),
				},
				expected: [][]interface{}{
					[]interface{}{int64(3)},
				},
			},
			{
				name: "Function_Sign",
				sql:  `SELECT SIGN(1)`,
				expected: [][]interface{}{
					[]interface{}{int64(1)},
				},
			},
			{
				name: "Function_Sign2",
				sql:  `SELECT SIGN(-1)`,
				expected: [][]interface{}{
					[]interface{}{int64(-1)},
				},
			},
			{
				name: "Function_Sign3",
				sql:  `SELECT SIGN(0)`,
				expected: [][]interface{}{
					[]interface{}{int64(0)},
				},
			},
			{
				name: "Function_StartsWith",
				sql:  `SELECT STARTS_WITH("abc", "ab")`,
				expected: [][]interface{}{
					[]interface{}{true},
				},
			},
			{
				name: "Function_StartsWith2",
				sql:  `SELECT STARTS_WITH("abc", "xx")`,
				expected: [][]interface{}{
					[]interface{}{false},
				},
			},
			{
				name: "Function_StartsWith_Param",
				sql:  `SELECT STARTS_WITH(@a, @b)`,
				params: map[string]Value{
					"a": makeTestValue("xyz"),
					"b": makeTestValue("xy"),
				},
				expected: [][]interface{}{
					[]interface{}{true},
				},
			},
			{
				name: "Function_Max_Int",
				sql:  `SELECT MAX(x) FROM UNNEST([100, 200, 300]) AS x`,
				expected: [][]interface{}{
					[]interface{}{int64(300)},
				},
			},
			{
				name: "Function_Max_String",
				sql:  `SELECT MAX(x) FROM UNNEST(["xxx", "zz", "yy"]) AS x`,
				expected: [][]interface{}{
					[]interface{}{"zz"},
				},
			},
			{
				name: "Function_Min_Int",
				sql:  `SELECT MIN(x) FROM UNNEST([100, 200, 300]) AS x`,
				expected: [][]interface{}{
					[]interface{}{int64(100)},
				},
			},
			{
				name: "Function_Min_String",
				sql:  `SELECT MIN(x) FROM UNNEST(["xxx", "zz", "yy"]) AS x`,
				expected: [][]interface{}{
					[]interface{}{"xxx"},
				},
			},
			{
				name: "Function_Avg",
				sql:  `SELECT AVG(x) as avg FROM UNNEST([100, 200, NULL, 300, 100]) AS x`,
				expected: [][]interface{}{
					[]interface{}{float64(175)},
				},
			},
			{
				name: "Function_Avg_Distinct",
				sql:  `SELECT AVG(DISTINCT x) as avg FROM UNNEST([100, 200, NULL, 300, 100]) AS x`,
				expected: [][]interface{}{
					[]interface{}{float64(200)},
				},
			},
			{
				name: "Function_Sum",
				sql:  `SELECT SUM(x) as avg FROM UNNEST([100, 200, 300]) AS x`,
				expected: [][]interface{}{
					[]interface{}{int64(600)},
				},
			},
			// TODO: SUM with float

			{
				name: "Function_Concat",
				sql:  `SELECT CONCAT("xx", "yy")`,
				expected: [][]interface{}{
					[]interface{}{"xxyy"},
				},
			},

			{
				name: "Function_Extract_Timestamp",
				sql: `SELECT` +
					` EXTRACT(NANOSECOND  FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(MICROSECOND FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(MILLISECOND FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(SECOND      FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(MINUTE      FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(HOUR        FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(DAYOFWEEK   FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(DAY         FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(DAYOFYEAR   FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(ISOWEEK     FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(QUARTER     FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(YEAR        FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(ISOYEAR     FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(DATE        FROM t AT TIME ZONE "UTC")` +
					` FROM (SELECT TIMESTAMP '2012-01-02 12:34:56.987654321Z' t)`,
				expected: [][]interface{}{
					[]interface{}{
						int64(987654321), int64(987654), int64(987),
						int64(56), int64(34), int64(12), // sec,min,hour
						int64(1), int64(2), int64(2), // dayofweek, day, dayofyear
						int64(1),                           // isoweek
						int64(1), int64(2012), int64(2012), // quarter, year, isoyear
						"2012-01-02", // date
					},
				},
			},
			{
				name: "Function_Extract_TimeZone",
				sql: `SELECT` +
					` EXTRACT(DAY  FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(HOUR FROM t AT TIME ZONE "UTC"),` +
					` EXTRACT(DAY  FROM t AT TIME ZONE "Asia/Tokyo"),` +
					` EXTRACT(HOUR FROM t AT TIME ZONE "Asia/Tokyo"),` +
					` EXTRACT(DAY  FROM t AT TIME ZONE "America/Los_Angeles"),` +
					` EXTRACT(HOUR FROM t AT TIME ZONE "America/Los_Angeles")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 00:00:00.999999999Z' t)`,
				expected: [][]interface{}{
					[]interface{}{int64(1), int64(0), int64(1), int64(9), int64(31), int64(16)},
				},
			},
			{
				name: "Function_Extract_TimeZone_Invalid",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE "xxx")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 00:00:00.999999999Z' t)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Invalid time zone: xxx`),
			},
			{
				name: "Function_Extract_FixedTimeZone",
				sql: `SELECT` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "+01:20"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "+01:20"),` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "-01:20"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "-01:20")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				expected: [][]interface{}{
					[]interface{}{int64(13), int64(20), int64(10), int64(40)},
				},
			},
			{
				name: "Function_Extract_FixedTimeZone2",
				sql: `SELECT` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "+1"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "+1"),` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "-1"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "-1")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				expected: [][]interface{}{
					[]interface{}{int64(13), int64(0), int64(11), int64(0)},
				},
			},
			{
				name: "Function_Extract_FixedTimeZone3",
				sql: `SELECT` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "+11"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "+11"),` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "-11"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "-11")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				expected: [][]interface{}{
					[]interface{}{int64(23), int64(0), int64(1), int64(0)},
				},
			},
			{
				name: "Function_Extract_FixedTimeZone4",
				sql: `SELECT` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "+11:3"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "+11:3"),` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "-11:3"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "-11:3")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				expected: [][]interface{}{
					[]interface{}{int64(23), int64(3), int64(0), int64(57)},
				},
			},
			{
				name: "Function_Extract_FixedTimeZone5",
				sql: `SELECT` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "+1:35"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "+1:35"),` +
					` EXTRACT(HOUR   FROM t AT TIME ZONE "-1:35"),` +
					` EXTRACT(MINUTE FROM t AT TIME ZONE "-1:35")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				expected: [][]interface{}{
					[]interface{}{int64(13), int64(35), int64(10), int64(25)},
				},
			},
			{
				name: "Function_Extract_FixedTimeZone_Invalid",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE "11:11")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Invalid time zone: 11:11`),
			},
			{
				name: "Function_Extract_FixedTimeZone_Invalid2",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE "+:11")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Invalid time zone: \+:11`),
			},
			{
				name: "Function_Extract_FixedTimeZone_Invalid3",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE "+11:")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Invalid time zone: \+11:`),
			},
			{
				name: "Function_Extract_FixedTimeZone_Invalid4",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE "+")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Invalid time zone: \+`),
			},
			{
				name: "Function_Extract_FixedTimeZone_Invalid5",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE "+25:00")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Invalid time zone: \+25:00`),
			},
			{
				name: "Function_Extract_FixedTimeZone_Invalid6",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE "+10:61")` +
					` FROM (SELECT TIMESTAMP '2012-01-01 12:00:00.999999999Z' t)`,
				code: codes.OutOfRange,
				msg:  regexp.MustCompile(`^Invalid time zone: \+10:61`),
			},
			{
				name: "Function_Extract_Timestamp_WithoutTimezone",
				sql: `SELECT EXTRACT(HOUR FROM t)` +
					` FROM (SELECT TIMESTAMP '2012-01-02 12:34:56.987654321Z' t)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^handy-spanner: please specify timezone explicitly.`),
			},
			{
				name: "Function_Extract_Timestamp_TimeZone_ByParam",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE @foo)` +
					` FROM (SELECT TIMESTAMP '2012-01-02 12:34:56.987654321Z' t)`,
				params: map[string]Value{
					"foo": makeTestValue("UTC"),
				},
				expected: [][]interface{}{
					[]interface{}{int64(12)},
				},
			},
			{
				name: "Function_Extract_Timestamp_TimeZone_InvalidType",
				sql: `SELECT EXTRACT(HOUR FROM t AT TIME ZONE @foo)` +
					` FROM (SELECT TIMESTAMP '2012-01-02 12:34:56.987654321Z' t)`,
				params: map[string]Value{
					"foo": makeTestValue(int64(100)),
				},
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^No matching signature for function EXTRACT for argument types: DATE_TIME_PART FROM TIMESTAMP AT TIME ZONE INT64. Supported signatures`),
			},

			{
				name: "Function_Extract_Date",
				sql: `SELECT` +
					` EXTRACT(DAYOFWEEK FROM t),` +
					` EXTRACT(DAY       FROM t),` +
					` EXTRACT(DAYOFYEAR FROM t),` +
					` EXTRACT(ISOWEEK   FROM t),` +
					` EXTRACT(QUARTER   FROM t),` +
					` EXTRACT(YEAR      FROM t),` +
					` EXTRACT(ISOYEAR   FROM t)` +
					` FROM (SELECT DATE '2012-01-02' t)`,
				expected: [][]interface{}{
					[]interface{}{
						int64(1), int64(2), int64(2), // dayofweek, day, dayofyear
						int64(1),                           // isoweek
						int64(1), int64(2012), int64(2012), // quarter, year, isoyear
					},
				},
			},
			{
				name: "Function_Extract_Date_InvalidPart_Nano",
				sql: `SELECT EXTRACT(NANOSECOND FROM t)` +
					` FROM (SELECT DATE '2012-01-02' t)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^EXTRACT from DATE does not support the NANOSECOND date part`),
			},
			{
				name: "Function_Extract_Date_InvalidPart_Date",
				sql: `SELECT EXTRACT(DATE FROM t)` +
					` FROM (SELECT DATE '2012-01-02' t)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^EXTRACT from DATE does not support the DATE date part`),
			},
			{
				name: "Function_Extract_Date_WithTimeZone",
				sql: `SELECT EXTRACT(DAY FROM t AT TIME ZONE "UTC")` +
					` FROM (SELECT DATE '2012-01-02' t)`,
				code: codes.InvalidArgument,
				msg:  regexp.MustCompile(`^EXTRACT from DATE does not support AT TIME ZONE`),
			},
		},
	}

	for name, tcs := range table {
		t.Run(name, func(t *testing.T) {
			tcs := tcs
			t.Parallel()
			for _, tc := range tcs {
				t.Run(tc.name, func(t *testing.T) {
					tc := tc
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
						it, err := db.Query(ctx, stmt, tc.params)
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
						it, err := db.Query(ctx, stmt, tc.params)
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
			}
		})
	}
}
