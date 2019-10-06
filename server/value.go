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
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/MakeNowJust/memefish/pkg/ast"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var NullExpr = Expr{}
var NullValue = Value{}

type Expr struct {
	Raw       string
	ValueType ValueType
}

type RowIterator interface {
	ResultSet() []ResultItem

	Next() ([]interface{}, bool)
}

type Value struct {
	Data interface{}
	Type ValueType
}

type ValueType struct {
	Code       TypeCode
	ArrayType  *ValueType
	StructType *StructType
}

func (t ValueType) IsArray() bool {
	return t.Code == TCArray
}

func (t ValueType) IsStruct() bool {
	return t.Code == TCStruct
}

func (t ValueType) String() string {
	switch t.Code {
	case TCBool:
		return "BOOL"
	case TCInt64:
		return "INT64"
	case TCFloat64:
		return "FLOAT64"
	case TCTimestamp:
		return "TIMESTAMP"
	case TCDate:
		return "DATE"
	case TCString:
		return "STRING"
	case TCBytes:
		return "BYTES"
	case TCArray:
		return "ARRAY" // TODO
	case TCStruct:
		return "STRUCT" // TODO
	}
	return "(unknown type)"
}

func compareValueType(a, b ValueType) bool {
	return a == b
}

type StructType struct {
	FieldNames []string
	FieldTypes map[string]*ValueType
}

type TypeCode int32

const (
	TCBool TypeCode = iota + 1
	TCInt64
	TCFloat64
	TCTimestamp
	TCDate
	TCString
	TCBytes
	TCArray
	TCStruct
)

type rows struct {
	rows        *sql.Rows
	resultItems []ResultItem

	lastErr error
}

func (r *rows) ResultSet() []ResultItem {
	return r.resultItems
}

func (it *rows) Next() ([]interface{}, bool) {
	ok := it.rows.Next()
	if !ok {
		return nil, false
	}

	values := make([]reflect.Value, len(it.resultItems))
	ptrs := make([]interface{}, len(it.resultItems))
	for i, item := range it.resultItems {
		switch item.ValueType.Code {
		case TCBool:
			values[i] = reflect.New(reflect.TypeOf(sql.NullBool{}))
		case TCInt64:
			values[i] = reflect.New(reflect.TypeOf(sql.NullInt64{}))
		case TCFloat64:
			values[i] = reflect.New(reflect.TypeOf(sql.NullFloat64{}))
		case TCTimestamp, TCDate, TCString:
			values[i] = reflect.New(reflect.TypeOf(sql.NullString{}))
		case TCBytes:
			values[i] = reflect.New(reflect.TypeOf(&[]byte{}))
		case TCArray, TCStruct:
			panic(fmt.Sprintf("unknown supported type: %v", item.ValueType.Code))
		}
		ptrs[i] = values[i].Interface()
	}

	if err := it.rows.Scan(ptrs...); err != nil {
		it.lastErr = err
		panic(err)
		return nil, false
	}

	data := make([]interface{}, len(it.resultItems))

	for i := range values {
		v := reflect.Indirect(values[i]).Interface()
		switch vv := v.(type) {
		case sql.NullBool:
			if !vv.Valid {
				data[i] = nil
			} else {
				data[i] = vv.Bool
			}

		case sql.NullString:
			if !vv.Valid {
				data[i] = nil
			} else {
				data[i] = vv.String
			}
		case sql.NullInt64:
			if !vv.Valid {
				data[i] = nil
			} else {
				data[i] = vv.Int64
			}
		case sql.NullFloat64:
			if !vv.Valid {
				data[i] = nil
			} else {
				data[i] = vv.Float64
			}
		case *[]byte:
			if vv == nil {
				data[i] = nil
			} else {
				data[i] = *vv
			}
		default:
			data[i] = v
		}
	}

	return data, true
}

func convertToDatabaseValues(lv *structpb.ListValue, columns []*Column) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	for i, v := range lv.Values {
		column := columns[i]
		vv, err := spannerValue2DatabaseValue(v, *column)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		values[i] = vv
	}
	return values, nil
}

func spannerValue2DatabaseValue(v *structpb.Value, col Column) (interface{}, error) {
	if _, ok := v.Kind.(*structpb.Value_NullValue); ok {
		return nil, nil
	}

	switch col.dataType {
	case ast.BoolTypeName:
		vv, ok := v.Kind.(*structpb.Value_BoolValue)
		if ok {
			return vv.BoolValue, nil
		}

	case ast.Int64TypeName:
		vv, ok := v.Kind.(*structpb.Value_StringValue)
		if ok {
			n, err := strconv.ParseInt(vv.StringValue, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("unexpected format %q as int64: %v", vv.StringValue, err)
			}
			return n, nil
		}

	case ast.Float64TypeName:
		vv, ok := v.Kind.(*structpb.Value_NumberValue)
		if ok {
			return vv.NumberValue, nil
		}

	case ast.StringTypeName:
		vv, ok := v.Kind.(*structpb.Value_StringValue)
		if ok {
			return vv.StringValue, nil
		}

	case ast.BytesTypeName:
		vv, ok := v.Kind.(*structpb.Value_StringValue)
		if ok {
			return []byte(vv.StringValue), nil
		}

	case ast.DateTypeName:
		vv, ok := v.Kind.(*structpb.Value_StringValue)
		if ok {
			s := vv.StringValue
			if _, err := time.Parse("2006-01-02", s); err != nil {
				return nil, fmt.Errorf("unexpected for %q as date: %v", s, err)
			}
			return s, nil
		}

	case ast.TimestampTypeName:
		vv, ok := v.Kind.(*structpb.Value_StringValue)
		if ok {
			s := vv.StringValue
			if s == "spanner.commit_timestamp()" {
				if !col.allowCommitTimestamp {
					msg := "Cannot write commit timestamp because the allow_commit_timestamp column option is not set to true for column %s, or for all corresponding shared key columns in this table's interleaved table hierarchy."
					return nil, fmt.Errorf(msg, col.Name) // TODO: return FailedPrecondition
				}
				now := time.Now().UTC()
				s = now.Format(time.RFC3339Nano)
			}
			if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
				return nil, fmt.Errorf("unexpected format %q as timestamp: %v", s, err)
			}
			return s, nil
		}
	}

	return nil, fmt.Errorf("unexpected value %v for type %v", v.Kind, col)
}
