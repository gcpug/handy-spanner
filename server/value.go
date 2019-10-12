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
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

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

func compatibleValueType(a, b ValueType) (ValueType, bool) {
	if a.Code == TCInt64 && b.Code == TCFloat64 {
		return b, true
	}
	if b.Code == TCInt64 && a.Code == TCFloat64 {
		return a, true
	}
	return a, a == b
}

func decideArrayElementsValueType(vts ...ValueType) (ValueType, error) {
	vt := ValueType{Code: TCInt64}
	if len(vts) > 0 {
		vt = vts[0]
	}

	used := map[string]struct{}{}

	for i := range vts {
		used[vts[i].String()] = struct{}{}
	}

	for i := range vts {
		var ok bool
		// TODO: if ValueType is changed, types of all values also need changing
		// vt, ok = compatibleValueType(vt, vts[i])
		ok = compareValueType(vt, vts[i])
		if !ok {
			var typ string
			first := true
			for n := range used {
				if !first {
					typ += ", "
				}
				typ += n
				first = false
			}

			return ValueType{}, fmt.Errorf("Array elements of types {%s} do not have a common supertype", typ)
		}
	}

	return vt, nil
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

type ArrayValue interface {
	Elements() interface{}
}

var _ ArrayValue = (*ArrayBool)(nil)
var _ ArrayValue = (*ArrayString)(nil)
var _ ArrayValue = (*ArrayFloat64)(nil)
var _ ArrayValue = (*ArrayInt64)(nil)
var _ ArrayValue = (*ArrayBytes)(nil)

type ArrayBool struct {
	Data    []*bool
	Invalid bool
}

type ArrayString struct {
	Data    []*string
	Invalid bool
}

type ArrayFloat64 struct {
	Data    []*float64
	Invalid bool
}

type ArrayInt64 struct {
	Data    []*int64
	Invalid bool
}

type ArrayBytes struct {
	Data    [][]byte
	Invalid bool
}

func (a *ArrayBool) Elements() interface{} {
	return a.Data
}

func (a *ArrayBool) Value() (driver.Value, error) {
	if a.Invalid {
		return nil, fmt.Errorf("cannot use invalid value")
	}

	b, err := json.Marshal(a.Data)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal failed in %T: %v", a, err)
	}

	return driver.Value(string(b)), nil
}

func (a *ArrayBool) Scan(src interface{}) error {
	if src == nil {
		a.Invalid = true
		return nil
	}

	v, ok := src.(string)
	if !ok {
		return fmt.Errorf("unexpected type %T for %T", src, a)
	}

	if err := json.Unmarshal([]byte(v), &a.Data); err != nil {
		return fmt.Errorf("json.Unmarshal failed in %T: %v", a, err)
	}

	return nil
}

func (a *ArrayString) Elements() interface{} {
	return a.Data
}

func (a *ArrayString) Value() (driver.Value, error) {
	if a.Invalid {
		return nil, fmt.Errorf("cannot use invalid value")
	}

	b, err := json.Marshal(a.Data)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal failed in %T: %v", a, err)
	}

	return driver.Value(string(b)), nil
}

func (a *ArrayString) Scan(src interface{}) error {
	if src == nil {
		a.Invalid = true
		return nil
	}

	v, ok := src.(string)
	if !ok {
		return fmt.Errorf("unexpected type %T for %T", src, a)
	}

	if err := json.Unmarshal([]byte(v), &a.Data); err != nil {
		return fmt.Errorf("json.Unmarshal failed in %T: %v", a, err)
	}

	return nil
}

func (a *ArrayFloat64) Elements() interface{} {
	return a.Data
}

func (a *ArrayFloat64) Value() (driver.Value, error) {
	if a.Invalid {
		return nil, fmt.Errorf("cannot use invalid value")
	}

	b, err := json.Marshal(a.Data)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal failed in %T: %v", a, err)
	}

	return driver.Value(string(b)), nil
}

func (a *ArrayFloat64) Scan(src interface{}) error {
	if src == nil {
		a.Invalid = true
		return nil
	}

	v, ok := src.(string)
	if !ok {
		return fmt.Errorf("unexpected type %T for %T", src, a)
	}

	if err := json.Unmarshal([]byte(v), &a.Data); err != nil {
		return fmt.Errorf("json.Unmarshal failed in %T: %v", a, err)
	}

	return nil
}

func (a *ArrayInt64) Elements() interface{} {
	return a.Data
}

func (a *ArrayInt64) Value() (driver.Value, error) {
	if a.Invalid {
		return nil, fmt.Errorf("cannot use invalid value")
	}

	b, err := json.Marshal(a.Data)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal failed in %T: %v", a, err)
	}

	return driver.Value(string(b)), nil
}

func (a *ArrayInt64) Scan(src interface{}) error {
	if src == nil {
		a.Invalid = true
		return nil
	}

	v, ok := src.(string)
	if !ok {
		return fmt.Errorf("unexpected type %T for %T", src, a)
	}

	if err := json.Unmarshal([]byte(v), &a.Data); err != nil {
		return fmt.Errorf("json.Unmarshal failed in %T: %v", a, err)
	}

	return nil
}

func (a *ArrayBytes) Elements() interface{} {
	return a.Data
}

func (a *ArrayBytes) Value() (driver.Value, error) {
	if a.Invalid {
		return nil, fmt.Errorf("cannot use invalid value")
	}

	b, err := json.Marshal(a.Data)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal failed in %T: %v", a, err)
	}

	return driver.Value(string(b)), nil
}

func (a *ArrayBytes) Scan(src interface{}) error {
	if src == nil {
		a.Invalid = true
		return nil
	}

	v, ok := src.(string)
	if !ok {
		return fmt.Errorf("unexpected type %T for %T", src, a)
	}

	if err := json.Unmarshal([]byte(v), &a.Data); err != nil {
		return fmt.Errorf("json.Unmarshal failed in %T: %v", a, err)
	}

	return nil
}

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
		case TCArray:
			switch item.ValueType.ArrayType.Code {
			case TCBool:
				values[i] = reflect.New(reflect.TypeOf(ArrayBool{}))
			case TCInt64:
				values[i] = reflect.New(reflect.TypeOf(ArrayInt64{}))
			case TCFloat64:
				values[i] = reflect.New(reflect.TypeOf(ArrayFloat64{}))
			case TCTimestamp, TCDate, TCString:
				values[i] = reflect.New(reflect.TypeOf(ArrayString{}))
			case TCBytes:
				values[i] = reflect.New(reflect.TypeOf(ArrayBytes{}))

			default:
				panic(fmt.Sprintf("unknown supported type for Array: %v", item.ValueType.ArrayType.Code))
			}
		case TCStruct:
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
		case ArrayBool:
			if vv.Invalid {
				data[i] = nil
			} else {
				data[i] = &vv
			}
		case ArrayString:
			if vv.Invalid {
				data[i] = nil
			} else {
				data[i] = &vv
			}
		case ArrayFloat64:
			if vv.Invalid {
				data[i] = nil
			} else {
				data[i] = &vv
			}
		case ArrayInt64:
			if vv.Invalid {
				data[i] = nil
			} else {
				data[i] = &vv
			}
		case ArrayBytes:
			if vv.Invalid {
				data[i] = nil
			} else {
				data[i] = &vv
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
	// special handling of commit_stamp
	// It needs to be checked if the column allows to use commit_timestamp
	if col.valueType.Code == TCTimestamp {
		if vv, ok := v.Kind.(*structpb.Value_StringValue); ok {
			s := vv.StringValue
			if s == "spanner.commit_timestamp()" {
				if !col.allowCommitTimestamp {
					msg := "Cannot write commit timestamp because the allow_commit_timestamp column option is not set to true for column %s, or for all corresponding shared key columns in this table's interleaved table hierarchy."
					return nil, fmt.Errorf(msg, col.Name) // TODO: return FailedPrecondition
				}
				now := time.Now().UTC()
				vv.StringValue = now.Format(time.RFC3339Nano)
			}
		}
	}

	vv, err := makeDataFromSpannerValue(v, col.valueType)
	if err != nil {
		return nil, err
	}

	// sqlite doesn not support nil with type like []string(nil)
	// explicitly convert those values to nil to store as null value
	rv := reflect.ValueOf(vv)
	if rv.Kind() == reflect.Slice && rv.IsNil() {
		return nil, nil
	}

	return vv, nil
}

func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func decodeBase64(s string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		// seems Spanner tries to use both padding and no-padding
		return base64.RawStdEncoding.DecodeString(s)
	}
	return b, nil
}
