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
	"strings"
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

	Do(func([]interface{}) error) error
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
	case TCBool, TCInt64, TCFloat64, TCTimestamp, TCDate, TCString, TCBytes:
		return t.Code.String()
	case TCArray:
		return fmt.Sprintf("ARRAY<%s>", t.ArrayType.Code.String())
	case TCStruct:
		n := len(t.StructType.FieldTypes)
		ss := make([]string, n)
		for i := 0; i < n; i++ {
			name := t.StructType.FieldNames[i]
			vt := t.StructType.FieldTypes[i]
			if name == "" {
				ss[i] = vt.String()
			} else {
				ss[i] = name + " " + vt.String()

			}
		}
		return fmt.Sprintf("STRUCT<%s>", strings.Join(ss, ", "))
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
	FieldTypes []*ValueType

	// Table can be struct but it behaves differently.
	// So a struct created from table should be marked.
	IsTable bool
}

func (s *StructType) AllItems() []ResultItem {
	n := len(s.FieldTypes)
	items := make([]ResultItem, n)
	for i := 0; i < n; i++ {
		name := s.FieldNames[i]
		vt := s.FieldTypes[i]
		items[i] = ResultItem{
			Name:      name,
			ValueType: *vt,
			Expr: Expr{
				Raw:       name,
				ValueType: *vt,
			},
		}
	}

	return items
}

type TypeCode int32

func (c TypeCode) String() string {
	switch c {
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
		return "ARRAY"
	case TCStruct:
		return "STRUCT"
	default:
		return "(unknown)"
	}
}

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

type ArrayValueEncoder struct {
	Values  interface{}
	Invalid bool
}

func (a *ArrayValueEncoder) Value() (driver.Value, error) {
	if a.Invalid {
		return nil, fmt.Errorf("cannot use invalid value")
	}

	b, err := json.Marshal(a.Values)
	if err != nil {
		return nil, fmt.Errorf("json.Marshal failed in %T: %v", a, err)
	}

	return driver.Value(string(b)), nil
}

func (a *ArrayValueEncoder) Elements() interface{} {
	return a.Values
}

type BoolDecoder struct {
	Bool *bool
}

func (b *BoolDecoder) UnmarshalJSON(data []byte) error {
	if len(data) == 1 {
		var v bool
		if data[0] == '0' {
			v = false
			b.Bool = &v
			return nil
		} else if data[0] == '1' {
			v = true
			b.Bool = &v
			return nil
		}
	}

	if err := json.Unmarshal(data, &b.Bool); err != nil {
		return fmt.Errorf("json.Unmarshal failed for bool: %v", err)
	}

	return nil
}

type ArrayValueDecoder struct {
	Values  interface{}
	Type    ValueType
	Invalid bool `json:"-"`
}

func (a *ArrayValueDecoder) Value() interface{} {
	return a.Values
}

func (a *ArrayValueDecoder) Scan(src interface{}) error {
	if src == nil {
		a.Invalid = true
		return nil
	}

	v, ok := src.(string)
	if !ok {
		return fmt.Errorf("unexpected type %T for %T", src, a)
	}

	return a.UnmarshalJSON([]byte(v))
}

func (a *ArrayValueDecoder) UnmarshalJSON(b []byte) error {
	if a.Type.ArrayType.Code == TCStruct {
		var arr []json.RawMessage
		if err := json.Unmarshal(b, &arr); err != nil {
			return fmt.Errorf("json.Unmarshal failed in %T: %v", a, err)
		}

		var svs []*StructValue
		for _, b2 := range arr {
			vv, err := a.decodeStruct(b2, a.Type.ArrayType.StructType)
			if err != nil {
				return err
			}

			svs = append(svs, vv)
		}

		a.Values = svs
	} else {
		v, err := a.decodeValue(b, a.Type)
		if err != nil {
			return err
		}

		a.Values = v
	}

	return nil
}

func (a *ArrayValueDecoder) decodeStruct(b []byte, typ *StructType) (*StructValue, error) {
	var vv struct {
		Keys   []string          `json:"keys"`
		Values []json.RawMessage `json:"values"`
	}

	if err := json.Unmarshal(b, &vv); err != nil {
		return nil, fmt.Errorf("json.Unmarshal failed in %T: %v", a, err)
	}

	var values []interface{}
	for i, value := range vv.Values {
		typ := typ.FieldTypes[i]
		vvv, err := a.decodeValue(value, *typ)
		if err != nil {
			return nil, err
		}

		values = append(values, vvv)
	}

	return &StructValue{
		Keys:   vv.Keys,
		Values: values,
	}, nil
}

func (a *ArrayValueDecoder) decodeValue(b []byte, typ ValueType) (interface{}, error) {
	var rv reflect.Value
	switch typ.Code {
	case TCBool:
		rv = reflect.New(reflect.TypeOf(BoolDecoder{}))
	case TCInt64:
		rv = reflect.New(reflect.TypeOf(int64(0)))
	case TCFloat64:
		rv = reflect.New(reflect.TypeOf(float64(0)))
	case TCTimestamp, TCDate, TCString:
		rv = reflect.New(reflect.TypeOf(string("")))
	case TCBytes:
		rv = reflect.New(reflect.TypeOf([]byte{}))
	case TCArray:
		switch typ.ArrayType.Code {
		case TCBool:
			rv = reflect.New(reflect.TypeOf([]*BoolDecoder{}))
		case TCInt64:
			rv = reflect.New(reflect.TypeOf([]*int64{}))
		case TCFloat64:
			rv = reflect.New(reflect.TypeOf([]*float64{}))
		case TCTimestamp, TCDate, TCString:
			rv = reflect.New(reflect.TypeOf([]*string{}))
		case TCBytes:
			rv = reflect.New(reflect.TypeOf([][]byte{}))
		case TCStruct:
			v := reflect.New(reflect.TypeOf(ArrayValueDecoder{}))
			reflect.Indirect(v).FieldByName("Type").Set(reflect.ValueOf(typ))
			rv = v

		default:
			return nil, fmt.Errorf("unknownn supported type for Array: %v", typ.ArrayType.Code)
		}
	case TCStruct:
		return nil, fmt.Errorf("unknown supported type: %v", typ.Code)
	}

	rvv := rv.Interface()
	if err := json.Unmarshal([]byte(b), rvv); err != nil {
		return nil, fmt.Errorf("json.Unmarshalll failed for %T in %T: %v", rvv, a, err)
	}

	var value interface{}
	switch vv := rv.Interface().(type) {
	case *BoolDecoder:
		value = *vv.Bool
	case *[]*BoolDecoder:
		if vv == nil {
			value = nil
		} else {
			vv := *vv
			vs := make([]*bool, len(vv))
			for i := 0; i < len(vv); i++ {
				if vv[i] == nil {
					vs[i] = nil
				} else {
					vs[i] = vv[i].Bool
				}
			}
			value = vs
		}
	case *ArrayValueDecoder:
		value = vv.Value()
	default:
		value = reflect.Indirect(rv).Interface()
	}

	return value, nil
}

type StructValue struct {
	Keys   []string      `json:"keys"`
	Values []interface{} `json:"values"`
}

type rows struct {
	rows        *sql.Rows
	resultItems []ResultItem

	lastErr error
}

func (r *rows) ResultSet() []ResultItem {
	return r.resultItems
}

func (it *rows) Do(fn func([]interface{}) error) error {
	var lastErr error
	for {
		row, ok := it.next()
		if !ok {
			break
		}
		if err := fn(row); err != nil {
			lastErr = err
			break
		}
	}
	if it.lastErr != nil {
		if lastErr == nil {
			lastErr = it.lastErr
		}
	}

	if err := it.rows.Err(); err != nil {
		if lastErr == nil {
			lastErr = err
		}
	}
	if err := it.rows.Close(); err != nil {
		if lastErr == nil {
			lastErr = err
		}
	}

	// convert sqlite runtime error as InvalidArgument error if it is SqliteArgumentRuntimeError.
	if lastErr != nil {
		msg := lastErr.Error()
		if strings.HasPrefix(msg, SqliteArgumentRuntimeErrorPrefix) {
			msg = strings.TrimPrefix(msg, SqliteArgumentRuntimeErrorPrefix)
			return status.Errorf(codes.InvalidArgument, "%s", msg)
		}
		if strings.HasPrefix(msg, SqliteOutOfRangeRuntimeErrorPrefix) {
			msg = strings.TrimPrefix(msg, SqliteOutOfRangeRuntimeErrorPrefix)
			return status.Errorf(codes.OutOfRange, "%s", msg)
		}
	}

	return lastErr
}

func (it *rows) next() ([]interface{}, bool) {
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
			v := reflect.New(reflect.TypeOf(ArrayValueDecoder{}))
			reflect.Indirect(v).FieldByName("Type").Set(reflect.ValueOf(item.ValueType))
			values[i] = v
		case TCStruct:
			it.lastErr = fmt.Errorf("unknown supported type: %v", item.ValueType.Code)
			return nil, false
		}
		ptrs[i] = values[i].Interface()
	}

	if err := it.rows.Scan(ptrs...); err != nil {
		it.lastErr = err
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
		case ArrayValueDecoder:
			if vv.Invalid {
				data[i] = nil
			} else {
				data[i] = vv.Value()
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
