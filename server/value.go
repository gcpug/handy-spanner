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
	"strconv"
	"strings"
	"time"

	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

var NullExpr = Expr{}
var NullValue = Value{}

type Expr struct {
	Raw       string
	ValueType ValueType
	Args      []interface{}
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
		return fmt.Sprintf("ARRAY<%s>", t.ArrayType.String())
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
	if a.Code != b.Code {
		return false
	}

	if a.Code == TCStruct && b.Code == TCStruct {
		aStr := a.StructType
		bStr := b.StructType

		if len(aStr.FieldTypes) != len(bStr.FieldTypes) {
			return false
		}
		for i := 0; i < len(aStr.FieldTypes); i++ {
			b := compareValueType(*aStr.FieldTypes[i], *bStr.FieldTypes[i])
			if !b {
				return false
			}
		}

		return true
	}

	if a.Code == TCArray && b.Code == TCArray {
		return compareValueType(*a.ArrayType, *b.ArrayType)
	}

	return true
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
	TCJson
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
	transaction *transaction

	lastErr error
}

func (r *rows) ResultSet() []ResultItem {
	return r.resultItems
}

func (it *rows) Do(fn func([]interface{}) error) error {
	var lastErr error
	var rows []interface{}
	for {
		row, ok := it.next()
		if !ok {
			break
		}
		rows = append(rows, row)
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

	// database/sql has a possible bug that cannot read any data without error.
	// It may happen context is canceled in bad timing.
	// Here checks the transaction is available or not and if it's not available return aborted error.
	if !it.transaction.Available() {
		lastErr = status.Errorf(codes.Aborted, "transaction aborted")
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
		case TCTimestamp, TCDate, TCString, TCJson:
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
	values := make([]interface{}, 0, len(columns))
	for i, v := range lv.Values {
		column := columns[i]
		vv, err := spannerValue2DatabaseValue(v, *column)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		values = append(values, vv)
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

	vv, err := makeDataFromSpannerValue(col.Name(), v, col.valueType)
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

func makeSpannerTypeFromValueType(typ ValueType) *spannerpb.Type {
	var code spannerpb.TypeCode
	switch typ.Code {
	case TCBool:
		code = spannerpb.TypeCode_BOOL
	case TCInt64:
		code = spannerpb.TypeCode_INT64
	case TCFloat64:
		code = spannerpb.TypeCode_FLOAT64
	case TCTimestamp:
		code = spannerpb.TypeCode_TIMESTAMP
	case TCDate:
		code = spannerpb.TypeCode_DATE
	case TCString:
		code = spannerpb.TypeCode_STRING
	case TCBytes:
		code = spannerpb.TypeCode_BYTES
	case TCArray:
		code = spannerpb.TypeCode_ARRAY
	case TCStruct:
		code = spannerpb.TypeCode_STRUCT
	case TCJson:
		code = spannerpb.TypeCode_JSON
	}

	st := &spannerpb.Type{Code: code}
	if code == spannerpb.TypeCode_ARRAY {
		st = &spannerpb.Type{
			Code:             code,
			ArrayElementType: makeSpannerTypeFromValueType(*typ.ArrayType),
		}
	}
	if code == spannerpb.TypeCode_STRUCT {
		n := len(typ.StructType.FieldTypes)
		fields := make([]*spannerpb.StructType_Field, n)
		for i := 0; i < n; i++ {
			fields[i] = &spannerpb.StructType_Field{
				Name: typ.StructType.FieldNames[i],
				Type: makeSpannerTypeFromValueType(*typ.StructType.FieldTypes[i]),
			}
		}

		st = &spannerpb.Type{
			Code: code,
			StructType: &spannerpb.StructType{
				Fields: fields,
			},
		}
	}
	return st
}

func makeValueTypeFromSpannerType(typ *spannerpb.Type) (ValueType, error) {
	switch typ.Code {
	case spannerpb.TypeCode_BOOL:
		return ValueType{
			Code: TCBool,
		}, nil
	case spannerpb.TypeCode_INT64:
		return ValueType{
			Code: TCInt64,
		}, nil
	case spannerpb.TypeCode_FLOAT64:
		return ValueType{
			Code: TCFloat64,
		}, nil
	case spannerpb.TypeCode_TIMESTAMP:
		return ValueType{
			Code: TCTimestamp,
		}, nil
	case spannerpb.TypeCode_DATE:
		return ValueType{
			Code: TCDate,
		}, nil
	case spannerpb.TypeCode_STRING:
		return ValueType{
			Code: TCString,
		}, nil
	case spannerpb.TypeCode_BYTES:
		return ValueType{
			Code: TCBytes,
		}, nil
	case spannerpb.TypeCode_ARRAY:
		var array *ValueType
		if typ.ArrayElementType != nil {
			vt, err := makeValueTypeFromSpannerType(typ.ArrayElementType)
			if err != nil {
				return ValueType{}, err
			}
			array = &vt
		}
		return ValueType{
			Code:      TCArray,
			ArrayType: array,
		}, nil
	case spannerpb.TypeCode_STRUCT:
		fields := typ.GetStructType().GetFields()
		names := make([]string, len(fields))
		types := make([]*ValueType, len(fields))
		for i, field := range fields {
			vt, err := makeValueTypeFromSpannerType(field.Type)
			if err != nil {
				return ValueType{}, err
			}
			names[i] = field.Name
			types[i] = &vt
		}
		return ValueType{
			Code: TCStruct,
			StructType: &StructType{
				FieldNames: names,
				FieldTypes: types,
			},
		}, nil
	case spannerpb.TypeCode_JSON:
		return ValueType{
			Code: TCJson,
		}, nil
	}

	return ValueType{}, fmt.Errorf("unknown code for spanner.Type: %v", typ.Code)
}

func spannerValueFromValue(x interface{}) (*structpb.Value, error) {
	switch x := x.(type) {
	case nil:
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}, nil

	case bool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{x}}, nil
	case int64:
		// The Spanner int64 is actually a decimal string.
		s := strconv.FormatInt(x, 10)
		return &structpb.Value{Kind: &structpb.Value_StringValue{s}}, nil
	case float64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{x}}, nil
	case string:
		return &structpb.Value{Kind: &structpb.Value_StringValue{x}}, nil
	case []byte:
		if x == nil {
			return &structpb.Value{Kind: &structpb.Value_NullValue{}}, nil
		} else {
			return &structpb.Value{Kind: &structpb.Value_StringValue{encodeBase64(x)}}, nil
		}
	case StructValue:
		n := len(x.Values)
		fields := make(map[string]*structpb.Value)
		for i := 0; i < n; i++ {
			elem := x.Values[i]
			v, err := spannerValueFromValue(elem)
			if err != nil {
				return nil, err
			}
			fields[x.Keys[i]] = v
		}
		return &structpb.Value{Kind: &structpb.Value_StructValue{
			StructValue: &structpb.Struct{
				Fields: fields,
			},
		}}, nil

	case []*bool, []*int64, []*float64, []*string, []*StructValue, [][]byte:
		rv := reflect.ValueOf(x)
		n := rv.Len()
		vs := make([]*structpb.Value, n)
		for i := 0; i < n; i++ {
			var elem interface{}

			rvv := rv.Index(i)
			if !rvv.IsNil() {
				elem = reflect.Indirect(rv.Index(i)).Interface()
			}

			v, err := spannerValueFromValue(elem)
			if err != nil {
				return nil, err
			}
			vs[i] = v
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{
			&structpb.ListValue{Values: vs},
		}}, nil

	default:
		return nil, fmt.Errorf("unknown database value type %T", x)
	}
}

func makeDataFromSpannerValue(key string, v *structpb.Value, typ ValueType) (interface{}, error) {
	if typ.StructType != nil {
		return nil, newBindingErrorf(key, typ, "Struct type is not supported yet")
	}

	if typ.Code == TCArray {
		if typ.ArrayType == nil {
			return nil, status.Error(codes.InvalidArgument, "The array_element_type field is required for ARRAYs")
		}

		if _, ok := v.Kind.(*structpb.Value_NullValue); ok {
			switch typ.ArrayType.Code {
			case TCBool:
				return []bool(nil), nil
			case TCInt64:
				return []int64(nil), nil
			case TCFloat64:
				return []float64(nil), nil
			case TCTimestamp, TCDate, TCString:
				return []string(nil), nil
			case TCBytes:
				return [][]byte(nil), nil
			case TCArray, TCStruct:
				// this should not be error actually but no reason to support.
				return nil, newBindingErrorf(key, typ, "nested Array or Struct for Array is not supported yet")
			default:
				return nil, fmt.Errorf("unexpected type %d for Null value as Array", typ.ArrayType.Code)
			}
		}

		vv, ok := v.Kind.(*structpb.Value_ListValue)
		if !ok {
			return nil, newBindingErrorf(key, typ, "unexpected value %T and type %s as Array", v.Kind, typ)
		}

		n := len(vv.ListValue.Values)
		switch typ.ArrayType.Code {
		case TCBool:
			ret := make([]*bool, n)
			for i, vv := range vv.ListValue.Values {
				elemKey := fmt.Sprintf("%s[%d]", key, i)
				vvv, err := makeDataFromSpannerValue(elemKey, vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				if vvv == nil {
					ret[i] = nil
				} else {
					vvvv, ok := vvv.(bool)
					if !ok {
						panic(fmt.Sprintf("unexpected value type: %T", vvv))
					}
					ret[i] = &vvvv
				}
			}
			return &ArrayValueEncoder{Values: ret}, nil
		case TCInt64:
			ret := make([]*int64, n)
			for i, vv := range vv.ListValue.Values {
				elemKey := fmt.Sprintf("%s[%d]", key, i)
				vvv, err := makeDataFromSpannerValue(elemKey, vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				if vvv == nil {
					ret[i] = nil
				} else {
					vvvv, ok := vvv.(int64)
					if !ok {
						panic(fmt.Sprintf("unexpected value type: %T", vvv))
					}
					ret[i] = &vvvv
				}
			}
			return &ArrayValueEncoder{Values: ret}, nil
		case TCFloat64:
			ret := make([]*float64, n)
			for i, vv := range vv.ListValue.Values {
				elemKey := fmt.Sprintf("%s[%d]", key, i)
				vvv, err := makeDataFromSpannerValue(elemKey, vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				if vvv == nil {
					ret[i] = nil
				} else {
					vvvv, ok := vvv.(float64)
					if !ok {
						panic(fmt.Sprintf("unexpected value type: %T", vvv))
					}
					ret[i] = &vvvv
				}
			}
			return &ArrayValueEncoder{Values: ret}, nil
		case TCTimestamp, TCDate, TCString:
			ret := make([]*string, n)
			for i, vv := range vv.ListValue.Values {
				elemKey := fmt.Sprintf("%s[%d]", key, i)
				vvv, err := makeDataFromSpannerValue(elemKey, vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				if vvv == nil {
					ret[i] = nil
				} else {
					vvvv, ok := vvv.(string)
					if !ok {
						panic(fmt.Sprintf("unexpected value type: %T", vvv))
					}
					ret[i] = &vvvv
				}
			}
			return &ArrayValueEncoder{Values: ret}, nil
		case TCBytes:
			ret := make([][]byte, n)
			for i, vv := range vv.ListValue.Values {
				elemKey := fmt.Sprintf("%s[%d]", key, i)
				vvv, err := makeDataFromSpannerValue(elemKey, vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
				if vvv == nil {
					ret[i] = nil
				} else {
					vvvv, ok := vvv.([]byte)
					if !ok {
						panic(fmt.Sprintf("unexpected value type: %T", vvv))
					}
					ret[i] = vvvv
				}
			}
			return &ArrayValueEncoder{Values: ret}, nil
		case TCArray, TCStruct:
			// just visit elements for appropriate error handling
			for i, vv := range vv.ListValue.Values {
				elemKey := fmt.Sprintf("%s[%d]", key, i)
				_, err := makeDataFromSpannerValue(elemKey, vv, *typ.ArrayType)
				if err != nil {
					return nil, err
				}
			}

			// must be unreachable
			panic("array of array or array of struct is not supported")

		default:
			return nil, fmt.Errorf("unknown TypeCode for ArrayElement %v", typ.Code)
		}
	}

	if _, ok := v.Kind.(*structpb.Value_NullValue); ok {
		return nil, nil
	}

	switch typ.Code {
	case TCBool:
		switch vv := v.Kind.(type) {
		case *structpb.Value_BoolValue:
			return vv.BoolValue, nil
		}
	case TCInt64:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			// base is always 10
			n, err := strconv.ParseInt(vv.StringValue, 10, 64)
			if err != nil {
				return nil, newBindingErrorf(key, typ, "unexpected format %q as int64: %v", vv.StringValue, err)
			}
			return n, nil
		}

	case TCFloat64:
		switch vv := v.Kind.(type) {
		case *structpb.Value_NumberValue:
			return vv.NumberValue, nil
		}

	case TCTimestamp:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			s := vv.StringValue
			if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
				return nil, newBindingErrorf(key, typ, "unexpected format %q as timestamp: %v", s, err)
			}
			return s, nil
		}

	case TCDate:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			s := vv.StringValue
			if _, err := time.Parse("2006-01-02", s); err != nil {
				return nil, newBindingErrorf(key, typ, "unexpected format for %q as date: %v", s, err)
			}
			return s, nil
		}

	case TCString, TCJson:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			return vv.StringValue, nil
		}
	case TCBytes:
		switch vv := v.Kind.(type) {
		case *structpb.Value_StringValue:
			b, err := decodeBase64(vv.StringValue)
			if err != nil {
				return nil, newBindingErrorf(key, typ, "decoding base64 failed: %v", err)
			}
			return b, nil
		}
	default:
		return nil, fmt.Errorf("unknown Type %s", typ)
	}

	return nil, newBindingErrorf(key, typ, "unexpected value %T and type %s", v.Kind, typ)
}

func makeValueFromSpannerValue(key string, v *structpb.Value, typ *spannerpb.Type) (Value, error) {
	vt, err := makeValueTypeFromSpannerType(typ)
	if err != nil {
		return Value{}, err
	}

	data, err := makeDataFromSpannerValue(key, v, vt)
	if err != nil {
		return Value{}, err
	}

	return Value{
		Data: data,
		Type: vt,
	}, nil
}

type bindingError struct {
	key      string
	msg      string
	expected ValueType
}

func (e *bindingError) Error() string {
	return e.msg
}

func (e *bindingError) GRPCStatus() *status.Status {
	return status.Newf(codes.InvalidArgument, "Invalid value for bind parameter %s: Expected %s.", e.key, e.expected)
}

func newBindingErrorf(key string, expected ValueType, format string, a ...interface{}) error {
	return &bindingError{
		key:      key,
		expected: expected,
		msg:      fmt.Sprintf(format, a...),
	}
}
