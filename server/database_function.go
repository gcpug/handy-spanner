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
	"math"
	"strconv"
	"strings"

	sqlite "github.com/mattn/go-sqlite3"
)

func init() {
	sql.Register("sqlite3_spanner", &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {
			for name, fn := range customFunctions {
				name = strings.ToLower(name)
				if fn.Func == nil {
					continue
				}
				if err := conn.RegisterFunc(name, fn.Func, true); err != nil {
					return err
				}
			}
			return nil
		},
	})
}

type CustomFunction struct {
	Name       string
	Func       interface{}
	NArgs      int
	ArgTypes   func([]ValueType) bool
	ReturnType func([]ValueType) ValueType
}

func exactMatchArgumentTypes(vts ...ValueType) func([]ValueType) bool {
	return func(vts2 []ValueType) bool {
		if len(vts) != len(vts2) {
			return false
		}
		for i := range vts {
			ok := compareValueType(vts[i], vts2[i])
			if !ok {
				return false
			}
		}

		return true
	}
}

func staticReturnType(vt ValueType) func([]ValueType) ValueType {
	return func([]ValueType) ValueType {
		return vt
	}
}

const SqliteArgumentRuntimeErrorPrefix = "_sqlite_argument_runtime_error_: "
const SqliteOutOfRangeRuntimeErrorPrefix = "_sqlite_ooo_runtime_error_: "

// sqliteArgumentRuntimeError is runtime error to run query in sqlite.
// Runtime error only can be returned as string of error in sqlite.
// To distinguish error as InvalidArgument or InternalError, the error message
// by this has a specific prefix. RowsIterator checks the prefix and returns the error
// as InvalidArgument.
type sqliteArgumentRuntimeError struct {
	msg string
}

func (e *sqliteArgumentRuntimeError) Error() string {
	return SqliteArgumentRuntimeErrorPrefix + e.msg
}

type sqliteOutOfRangeRuntimeError struct {
	msg string
}

func (e *sqliteOutOfRangeRuntimeError) Error() string {
	return SqliteOutOfRangeRuntimeErrorPrefix + e.msg
}

// customFunctions is functions for spanner.
//
// sqlite cannot register function which returns interface{}.
// If spanner function may return different type value, we need to register multiple functions
// for each returned type.
var customFunctions map[string]CustomFunction = map[string]CustomFunction{
	"SIGN": {
		Func:  sqlite3FnSign,
		NArgs: 1,
		ArgTypes: exactMatchArgumentTypes(
			ValueType{Code: TCInt64},
		),
		ReturnType: staticReturnType(ValueType{
			Code: TCInt64,
		}),
	},
	"STARTS_WITH": {
		Func:  sqlite3FnStartsWith,
		NArgs: 2,
		ArgTypes: exactMatchArgumentTypes(
			ValueType{Code: TCString},
			ValueType{Code: TCString},
		),
		ReturnType: staticReturnType(ValueType{
			Code: TCBool,
		}),
	},
	"ENDS_WITH": {
		Func:  sqlite3FnEndsWith,
		NArgs: 2,
		ArgTypes: exactMatchArgumentTypes(
			ValueType{Code: TCString},
			ValueType{Code: TCString},
		),
		ReturnType: staticReturnType(ValueType{
			Code: TCBool,
		}),
	},
	"CONCAT": {
		Func:  sqlite3FnConcat,
		NArgs: -1,
		ArgTypes: func(vts []ValueType) bool {
			if len(vts) == 0 {
				return false
			}
			vt := vts[0]
			for i := range vts {
				if !compareValueType(vt, vts[i]) {
					return false
				}
			}
			if vt.Code != TCString && vt.Code != TCBytes {
				return false
			}
			return true
		},
		ReturnType: func(vts []ValueType) ValueType {
			return vts[0]
		},
	},

	"COUNT": {
		Func:  nil,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return true
		},
		ReturnType: staticReturnType(ValueType{
			Code: TCInt64,
		}),
	},
	"MAX": {
		Func:  nil,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			code := vts[0].Code
			if code == TCArray || code == TCStruct {
				return false
			}
			return true
		},
		ReturnType: func(vts []ValueType) ValueType {
			return vts[0]
		},
	},
	"MIN": {
		Func:  nil,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			code := vts[0].Code
			if code == TCArray || code == TCStruct {
				return false
			}
			return true
		},
		ReturnType: func(vts []ValueType) ValueType {
			return vts[0]
		},
	},
	"AVG": {
		Func:  nil,
		NArgs: -1,
		ArgTypes: func(vts []ValueType) bool {
			for _, vt := range vts {
				if vt.Code != TCInt64 && vt.Code == TCFloat64 {
					return false
				}
			}
			return true
		},
		ReturnType: staticReturnType(ValueType{
			Code: TCFloat64,
		}),
	},
	"SUM": {
		Func:  nil,
		NArgs: -1,
		ArgTypes: func(vts []ValueType) bool {
			for _, vt := range vts {
				if vt.Code != TCInt64 && vt.Code == TCFloat64 {
					return false
				}
			}
			return true
		},
		ReturnType: func(vts []ValueType) ValueType {
			for _, vt := range vts {
				if vt.Code == TCFloat64 {
					return ValueType{Code: TCFloat64}
				}
			}
			return ValueType{Code: TCInt64}
		},
	},

	"___CAST_INT64_TO_BOOL": {
		Func:  sqlite3FnCastInt64ToBool,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCInt64
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCBool}
		},
	},
	"___CAST_INT64_TO_STRING": {
		Func:  sqlite3FnCastInt64ToString,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCInt64
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCString}
		},
	},
	"___CAST_INT64_TO_FLOAT64": {
		Func:  sqlite3FnCastInt64ToFloat64,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCInt64
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCFloat64}
		},
	},
	"___CAST_FLOAT64_TO_STRING": {
		Func:  sqlite3FnCastFloat64ToString,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCFloat64
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCString}
		},
	},
	"___CAST_FLOAT64_TO_INT64": {
		Func:  sqlite3FnCastFloat64ToInt64,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCFloat64
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCInt64}
		},
	},
	"___CAST_BOOL_TO_STRING": {
		Func:  sqlite3FnCastBoolToString,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCBool
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCString}
		},
	},
	"___CAST_BOOL_TO_INT64": {
		Func:  sqlite3FnCastBoolToInt64,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCBool
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCInt64}
		},
	},
	"___CAST_STRING_TO_BOOL": {
		Func:  sqlite3FnCastStringToBool,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCString
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCBool}
		},
	},
	"___CAST_STRING_TO_INT64": {
		Func:  sqlite3FnCastStringToInt64,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCString
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCInt64}
		},
	},
	"___CAST_STRING_TO_FLOAT64": {
		Func:  sqlite3FnCastStringToFloat64,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCString
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCFloat64}
		},
	},
}

func sqlite3FnSign(x int64) int64 {
	if x > 0 {
		return 1
	}
	if x < 0 {
		return -1
	}
	return 0
}

func sqlite3FnStartsWith(a, b string) bool {
	return strings.HasPrefix(a, b)
}

func sqlite3FnEndsWith(a, b string) bool {
	return strings.HasSuffix(a, b)
}

func sqlite3FnConcat(xs ...string) string {
	return strings.Join(xs, "")
}

func sqlite3FnCastInt64ToBool(i int64) bool {
	return i != 0
}

func sqlite3FnCastInt64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

func sqlite3FnCastInt64ToFloat64(i int64) float64 {
	return float64(i)
}

func sqlite3FnCastFloat64ToString(f float64) string {
	if math.IsInf(f, 0) {
		if f < 0 {
			return "-inf"
		}
		return "inf"
	}

	return strconv.FormatFloat(f, 'g', -1, 64)
}

func sqlite3FnCastFloat64ToInt64(f float64) (int64, error) {
	if math.IsNaN(f) {
		// OutOfRange error
		msg := "Illegal conversion of non-finite floating point number to an integer: nan"
		return 0, &sqliteOutOfRangeRuntimeError{msg: msg}
	}
	if math.IsInf(f, 0) {
		// OutOfRange error
		inf := "inf"
		if f < 0 {
			inf = "-inf"
		}
		msg := "Illegal conversion of non-finite floating point number to an integer: " + inf
		return 0, &sqliteOutOfRangeRuntimeError{msg: msg}
	}

	if f < 0 {
		return int64(f - 0.5), nil
	}
	return int64(f + 0.5), nil
}

func sqlite3FnCastBoolToString(b bool) string {
	if b {
		return "TRUE"
	}
	return "FALSE"
}

func sqlite3FnCastBoolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func sqlite3FnCastStringToBool(s string) (bool, error) {
	ss := strings.ToUpper(s)
	if ss == "TRUE" {
		return true, nil
	}
	if ss == "FALSE" {
		return false, nil
	}

	// OutOfRange error
	return false, &sqliteOutOfRangeRuntimeError{msg: fmt.Sprintf("Bad bool value: %s", s)}
}

func sqlite3FnCastStringToInt64(s string) (int64, error) {
	if s == "" {
		// OutOfRange error
		return 0, &sqliteOutOfRangeRuntimeError{msg: fmt.Sprintf("Bad int64 value: %s", s)}
	}

	orig := s
	var neg bool
	if s[0] == '+' {
		s = s[1:]
	} else if s[0] == '-' {
		neg = true
		s = s[1:]
	}

	// Base is available only for 10 or 16 in spanner.
	base := 10
	if len(s) > 2 && s[0] == '0' && s[1] == 'x' {
		base = 16
		s = s[2:]
	}

	n, err := strconv.ParseInt(s, base, 64)
	if err != nil {
		// OutOfRange error
		return 0, &sqliteOutOfRangeRuntimeError{msg: fmt.Sprintf("Bad int64 value: %s", orig)}
	}

	if n < 0 {
		// OutOfRange error
		return 0, &sqliteOutOfRangeRuntimeError{msg: fmt.Sprintf("Bad int64 value: %s", orig)}
	}

	if neg {
		n = -n
	}

	return n, nil
}

func sqlite3FnCastStringToFloat64(s string) (float64, error) {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// OutOfRange error
		return 0, &sqliteOutOfRangeRuntimeError{msg: fmt.Sprintf("Bad double value: %s", s)}
	}

	return n, nil
}
