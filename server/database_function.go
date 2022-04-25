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
	"time"

	"github.com/dgryski/go-farm"
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

// customFunctionNamesMap is naming map from Spanner's function to sqlite's custom function.
// This is used to avoid conflict of reserved name in sqlite.
var customFunctionNamesMap map[string]string = map[string]string{
	"CURRENT_TIMESTAMP": "___CURRENT_TIMESTAMP",
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

	"___EXTRACT_FROM_TIMESTAMP": {
		Func:  sqlite3FnExtractFromTimestamp,
		NArgs: 3,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCString &&
				vts[1].Code == TCTimestamp &&
				vts[2].Code == TCString
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCInt64}
		},
	},
	"___EXTRACT_FROM_DATE": {
		Func:  sqlite3FnExtractFromDate,
		NArgs: 2,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCString && vts[1].Code == TCDate
		},
		ReturnType: func(vts []ValueType) ValueType {
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
	"___CAST_STRING_TO_DATE": {
		Func:  sqlite3FnCastStringToDate,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCString
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCDate}
		},
	},
	"___CAST_STRING_TO_TIMESTAMP": {
		Func:  sqlite3FnCastStringToTimestamp,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCString
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCTimestamp}
		},
	},
	"___CAST_DATE_TO_STRING": {
		Func:  sqlite3FnCastDateToString,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCDate
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCString}
		},
	},
	"___CAST_DATE_TO_TIMESTAMP": {
		Func:  sqlite3FnCastDateToTimestamp,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCDate
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCTimestamp}
		},
	},
	"___CAST_TIMESTAMP_TO_STRING": {
		Func:  sqlite3FnCastTimestampToString,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCTimestamp
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCString}
		},
	},
	"___CAST_TIMESTAMP_TO_DATE": {
		Func:  sqlite3FnCastTimestampToDate,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCTimestamp
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCDate}
		},
	},
	"PENDING_COMMIT_TIMESTAMP": getCustomFunctionForCurrentTime(),
	"___CURRENT_TIMESTAMP":     getCustomFunctionForCurrentTime(),
	"IFNULL": {
		Func:  nil,
		NArgs: 2,
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
			return true
		},
		ReturnType: func(vts []ValueType) ValueType {
			return vts[0]
		},
	},
	"NULLIF": {
		Func:  nil,
		NArgs: 2,
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
			return true
		},
		ReturnType: func(vts []ValueType) ValueType {
			return vts[0]
		},
	},
	"FARM_FINGERPRINT": {
		Func:  sqlite3FnFarmFingerprint,
		NArgs: 1,
		ArgTypes: func(vts []ValueType) bool {
			return vts[0].Code == TCString
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCInt64}
		},
	},
}

func sqlite3FnFarmFingerprint(s string) int64 {
	return int64(farm.Fingerprint64([]byte(s)))
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

// sqlite3FnExtractFromTimestamp is simulation function of EXTRACT.
// It supports except DATE part.
func sqlite3FnExtractFromTimestamp(part string, timestamp string, tz string) (int64, error) {
	tzErr := &sqliteOutOfRangeRuntimeError{msg: fmt.Sprintf("Invalid time zone: %s", tz)}
	if len(tz) == 0 {
		return 0, tzErr
	}

	var location *time.Location

	// (+|-)H[H][:M[M]]
	if tz[0] == '-' || tz[0] == '+' {
		s := tz
		var neg bool
		if s[0] == '-' {
			neg = true
		}

		s = s[1:]

		var colonFound bool
		hour := -1
		min := 0
		for _, c := range s {
			if c == ':' {
				min = -1
				colonFound = true
				continue
			}
			if c < '0' && '9' < c {
				return 0, tzErr
			}

			n := int(c - '0')

			if colonFound {
				if min == -1 {
					min = n
				} else {
					min = 10*min + n
				}
			} else {
				if hour == -1 {
					hour = n
				} else {
					hour = 10*hour + n
				}
			}
		}

		if hour == -1 || min == -1 {
			return 0, tzErr
		}
		if hour > 24 || min > 60 {
			return 0, tzErr
		}

		offset := hour*3600 + min*60
		if neg {
			offset = -offset
		}
		location = time.FixedZone(tz, offset)
	} else {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return 0, tzErr
		}
		location = loc
	}

	t, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		// Must not happen
		return 0, fmt.Errorf("___EXTRACT_FROM_TIMESTAMP: unexpected format %q as timestamp: %v", timestamp, err)
	}
	t = t.In(location)

	switch strings.ToUpper(part) {
	case "NANOSECOND":
		return int64(t.Nanosecond()), nil
	case "MICROSECOND":
		return int64(t.Nanosecond() / 1000), nil
	case "MILLISECOND":
		return int64(t.Nanosecond() / 1000000), nil
	case "SECOND":
		return int64(t.Second()), nil
	case "MINUTE":
		return int64(t.Minute()), nil
	case "HOUR":
		return int64(t.Hour()), nil
	case "DAYOFWEEK":
		return int64(t.Weekday()), nil
	case "DAY":
		return int64(t.Day()), nil
	case "DAYOFYEAR":
		return int64(t.YearDay()), nil
	case "WEEK":
		// TODO: calculate week from timestamp
		return 0, fmt.Errorf("___EXTRACT_FROM_TIMESTAMP: WEEK not supported for now")
	case "ISOWEEK":
		_, w := t.ISOWeek()
		return int64(w), nil
	case "MONTH":
		return int64(t.Month()), nil
	case "QUARTER":
		// 1 for Jan-Mar, 2 for Apr-Jun, 3 for Jul-Sep, 4 for Oct-Dec
		m := t.Month()
		return int64(m/4 + 1), nil
	case "YEAR":
		return int64(t.Year()), nil
	case "ISOYEAR":
		y, _ := t.ISOWeek()
		return int64(y), nil
	default:
		// Must not happen
		return 0, fmt.Errorf("___EXTRACT_FROM_TIMESTAMP: unexpected part: %s", part)
	}
}

// sqlite3FnExtractFromDate is simulation function of EXTRACT.
func sqlite3FnExtractFromDate(part string, date string) (int64, error) {
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		// Must not happen
		return 0, fmt.Errorf("___EXTRACT_FROM_DATE: unexpected format %q as date: %v", date, err)
	}

	switch strings.ToUpper(part) {
	case "DAYOFWEEK":
		return int64(t.Weekday()), nil
	case "DAY":
		return int64(t.Day()), nil
	case "DAYOFYEAR":
		return int64(t.YearDay()), nil
	case "WEEK":
		// TODO: calculate week from timestamp
		return 0, fmt.Errorf("___EXTRACT_FROM_DATE: WEEK not supported for now")
	case "ISOWEEK":
		_, w := t.ISOWeek()
		return int64(w), nil
	case "MONTH":
		return int64(t.Month()), nil
	case "QUARTER":
		// 1 for Jan-Mar, 2 for Apr-Jun, 3 for Jul-Sep, 4 for Oct-Dec
		m := t.Month()
		return int64(m/4 + 1), nil
	case "YEAR":
		return int64(t.Year()), nil
	case "ISOYEAR":
		y, _ := t.ISOWeek()
		return int64(y), nil
	default:
		// Must not happen
		return 0, fmt.Errorf("___EXTRACT_FROM_DATE: unexpected part: %s", part)
	}
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

func sqlite3FnCastStringToDate(s string) (string, error) {
	t, ok := parseDateLiteral(s)
	if !ok {
		// InvalidArgument error
		return "", &sqliteArgumentRuntimeError{msg: fmt.Sprintf("Could not cast literal %q to type DATE", s)}
	}

	return t.Format("2006-01-02"), nil
}

func sqlite3FnCastStringToTimestamp(s string) (string, error) {
	t, ok := parseTimestampLiteral(s)
	if !ok {
		// InvalidArgument error
		return "", &sqliteArgumentRuntimeError{msg: fmt.Sprintf("Could not cast literal %q to type TIMESTAMP", s)}
	}

	return t.Format(time.RFC3339Nano), nil
}

func sqlite3FnCastDateToString(s string) (string, error) {
	t, ok := parseDateLiteral(s)
	if !ok {
		// InvalidArgument error
		return "", &sqliteArgumentRuntimeError{msg: fmt.Sprintf("Could not cast literal %q to type STRING", s)}
	}

	return t.Format("2006-01-02"), nil
}

func sqlite3FnCastDateToTimestamp(s string) (string, error) {
	t, ok := parseDateLiteral(s)
	if !ok {
		// InvalidArgument error
		return "", &sqliteArgumentRuntimeError{msg: fmt.Sprintf("Could not cast literal %q to type TIMESTAMP", s)}
	}

	return t.UTC().Format(time.RFC3339Nano), nil
}

func sqlite3FnCastTimestampToString(s string) (string, error) {
	t, ok := parseTimestampLiteral(s)
	if !ok {
		// InvalidArgument error
		return "", &sqliteArgumentRuntimeError{msg: fmt.Sprintf("Could not cast literal %q to type STRING", s)}
	}

	return t.In(parseLocation).Format("2006-01-02 15:04:05.999999999-07"), nil
}

func sqlite3FnCastTimestampToDate(s string) (string, error) {
	t, ok := parseTimestampLiteral(s)
	if !ok {
		// InvalidArgument error
		return "", &sqliteArgumentRuntimeError{msg: fmt.Sprintf("Could not cast literal %q to type DATE", s)}
	}

	return t.In(parseLocation).Format("2006-01-02"), nil
}

func sqlite3FnCurrentTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func getCustomFunctionForCurrentTime() CustomFunction {
	return CustomFunction{
		Func:  sqlite3FnCurrentTimestamp,
		NArgs: 0,
		ArgTypes: func(vts []ValueType) bool {
			return true
		},
		ReturnType: func(vts []ValueType) ValueType {
			return ValueType{Code: TCTimestamp}
		},
	}
}
