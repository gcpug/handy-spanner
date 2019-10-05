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
