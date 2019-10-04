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
			for _, fn := range customFunctions {
				if fn.Func == nil {
					continue
				}
				if err := conn.RegisterFunc(fn.Name, fn.Func, true); err != nil {
					return err
				}
			}
			return nil
		},
	})
}

type CustomFunction struct {
	Name  string
	Func  interface{}
	NArgs int
	Args  []interface{}
	Ret   ValueType
}

var customFunctions []CustomFunction = []CustomFunction{
	{
		Name:  "sign",
		Func:  sqlite3FnSign,
		NArgs: 1,
		Args:  []interface{}{int64(0)},
		Ret: ValueType{
			Code: TCInt64,
		},
	},
	{
		Name:  "starts_with",
		Func:  sqlite3FnStartsWith,
		NArgs: 2,
		Args:  []interface{}{"", ""},
		Ret: ValueType{
			Code: TCBool,
		},
	},
	{
		Name:  "count",
		Func:  nil,
		NArgs: 1,
		Args:  []interface{}{""},
		Ret: ValueType{
			Code: TCInt64,
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
