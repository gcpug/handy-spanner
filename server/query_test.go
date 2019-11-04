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
	"testing"

	cmp "github.com/google/go-cmp/cmp"
)

func newTestQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func TestQueryBuilder_ExpandParamByPlaceholders(t *testing.T) {
	b := newTestQueryBuilder()

	table := []struct {
		v    Value
		ph   string
		args []interface{}
	}{
		{
			v: Value{
				Data: []bool{true, false},
			},
			ph:   "JSON_ARRAY((?), (?))",
			args: []interface{}{true, false},
		},
		{
			v: Value{
				Data: []int64{(100), int64(101)},
			},
			ph:   "JSON_ARRAY((?), (?))",
			args: []interface{}{int64(100), int64(101)},
		},
		{
			v: Value{
				Data: []float64{float64(1.1), float64(1.2)},
			},
			ph:   "JSON_ARRAY((?), (?))",
			args: []interface{}{float64(1.1), float64(1.2)},
		},
		{
			v: Value{
				Data: []string{"aa", "bb", "cc"},
			},
			ph:   "JSON_ARRAY((?), (?), (?))",
			args: []interface{}{"aa", "bb", "cc"},
		},
		{
			v: Value{
				Data: [][]byte{[]byte("aa"), []byte("bb"), []byte("cc")},
			},
			ph:   "JSON_ARRAY((?), (?), (?))",
			args: []interface{}{[]byte("aa"), []byte("bb"), []byte("cc")},
		},
	}

	for _, tc := range table {
		s, err := b.expandParamByPlaceholders(tc.v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if s.Raw != tc.ph {
			t.Errorf("expect placeholder %q, but got %q", tc.ph, s.Raw)
		}

		if diff := cmp.Diff(tc.args, s.Args); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

	}
}
