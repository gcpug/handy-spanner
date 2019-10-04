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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func TestQueryBuilder_UnnestValue_Success(t *testing.T) {
	b := newTestQueryBuilder()

	table := []struct {
		v    interface{}
		ph   string
		args []interface{}
	}{
		{
			v:    []bool{true, false},
			ph:   "VALUES (?), (?)",
			args: []interface{}{true, false},
		},
		{
			v:    []int64{(100), int64(101)},
			ph:   "VALUES (?), (?)",
			args: []interface{}{int64(100), int64(101)},
		},
		{
			v:    []float64{float64(1.1), float64(1.2)},
			ph:   "VALUES (?), (?)",
			args: []interface{}{float64(1.1), float64(1.2)},
		},
		{
			v:    []string{"aa", "bb", "cc"},
			ph:   "VALUES (?), (?), (?)",
			args: []interface{}{"aa", "bb", "cc"},
		},
		{
			v:    [][]byte{[]byte("aa"), []byte("bb"), []byte("cc")},
			ph:   "VALUES (?), (?), (?)",
			args: []interface{}{[]byte("aa"), []byte("bb"), []byte("cc")},
		},
	}

	for _, tc := range table {
		s, d, err := b.unnestValue(tc.v)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if s.Raw != tc.ph {
			t.Errorf("expect placeholder %q, but got %q", tc.ph, s.Raw)
		}

		if diff := cmp.Diff(tc.args, d); diff != "" {
			t.Errorf("(-got, +want)\n%s", diff)
		}

	}
}

func TestQueryBuilder_UnnestValue_Error(t *testing.T) {
	b := newTestQueryBuilder()

	table := []struct {
		v    interface{}
		code codes.Code
	}{
		{
			v:    nil,
			code: codes.Unknown,
		},
		{
			v:    true,
			code: codes.InvalidArgument,
		},
		{
			v:    int64(100),
			code: codes.InvalidArgument,
		},
		{
			v:    float64(100),
			code: codes.InvalidArgument,
		},
		{
			v:    "x",
			code: codes.InvalidArgument,
		},
		{
			v:    []byte("x"),
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range table {
		_, _, err := b.unnestValue(tc.v)
		st := status.Convert(err)
		if st.Code() != tc.code {
			t.Errorf("expect code %v, but got %v", tc.code, st.Code())
		}
	}
}
