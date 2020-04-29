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
	"fmt"
	"strings"

	structpb "github.com/golang/protobuf/ptypes/struct"
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KeySet struct {
	All    bool
	Keys   []*structpb.ListValue
	Ranges []*KeyRange
}

type KeyRange struct {
	start       *structpb.ListValue
	end         *structpb.ListValue
	startClosed bool
	endClosed   bool
}

func makeKeySet(set *spannerpb.KeySet) *KeySet {
	ranges := make([]*KeyRange, 0, len(set.Ranges))
	for _, r := range set.Ranges {
		ranges = append(ranges, makeKeyRange(r))
	}
	return &KeySet{
		All:    set.All,
		Keys:   set.Keys,
		Ranges: ranges,
	}
}

func makeKeyRange(r *spannerpb.KeyRange) *KeyRange {
	var kr KeyRange
	switch s := r.StartKeyType.(type) {
	case *spannerpb.KeyRange_StartClosed:
		kr.start = s.StartClosed
		kr.startClosed = true
	case *spannerpb.KeyRange_StartOpen:
		kr.start = s.StartOpen
	}
	switch e := r.EndKeyType.(type) {
	case *spannerpb.KeyRange_EndClosed:
		kr.end = e.EndClosed
		kr.endClosed = true
	case *spannerpb.KeyRange_EndOpen:
		kr.end = e.EndOpen
	}
	return &kr
}

func buildWhereClauseFromKeySet(keyset *KeySet, indexColumnsName string, indexColumns []*Column) (string, []interface{}, error) {
	if keyset.All {
		return "", nil, nil
	}

	var conditions []string
	var subargs [][]interface{}

	if len(keyset.Keys) != 0 {
		q, a, err := buildKeySetQuery(indexColumnsName, indexColumns, keyset.Keys)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, q)
		subargs = append(subargs, a)
	}

	if len(keyset.Ranges) != 0 {
		for _, keyrange := range keyset.Ranges {
			q, a, err := buildKeyRangeQuery(indexColumnsName, indexColumns, keyrange)
			if err != nil {
				return "", nil, err
			}

			conditions = append(conditions, q)
			subargs = append(subargs, a)
		}
	}

	cond := strings.Join(conditions, " OR ")
	var args []interface{}
	for i := range subargs {
		args = append(args, subargs[i]...)
	}

	return fmt.Sprintf("WHERE %s", cond), args, nil
}

func buildKeySetQuery(pkeysName string, pkeyColumns []*Column, keys []*structpb.ListValue) (string, []interface{}, error) {
	numPKeys := len(pkeyColumns)
	args := make([]interface{}, 0, len(keys)*numPKeys)

	for _, key := range keys {
		if len(key.Values) != numPKeys {
			return "", nil, status.Errorf(codes.InvalidArgument, "TODO: invalid keys")
		}

		values, err := convertToDatabaseValues(key, pkeyColumns)
		if err != nil {
			return "", nil, err
		}

		args = append(args, values...)
	}

	// build placeholders for values e.g. (?, ?, ?)
	valuesPlaceholder := "(?" + strings.Repeat(", ?", numPKeys-1) + ")"

	// repeat placeholders for the number of keys e.g. (?, ?, ?), (?, ?, ?), (?, ?, ?)
	valuesExpr := valuesPlaceholder + strings.Repeat(", "+valuesPlaceholder, len(keys)-1)

	// e.g. WHERE (key1, key2, key3) IN ( VALUES (?, ?, ?), (?, ?, ?) )
	whereCondition := fmt.Sprintf("(%s) IN ( VALUES %s )", pkeysName, valuesExpr)

	return whereCondition, args, nil
}

func buildKeyRangeQuery(pkeysName string, pkeyColumns []*Column, keyrange *KeyRange) (string, []interface{}, error) {
	numPKeys := len(pkeyColumns)
	if numPKeys < len(keyrange.start.Values) {
		return "", nil, status.Errorf(codes.InvalidArgument, "TODO: invalid start range key")
	}
	if numPKeys < len(keyrange.end.Values) {
		return "", nil, status.Errorf(codes.InvalidArgument, "TODO: invalid end range key")
	}

	startKeyValues, err := convertToDatabaseValues(keyrange.start, pkeyColumns)
	if err != nil {
		return "", nil, err
	}
	endKeyValues, err := convertToDatabaseValues(keyrange.end, pkeyColumns)
	if err != nil {
		return "", nil, err
	}

	whereClause := make([]string, 0, len(startKeyValues)+len(endKeyValues))
	args := make([]interface{}, 0, len(whereClause))

	var maxLen int
	if len(startKeyValues) > len(endKeyValues) {
		maxLen = len(startKeyValues)
	} else {
		maxLen = len(endKeyValues)
	}
	for i := 0; i < maxLen; i++ {
		pk := QuoteString(pkeyColumns[i].Name())
		if len(startKeyValues) > i && len(endKeyValues) > i && startKeyValues[i] == endKeyValues[i] {
			whereClause = append(whereClause, fmt.Sprintf("%s = ?", pk))
			args = append(args, startKeyValues[i])
			continue
		}
		if len(startKeyValues) > i {
			if keyrange.startClosed {
				whereClause = append(whereClause, fmt.Sprintf("%s >= ?", pk))
			} else {
				whereClause = append(whereClause, fmt.Sprintf("%s > ?", pk))
			}
			args = append(args, startKeyValues[i])
		}
		if len(endKeyValues) > i {
			if keyrange.endClosed {
				whereClause = append(whereClause, fmt.Sprintf("%s <= ?", pk))
			} else {
				whereClause = append(whereClause, fmt.Sprintf("%s < ?", pk))
			}
			args = append(args, endKeyValues[i])
		}
	}

	return fmt.Sprintf("(%s)", strings.Join(whereClause, " AND ")), args, nil
}
