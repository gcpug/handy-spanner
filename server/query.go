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
	"reflect"
	"strconv"
	"strings"

	"github.com/MakeNowJust/memefish/pkg/ast"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type QueryBuilder struct {
	db   *database
	stmt ast.QueryExpr

	views          map[string]*TableView
	allResultItems [][]ResultItem
	resultItems    map[string][]ResultItem
	params         map[string]Value

	args []interface{}

	frees []func()

	unnestViewNum   int
	subqueryViewNum int
}

func BuildQuery(db *database, stmt ast.QueryExpr, params map[string]Value) (string, []interface{}, []ResultItem, error) {
	b := &QueryBuilder{
		db:          db,
		stmt:        stmt,
		views:       make(map[string]*TableView),
		resultItems: make(map[string][]ResultItem),
		params:      params,
	}
	return b.Build()
}

func (b *QueryBuilder) close() {
	for _, free := range b.frees {
		free()
	}
}

func (b *QueryBuilder) Build() (string, []interface{}, []ResultItem, error) {
	defer b.close()

	switch q := b.stmt.(type) {
	case *ast.Select:
		return b.buildSelectQuery(q)
	case *ast.SubQuery:
		return b.buildSubQuery(q)
	case *ast.CompoundQuery:
		return b.buildCompoundQuery(q)
	}

	return "", nil, nil, status.Errorf(codes.Unimplemented, "not unknown expression %T", b.stmt)
}

func (b *QueryBuilder) buildSelectQuery(selectStmt *ast.Select) (string, []interface{}, []ResultItem, error) {
	if len(selectStmt.Results) == 0 {
		return "", nil, nil, status.Errorf(codes.InvalidArgument, "Invalid query")
	}

	var fromClause string
	var fromData []interface{}
	if selectStmt.From != nil {
		_, s, d, err := b.buildQueryTable(selectStmt.From.Source)
		if err != nil {
			return "", nil, nil, err
		}
		fromClause = "FROM " + s
		fromData = d
	}

	resultItems, selectQuery, err := b.buildResultSet(selectStmt.Results)
	if err != nil {
		return "", nil, nil, err
	}

	b.args = append(b.args, fromData...)

	whereClause, err := b.buildQuery(selectStmt)
	if err != nil {
		return "", nil, nil, err
	}

	query := fmt.Sprintf(`SELECT %s %s %s`, selectQuery, fromClause, whereClause)

	return query, b.args, resultItems, nil
}

func (b *QueryBuilder) buildSubQuery(sub *ast.SubQuery) (string, []interface{}, []ResultItem, error) {
	s, data, items, err := BuildQuery(b.db, sub.Query, b.params)
	if err != nil {
		return "", nil, nil, err
	}

	var orderByClause string
	if sub.OrderBy != nil {
		itemsMap := make(map[string][]ResultItem)
		for _, i := range items {
			if i.Name == "" {
				continue
			}
			itemsMap[i.Name] = append(itemsMap[i.Name], i)
		}
		s, data, err := b.buildQueryOrderByClause(sub.OrderBy, itemsMap)
		if err != nil {
			return "", nil, nil, err
		}
		orderByClause = fmt.Sprintf(" ORDER BY %s", s)
		b.args = append(b.args, data...)
	}

	var limitClause string
	if sub.Limit != nil {
		s, data, err := b.buildQueryLimitOffset(sub.Limit)
		if err != nil {
			return "", nil, nil, err
		}
		limitClause = fmt.Sprintf(" LIMIT %s", s)
		b.args = append(b.args, data...)
	}

	return fmt.Sprintf("(%s%s%s)", s, limitClause, orderByClause), data, items, nil
}

func (b *QueryBuilder) buildCompoundQuery(compound *ast.CompoundQuery) (string, []interface{}, []ResultItem, error) {
	var ss []string

	var op string
	var fullOpName string
	switch compound.Op {
	case ast.SetOpUnion:
		if compound.Distinct {
			// distinct is used by default in sqlite
			op = "UNION"
			fullOpName = "UNION DISTINCT"
		} else {
			op = "UNION ALL"
			fullOpName = "UNION ALL"
		}
	case ast.SetOpIntersect:
		if compound.Distinct {
			// distinct is used by default in sqlite
			op = "INTERSECT"
			fullOpName = "INTERSECT DISTINCT"
		} else {
			// sqlite does not support INTERSECT ALL
			// TODO: simulation of INTERSECT ALL
			return "", nil, nil, status.Errorf(codes.Unimplemented, "INTERSECT ALL is not supported yet")
		}
	case ast.SetOpExcept:
		if compound.Distinct {
			// distinct is used by default in sqlite
			op = "EXCEPT"
			fullOpName = "EXCEPT DISTINCT"
		} else {
			// sqlite does not support EXCEPT ALL
			// TODO: simulation of EXCEPT ALL
			return "", nil, nil, status.Errorf(codes.Unimplemented, "EXCEPT ALL is not supported yet")
		}
	}

	s, data, items, err := BuildQuery(b.db, compound.Queries[0], b.params)
	if err != nil {
		return "", nil, nil, err
	}

	// sqlite does not allow to put parentheses around select statement like:
	// (SELECT .. FROM xxx) UNION ALL (SELECT ... FROM yyy)
	//
	// This causes ambiguous LIMIT clause like:
	// SELECT .. FROM xxx UNION ALL SELECT ... FROM yyy LIMIT 2
	//
	// This is possibly interpreted as
	// SELECT .. FROM xxx UNION ALL (SELECT ... FROM yyy LIMIT 2)
	// SELECT .. FROM xxx UNION ALL (SELECT ... FROM yyy) LIMIT 2
	//
	// So use subquery to fix the issue like this:
	// SELECT * FROM (SELECT .. FROM xxx) UNION ALL SELECT * FROM (SELECT ... FROM yyy)
	ss = append(ss, fmt.Sprintf("SELECT * FROM (%s)", s))

	for i, query := range compound.Queries[1:] {
		s, d, items2, err := BuildQuery(b.db, query, b.params)
		if err != nil {
			return "", nil, nil, err
		}

		if len(items) != len(items2) {
			return "", nil, nil, status.Errorf(codes.InvalidArgument,
				"Queries in %s have mismatched column count; query 1 has %d column, query %d has %d columns",
				fullOpName, len(items), i+2, len(items2))
		}

		// check the result items match to the first query's items
		for j := range items {
			if !compareValueType(items[j].ValueType, items2[j].ValueType) {
				return "", nil, nil, status.Errorf(codes.InvalidArgument,
					"Column %d in %s has incompatible types: %s, %s",
					j+1, fullOpName, items[j].ValueType, items2[j].ValueType)
			}
		}

		ss = append(ss, fmt.Sprintf("SELECT * FROM (%s)", s))
		data = append(data, d...)
	}

	var orderByClause string
	if compound.OrderBy != nil {
		itemsMap := make(map[string][]ResultItem)
		for _, i := range items {
			if i.Name == "" {
				continue
			}
			itemsMap[i.Name] = append(itemsMap[i.Name], i)
		}
		s, data, err := b.buildQueryOrderByClause(compound.OrderBy, itemsMap)
		if err != nil {
			return "", nil, nil, err
		}
		orderByClause = fmt.Sprintf(" ORDER BY %s", s)
		b.args = append(b.args, data...)
	}

	var limitClause string
	if compound.Limit != nil {
		s, data, err := b.buildQueryLimitOffset(compound.Limit)
		if err != nil {
			return "", nil, nil, err
		}
		limitClause = fmt.Sprintf(" LIMIT %s", s)
		b.args = append(b.args, data...)
	}

	query := strings.Join(ss, fmt.Sprintf(" %s ", op))
	query = fmt.Sprintf("%s%s%s", query, orderByClause, limitClause)
	return query, data, items, nil
}

func (b *QueryBuilder) buildQueryTable(exp ast.TableExpr) (*TableView, string, []interface{}, error) {
	switch src := exp.(type) {
	case *ast.TableName:
		t, free, err := b.db.readTable(src.Table.Name)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil, "", nil, status.Error(codes.InvalidArgument, err.Error())
			}
			return nil, "", nil, err
		}
		b.frees = append(b.frees, free)
		view := t.TableView()

		var query string
		if src.As == nil {
			query = t.Name
			b.views["."+t.Name] = view
		} else {
			query = fmt.Sprintf("%s AS %s", t.Name, src.As.Alias.Name)
			b.views["."+src.As.Alias.Name] = view
		}
		for key, item := range view.ResultItemsMap {
			b.resultItems[key] = append(b.resultItems[key], item)
		}
		b.allResultItems = append(b.allResultItems, view.ResultItems)

		return view, query, nil, nil
	case *ast.Join:
		var data []interface{}
		view1, q1, d1, err := b.buildQueryTable(src.Left)
		if err != nil {
			return nil, "", nil, fmt.Errorf("left table error %v", err)
		}
		if view1 == nil {
			return nil, "", nil, fmt.Errorf("left table view is nil")
		}

		view2, q2, d2, err := b.buildQueryTable(src.Right)
		if err != nil {
			return nil, "", nil, fmt.Errorf("right table error %v", err)
		}
		if view2 == nil {
			return nil, "", nil, fmt.Errorf("right table view is nil")
		}

		data = append(data, d1...)
		data = append(data, d2...)

		var condition string
		switch cond := src.Cond.(type) {
		case *ast.On:
			switch expr := cond.Expr.(type) {
			case *ast.BinaryExpr:
				s, d, err := b.buildExpr(expr)
				if err != nil {
					return nil, "", nil, wrapExprError(err, expr, "ON")
				}
				condition = fmt.Sprintf("ON %s", s.Raw)
				data = append(data, d...)

			default:
				return nil, "", nil, status.Errorf(codes.Unimplemented, "not supported expression %T for JOIN condition", expr)
			}

		case *ast.Using:
			names := make([]string, len(cond.Idents))
			for i := range cond.Idents {
				name := cond.Idents[i].Name
				if _, ok := view1.ResultItemsMap[name]; !ok {
					return nil, "", nil, newExprErrorf(nil, true, "Column %s in USING clause not found on left side of join", name)
				}
				if _, ok := view2.ResultItemsMap[name]; !ok {
					return nil, "", nil, newExprErrorf(nil, true, "Column %s in USING clause not found on right side of join", name)
				}
				names[i] = name
			}
			condition = fmt.Sprintf("USING (%s)", strings.Join(names, ", "))
		default:
			return nil, "", nil, status.Errorf(codes.Unimplemented, "unknown condition for JOIN: %T", cond)
		}

		// TODO: return joined view
		return nil, fmt.Sprintf("%s %s %s %s", q1, src.Op, q2, condition), data, nil

	case *ast.Unnest:
		return b.buildUnnestView(src)

	case *ast.SubQueryTableExpr:
		query, data, items, err := BuildQuery(b.db, src.Query, b.params)
		if err != nil {
			return nil, "", nil, fmt.Errorf("Subquery error: %v", err)
		}

		itemsMap := make(map[string]ResultItem, len(items))
		for i := range items {
			name := items[i].Name
			if name != "" {
				itemsMap[name] = items[i]
			}
		}
		view := &TableView{
			ResultItems:    items,
			ResultItemsMap: itemsMap,
		}
		b.allResultItems = append(b.allResultItems, view.ResultItems)

		var viewName string
		if src.As == nil {
			viewName = fmt.Sprintf("__SUBQUERY%d", b.subqueryViewNum)
			b.subqueryViewNum++
		} else {
			viewName = src.As.Alias.Name
			b.views["."+viewName] = view
		}

		return view, fmt.Sprintf("(%s) AS %s", query, viewName), data, nil
	case *ast.ParenTableExpr:
		view, q, d, err := b.buildQueryTable(src.Source)
		if err != nil {
			return nil, "", nil, err
		}
		return view, fmt.Sprintf("(%s)", q), d, nil
	default:
		return nil, "", nil, status.Errorf(codes.Unknown, "unknown expression %T for FROM", src)
	}
}

func (b *QueryBuilder) buildUnnestView(src *ast.Unnest) (*TableView, string, []interface{}, error) {
	s, data, err := b.buildUnnestExpr(src.Expr)
	if err != nil {
		return nil, "", nil, wrapExprError(err, src.Expr, "UNNEST")
	}

	var viewName string
	if src.As == nil {
		viewName = fmt.Sprintf("__UNNEST%d", b.unnestViewNum)
		b.unnestViewNum++
	} else {
		viewName = src.As.Alias.Name
	}

	view := &TableView{
		ResultItems: []ResultItem{ResultItem{
			Name:      "",
			ValueType: *s.ValueType.ArrayType,
			Expr: Expr{
				Raw:       fmt.Sprintf("%s.*", viewName),
				ValueType: *s.ValueType.ArrayType,
			},
		}},
		ResultItemsMap: make(map[string]ResultItem),
	}
	b.allResultItems = append(b.allResultItems, view.ResultItems)

	if src.As != nil {
		b.views["."+viewName] = view
	}

	return view, fmt.Sprintf("(%s) AS %s", s.Raw, viewName), data, nil
}

func (b *QueryBuilder) buildResultSet(selectItems []ast.SelectItem) ([]ResultItem, string, error) {
	var data []interface{}
	n := len(selectItems) + len(b.allResultItems)
	items := make([]ResultItem, 0, n)
	exprs := make([]string, 0, len(selectItems))
	for _, item := range selectItems {
		switch i := item.(type) {
		case *ast.Star:
			for i := range b.allResultItems {
				items = append(items, b.allResultItems[i]...)
			}
			exprs = append(exprs, "*")
		case *ast.DotStar:
			switch e := i.Expr.(type) {
			case *ast.Ident:
				view, ok := b.views["."+e.Name]
				if !ok {
					return nil, "", status.Errorf(codes.InvalidArgument, "Unrecognized name: %s", e.Name)
				}
				for _, item := range view.ResultItems {
					items = append(items, ResultItem{
						Name:      item.Name,
						ValueType: item.ValueType,
						Expr: Expr{
							Raw:       fmt.Sprintf("%s.%s", e.Name, item.Expr.Raw),
							ValueType: item.Expr.ValueType,
						},
					})
				}
				exprs = append(exprs, fmt.Sprintf("%s.*", e.Name))
			default:
				return nil, "", status.Errorf(codes.Unimplemented, "unknown expression %T in result set for DotStar", e)
			}

		case *ast.ExprSelectItem:
			var alias string
			switch e := i.Expr.(type) {
			case *ast.Ident:
				alias = e.Name
			case *ast.Path:
				if len(e.Idents) != 2 {
					return nil, "", status.Errorf(codes.Unimplemented, "path expression not supported")
				}
				alias = e.Idents[1].Name
			}
			s, d, err := b.buildExpr(i.Expr)
			if err != nil {
				return nil, "", wrapExprError(err, i.Expr, "Path")
			}

			data = append(data, d...)
			items = append(items, ResultItem{
				Name:      alias,
				ValueType: s.ValueType,
				Expr:      s,
			})
			exprs = append(exprs, s.Raw)

		case *ast.Alias:
			alias := i.As.Alias.Name
			s, d, err := b.buildExpr(i.Expr)
			if err != nil {
				return nil, "", wrapExprError(err, i.Expr, "Alias")
			}

			data = append(data, d...)
			items = append(items, ResultItem{
				Name:      alias,
				ValueType: s.ValueType,
				Expr:      s,
			})
			exprs = append(exprs, s.Raw)

		default:
			return nil, "", status.Errorf(codes.Unimplemented, "not supported %T in result set", item)
		}
	}

	b.args = append(b.args, data...)
	seletQuery := strings.Join(exprs, ", ")

	return items, seletQuery, nil
}

func (b *QueryBuilder) buildQuery(stmt *ast.Select) (string, error) {
	var whereClause string
	if stmt.Where != nil {
		s, data, err := b.buildQueryWhereClause(stmt.Where)
		if err != nil {
			return "", err
		}
		whereClause = s
		b.args = append(b.args, data...)
	}

	var groupByClause string
	if stmt.GroupBy != nil {
		s, data, err := b.buildQueryGroupByClause(stmt.GroupBy)
		if err != nil {
			return "", err
		}
		groupByClause = s
		b.args = append(b.args, data...)
	}

	var havingClause string
	if stmt.Having != nil {
		s, data, err := b.buildExpr(stmt.Having.Expr)
		if err != nil {
			return "", wrapExprError(err, stmt.Having.Expr, "Having")
		}
		havingClause = s.Raw
		b.args = append(b.args, data...)
	}

	var orderByClause string
	if stmt.OrderBy != nil {
		s, data, err := b.buildQueryOrderByClause(stmt.OrderBy, b.resultItems)
		if err != nil {
			return "", err
		}
		orderByClause = s
		b.args = append(b.args, data...)
	}

	var limitClause string
	if stmt.Limit != nil {
		s, data, err := b.buildQueryLimitOffset(stmt.Limit)
		if err != nil {
			return "", err
		}
		limitClause = s
		b.args = append(b.args, data...)
	}

	var query string
	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}
	if groupByClause != "" {
		query += fmt.Sprintf(" GROUP BY %s", groupByClause)
	}
	if havingClause != "" {
		query += fmt.Sprintf(" HAVING %s", havingClause)
	}
	if len(orderByClause) != 0 {
		query += fmt.Sprintf(" ORDER BY %s", orderByClause)
	}
	if limitClause != "" {
		query += fmt.Sprintf(" LIMIT %s", limitClause)
	}

	return query, nil
}

func (b *QueryBuilder) buildQueryWhereClause(where *ast.Where) (string, []interface{}, error) {
	s, data, err := b.buildExpr(where.Expr)
	if err != nil {
		return "", nil, wrapExprError(err, where.Expr, "Building WHERE clause error")
	}
	return s.Raw, data, nil
}

func (b *QueryBuilder) buildQueryGroupByClause(groupby *ast.GroupBy) (string, []interface{}, error) {
	var groupByClause []string
	var data []interface{}

	for _, expr := range groupby.Exprs {
		s, d, err := b.buildExpr(expr)
		if err != nil {
			return "", nil, wrapExprError(err, expr, "Building GROUP BY error")
		}
		groupByClause = append(groupByClause, s.Raw)
		data = append(data, d...)
	}

	return strings.Join(groupByClause, ", "), data, nil
}

func (b *QueryBuilder) buildQueryOrderByClause(orderby *ast.OrderBy, resultItemsMap map[string][]ResultItem) (string, []interface{}, error) {
	var orderByClause []string
	var data []interface{}

	for _, item := range orderby.Items {
		var expr Expr
		switch e := item.Expr.(type) {
		case *ast.Ident:
			i, ok := resultItemsMap[e.Name]
			if !ok {
				return "", nil, newExprErrorf(e, true, "Unrecognized name: %s", e.Name)
			}
			expr = i[0].Expr
		}

		collate := ""
		if item.Collate != nil {
			switch v := item.Collate.Value.(type) {
			case *ast.Param:
				vv, ok := b.params[v.Name]
				if !ok {
					return "", nil, fmt.Errorf("params not found: %v", v.Name)
				}
				collate = "?"
				data = append(data, vv)
			case *ast.StringLiteral:
				collate = v.Value
			}
		}
		orderByClause = append(orderByClause, fmt.Sprintf("%s %s %s", expr.Raw, collate, item.Dir))
	}

	return strings.Join(orderByClause, ", "), data, nil
}

func (b *QueryBuilder) buildQueryLimitOffset(limit *ast.Limit) (string, []interface{}, error) {
	e, data, err := b.buildIntValue(limit.Count)
	if err != nil {
		return "", nil, err
	}
	limitClause := e.Raw

	if limit.Offset != nil {
		e, d, err := b.buildIntValue(limit.Offset.Value)
		if err != nil {
			return "", nil, err
		}
		data = append(data, d...)
		limitClause += fmt.Sprintf(" OFFSET %s", e.Raw)
	}

	return limitClause, data, nil
}

func (b *QueryBuilder) buildIntValue(intValue ast.IntValue) (Expr, []interface{}, error) {
	switch iv := intValue.(type) {
	case *ast.Param:
		v, ok := b.params[iv.Name]
		if !ok {
			return NullExpr, nil, fmt.Errorf("params not found: %v", iv.Name)
		}
		return Expr{
			ValueType: v.Type,
			Raw:       "?",
		}, []interface{}{v.Data}, nil
	case *ast.IntLiteral:
		n, err := strconv.ParseInt(iv.Value, iv.Base, 64)
		if err != nil {
			return NullExpr, nil, fmt.Errorf("unexpected format %q as int64: %v", iv.Value, err)
		}
		return Expr{
			ValueType: ValueType{Code: TCInt64},
			Raw:       strconv.FormatInt(n, 10),
		}, nil, nil

	case *ast.CastIntValue:
		return NullExpr, nil, fmt.Errorf("CAST is not supported yet")
	default:
		return NullExpr, nil, fmt.Errorf("unknown LIMIT type")
	}
}

func (b *QueryBuilder) buildInCondition(cond ast.InCondition) (Expr, []interface{}, error) {
	switch c := cond.(type) {
	case *ast.ValuesInCondition:
		var ss []string
		var data []interface{}
		for _, e := range c.Exprs {
			s, d, err := b.buildExpr(e)
			if err != nil {
				return NullExpr, nil, wrapExprError(err, e, "IN condition")
			}
			ss = append(ss, s.Raw)
			data = append(data, d...)
		}
		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       "(" + strings.Join(ss, ", ") + ")",
		}, data, nil

	case *ast.SubQueryInCondition:
		query, data, items, err := BuildQuery(b.db, c.Query, b.params)
		if err != nil {
			return NullExpr, nil, newExprErrorf(nil, false, "BuildQuery error for SubqueryInCondition: %v", err)
		}
		if len(items) != 1 {
			return NullExpr, nil, newExprErrorf(nil, true, "Subquery of type IN must have only one output column")
		}
		return Expr{
			ValueType: items[0].ValueType, // inherit ValueType from result item
			Raw:       fmt.Sprintf("(%s)", query),
		}, data, nil

	case *ast.UnnestInCondition:
		s, d, err := b.buildUnnestExpr(c.Expr)
		if err != nil {
			return NullExpr, nil, err
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("(%s)", s.Raw),
		}, d, nil
	default:
		return NullExpr, nil, fmt.Errorf("not supported InCondition %T", c)
	}
}

func (b *QueryBuilder) buildUnnestExpr(expr ast.Expr) (Expr, []interface{}, error) {
	switch e := expr.(type) {
	case *ast.Param:
		v, ok := b.params[e.Name]
		if !ok {
			return NullExpr, nil, fmt.Errorf("params not found: %v", e.Name)
		}
		return b.unnestValue(v)
	case *ast.ArrayLiteral:
		// TODO: check all of Array values are the same type

		var ss []string
		var data []interface{}
		var vt *ValueType
		for _, v := range e.Values {
			s, d, err := b.buildExpr(v)
			if err != nil {
				return NullExpr, nil, wrapExprError(err, v, "Array Literal")
			}
			ss = append(ss, fmt.Sprintf("(%s)", s.Raw))
			data = append(data, d...)

			if vt == nil {
				vt = &s.ValueType
			}
		}

		return Expr{
			ValueType: ValueType{
				Code:      TCArray,
				ArrayType: vt, // TODO: nil?
			},
			Raw: "VALUES " + strings.Join(ss, ", "),
		}, data, nil
	}

	return NullExpr, nil, fmt.Errorf("unexpected expression type for UNNEST: %T", expr)
}

func (b *QueryBuilder) unnestValue(v Value) (Expr, []interface{}, error) {
	errMsg := "Second argument of IN UNNEST must be an array but was %s"
	switch v.Data.(type) {
	case nil:
		// spanner returns Unknown
		return NullExpr, nil, status.Errorf(codes.Unknown, "failed to bind query parameter")
	case bool:
		return NullExpr, nil, status.Errorf(codes.InvalidArgument, errMsg, "BOOL")
	case int64:
		return NullExpr, nil, status.Errorf(codes.InvalidArgument, errMsg, "INT64")
	case float64:
		return NullExpr, nil, status.Errorf(codes.InvalidArgument, errMsg, "FLOAT64")
	case string:
		return NullExpr, nil, status.Errorf(codes.InvalidArgument, errMsg, "STRING")
	case []byte:
		return NullExpr, nil, status.Errorf(codes.InvalidArgument, errMsg, "BYTES")
	// TODO: timestamp, date
	case []bool, []int64, []float64, []string, [][]byte:
		vv := reflect.ValueOf(v.Data)
		n := vv.Len()
		var placeholders string
		if n == 1 {
			placeholders = "VALUES (?)"
		} else if n > 1 {
			placeholders = "VALUES (?)" + strings.Repeat(", (?)", n-1)
		}
		args := make([]interface{}, n)
		for i := 0; i < n; i++ {
			args[i] = vv.Index(i).Interface()
		}
		return Expr{
			ValueType: v.Type, // TODO: check correct type or not
			Raw:       placeholders,
		}, args, nil
	}

	return NullExpr, nil, fmt.Errorf("unexpected parameter type for UNNEST: %T", v)
}

func (b *QueryBuilder) buildExpr(expr ast.Expr) (Expr, []interface{}, error) {
	switch e := expr.(type) {
	case *ast.UnaryExpr:
		s, data, err := b.buildExpr(e.Expr)
		if err != nil {
			return NullExpr, nil, wrapExprError(err, expr, "Unary")
		}

		return Expr{
			ValueType: s.ValueType,
			Raw:       fmt.Sprintf("%s %s", e.Op, s.Raw),
		}, data, nil

	case *ast.BinaryExpr:
		left, ldata, lerr := b.buildExpr(e.Left)
		right, rdata, rerr := b.buildExpr(e.Right)

		if lerr != nil {
			return NullExpr, nil, wrapExprError(lerr, expr, "Left")
		}
		if rerr != nil {
			return NullExpr, nil, wrapExprError(rerr, expr, "Right")
		}

		var data []interface{}
		data = append(data, ldata...)
		data = append(data, rdata...)

		var vt ValueType
		switch e.Op {
		case ast.OpOr, ast.OpAnd, ast.OpEqual, ast.OpNotEqual, ast.OpLess, ast.OpGreater, ast.OpLessEqual, ast.OpGreaterEqual, ast.OpLike, ast.OpNotLike:
			vt = ValueType{
				Code: TCBool,
			}
		case ast.OpBitOr, ast.OpBitXor, ast.OpBitAnd, ast.OpBitLeftShift, ast.OpBitRightShift:
			vt = ValueType{
				Code: TCInt64, // TODO
			}
		case ast.OpAdd, ast.OpSub, ast.OpMul:
			code := TCInt64
			if left.ValueType.Code == TCFloat64 || right.ValueType.Code == TCFloat64 {
				code = TCFloat64
			}
			vt = ValueType{
				Code: code,
			}
		case ast.OpDiv:
			left.Raw = fmt.Sprintf("CAST(%s AS REAL)", left.Raw)
			vt = ValueType{
				Code: TCFloat64,
			}
		default:
			return NullExpr, nil, fmt.Errorf("%T: unknown op %v", e, e.Op)
		}

		raw := fmt.Sprintf("%s %s %s", left.Raw, e.Op, right.Raw)
		if e.Op == ast.OpBitXor {
			raw = fmt.Sprintf("(%s | %s) - (%s & %s)", left.Raw, right.Raw, left.Raw, right.Raw)
		}

		return Expr{
			ValueType: vt,
			Raw:       raw,
		}, data, nil

	case *ast.InExpr:
		left, ldata, lerr := b.buildExpr(e.Left)
		right, rdata, rerr := b.buildInCondition(e.Right)
		if lerr != nil {
			return NullExpr, nil, wrapExprError(lerr, expr, "Left")
		}
		if rerr != nil {
			return NullExpr, nil, wrapExprError(rerr, expr, "Right")
		}

		var data []interface{}
		data = append(data, ldata...)
		data = append(data, rdata...)

		op := "IN"
		if e.Not {
			op = "NOT IN"
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("%s %s %s", left.Raw, op, right.Raw),
		}, data, nil

	case *ast.BetweenExpr:
		left, ldata, lerr := b.buildExpr(e.Left)
		rstart, rsdata, rserr := b.buildExpr(e.RightStart)
		rend, redata, reerr := b.buildExpr(e.RightEnd)
		if lerr != nil {
			return NullExpr, nil, wrapExprError(lerr, expr, "Left")
		}
		if rserr != nil {
			return NullExpr, nil, wrapExprError(rserr, expr, "RightStart")
		}
		if reerr != nil {
			return NullExpr, nil, wrapExprError(reerr, expr, "RightEnd")
		}

		var data []interface{}
		data = append(data, ldata...)
		data = append(data, rsdata...)
		data = append(data, redata...)

		op := "BETWEEN"
		if e.Not {
			op = "NOT BETWEEN"
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("%s %s %s AND %s", left.Raw, op, rstart.Raw, rend.Raw),
		}, data, nil

	case *ast.SelectorExpr:
		return NullExpr, nil, newExprErrorf(expr, false, "Selector not supported yet")

	case *ast.IndexExpr:
		return NullExpr, nil, newExprErrorf(expr, false, "Index not supported yet")

	case *ast.IsNullExpr:
		left, ldata, lerr := b.buildExpr(e.Left)
		if lerr != nil {
			return NullExpr, nil, wrapExprError(lerr, expr, "Left")
		}

		var data []interface{}
		data = append(data, ldata...)

		op := "IS NULL"
		if e.Not {
			op = "IS NOT NULL"
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("%s %s", left.Raw, op),
		}, data, nil

	case *ast.IsBoolExpr:
		left, ldata, lerr := b.buildExpr(e.Left)
		if lerr != nil {
			return NullExpr, nil, wrapExprError(lerr, expr, "Left")
		}

		var data []interface{}
		data = append(data, ldata...)

		op := "IS"
		if e.Not {
			op = "IS NOT"
		}
		b := "TRUE"
		if e.Right {
			b = "FALSE"
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("%s %s %s", left.Raw, op, b),
		}, data, nil

	case *ast.CallExpr:
		name := strings.ToUpper(e.Func.Name)
		fn, ok := customFunctions[name]
		if !ok {
			return NullExpr, nil, newExprErrorf(expr, false, "unsupported CALL function: %s", name)
		}

		// when fn.NArgs < 0, args is variadic
		if fn.NArgs >= 0 && fn.NArgs != len(e.Args) {
			return NullExpr, nil, newExprErrorf(expr, true, "%s requires %d arguments", name, fn.NArgs)
		}

		var data []interface{}
		var ss []string
		var vts []ValueType
		for i := range e.Args {
			s, d, err := b.buildExpr(e.Args[i].Expr)
			if err != nil {
				return NullExpr, nil, wrapExprError(err, expr, "Args")
			}

			data = append(data, d...)
			ss = append(ss, s.Raw)
			vts = append(vts, s.ValueType)
		}

		if ok := fn.ArgTypes(vts); !ok {
			return NullExpr, nil, newExprErrorf(expr, true, "arguments does not match for %s", name)
		}

		var distinct string
		if e.Distinct {
			distinct = "DISTINCT "
		}

		return Expr{
			ValueType: fn.ReturnType(vts),
			Raw:       fmt.Sprintf(`%s(%s%s)`, name, distinct, strings.Join(ss, ", ")),
		}, data, nil

	case *ast.CountStarExpr:
		return Expr{
			ValueType: ValueType{Code: TCInt64},
			Raw:       "COUNT(*)",
		}, nil, nil

	case *ast.CastExpr:
		return NullExpr, nil, newExprErrorf(expr, false, "Cast not supported yet")

	case *ast.ExtractExpr:
		return NullExpr, nil, newExprErrorf(expr, false, "Extract not supported yet")

	case *ast.CaseExpr:
		return NullExpr, nil, newExprErrorf(expr, false, "Case not supported yet")

	case *ast.ParenExpr:
		s, data, err := b.buildExpr(e.Expr)
		if err != nil {
			return NullExpr, nil, wrapExprError(err, expr, "Paren")
		}

		return Expr{
			ValueType: s.ValueType,
			Raw:       fmt.Sprintf("(%s)", s.Raw),
		}, data, nil

	case *ast.ScalarSubQuery:
		switch q := e.Query.(type) {
		case *ast.Select:
			query, data, items, err := BuildQuery(b.db, q, b.params)
			if err != nil {
				return NullExpr, nil, newExprErrorf(expr, false, "BuildQuery error for ScalarSubquery: %v", err)
			}
			if len(items) != 1 {
				return NullExpr, nil, newExprErrorf(expr, true, "Scalar subquery cannot have more than one column unless using SELECT AS STRUCT to build STRUCT values")
			}
			return Expr{
				ValueType: items[0].ValueType, // inherit ValueType from result item
				Raw:       fmt.Sprintf("(%s)", query),
			}, data, nil

		case *ast.SubQuery:
			return NullExpr, nil, newExprErrorf(expr, false, "SubQuery for ScalarSubquery not supported yet")
		case *ast.CompoundQuery:
			return NullExpr, nil, newExprErrorf(expr, false, "CompoundQuery for ScalarSubquery not supported yet")
		}
		return NullExpr, nil, newExprErrorf(expr, false, "ScalarSubquery not supported yet")

	case *ast.ArraySubQuery:
		return NullExpr, nil, newExprErrorf(expr, false, "ArraySubquery not supported yet")

	case *ast.ExistsSubQuery:
		switch q := e.Query.(type) {
		case *ast.Select:
			query, data, _, err := BuildQuery(b.db, q, b.params)
			if err != nil {
				return NullExpr, nil, newExprErrorf(expr, false, "BuildQuery error for %T: %v", err, e)
			}
			return Expr{
				ValueType: ValueType{
					Code: TCBool,
				},
				Raw: fmt.Sprintf("EXISTS(%s)", query),
			}, data, nil

		case *ast.SubQuery:
			return NullExpr, nil, newExprErrorf(expr, false, "SubQuery for ScalarSubquery not supported yet")
		case *ast.CompoundQuery:
			return NullExpr, nil, newExprErrorf(expr, false, "CompoundQuery for ScalarSubquery not supported yet")
		}
		return NullExpr, nil, newExprErrorf(expr, false, "ExistsSubquery not supported yet")

	case *ast.ArrayLiteral:
		return NullExpr, nil, newExprErrorf(expr, false, "ArrayLiteral not supported yet")

	case *ast.StructLiteral:
		return NullExpr, nil, newExprErrorf(expr, false, "StructLiteral not supported yet")

	case *ast.NullLiteral:
		return Expr{
			ValueType: ValueType{Code: TCString}, // TODO
			Raw:       "NULL",
		}, nil, nil

	case *ast.BoolLiteral:
		if e.Value {
			return Expr{
				ValueType: ValueType{Code: TCBool},
				Raw:       "TRUE",
			}, nil, nil
		}
		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       "FALSE",
		}, nil, nil

	case *ast.FloatLiteral:
		return Expr{
			ValueType: ValueType{Code: TCFloat64},
			Raw:       e.Value,
		}, nil, nil

	case *ast.IntLiteral:
		n, err := strconv.ParseInt(e.Value, 0, 64)
		if err != nil {
			return NullExpr, nil, newExprErrorf(expr, false, "unexpected format %q as int64: %v", e.Value, err)
		}
		return Expr{
			ValueType: ValueType{Code: TCInt64},
			Raw:       strconv.FormatInt(n, 10),
		}, nil, nil

	case *ast.StringLiteral:
		return Expr{
			ValueType: ValueType{Code: TCString},
			Raw:       fmt.Sprintf("%q", e.Value),
		}, nil, nil

	case *ast.BytesLiteral:
		return NullExpr, nil, newExprErrorf(expr, false, "BytesLiteral not supported yet")

	case *ast.DateLiteral:
		return NullExpr, nil, newExprErrorf(expr, false, "DateLiteral not supported yet")

	case *ast.TimestampLiteral:
		return NullExpr, nil, newExprErrorf(expr, false, "TimestampLiteral not supported yet")

	case *ast.Param:
		v, ok := b.params[e.Name]
		if !ok {
			return NullExpr, nil, newExprErrorf(expr, true, "params not found: %s", e.Name)
		}
		return Expr{
			ValueType: v.Type,
			Raw:       "?",
		}, []interface{}{v.Data}, nil
	case *ast.Ident:
		item, ok := b.resultItems[e.Name]
		if !ok {
			return NullExpr, nil, newExprErrorf(expr, true, "Unrecognized name: %s", e.Name)
		}
		return item[0].Expr, nil, nil
	case *ast.Path:
		if len(e.Idents) != 2 {
			return NullExpr, nil, newExprErrorf(expr, false, "path expression not supported")
		}
		first := e.Idents[0]
		second := e.Idents[1]

		tbl, ok := b.views["."+first.Name]
		if !ok {
			return NullExpr, nil, newExprErrorf(expr, true, "Unrecognized name: %s", first.Name)
		}
		item, ok := tbl.ResultItemsMap[second.Name]
		if !ok {
			return NullExpr, nil, newExprErrorf(expr, true, "Name %s not found inside %s", second.Name, first.Name)
		}
		return Expr{
			ValueType: item.Expr.ValueType,
			Raw:       fmt.Sprintf("%s.%s", first.Name, item.Name),
		}, nil, nil
	default:
		return NullExpr, nil, newExprErrorf(expr, false, "unknown expression")
	}
}

type exprError struct {
	expr ast.Expr
	msg  string

	invalid bool
}

func (e *exprError) Error() string {
	return fmt.Sprintf("%T: %s", e.expr, e.msg)
}

func (e exprError) GRPCStatus() *status.Status {
	code := codes.Unknown
	if e.invalid {
		code = codes.InvalidArgument
	}
	return status.New(code, e.msg)
}

func newExprErrorf(expr ast.Expr, invalid bool, format string, a ...interface{}) error {
	return &exprError{
		expr:    expr,
		msg:     fmt.Sprintf(format, a...),
		invalid: invalid,
	}
}

func wrapExprError(err error, expr ast.Expr, msg string) error {
	exprErr, ok := err.(*exprError)
	if !ok {
		return fmt.Errorf("unknown error in wrapExprError: %v", err)
	}

	// if error is invalid it is invalig argument, so return it as is
	if exprErr.invalid {
		return err
	}

	return newExprErrorf(expr, false, "%s, %s", msg, err.Error())
}
