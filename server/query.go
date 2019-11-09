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
	"time"

	"github.com/MakeNowJust/memefish/pkg/ast"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var defaultTimeZone = "America/Los_Angeles"
var parseLocation *time.Location

func init() {
	loc, err := time.LoadLocation(defaultTimeZone)
	if err != nil {
		panic(err)
	}
	parseLocation = loc
}

type QueryBuilder struct {
	db   *database
	stmt ast.QueryExpr

	views    map[string]*TableView
	rootView *TableView
	params   map[string]Value

	args []interface{}

	frees []func()

	unnestViewNum   int
	subqueryViewNum int

	// forceColumnAlias is used only for array subquery.
	// array subquery requires column name for the result item.
	// When forceColumnAlias is specified, random column names is used if the item does not have name.
	forceColumnAlias bool
}

func BuildQuery(db *database, stmt ast.QueryExpr, params map[string]Value, forceColumnAlias bool) (string, []interface{}, []ResultItem, error) {
	b := &QueryBuilder{
		db:               db,
		stmt:             stmt,
		views:            make(map[string]*TableView),
		params:           params,
		forceColumnAlias: forceColumnAlias,
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

func (b *QueryBuilder) registerTableAlias(view *TableView, name string) error {
	if _, ok := b.views[name]; ok {
		return newExprErrorf(nil, true, "Duplicate table alias a in the same FROM clause")
	}
	b.views[name] = view
	return nil
}

func (b *QueryBuilder) buildSelectQuery(selectStmt *ast.Select) (string, []interface{}, []ResultItem, error) {
	if len(selectStmt.Results) == 0 {
		return "", nil, nil, status.Errorf(codes.InvalidArgument, "Invalid query")
	}

	var fromClause string
	var fromData []interface{}
	if selectStmt.From != nil {
		view, s, d, err := b.buildQueryTable(selectStmt.From.Source)
		if err != nil {
			return "", nil, nil, err
		}
		fromClause = "FROM " + s
		fromData = d
		b.rootView = view
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

	if selectStmt.Distinct {
		selectQuery = "DISTINCT " + selectQuery
	}

	query := fmt.Sprintf(`SELECT %s %s %s`, selectQuery, fromClause, whereClause)

	var originalNames []string
	if b.forceColumnAlias || selectStmt.AsStruct {
		newResultItems := make([]ResultItem, len(resultItems))
		names := make([]string, len(resultItems))
		originalNames = make([]string, len(resultItems))
		for i := range resultItems {
			name := fmt.Sprintf("___column%d", i)
			names[i] = name
			originalNames[i] = resultItems[i].Name
			newResultItems[i] = ResultItem{
				Name:      name,
				ValueType: resultItems[i].ValueType,
				Expr: Expr{
					Raw:       name,
					ValueType: resultItems[i].ValueType,
				},
			}
		}
		query = fmt.Sprintf(`WITH ___CTE(%s) as (%s) select * from ___CTE`, strings.Join(names, ", "), query)
		resultItems = newResultItems
	}

	if selectStmt.AsStruct {
		values := make([]string, len(resultItems))
		quotedNames := make([]string, len(resultItems))
		vts := make([]*ValueType, len(resultItems))
		for i := range resultItems {
			if resultItems[i].ValueType.Code == TCArray {
				// column with JSON type needs converting to JSON explictly when reading from table
				// otherwise it is parsed as string
				// TODO: do this in referring timing
				values[i] = fmt.Sprintf("JSON(%s)", resultItems[i].Name)
			} else {
				values[i] = resultItems[i].Name
			}
			quotedNames[i] = fmt.Sprintf("'%s'", originalNames[i])
			vts[i] = &resultItems[i].ValueType
		}

		vt := ValueType{
			Code: TCStruct,
			StructType: &StructType{
				FieldNames: originalNames,
				FieldTypes: vts,
			},
		}

		result := ResultItem{
			Name:      "",
			ValueType: vt,
			Expr: Expr{
				Raw:       "___AsStruct",
				ValueType: vt,
			},
		}

		namesObj := fmt.Sprintf("JSON_ARRAY(%s)", strings.Join(quotedNames, ", "))
		valuesObj := fmt.Sprintf("JSON_ARRAY(%s)", strings.Join(values, ", "))
		query = fmt.Sprintf(`SELECT JSON_OBJECT("keys", %s, "values", %s) AS ___AsStruct FROM (%s)`, namesObj, valuesObj, query)
		resultItems = []ResultItem{result}
	}

	return query, b.args, resultItems, nil
}

func (b *QueryBuilder) buildSubQuery(sub *ast.SubQuery) (string, []interface{}, []ResultItem, error) {
	s, data, items, err := BuildQuery(b.db, sub.Query, b.params, b.forceColumnAlias)
	if err != nil {
		return "", nil, nil, err
	}

	var orderByClause string
	if sub.OrderBy != nil {
		view := createTableViewFromItems(items, nil)
		s, data, err := b.buildQueryOrderByClause(sub.OrderBy, view)
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

	s, data, items, err := BuildQuery(b.db, compound.Queries[0], b.params, b.forceColumnAlias)
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
		s, d, items2, err := BuildQuery(b.db, query, b.params, false)
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
		view := createTableViewFromItems(items, nil)
		s, data, err := b.buildQueryOrderByClause(compound.OrderBy, view)
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
			if err := b.registerTableAlias(view, t.Name); err != nil {
				return nil, "", nil, err
			}
		} else {
			query = fmt.Sprintf("%s AS %s", t.Name, src.As.Alias.Name)
			if err := b.registerTableAlias(view, src.As.Alias.Name); err != nil {
				return nil, "", nil, err
			}
		}

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
				ex, err := b.buildExpr(expr)
				if err != nil {
					return nil, "", nil, wrapExprError(err, expr, "ON")
				}
				condition = fmt.Sprintf("ON %s", ex.Raw)
				data = append(data, ex.Args...)

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
		}

		newView := createTableViewFromItems(view1.AllItems(), view2.AllItems())
		return newView, fmt.Sprintf("%s %s %s %s", q1, src.Op, q2, condition), data, nil

	case *ast.Unnest:
		return b.buildUnnestView(src)

	case *ast.SubQueryTableExpr:
		query, data, items, err := BuildQuery(b.db, src.Query, b.params, false)
		if err != nil {
			return nil, "", nil, fmt.Errorf("Subquery error: %v", err)
		}

		view := createTableViewFromItems(items, nil)

		var viewName string
		if src.As == nil {
			viewName = fmt.Sprintf("__SUBQUERY%d", b.subqueryViewNum)
			b.subqueryViewNum++
		} else {
			viewName = src.As.Alias.Name
			if err := b.registerTableAlias(view, viewName); err != nil {
				return nil, "", nil, err
			}
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

// buildUnnestView creates a Tableview from an UNNEST() expression.
//
// buildUnnestExpr creates a table like `VALUES (a), (b), (c)`, but the columns are unnamed.
// If aliases for unnested values or offsets are used, they can be specified by the name.
// e.g. SELECT MAX(x), MAX(y) FROM UNNEST() AS x OFFSET WITH AS y
//
// See the doc comment of buildUnnestExpr about the generated query.
func (b *QueryBuilder) buildUnnestView(src *ast.Unnest) (*TableView, string, []interface{}, error) {
	var offset bool
	var offsetAlias string
	if src.WithOffset != nil {
		offset = true
		if src.WithOffset.As != nil {
			offsetAlias = src.WithOffset.As.Alias.Name
		}
	}

	s, err := b.buildUnnestExpr(src.Expr, offset, true)
	if err != nil {
		return nil, "", nil, wrapExprError(err, src.Expr, "UNNEST")
	}

	viewName := fmt.Sprintf("__UNNEST%d", b.unnestViewNum)
	b.unnestViewNum++

	// If alias is specified, it names unnested column
	itemName := ""
	if src.As != nil {
		itemName = src.As.Alias.Name
	}

	items := []ResultItem{
		{
			Name:      itemName,
			ValueType: *s.ValueType.ArrayType,
			Expr: Expr{
				Raw:       fmt.Sprintf("%s.value", viewName), // implicitly named column1
				ValueType: *s.ValueType.ArrayType,
			},
		},
	}

	if offset {
		items = append(items, ResultItem{
			Name:      offsetAlias,
			ValueType: ValueType{Code: TCInt64},
			Expr: Expr{
				Raw:       fmt.Sprintf("%s.offset", viewName),
				ValueType: ValueType{Code: TCInt64},
			},
		})
	}

	view := createTableViewFromItems(items, nil)
	return view, fmt.Sprintf("(%s) AS %s", s.Raw, viewName), s.Args, nil
}

func (b *QueryBuilder) buildResultSet(selectItems []ast.SelectItem) ([]ResultItem, string, error) {
	var data []interface{}
	n := len(selectItems)
	if b.rootView != nil {
		n += len(b.rootView.ResultItems)
	}
	items := make([]ResultItem, 0, n)
	exprs := make([]string, 0, len(selectItems))
	for _, item := range selectItems {
		switch i := item.(type) {
		case *ast.Star:
			if b.rootView == nil {
				return nil, "", status.Errorf(codes.InvalidArgument, `SELECT * must have a FROM clause`)
			}
			items = append(items, b.rootView.AllItems()...)
			exprs = append(exprs, "*")
		case *ast.DotStar:
			ex, err := b.buildExpr(i.Expr)
			if err != nil {
				return nil, "", wrapExprError(err, i.Expr, "DotStar")
			}
			data = append(data, ex.Args...)

			if ex.ValueType.Code != TCStruct {
				return nil, "", status.Errorf(codes.InvalidArgument, "Dot-star is not supported for type %s", ex.ValueType)

			}
			st := ex.ValueType.StructType

			items = append(items, st.AllItems()...)
			if st.IsTable {
				exprs = append(exprs, fmt.Sprintf("%s.*", ex.Raw))
			} else {
				n := len(st.FieldTypes)
				for i := 0; i < n; i++ {
					exprs = append(exprs, fmt.Sprintf("JSON_EXTRACT(%s, '$.values[%d]')", ex.Raw, i))
				}
			}

		case *ast.ExprSelectItem:
			var alias string
			switch e := i.Expr.(type) {
			case *ast.Ident:
				alias = e.Name
			case *ast.Path:
				alias = e.Idents[len(e.Idents)-1].Name
			}
			ex, err := b.buildExpr(i.Expr)
			if err != nil {
				return nil, "", wrapExprError(err, i.Expr, "Path")
			}

			data = append(data, ex.Args...)
			items = append(items, ResultItem{
				Name:      alias,
				ValueType: ex.ValueType,
				Expr: Expr{
					// This is a trick. This Expr is referred as subquery.
					// At the reference, it should be just referred as alias name instead of s.Raw.
					Raw:       alias,
					ValueType: ex.ValueType,
				},
			})

			exprs = append(exprs, ex.Raw)

		case *ast.Alias:
			alias := i.As.Alias.Name
			ex, err := b.buildExpr(i.Expr)
			if err != nil {
				return nil, "", wrapExprError(err, i.Expr, "Alias")
			}
			data = append(data, ex.Args...)
			items = append(items, ResultItem{
				Name:      alias,
				ValueType: ex.ValueType,
				Expr: Expr{
					// This is a trick. This Expr is referred as subquery.
					// At the reference, it should be just referred as alias name instead of s.Raw.
					Raw:       alias,
					ValueType: ex.ValueType,
				},
			})
			exprs = append(exprs, fmt.Sprintf("%s AS %s", ex.Raw, alias))

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
		ex, err := b.buildExpr(stmt.Having.Expr)
		if err != nil {
			return "", wrapExprError(err, stmt.Having.Expr, "Having")
		}
		havingClause = ex.Raw
		b.args = append(b.args, ex.Args...)
	}

	var orderByClause string
	if stmt.OrderBy != nil {
		s, data, err := b.buildQueryOrderByClause(stmt.OrderBy, b.rootView)
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
	ex, err := b.buildExpr(where.Expr)
	if err != nil {
		return "", nil, wrapExprError(err, where.Expr, "Building WHERE clause error")
	}
	return ex.Raw, ex.Args, nil
}

func (b *QueryBuilder) buildQueryGroupByClause(groupby *ast.GroupBy) (string, []interface{}, error) {
	var groupByClause []string
	var args []interface{}

	for _, expr := range groupby.Exprs {
		ex, err := b.buildExpr(expr)
		if err != nil {
			return "", nil, wrapExprError(err, expr, "Building GROUP BY error")
		}
		groupByClause = append(groupByClause, ex.Raw)
		args = append(args, ex.Args...)
	}

	return strings.Join(groupByClause, ", "), args, nil
}

func (b *QueryBuilder) buildQueryOrderByClause(orderby *ast.OrderBy, view *TableView) (string, []interface{}, error) {
	var orderByClause []string
	var data []interface{}

	for _, item := range orderby.Items {
		// TODO: use b.buildExpr()
		var expr Expr
		switch e := item.Expr.(type) {
		case *ast.Ident:
			i, ambiguous, notfound := view.Get(e.Name)
			if notfound {
				return "", nil, newExprErrorf(nil, true, "Unrecognized name: %s", e.Name)
			}
			if ambiguous {
				return "", nil, newExprErrorf(nil, true, "Column name %s is ambiguous", e.Name)
			}
			expr = i.Expr
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
	var data []interface{}
	e, err := b.buildIntValue(limit.Count, "LIMIT")
	if err != nil {
		return "", nil, err
	}
	limitClause := e.Raw
	data = append(data, e.Args...)

	if limit.Offset != nil {
		e, err := b.buildIntValue(limit.Offset.Value, "OFFSET")
		if err != nil {
			return "", nil, err
		}
		data = append(data, e.Args...)
		limitClause += fmt.Sprintf(" OFFSET %s", e.Raw)
	}

	return limitClause, data, nil
}

func (b *QueryBuilder) buildIntValue(intValue ast.IntValue, caller string) (Expr, error) {
	switch iv := intValue.(type) {
	case *ast.Param:
		ex, err := b.buildExpr(iv)
		if err != nil {
			return NullExpr, wrapExprError(err, nil, "buildIntValue")
		}
		if ex.ValueType.Code != TCInt64 {
			return NullExpr, newExprErrorf(nil, true, "%s expects an integer literal or parameter", caller)
		}
		return ex, nil

	case *ast.IntLiteral:
		ex, err := b.buildExpr(iv)
		if err != nil {
			return NullExpr, wrapExprError(err, nil, "buildIntValue")
		}
		if ex.ValueType.Code != TCInt64 {
			return NullExpr, newExprErrorf(nil, true, "%s expects an integer literal or parameter", caller)
		}
		return ex, nil

	case *ast.CastIntValue:
		// CAST expression can be used but it seems not working actually.
		return NullExpr, newExprErrorf(nil, true, "handy-spanner: CAST in Limit/Offset seems not working in Spanner")
	default:
		return NullExpr, fmt.Errorf("unknown LIMIT type")
	}
}

func (b *QueryBuilder) buildInCondition(cond ast.InCondition) (Expr, error) {
	switch c := cond.(type) {
	case *ast.ValuesInCondition:
		var ss []string
		var args []interface{}
		for _, e := range c.Exprs {
			ex, err := b.buildExpr(e)
			if err != nil {
				return NullExpr, wrapExprError(err, e, "IN condition")
			}
			ss = append(ss, ex.Raw)
			args = append(args, ex.Args...)
		}
		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       "(" + strings.Join(ss, ", ") + ")",
			Args:      args,
		}, nil

	case *ast.SubQueryInCondition:
		query, data, items, err := BuildQuery(b.db, c.Query, b.params, false)
		if err != nil {
			return NullExpr, newExprErrorf(nil, false, "BuildQuery error for SubqueryInCondition: %v", err)
		}
		if len(items) != 1 {
			return NullExpr, newExprErrorf(nil, true, "Subquery of type IN must have only one output column")
		}
		return Expr{
			ValueType: items[0].ValueType, // inherit ValueType from result item
			Raw:       fmt.Sprintf("(%s)", query),
			Args:      data,
		}, nil

	case *ast.UnnestInCondition:
		s, err := b.buildUnnestExpr(c.Expr, false, false)
		if err != nil {
			return NullExpr, err
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("(%s)", s.Raw),
			Args:      s.Args,
		}, nil
	default:
		return NullExpr, fmt.Errorf("not supported InCondition %T", c)
	}
}

// buildUnnestExpr creates a table from UNNEST() expression.
//
// sqlite cannot handle array type and UNNEST() directly.
// Array type is handled as JSON, so UNNEST canbe simulated by JSON_EACH().
//
// `UNNEST([a, b, c])` in spanner results in the following expression in sqlite:
// `SELECT value JSON_EACH(JSON_ARRAY((a), (b), (c))`
//
// When array param is specified like `UNNEST(@foo)`, it expands all elements of the array like:
// `SELECT value JSON_EACH(JSON_ARRAY((?), (?) (?)))`
//
// If offset for unnest is needed `UNNEST([a, b, c])` generates:
// `SELECT value, key as offset JSON_EACH(JSON_ARRAY(a, b, c))`
func (b *QueryBuilder) buildUnnestExpr(expr ast.Expr, offset bool, asview bool) (Expr, error) {
	ex, err := b.buildExpr(expr)
	if err != nil {
		return NullExpr, wrapExprError(err, expr, "UNNEST")
	}

	if ex.ValueType.Code != TCArray {
		if asview {
			msg := "Values referenced in UNNEST must be arrays. UNNEST contains expression of type %s"
			return NullExpr, newExprErrorf(expr, true, msg, ex.ValueType.Code)
		} else {
			msg := "Second argument of IN UNNEST must be an array but was %s"
			return NullExpr, newExprErrorf(expr, true, msg, ex.ValueType.Code)
		}
	}

	var raw string
	if offset {
		raw = fmt.Sprintf("SELECT value, key as offset FROM JSON_EACH(%s)", ex.Raw)
	} else {
		raw = fmt.Sprintf("SELECT value FROM JSON_EACH(%s)", ex.Raw)
	}

	return Expr{
		ValueType: ex.ValueType,
		Raw:       raw,
		Args:      ex.Args,
	}, nil
}

func (b *QueryBuilder) expandParamByPlaceholders(v Value) (Expr, error) {
	switch v.Data.(type) {
	case nil, bool, int64, float64, string, []byte:
		return Expr{
			ValueType: v.Type,
			Raw:       "?",
			Args:      []interface{}{v.Data},
		}, nil
	case []bool, []int64, []float64, []string, [][]byte:
		vv := reflect.ValueOf(v.Data)
		n := vv.Len()

		placeholders := make([]string, n)
		args := make([]interface{}, n)
		for i := 0; i < n; i++ {
			placeholders[i] = "(?)"
			args[i] = vv.Index(i).Interface()
		}
		var raw string
		if n > 0 {
			raw = "JSON_ARRAY(" + strings.Join(placeholders, ", ") + ")"
		}
		return Expr{
			ValueType: v.Type, // TODO: check correct type or not
			Raw:       raw,
			Args:      args,
		}, nil

	case ArrayValue:
		rv := reflect.ValueOf(v.Data.(ArrayValue).Elements())
		n := rv.Len()

		placeholders := make([]string, n)
		args := make([]interface{}, n)
		for i := 0; i < n; i++ {
			placeholders[i] = "(?)"
			rvv := rv.Index(i)
			if rvv.IsNil() {
				args[i] = nil
			} else {
				args[i] = rvv.Interface()
			}
		}
		var raw string
		if n > 0 {
			raw = "JSON_ARRAY(" + strings.Join(placeholders, ", ") + ")"
		}
		return Expr{
			ValueType: v.Type, // TODO: check correct type or not
			Raw:       raw,
			Args:      args,
		}, nil
	}

	return NullExpr, fmt.Errorf("unexpected parameter type for expandParamByPlaceholders: %T", v)
}

func (b *QueryBuilder) accessField(expr Expr, name string) (Expr, error) {
	if expr.ValueType.Code != TCStruct {
		msg := "Cannot access field %s on a value with type %s"
		return NullExpr, newExprErrorf(nil, true, msg, name, expr.ValueType)
	}

	st := expr.ValueType.StructType

	idx := -1
	for i := range st.FieldNames {
		if st.FieldNames[i] == name {
			if idx != -1 {
				return NullExpr, newExprErrorf(nil, true, "Column name %s is ambiguous", name)
			}

			idx = i
		}
	}
	if idx == -1 {
		if st.IsTable {
			msg := "Name %s not found inside %s"
			return NullExpr, newExprErrorf(nil, true, msg, name, expr.Raw)
		} else {
			msg := "Field name %s does not exist in %s"
			return NullExpr, newExprErrorf(nil, true, msg, name, expr.ValueType)
		}
	}

	var raw string
	if st.IsTable {
		raw = fmt.Sprintf("%s.%s", expr.Raw, name)
	} else {
		raw = fmt.Sprintf("JSON_EXTRACT(%s, '$.values[%d]')", expr.Raw, idx)
	}

	return Expr{
		ValueType: *st.FieldTypes[idx],
		Raw:       raw,
		Args:      expr.Args,
	}, nil
}

func (b *QueryBuilder) buildExpr(expr ast.Expr) (Expr, error) {
	switch e := expr.(type) {
	case *ast.UnaryExpr:
		ex, err := b.buildExpr(e.Expr)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Unary")
		}

		return Expr{
			ValueType: ex.ValueType,
			Raw:       fmt.Sprintf("%s %s", e.Op, ex.Raw),
			Args:      ex.Args,
		}, nil

	case *ast.BinaryExpr:
		left, lerr := b.buildExpr(e.Left)
		right, rerr := b.buildExpr(e.Right)

		if lerr != nil {
			return NullExpr, wrapExprError(lerr, expr, "Left")
		}
		if rerr != nil {
			return NullExpr, wrapExprError(rerr, expr, "Right")
		}

		var args []interface{}
		args = append(args, left.Args...)
		args = append(args, right.Args...)

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
			return NullExpr, fmt.Errorf("%T: unknown op %v", e, e.Op)
		}

		raw := fmt.Sprintf("%s %s %s", left.Raw, e.Op, right.Raw)
		if e.Op == ast.OpBitXor {
			raw = fmt.Sprintf("(%s | %s) - (%s & %s)", left.Raw, right.Raw, left.Raw, right.Raw)
		}

		return Expr{
			ValueType: vt,
			Raw:       raw,
			Args:      args,
		}, nil

	case *ast.InExpr:
		left, lerr := b.buildExpr(e.Left)
		right, rerr := b.buildInCondition(e.Right)
		if lerr != nil {
			return NullExpr, wrapExprError(lerr, expr, "Left")
		}
		if rerr != nil {
			return NullExpr, wrapExprError(rerr, expr, "Right")
		}

		var args []interface{}
		args = append(args, left.Args...)
		args = append(args, right.Args...)

		op := "IN"
		if e.Not {
			op = "NOT IN"
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("%s %s %s", left.Raw, op, right.Raw),
			Args:      args,
		}, nil

	case *ast.BetweenExpr:
		left, lerr := b.buildExpr(e.Left)
		rstart, rserr := b.buildExpr(e.RightStart)
		rend, reerr := b.buildExpr(e.RightEnd)
		if lerr != nil {
			return NullExpr, wrapExprError(lerr, expr, "Left")
		}
		if rserr != nil {
			return NullExpr, wrapExprError(rserr, expr, "RightStart")
		}
		if reerr != nil {
			return NullExpr, wrapExprError(reerr, expr, "RightEnd")
		}

		var args []interface{}
		args = append(args, left.Args...)
		args = append(args, rstart.Args...)
		args = append(args, rend.Args...)

		op := "BETWEEN"
		if e.Not {
			op = "NOT BETWEEN"
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("%s %s %s AND %s", left.Raw, op, rstart.Raw, rend.Raw),
			Args:      args,
		}, nil

	case *ast.SelectorExpr:
		ex, err := b.buildExpr(e.Expr)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Expr")
		}

		ex2, err := b.accessField(ex, e.Ident.Name)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Expr")
		}

		return Expr{
			ValueType: ex2.ValueType,
			Raw:       ex2.Raw,
			Args:      ex2.Args,
		}, nil

	case *ast.IndexExpr:
		ex1, err := b.buildExpr(e.Expr)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Expr")
		}

		ex2, err := b.buildExpr(e.Index)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Index")
		}

		if ex1.ValueType.Code != TCArray {
			msg := "Element access using [] is not supported on values of type %s"
			return NullExpr, newExprErrorf(expr, true, msg, ex1.ValueType)
		}
		if ex2.ValueType.Code != TCInt64 {
			msg := "Array position in [] must be coercible to INT64 type, but has type %s"
			return NullExpr, newExprErrorf(expr, true, msg, ex2.ValueType)
		}

		var args []interface{}
		args = append(args, ex1.Args...)
		args = append(args, ex2.Args...)

		var raw string
		if e.Ordinal {
			raw = fmt.Sprintf("JSON_EXTRACT(%s, '$[' || (%s-1) || ']')", ex1.Raw, ex2.Raw)
		} else {
			raw = fmt.Sprintf("JSON_EXTRACT(%s, '$[' || %s || ']')", ex1.Raw, ex2.Raw)
		}

		return Expr{
			ValueType: *ex1.ValueType.ArrayType,
			Raw:       raw,
			Args:      args,
		}, nil

	case *ast.IsNullExpr:
		left, lerr := b.buildExpr(e.Left)
		if lerr != nil {
			return NullExpr, wrapExprError(lerr, expr, "Left")
		}

		op := "IS NULL"
		if e.Not {
			op = "IS NOT NULL"
		}

		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       fmt.Sprintf("%s %s", left.Raw, op),
			Args:      left.Args,
		}, nil

	case *ast.IsBoolExpr:
		left, lerr := b.buildExpr(e.Left)
		if lerr != nil {
			return NullExpr, wrapExprError(lerr, expr, "Left")
		}

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
			Args:      left.Args,
		}, nil

	case *ast.CallExpr:
		name := strings.ToUpper(e.Func.Name)
		fn, ok := customFunctions[name]
		if !ok {
			return NullExpr, newExprErrorf(expr, false, "unsupported CALL function: %s", name)
		}

		// TODO: distinct

		// when fn.NArgs < 0, args is variadic
		if fn.NArgs >= 0 && fn.NArgs != len(e.Args) {
			return NullExpr, newExprErrorf(expr, true, "%s requires %d arguments", name, fn.NArgs)
		}

		var args []interface{}
		var ss []string
		var vts []ValueType
		for i := range e.Args {
			ex, err := b.buildExpr(e.Args[i].Expr)
			if err != nil {
				return NullExpr, wrapExprError(err, expr, "Args")
			}

			args = append(args, ex.Args...)
			ss = append(ss, ex.Raw)
			vts = append(vts, ex.ValueType)
		}

		if ok := fn.ArgTypes(vts); !ok {
			return NullExpr, newExprErrorf(expr, true, "arguments does not match for %s", name)
		}

		var distinct string
		if e.Distinct {
			distinct = "DISTINCT "
		}

		return Expr{
			ValueType: fn.ReturnType(vts),
			Raw:       fmt.Sprintf(`%s(%s%s)`, name, distinct, strings.Join(ss, ", ")),
			Args:      args,
		}, nil

	case *ast.CountStarExpr:
		return Expr{
			ValueType: ValueType{Code: TCInt64},
			Raw:       "COUNT(*)",
		}, nil

	case *ast.CastExpr:
		ex, err := b.buildExpr(e.Expr)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Cast")
		}

		target := astTypeToValueType(e.Type)

		if target.Code == TCStruct {
			return NullExpr, newExprErrorf(expr, false, "Struct type is not supported in Cast")
		}

		raw := ex.Raw
		switch ex.ValueType.Code {
		case TCInt64:
			switch target.Code {
			case TCBool:
				raw = fmt.Sprintf("___CAST_INT64_TO_BOOL(%s)", raw)
			case TCString:
				raw = fmt.Sprintf("___CAST_INT64_TO_STRING(%s)", raw)
			case TCInt64:
				// do nothing
			case TCFloat64:
				raw = fmt.Sprintf("___CAST_INT64_TO_FLOAT64(%s)", raw)
			default:
				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType.Code, target.Code)
			}
		case TCFloat64:
			switch target.Code {
			case TCString:
				raw = fmt.Sprintf("___CAST_FLOAT64_TO_STRING(%s)", raw)
			case TCInt64:
				raw = fmt.Sprintf("___CAST_FLOAT64_TO_INT64(%s)", raw)
			case TCFloat64:
				// do nothing
			default:
				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType.Code, target.Code)
			}

		case TCBool:
			switch target.Code {
			case TCString:
				raw = fmt.Sprintf("___CAST_BOOL_TO_STRING(%s)", raw)
			case TCBool:
				// do nothing
			case TCInt64:
				raw = fmt.Sprintf("___CAST_BOOL_TO_INT64(%s)", raw)
			default:
				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType, target)
			}

		case TCString:
			switch target.Code {
			case TCString:
				// do nothing
			case TCBool:
				raw = fmt.Sprintf("___CAST_STRING_TO_BOOL(%s)", raw)
			case TCInt64:
				raw = fmt.Sprintf("___CAST_STRING_TO_INT64(%s)", raw)
			case TCFloat64:
				raw = fmt.Sprintf("___CAST_STRING_TO_FLOAT64(%s)", raw)
			case TCDate:
				raw = fmt.Sprintf("___CAST_STRING_TO_DATE(%s)", raw)
			case TCTimestamp:
				raw = fmt.Sprintf("___CAST_STRING_TO_TIMESTAMP(%s)", raw)
			default:
				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType, target)
			}

		case TCBytes:
			switch target.Code {
			case TCBytes:
				// do nothing
			case TCString:
				// do nothing?
			default:
				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType, target)
			}

		case TCDate:
			switch target.Code {
			case TCString:
				raw = fmt.Sprintf("___CAST_DATE_TO_STRING(%s)", raw)
			case TCDate:
				// do nothing
			case TCTimestamp:
				raw = fmt.Sprintf("___CAST_DATE_TO_TIMESTAMP(%s)", raw)
			default:
				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType, target)
			}

		case TCTimestamp:
			switch target.Code {
			case TCString:
				raw = fmt.Sprintf("___CAST_TIMESTAMP_TO_STRING(%s)", raw)
			case TCDate:
				raw = fmt.Sprintf("___CAST_TIMESTAMP_TO_DATE(%s)", raw)
			case TCTimestamp:
				// do nothing
			default:
				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType, target)
			}

		case TCArray:
			if !compareValueType(ex.ValueType, target) {
				if ex.ValueType.Code == TCArray && target.Code == TCArray {
					msg := "Casting between arrays with incompatible element types is not supported: Invalid cast from %s to %s"
					return NullExpr, newExprErrorf(expr, true, msg, ex.ValueType, target)
				}

				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType, target)
			}
		case TCStruct:
			if !compareValueType(ex.ValueType, target) {
				return NullExpr, newExprErrorf(expr, true, "Invalid cast from %s to %s", ex.ValueType, target)
			}
			return NullExpr, newExprErrorf(expr, false, "struct is not supported")
		}

		return Expr{
			ValueType: target,
			Raw:       raw,
			Args:      ex.Args,
		}, nil

		return NullExpr, newExprErrorf(expr, false, "Cast not supported yet")

	case *ast.ExtractExpr:
		ex, err := b.buildExpr(e.Expr)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Paren")
		}

		args := ex.Args

		var raw string
		code := TCInt64
		switch ex.ValueType.Code {
		case TCTimestamp:
			if e.AtTimeZone == nil {
				msg := fmt.Sprintf("handy-spanner: please specify timezone explicitly. Use %q for default timezone", defaultTimeZone)
				return NullExpr, newExprErrorf(expr, true, msg)
			}

			tz, err := b.buildExpr(e.AtTimeZone.Expr)
			if err != nil {
				return NullExpr, wrapExprError(err, expr, "AT TIME ZONE")
			}
			if tz.ValueType.Code != TCString {
				msg := "No matching signature for function EXTRACT for argument types: DATE_TIME_PART FROM TIMESTAMP AT TIME ZONE %s. Supported signatures: EXTRACT(DATE_TIME_PART FROM DATE); EXTRACT(DATE_TIME_PART FROM TIMESTAMP [AT TIME ZONE STRING])"
				return NullExpr, newExprErrorf(expr, true, msg, tz.ValueType)
			}
			args = append(args, tz.Args...)

			part := strings.ToUpper(e.Part.Name)
			switch part {
			case "NANOSECOND",
				"MICROSECOND",
				"MILLISECOND",
				"SECOND",
				"MINUTE",
				"HOUR",
				"DAYOFWEEK",
				"DAY",
				"DAYOFYEAR",
				"WEEK",
				"ISOWEEK",
				"MONTH",
				"QUARTER",
				"YEAR",
				"ISOYEAR":
				raw = fmt.Sprintf("___EXTRACT_FROM_TIMESTAMP(%q, %s, %s)", part, ex.Raw, tz.Raw)

			case "DATE":
				code = TCDate
				raw = fmt.Sprintf("DATE(%s)", ex.Raw)
			default:
				return NullExpr, newExprErrorf(expr, true, "A valid date part name is required but found %s", e.Part.Name)
			}

		case TCDate:
			if e.AtTimeZone != nil {
				return NullExpr, newExprErrorf(expr, true, "EXTRACT from DATE does not support AT TIME ZONE")
			}

			part := strings.ToUpper(e.Part.Name)
			switch part {
			case "DAYOFWEEK",
				"DAY",
				"DAYOFYEAR",
				"WEEK",
				"ISOWEEK",
				"MONTH",
				"QUARTER",
				"YEAR",
				"ISOYEAR":
				raw = fmt.Sprintf("___EXTRACT_FROM_DATE(%q, %s)", part, ex.Raw)

			case "NANOSECOND",
				"MICROSECOND",
				"MILLISECOND",
				"SECOND",
				"MINUTE",
				"HOUR",
				"DATE":
				return NullExpr, newExprErrorf(expr, true, "EXTRACT from DATE does not support the %s date part", part)
			default:
				return NullExpr, newExprErrorf(expr, true, "A valid date part name is required but found %s", e.Part.Name)
			}

		default:
			return NullExpr, newExprErrorf(expr, true, "EXTRACT does not support literal %s arguments", ex.ValueType)
		}

		return Expr{
			ValueType: ValueType{Code: code},
			Raw:       raw,
			Args:      args,
		}, nil

	case *ast.CaseExpr:
		return NullExpr, newExprErrorf(expr, false, "Case not supported yet")

	case *ast.ParenExpr:
		ex, err := b.buildExpr(e.Expr)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Paren")
		}

		return Expr{
			ValueType: ex.ValueType,
			Raw:       fmt.Sprintf("(%s)", ex.Raw),
			Args:      ex.Args,
		}, nil

	case *ast.ScalarSubQuery:
		query, args, items, err := BuildQuery(b.db, e.Query, b.params, false)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Scalar")
		}
		if len(items) != 1 {
			return NullExpr, newExprErrorf(expr, true, "Scalar subquery cannot have more than one column unless using SELECT AS STRUCT to build STRUCT values")
		}
		return Expr{
			ValueType: items[0].ValueType, // inherit ValueType from result item
			Raw:       fmt.Sprintf("(%s)", query),
			Args:      args,
		}, nil

	case *ast.ArraySubQuery:
		query, args, items, err := BuildQuery(b.db, e.Query, b.params, true)
		if err != nil {
			return NullExpr, wrapExprError(err, expr, "Array")
		}
		if len(items) != 1 {
			return NullExpr, newExprErrorf(expr, true, "ARRAY subquery cannot have more than one column unless using SELECT AS STRUCT to build STRUCT values")
		}
		if items[0].ValueType.Code == TCArray {
			msg := "Cannot use array subquery with column of type %s because nested arrays are not supported"
			return NullExpr, newExprErrorf(expr, true, msg, items[0].ValueType)
		}

		return Expr{
			ValueType: ValueType{
				Code:      TCArray,
				ArrayType: &items[0].ValueType,
			},
			Raw:  fmt.Sprintf("(SELECT JSON_GROUP_ARRAY(%s) FROM (%s))", items[0].Expr.Raw, query),
			Args: args,
		}, nil

	case *ast.ExistsSubQuery:
		query, args, _, err := BuildQuery(b.db, e.Query, b.params, false)
		if err != nil {
			return NullExpr, newExprErrorf(expr, false, "BuildQuery error for %T: %v", err, e)
		}
		return Expr{
			ValueType: ValueType{
				Code: TCBool,
			},
			Raw:  fmt.Sprintf("EXISTS(%s)", query),
			Args: args,
		}, nil

	case *ast.ArrayLiteral:
		var args []interface{}
		var ss []string
		var vts []ValueType
		for i := range e.Values {
			ex, err := b.buildExpr(e.Values[i])
			if err != nil {
				return NullExpr, wrapExprError(err, expr, "ArrayLiteral")
			}

			args = append(args, ex.Args...)
			ss = append(ss, ex.Raw)
			vts = append(vts, ex.ValueType)
		}

		vt, err := decideArrayElementsValueType(vts...)
		if err != nil {
			return NullExpr, newExprErrorf(expr, true, err.Error())
		}

		if vt.Code == TCArray {
			msg := "Cannot construct array with element type %s because nested arrays are not supported"
			return NullExpr, newExprErrorf(expr, true, msg, vt)
		}

		// TODO: allow to use both Int64 and Float64

		return Expr{
			ValueType: ValueType{
				Code:      TCArray,
				ArrayType: &vt,
			},
			Raw:  fmt.Sprintf("JSON_ARRAY(%s)", strings.Join(ss, ", ")),
			Args: args,
		}, nil

	case *ast.StructLiteral:
		var names []string
		var vt ValueType
		var typedef bool

		if e.Fields == nil {
			for i := 0; i < len(e.Values); i++ {
				names = append(names, `""`)
			}

			// just initialize here. The values will be populated later.
			vt = ValueType{
				Code:       TCStruct,
				StructType: &StructType{},
			}
		} else {
			if len(e.Fields) != len(e.Values) {
				return NullExpr, newExprErrorf(expr, true, "STRUCT type has %d fields but constructor call has %d fields", len(e.Fields), len(e.Values))
			}

			typedef = true
			vt = astTypeToValueType(&ast.StructType{
				Fields: e.Fields,
			})
			for _, name := range vt.StructType.FieldNames {
				names = append(names, `"`+name+`"`)
			}
		}

		namesObj := fmt.Sprintf("JSON_ARRAY(%s)", strings.Join(names, ", "))

		var args []interface{}
		var values []string
		for i, v := range e.Values {
			ex, err := b.buildExpr(v)
			if err != nil {
				return NullExpr, wrapExprError(err, expr, "Values")
			}
			args = append(args, ex.Args...)

			if ex.ValueType.Code == TCStruct {
				msg := `Unsupported query shape: A struct value cannot be returned as a column value. Rewrite the query to flatten the struct fields in the result.`
				return NullExpr, newExprUnimplementedErrorf(expr, msg)
			}

			// If types of fields are defined, check type compatibility with the value
			// otherwise use the type of the value as is
			if typedef {
				if !compareValueType(ex.ValueType, *vt.StructType.FieldTypes[i]) {
					msg := "Struct field %d has type literal %s which does not coerce to %s"
					return NullExpr, newExprErrorf(expr, true, msg, i+1, ex.ValueType, *vt.StructType.FieldTypes[i])
				}
			} else {
				vt.StructType.FieldNames = append(vt.StructType.FieldNames, "")
				vt.StructType.FieldTypes = append(vt.StructType.FieldTypes, &ex.ValueType)
			}
			values = append(values, ex.Raw)
		}
		valuesObj := fmt.Sprintf("JSON_ARRAY(%s)", strings.Join(values, ", "))
		raw := fmt.Sprintf(`JSON_OBJECT("keys", %s, "values", %s)`, namesObj, valuesObj)

		return Expr{
			Raw:       raw,
			ValueType: vt,
			Args:      args,
		}, nil

	case *ast.NullLiteral:
		return Expr{
			ValueType: ValueType{Code: TCInt64},
			Raw:       "NULL",
		}, nil

	case *ast.BoolLiteral:
		if e.Value {
			return Expr{
				ValueType: ValueType{Code: TCBool},
				Raw:       "TRUE",
			}, nil
		}
		return Expr{
			ValueType: ValueType{Code: TCBool},
			Raw:       "FALSE",
		}, nil

	case *ast.FloatLiteral:
		return Expr{
			ValueType: ValueType{Code: TCFloat64},
			Raw:       e.Value,
		}, nil

	case *ast.IntLiteral:
		n, err := strconv.ParseInt(e.Value, 0, 64)
		if err != nil {
			return NullExpr, newExprErrorf(expr, false, "unexpected format %q as int64: %v", e.Value, err)
		}
		return Expr{
			ValueType: ValueType{Code: TCInt64},
			Raw:       strconv.FormatInt(n, 10),
		}, nil

	case *ast.StringLiteral:
		return Expr{
			ValueType: ValueType{Code: TCString},
			Raw:       fmt.Sprintf("%q", e.Value),
		}, nil

	case *ast.BytesLiteral:
		return Expr{
			ValueType: ValueType{Code: TCBytes},
			Raw:       `"` + string(e.Value) + `"`,
		}, nil

	case *ast.DateLiteral:
		t, ok := parseDateLiteral(e.Value.Value)
		if !ok {
			return NullExpr, newExprErrorf(expr, true, "Invalid DATE literal")
		}
		return Expr{
			ValueType: ValueType{Code: TCDate},
			Raw:       `"` + t.Format("2006-01-02") + `"`,
		}, nil

	case *ast.TimestampLiteral:
		t, ok := parseTimestampLiteral(e.Value.Value)
		if !ok {
			return NullExpr, newExprErrorf(expr, true, "Invalid TIMESTAMP literal")
		}
		return Expr{
			ValueType: ValueType{Code: TCTimestamp},
			Raw:       `"` + t.Format(time.RFC3339Nano) + `"`,
		}, nil

	case *ast.Param:
		v, ok := b.params[e.Name]
		if !ok {
			return NullExpr, newExprErrorf(expr, true, "params not found: %s", e.Name)
		}
		return b.expandParamByPlaceholders(v)

	case *ast.Ident:
		tbl, ok := b.views[e.Name]
		if ok {
			return Expr{
				ValueType: ValueType{
					Code:       TCStruct,
					StructType: tbl.ToStruct(),
				},
				Raw: e.Name,
			}, nil
		} else {
			if b.rootView == nil {
				return NullExpr, newExprErrorf(expr, true, "Unrecognized name: %s", e.Name)
			}

			item, ambiguous, notfound := b.rootView.Get(e.Name)
			if notfound {
				return NullExpr, newExprErrorf(expr, true, "Unrecognized name: %s", e.Name)
			}
			if ambiguous {
				return NullExpr, newExprErrorf(expr, true, "Column name %s is ambiguous", e.Name)
			}
			return item.Expr, nil
		}
	case *ast.Path:
		firstName := e.Idents[0].Name
		var remains []*ast.Ident
		var curExpr Expr

		// Look tables first
		tbl, ok := b.views[firstName]
		if ok {
			curExpr = Expr{
				ValueType: ValueType{
					Code:       TCStruct,
					StructType: tbl.ToStruct(),
				},
				Raw: firstName,
			}
		} else {
			// If not found in tables, look the results items of root
			if b.rootView == nil {
				return NullExpr, newExprErrorf(expr, true, "Unrecognized name: %s", firstName)
			}

			item, ambiguous, notfound := b.rootView.Get(firstName)
			if notfound {
				return NullExpr, newExprErrorf(expr, true, "Unrecognized name: %s", firstName)
			}
			if ambiguous {
				return NullExpr, newExprErrorf(expr, true, "Column name %s is ambiguous", firstName)
			}

			curExpr = item.Expr
		}
		remains = e.Idents[1:]

		for _, ident := range remains {
			next, err := b.accessField(curExpr, ident.Name)
			if err != nil {
				return NullExpr, wrapExprError(err, expr, "Path")
			}

			curExpr = next
		}

		return curExpr, nil
	default:
		return NullExpr, newExprErrorf(expr, false, "unknown expression")
	}
}

type exprError struct {
	expr ast.Expr
	msg  string

	invalid       bool
	unimplemented bool
}

func (e *exprError) Error() string {
	return fmt.Sprintf("%T: %s", e.expr, e.msg)
}

func (e exprError) GRPCStatus() *status.Status {
	code := codes.Unknown
	if e.invalid {
		code = codes.InvalidArgument
	}
	if e.unimplemented {
		code = codes.Unimplemented
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

func newExprUnimplementedErrorf(expr ast.Expr, format string, a ...interface{}) error {
	return &exprError{
		expr:          expr,
		msg:           fmt.Sprintf(format, a...),
		unimplemented: true,
	}
}

func wrapExprError(err error, expr ast.Expr, msg string) error {
	exprErr, ok := err.(*exprError)
	if !ok {
		return fmt.Errorf("unknown error in wrapExprError: %v", err)
	}

	// if error is invalid or unimplemented it is invalig argument, so return it as is
	if exprErr.invalid || exprErr.unimplemented {
		return err
	}

	return newExprErrorf(expr, false, "%s, %s", msg, err.Error())
}

func astTypeToValueType(astType ast.Type) ValueType {
	switch t := astType.(type) {
	case *ast.SimpleType:
		return ValueType{Code: astTypeToTypeCode(t.Name)}
	case *ast.ArrayType:
		vt := astTypeToValueType(t.Item)
		return ValueType{
			Code:      TCArray,
			ArrayType: &vt,
		}
	case *ast.StructType:
		names := make([]string, 0, len(t.Fields))
		types := make([]*ValueType, 0, len(t.Fields))

		for _, field := range t.Fields {
			var name string
			if field.Ident != nil {
				name = field.Ident.Name
			}
			names = append(names, name)

			vt := astTypeToValueType(field.Type)
			types = append(types, &vt)

		}
		return ValueType{
			Code: TCStruct,
			StructType: &StructType{
				FieldNames: names,
				FieldTypes: types,
			},
		}
	}

	panic("unknown type")
}

func parseDateLiteral(s string) (time.Time, bool) {
	if t, err := time.ParseInLocation("2006-1-2", s, parseLocation); err == nil {
		return t, true
	}

	return time.Time{}, false
}

func parseTimestampLiteral(s string) (time.Time, bool) {
	// TODO: cannot parse these format
	// 1999-01-02 12:02:03.123456789+3
	// 1999-01-02 12:02:03.123456789 UTC

	if t, err := time.ParseInLocation("2006-1-2 15:4:5.999999999Z07:00", s, parseLocation); err == nil {
		return t.UTC(), true
	}

	if t, err := time.ParseInLocation("2006-1-2 15:4:5.999999999Z07", s, parseLocation); err == nil {
		return t.UTC(), true
	}

	if t, err := time.ParseInLocation("2006-1-2 15:4:5.999999999", s, parseLocation); err == nil {
		return t.UTC(), true
	}

	if t, err := time.ParseInLocation("2006-1-2T15:4:5.999999999Z07:00", s, parseLocation); err == nil {
		return t.UTC(), true
	}

	if t, err := time.ParseInLocation("2006-1-2T15:4:5.999999999Z07", s, parseLocation); err == nil {
		return t.UTC(), true
	}

	if t, err := time.ParseInLocation("2006-1-2T15:4:5.999999999", s, parseLocation); err == nil {
		return t.UTC(), true
	}

	if t, err := time.ParseInLocation("2006-1-2", s, parseLocation); err == nil {
		return t.UTC(), true
	}

	return time.Time{}, false
}
