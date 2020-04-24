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
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/MakeNowJust/memefish/pkg/ast"
	structpb "github.com/golang/protobuf/ptypes/struct"
	uuidpkg "github.com/google/uuid"
	sqlite "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Database interface {
	ApplyDDL(ctx context.Context, ddl ast.DDL) error

	Read(ctx context.Context, tbl, index string, cols []string, keyset *KeySet, limit int64) (RowIterator, error)
	Query(ctx context.Context, query *ast.QueryStatement, params map[string]Value) (RowIterator, error)

	Insert(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue) error
	Update(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue) error
	Replace(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue) error
	InsertOrUpdate(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue) error
	Delete(ctx context.Context, table string, keyset *KeySet) error

	Close() error
}

var _ Database = (*database)(nil)

type database struct {
	mu     sync.RWMutex
	tables map[string]*Table

	db *sql.DB
}

func newDatabase() *database {
	uuid := uuidpkg.New().String()
	db, err := sql.Open("sqlite3_spanner", fmt.Sprintf("file:%s.db?cache=shared&mode=memory", uuid))
	if err != nil {
		log.Fatal(err)
	}

	return &database{
		tables: make(map[string]*Table),
		db:     db,
	}
}

func (d *database) readTable(tbl string) (*Table, func(), error) {
	d.mu.RLock()

	table, ok := d.tables[tbl]
	if !ok {
		return nil, nil, status.Errorf(codes.NotFound, "Table not found: %s", tbl)
	}

	return table, func() { d.mu.RUnlock() }, nil
}

func (d *database) ApplyDDL(ctx context.Context, ddl ast.DDL) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	switch val := ddl.(type) {
	case *ast.CreateTable:
		if err := d.CreateTable(ctx, val); err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
		return nil

	case *ast.CreateIndex:
		if err := d.CreateIndex(ctx, val); err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
		return nil

	case *ast.DropTable:
		return status.Errorf(codes.Unimplemented, "Drop Table is not supported yet")

	case *ast.DropIndex:
		return status.Errorf(codes.Unimplemented, "Drop Index is not supported yet")

	case *ast.AlterTable:
		return status.Errorf(codes.Unimplemented, "Alter Table is not supported yet")

	default:
		return status.Errorf(codes.Unknown, "unknown DDL statement: %v", val)
	}
}

func (d *database) Read(ctx context.Context, tbl, idx string, cols []string, keyset *KeySet, limit int64) (RowIterator, error) {
	if keyset == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid StreamingRead request")
	}
	if tbl == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid StreamingRead request")
	}
	if len(cols) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid StreamingRead request")
	}

	table, free, err := d.readTable(tbl)
	if err != nil {
		return nil, err
	}
	defer free()

	index, ok := table.TableIndex(idx)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Index not found on table %s: %s", tbl, idx)
	}

	columns, err := table.getColumnsByName(cols)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	// Check the index table has the specified columns
	for _, column := range columns {
		if !index.HasColumn(column.Name()) {
			return nil, status.Errorf(codes.Unimplemented, "Reading non-indexed columns using an index is not supported. Consider adding %s to the index using a STORING clause.", column.Name())
		}
	}

	resultItems := make([]ResultItem, len(cols))
	for i := range columns {
		resultItems[i] = createResultItemFromColumn(columns[i])
	}

	indexColumnsName := strings.Join(index.IndexColumnNames(), ", ")
	indexColumns := index.IndexColumns()
	indexColumnDirs := index.IndexColumnDirections()
	colName := strings.Join(cols, ", ")

	var args []interface{}

	whereClause, whereArgs, err := buildWhereClauseFromKeySet(keyset, indexColumnsName, indexColumns)
	if err != nil {
		return nil, err
	}
	args = append(args, whereArgs...)

	orderByItems := make([]string, len(indexColumns))
	for i := range indexColumns {
		orderByItems[i] = fmt.Sprintf("%s %s", indexColumns[i].Name(), indexColumnDirs[i])
	}
	orderByClause := strings.Join(orderByItems, ", ")

	query := fmt.Sprintf(`SELECT %s FROM %s %s ORDER BY %s`, colName, table.Name, whereClause, orderByClause)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	r, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query failed: %v, query: %v", err, query)
	}

	return &rows{rows: r, resultItems: resultItems}, nil
}

func (d *database) Query(ctx context.Context, stmt *ast.QueryStatement, params map[string]Value) (RowIterator, error) {
	query, args, resultItems, err := BuildQuery(d, stmt.Query, params, false)
	if err != nil {
		return nil, err
	}

	r, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query failed: %v, query: %v", err, query)
	}

	return &rows{rows: r, resultItems: resultItems}, nil
}

func (d *database) write(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue,
	nonNullCheck bool,
	affectedRowsCheck bool,
	buildQueryFn func(*Table, []*Column) string,
	buildArgsFn func(*Table, []*Column, []interface{}) []interface{},
) error {
	table, free, err := d.readTable(tbl)
	if err != nil {
		return err
	}
	defer free()

	primaryKey := table.primaryKey

	// Check columns are defined in the table
	columns, err := table.getColumnsByName(cols)
	if err != nil {
		return status.Errorf(codes.NotFound, "%v", err)
	}

	// Ensure multiple values are not specified
	usedColumns := make(map[string]struct{}, len(cols))
	for _, c := range columns {
		n := c.Name()
		if _, ok := usedColumns[n]; ok {
			return status.Errorf(codes.InvalidArgument, "Multiple values for column %s", n)
		}
		usedColumns[n] = struct{}{}
	}

	// Check all primary keys are specified
	for _, colName := range primaryKey.IndexColumnNames() {
		if _, ok := usedColumns[colName]; !ok {
			return status.Errorf(codes.FailedPrecondition, "%s must not be NULL in table %s.", colName, tbl)
		}
	}

	// Check not nullable columns are specified for Insert/Replace
	if nonNullCheck {
		var noExsitNonNullableColumns []string
		for _, c := range table.columns {
			if c.nullable {
				continue
			}

			n := c.Name()
			if _, ok := usedColumns[n]; !ok {
				noExsitNonNullableColumns = append(noExsitNonNullableColumns, n)
			}
		}
		if len(noExsitNonNullableColumns) > 0 {
			columns := strings.Join(noExsitNonNullableColumns, ", ")
			return status.Errorf(codes.FailedPrecondition,
				"A new row in table %s does not specify a non-null value for these NOT NULL columns: %s",
				tbl, columns,
			)
		}
	}

	if len(values) == 0 {
		return nil
	}

	query := buildQueryFn(table, columns)
	if query == "" {
		return nil
	}

	for _, vs := range values {
		if len(vs.Values) != len(cols) {
			return status.Error(codes.InvalidArgument, "Mutation has mismatched number of columns and values.")
		}

		data := make([]interface{}, 0, len(cols))
		for i, v := range vs.Values {
			col := columns[i]

			vv, err := spannerValue2DatabaseValue(v, *col)
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "%v", err)
			}

			if !col.nullable && vv == nil {
				return status.Errorf(codes.FailedPrecondition,
					"%s must not be NULL in table %s.",
					col.Name(), tbl,
				)
			}

			data = append(data, vv)
		}

		args := buildArgsFn(table, columns, data)

		r, err := d.db.ExecContext(ctx, query, args...)
		if err != nil {
			if sqliteErr, ok := err.(sqlite.Error); ok {
				msg := sqliteErr.Error()
				switch sqliteErr.ExtendedCode {
				case sqlite.ErrConstraintPrimaryKey:
					return status.Errorf(codes.AlreadyExists, "Row %v in table %s already exists", data, tbl)
				case sqlite.ErrConstraintUnique:
					if n := strings.Index(msg, ": "); n > 0 {
						msg = msg[n+2:]
					}
					return status.Errorf(codes.AlreadyExists,
						"Unique index violation at index key [%v]. It conflicts with row %v in table %s",
						msg, data, tbl,
					)
				}
			}

			return status.Errorf(codes.Internal, "failed to write into sqlite: %v", err)
		}

		if affectedRowsCheck {
			// Check rows are updated
			// When the row does not exist, sqlite returns success.
			// But spanner should return NotFound
			n, err := r.RowsAffected()
			if err != nil {
				return status.Errorf(codes.Internal, "failed to get RowsAffected: %v", err)
			}
			if n == 0 {
				return status.Errorf(codes.NotFound, "Row %v in table %s is missing. Row cannot be updated.", data, tbl)
			}
		}
	}

	return nil
}

func (d *database) Insert(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue) error {
	buildQueryFn := func(table *Table, columns []*Column) string {
		columnName := strings.Join(cols, ", ")
		placeholder := "?"
		if len(cols) > 1 {
			placeholder += strings.Repeat(", ?", len(cols)-1)
		}
		return fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, tbl, columnName, placeholder)
	}

	buildArgsFn := func(table *Table, columns []*Column, data []interface{}) []interface{} {
		return data
	}

	return d.write(ctx, tbl, cols, values, true, false, buildQueryFn, buildArgsFn)
}

func (d *database) Update(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue) error {
	buildQueryFn := func(table *Table, columns []*Column) string {
		assigns := make([]string, 0, len(cols))
		for _, c := range columns {
			if c.isPrimaryKey {
				continue
			}
			assigns = append(assigns, fmt.Sprintf("%s = ?", c.Name()))
		}

		// If no columns to be updated exist, it should be no-op.
		if len(assigns) == 0 {
			return ""
		}

		setClause := strings.Join(assigns, ", ")

		pKeysNames := table.primaryKey.IndexColumnNames()
		pkeysAssign := make([]string, len(pKeysNames))
		for i, col := range pKeysNames {
			pkeysAssign[i] = fmt.Sprintf("%s = ?", col)
		}
		whereClause := strings.Join(pkeysAssign, " AND ")

		return fmt.Sprintf(`UPDATE %s SET %s WHERE %s`, tbl, setClause, whereClause)
	}

	buildArgsFn := func(table *Table, columns []*Column, data []interface{}) []interface{} {
		numPKeys := len(table.primaryKey.IndexColumns())
		values := make([]interface{}, 0, len(cols))
		pkeyValues := make([]interface{}, numPKeys)

		for i, column := range columns {
			if column.isPrimaryKey {
				pkeyValues[column.primaryKeyPos-1] = data[i]
			} else {
				values = append(values, data[i])
			}
		}

		// First N(size=columns-pkeys) values are for SET clause
		// Last M(size=pkeys) values are for WHERE clause
		values = append(values, pkeyValues...)

		return values
	}

	return d.write(ctx, tbl, cols, values, false, true, buildQueryFn, buildArgsFn)
}

func (d *database) Replace(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue) error {
	buildQueryFn := func(table *Table, columns []*Column) string {
		columnName := strings.Join(cols, ", ")
		placeholder := "?"
		if len(cols) > 1 {
			placeholder += strings.Repeat(", ?", len(cols)-1)
		}
		return fmt.Sprintf(`REPLACE INTO %s (%s) VALUES (%s)`, tbl, columnName, placeholder)
	}

	buildArgsFn := func(table *Table, columns []*Column, data []interface{}) []interface{} {
		return data
	}

	return d.write(ctx, tbl, cols, values, true, false, buildQueryFn, buildArgsFn)
}

func (d *database) InsertOrUpdate(ctx context.Context, tbl string, cols []string, values []*structpb.ListValue) error {
	buildQueryFn := func(table *Table, columns []*Column) string {
		assigns := make([]string, 0, len(cols))
		for _, c := range columns {
			if c.isPrimaryKey {
				continue
			}
			assigns = append(assigns, fmt.Sprintf("%s = ?", c.Name()))
		}
		setClause := strings.Join(assigns, ", ")

		pkeysNamesSlice := table.primaryKey.IndexColumnNames()
		pkeysNames := strings.Join(pkeysNamesSlice, ", ")

		columnName := strings.Join(cols, ", ")
		placeholder := "?"
		if len(cols) > 1 {
			placeholder += strings.Repeat(", ?", len(cols)-1)
		}

		return fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s`,
			tbl, columnName, placeholder, pkeysNames, setClause)
	}

	buildArgsFn := func(table *Table, columns []*Column, data []interface{}) []interface{} {
		setValues := make([]interface{}, 0, len(cols))

		for i, column := range columns {
			if !column.isPrimaryKey {
				setValues = append(setValues, data[i])
			}
		}

		// First N(size=columns) values are for VALUES clause
		// Last M(size=columns-pkeys) values are for SET clause
		data = append(data, setValues...)
		return data
	}

	return d.write(ctx, tbl, cols, values, false, false, buildQueryFn, buildArgsFn)
}

func (d *database) Delete(ctx context.Context, tbl string, keyset *KeySet) error {
	table, free, err := d.readTable(tbl)
	if err != nil {
		return err
	}
	defer free()

	index := table.primaryKey

	indexColumnsName := strings.Join(index.IndexColumnNames(), ", ")
	indexColumns := index.IndexColumns()

	whereClause, args, err := buildWhereClauseFromKeySet(keyset, indexColumnsName, indexColumns)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("DELETE FROM %s %s", tbl, whereClause)
	if _, err := d.db.ExecContext(ctx, query, args...); err != nil {
		return status.Errorf(codes.Internal, "failed to delete: %v", err)
	}
	return nil
}

func (db *database) CreateTable(ctx context.Context, stmt *ast.CreateTable) error {
	if _, ok := db.tables[stmt.Name.Name]; ok {
		return fmt.Errorf("duplicated table: %v", stmt.Name.Name)
	}

	t, err := createTableFromAST(stmt)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "CreateTable failed: %v", err)
	}
	db.tables[stmt.Name.Name] = t

	var columnDefs []string
	for _, col := range t.columns {
		s := fmt.Sprintf("  %s %s", col.Name(), col.dbDataType)
		if !col.nullable {
			// Array data type is not supported for now
			// so values for array data type are handled as null always
			if !col.isArray {
				s += " NOT NULL"
			}
		}
		columnDefs = append(columnDefs, s)
	}
	columnDefsQuery := strings.Join(columnDefs, ",\n")
	primaryKeysQuery := strings.Join(t.primaryKey.IndexColumnNames(), ", ")

	query := fmt.Sprintf("CREATE TABLE `%s` (\n%s,\n  PRIMARY KEY (%s)\n)", t.Name, columnDefsQuery, primaryKeysQuery)
	if _, err := db.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create table for %s: %v", t.Name, err)
	}

	return nil
}

func (db *database) CreateIndex(ctx context.Context, stmt *ast.CreateIndex) error {
	table, ok := db.tables[stmt.TableName.Name]
	if !ok {
		return fmt.Errorf("table does not exist: %v", stmt.Name.Name)
	}

	index, err := table.createIndex(stmt)
	if err != nil {
		return err
	}

	idxType := "INDEX"
	if index.unique {
		idxType = "UNIQUE INDEX"
	}
	var columns []string
	names := index.IndexColumnNames()
	dirs := index.IndexColumnDirections()
	if len(names) != len(dirs) {
		return fmt.Errorf("number of column names and direction is not same, names=%d, dirs=%d", len(names), len(dirs))
	}
	for n := range names {
		columns = append(columns, names[n]+" "+dirs[n])
	}
	columnsName := strings.Join(columns, ", ")

	query := fmt.Sprintf("CREATE %s `%s` ON %s (%s)", idxType, index.Name(), table.Name, columnsName)
	if _, err := db.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create index for %s: %v", index.Name(), err)
	}

	return nil
}

func (d *database) Close() error {
	if d.db == nil {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.db.Close(); err != nil {
		return err
	}

	d.db = nil

	return nil
}
