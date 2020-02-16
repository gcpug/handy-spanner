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
	"strconv"

	"github.com/MakeNowJust/memefish/pkg/ast"
)

type Table struct {
	ast  *ast.CreateTable
	Name string

	columns    []*Column
	columnsMap map[string]*Column

	primaryKey *TableIndex
	index      []*TableIndex
}

func newTable() *Table {
	return &Table{
		columnsMap: make(map[string]*Column),
	}
}

func (t *Table) TableIndex(idx string) (*TableIndex, bool) {
	if idx == "" {
		return t.primaryKey, true
	}

	for _, index := range t.index {
		if index.Name() == idx {
			return index, true
		}
	}

	return nil, false
}

func (t *Table) TableView() *TableView {
	return createTableViewFromTable(t, "")
}

func (t *Table) TableViewWithAlias(alias string) *TableView {
	return createTableViewFromTable(t, alias)
}

// NonNullableColumnsExist checks non nullable columns exist in the spciefied columns.
// It returns true and the columns if non nullable columns exist.
func (t *Table) NonNullableColumnsExist(columns []string) (bool, []string) {
	usedColumns := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		usedColumns[name] = struct{}{}
	}

	var noExsitNonNullableColumns []string
	for _, c := range t.columns {
		if c.nullable {
			continue
		}

		n := c.Name()
		if _, ok := usedColumns[n]; !ok {
			noExsitNonNullableColumns = append(noExsitNonNullableColumns, n)
		}
	}

	if len(noExsitNonNullableColumns) > 0 {
		return true, noExsitNonNullableColumns
	}

	return false, nil
}

func createTableFromAST(stmt *ast.CreateTable) (*Table, error) {
	t := newTable()
	t.Name = stmt.Name.Name
	t.ast = stmt

	for _, col := range stmt.Columns {
		t.addColumn(col)
	}
	t.reorderColumnPosition()

	if err := t.setPrimaryKeys(stmt.PrimaryKeys); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Table) addColumn(col *ast.ColumnDef) {
	column := newColumn(col)
	t.columns = append(t.columns, column)
	t.columnsMap[column.Name()] = column
}

func (t *Table) reorderColumnPosition() {
	for i := range t.columns {
		t.columns[i].setPosition(i + 1)
	}
}

func (t *Table) setPrimaryKeys(pkey []*ast.IndexKey) error {
	index, err := createPrimaryKey(t, pkey)
	if err != nil {
		return err
	}
	t.primaryKey = index

	for i, col := range index.IndexColumns() {
		col.markPrimaryKey(i + 1)
	}

	return nil
}

func (t *Table) createIndex(stmt *ast.CreateIndex) (*TableIndex, error) {
	idx, err := createTableIndexFromAST(t, stmt)
	if err != nil {
		return nil, err
	}
	t.index = append(t.index, idx)
	return idx, nil
}

func (t *Table) getColumn(name string) (*Column, error) {
	c, ok := t.columnsMap[name]
	if !ok {
		return nil, fmt.Errorf("Column not found: %s", name)
	}
	return c, nil
}

func (t *Table) getColumnsByName(names []string) ([]*Column, error) {
	columns := make([]*Column, len(names))
	for i, name := range names {
		c, ok := t.columnsMap[name]
		if !ok {
			return nil, fmt.Errorf("Column not found: %s", name)
		}
		columns[i] = c
	}

	return columns, nil
}

type Column struct {
	ast *ast.ColumnDef
	pos int

	alias string

	valueType  ValueType
	dbDataType dbDataType

	nullable bool
	isArray  bool
	isSized  bool
	isMax    bool
	size     int64

	allowCommitTimestamp bool

	isPrimaryKey  bool
	primaryKeyPos int
}

func (c *Column) Name() string {
	return c.ast.Name.Name
}

func (c *Column) Alias() string {
	if c.alias != "" {
		return c.alias
	}
	return c.ast.Name.Name
}

func (c *Column) setPosition(pos int) {
	c.pos = pos
}

func (c *Column) markPrimaryKey(pos int) {
	c.isPrimaryKey = true
	c.primaryKeyPos = pos
}

type columnType struct {
	dataType ast.ScalarTypeName

	isArray bool
	isSized bool
	isMax   bool
	size    int64
}

type dbDataType string

const (
	DBDTInteger dbDataType = "INTEGER"
	DBDTReal    dbDataType = "REAL"
	DBDTText    dbDataType = "TEXT"
	DBDTBlob    dbDataType = "BLOB"
	DBDTJson    dbDataType = "JSON"
)

func (ct columnType) SqliteDataType() string {
	switch ct.dataType {
	case ast.BoolTypeName:
		return "INTEGER"
	case ast.Int64TypeName:
		return "INTEGER"
	case ast.Float64TypeName:
		return "REAL"
	case ast.StringTypeName:
		return "TEXT"
	case ast.BytesTypeName:
		return "BLOB"
	case ast.DateTypeName:
		return "TEXT"
	case ast.TimestampTypeName:
		return "TEXT"
	}

	panic(fmt.Sprintf("unknown data type: %s", ct.dataType))
}

func newColumn(def *ast.ColumnDef) *Column {
	ct := toColumnType(def.Type)
	vt := toValueType(def.Type)

	var dbdt dbDataType
	switch vt.Code {
	default:
		panic(fmt.Sprintf("unknown value type %#v", vt))
	case TCBool:
		dbdt = DBDTInteger
	case TCInt64:
		dbdt = DBDTInteger
	case TCFloat64:
		dbdt = DBDTReal
	case TCString:
		dbdt = DBDTText
	case TCBytes:
		dbdt = DBDTBlob
	case TCDate:
		dbdt = DBDTText
	case TCTimestamp:
		dbdt = DBDTText
	case TCArray:
		dbdt = DBDTJson
	}

	var allowCommitTimestamp bool
	if def.Options != nil {
		allowCommitTimestamp = def.Options.AllowCommitTimestamp
	}

	return &Column{
		ast: def,

		valueType:  vt,
		dbDataType: dbdt,

		nullable: !def.NotNull,
		isArray:  ct.isArray,
		isSized:  ct.isSized,
		isMax:    ct.isMax,
		size:     ct.size,

		allowCommitTimestamp: allowCommitTimestamp,
	}
}

func astTypeToTypeCode(astTypeName ast.ScalarTypeName) TypeCode {
	switch astTypeName {
	case ast.BoolTypeName:
		return TCBool
	case ast.Int64TypeName:
		return TCInt64
	case ast.Float64TypeName:
		return TCFloat64
	case ast.StringTypeName:
		return TCString
	case ast.BytesTypeName:
		return TCBytes
	case ast.DateTypeName:
		return TCDate
	case ast.TimestampTypeName:
		return TCTimestamp
	default:
		panic("unknown type")
	}
}

func toValueType(t ast.SchemaType) ValueType {
	switch v := t.(type) {
	case *ast.ScalarSchemaType:
		return ValueType{Code: astTypeToTypeCode(v.Name)}

	case *ast.SizedSchemaType:
		return ValueType{Code: astTypeToTypeCode(v.Name)}

	case *ast.ArraySchemaType:
		arrType := toValueType(v.Item)
		return ValueType{
			Code:      TCArray,
			ArrayType: &arrType,
		}
	default:
		panic(fmt.Sprintf("unknow type %v", t))
	}
}

func schemaTypetoTypString(t ast.SchemaType) string {
	switch v := t.(type) {
	case *ast.ScalarSchemaType:
		return astTypeToTypeCode(v.Name).String()

	case *ast.SizedSchemaType:
		typ := astTypeToTypeCode(v.Name).String()
		size := "MAX"
		if !v.Max {
			intLit := v.Size.(*ast.IntLiteral)
			size = intLit.Value // TODO: respect base?
		}
		return fmt.Sprintf("%s(%s)", typ, size)

	case *ast.ArraySchemaType:
		arrType := schemaTypetoTypString(v.Item)
		return fmt.Sprintf("ARRAY<%s>", arrType)
	default:
		panic(fmt.Sprintf("unknow type %v", t))
	}
}

func toColumnType(t ast.SchemaType) columnType {
	switch v := t.(type) {
	case *ast.ScalarSchemaType:
		return columnType{
			dataType: v.Name,
		}
	case *ast.SizedSchemaType:
		if v.Max {
			return columnType{
				dataType: v.Name,
				isSized:  true,
				isMax:    true,
			}
		}

		intLit, ok := v.Size.(*ast.IntLiteral)
		if !ok {
			panic(fmt.Sprintf("expected IntLiteral but %v", v.Size))
		}

		n, err := strconv.ParseInt(intLit.Value, intLit.Base, 64)
		if err != nil {
			panic(fmt.Sprintf("cannot parse IntLiteral: %v", intLit))
		}

		return columnType{
			dataType: v.Name,
			isSized:  true,
			isMax:    false,
			size:     n,
		}

	case *ast.ArraySchemaType:
		ct := toColumnType(v.Item)
		ct.isArray = true
		return ct

	default:
		panic(fmt.Sprintf("unknow type %v", t))
	}

}

type TableIndex struct {
	ast          *ast.CreateIndex
	astIndexKeys []*ast.IndexKey

	name  string
	table *Table

	unique       bool
	nullFiltered bool

	columnsRef      []*Column
	columnNames     []string
	columnDirctions []string
	storedColumns   map[string]struct{}
}

func createTableIndexFromAST(table *Table, stmt *ast.CreateIndex) (*TableIndex, error) {
	return createTableIndex(table, stmt.Keys, stmt)
}

func createPrimaryKey(table *Table, pkeys []*ast.IndexKey) (*TableIndex, error) {
	return createTableIndex(table, pkeys, nil)
}

func createTableIndex(table *Table, keys []*ast.IndexKey, secondaryIdx *ast.CreateIndex) (*TableIndex, error) {
	columns := make([]*Column, len(keys))
	columnNames := make([]string, len(keys))
	columnDirctions := make([]string, len(keys))
	for i, key := range keys {
		col, ok := table.columnsMap[key.Name.Name]
		if !ok {
			return nil, fmt.Errorf("primary key not found: %s", key.Name.Name)
		}

		columns[i] = col
		columnNames[i] = col.Name()
		dir := string(key.Dir)
		if dir == "" { // work around
			dir = "ASC"
		}
		columnDirctions[i] = dir
	}

	name := "PRIMARY_KEY"
	unique := true
	nullFiltered := false
	storedColumns := make(map[string]struct{})

	if secondaryIdx != nil {
		name = secondaryIdx.Name.Name
		unique = secondaryIdx.Unique
		nullFiltered = secondaryIdx.NullFiltered

		// columns for Index Keys
		for _, c := range columns {
			storedColumns[c.Name()] = struct{}{}
		}

		// secondary index also has primary key columns by default
		for _, name := range table.primaryKey.IndexColumnNames() {
			storedColumns[name] = struct{}{}
		}

		// storing columns
		if secondaryIdx.Storing != nil {
			for _, c := range secondaryIdx.Storing.Columns {
				storedColumns[c.Name] = struct{}{}
			}
		}
	} else {
		// Primry Keys have all columns
		for _, c := range table.columns {
			storedColumns[c.Name()] = struct{}{}
		}
	}

	return &TableIndex{
		ast:          secondaryIdx,
		astIndexKeys: keys,

		name:  name,
		table: table,

		unique:       unique,
		nullFiltered: nullFiltered,

		columnsRef:      columns,
		columnNames:     columnNames,
		storedColumns:   storedColumns,
		columnDirctions: columnDirctions,
	}, nil
}

func (i *TableIndex) Name() string {
	return i.name
}

func (i *TableIndex) IndexColumns() []*Column {
	return i.columnsRef
}

func (i *TableIndex) IndexColumnNames() []string {
	return i.columnNames
}

func (i *TableIndex) IndexColumnDirections() []string {
	return i.columnDirctions
}

func (i *TableIndex) HasColumn(c string) bool {
	_, ok := i.storedColumns[c]
	return ok
}

type TableView struct {
	ResultItems    []ResultItem
	ResultItemsMap map[string]ResultItem
	ambiguous      map[string]struct{}
}

func (v *TableView) AllItems() []ResultItem {
	return v.ResultItems
}

func (v *TableView) Get(id string) (ResultItem, bool, bool) {
	if _, ok := v.ambiguous[id]; ok {
		return ResultItem{}, true, false
	}
	item, ok := v.ResultItemsMap[id]
	if !ok {
		return ResultItem{}, false, true
	}
	return item, false, false
}

func (v *TableView) ToStruct() *StructType {
	names := make([]string, len(v.ResultItems))
	vts := make([]*ValueType, len(v.ResultItems))
	for i := range v.ResultItems {
		names[i] = v.ResultItems[i].Name
		vts[i] = &v.ResultItems[i].ValueType
	}

	return &StructType{
		FieldNames: names,
		FieldTypes: vts,
		IsTable:    true,
	}
}

func createTableViewFromItems(items1 []ResultItem, items2 []ResultItem) *TableView {
	newItems := make([]ResultItem, 0, len(items1)+len(items2))
	newItemsMap := make(map[string]ResultItem, len(items1)+len(items2))
	ambiguous := make(map[string]struct{})

	for _, items := range [][]ResultItem{items1, items2} {
		for _, item := range items {
			newItems = append(newItems, item)
			if item.Name != "" {
				_, ok := newItemsMap[item.Name]
				if ok {
					ambiguous[item.Name] = struct{}{}
				} else {
					newItemsMap[item.Name] = item
				}
			}
		}
	}

	return &TableView{
		ResultItems:    newItems,
		ResultItemsMap: newItemsMap,
		ambiguous:      ambiguous,
	}
}

func createTableViewFromTable(table *Table, alias string) *TableView {
	items := make([]ResultItem, 0, len(table.columns))
	itemsMap := make(map[string]ResultItem, len(table.columns))
	for _, column := range table.columns {
		item := createResultItemFromColumn(column)
		// if alias specified, add the alias to Expr
		if alias != "" {
			item.Expr.Raw = fmt.Sprintf("%s.%s", QuoteString(alias), item.Expr.Raw)
		}
		items = append(items, item)
		itemsMap[column.Name()] = item
	}
	return &TableView{
		ResultItems:    items,
		ResultItemsMap: itemsMap,
	}
}

type ResultItem struct {
	Name      string
	ValueType ValueType

	Expr Expr
}

func createResultItemFromColumn(column *Column) ResultItem {
	return ResultItem{
		Name:      column.Name(),
		ValueType: column.valueType,
		Expr: Expr{
			Raw:       QuoteString(column.Name()),
			ValueType: column.valueType,
		},
	}
}
