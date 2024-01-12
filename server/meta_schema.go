// Copyright 2020 Masahiro Sano
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
	"github.com/cloudspannerecosystem/memefish/ast"
)

var metaTablesMap = map[string]string{
	"INFORMATION_SCHEMA.SCHEMATA":       "__INFORMATION_SCHEMA__SCHEMATA",
	"INFORMATION_SCHEMA.TABLES":         "__INFORMATION_SCHEMA__TABLES",
	"INFORMATION_SCHEMA.COLUMNS":        "__INFORMATION_SCHEMA__COLUMNS",
	"INFORMATION_SCHEMA.INDEXES":        "__INFORMATION_SCHEMA__INDEXES",
	"INFORMATION_SCHEMA.INDEX_COLUMNS":  "__INFORMATION_SCHEMA__INDEX_COLUMNS",
	"INFORMATION_SCHEMA.COLUMN_OPTIONS": "__INFORMATION_SCHEMA__COLUMN_OPTIONS",
}

var metaTablesReverseMap = map[string][]string{
	"__INFORMATION_SCHEMA__SCHEMATA":       []string{"INFORMATION_SCHEMA", "SCHEMATA"},
	"__INFORMATION_SCHEMA__TABLES":         []string{"INFORMATION_SCHEMA", "TABLES"},
	"__INFORMATION_SCHEMA__COLUMNS":        []string{"INFORMATION_SCHEMA", "COLUMNS"},
	"__INFORMATION_SCHEMA__INDEXES":        []string{"INFORMATION_SCHEMA", "INDEXES"},
	"__INFORMATION_SCHEMA__INDEX_COLUMNS":  []string{"INFORMATION_SCHEMA", "INDEX_COLUMNS"},
	"__INFORMATION_SCHEMA__COLUMN_OPTIONS": []string{"INFORMATION_SCHEMA", "COLUMN_OPTIONS"},
}

var metaTables = []*ast.CreateTable{
	{
		Name: &ast.Ident{Name: "__INFORMATION_SCHEMA__SCHEMATA"},
		Columns: []*ast.ColumnDef{
			{
				Name:    &ast.Ident{Name: "CATALOG_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "SCHEMA_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "EFFECTIVE_TIMESTAMP"},
				Type:    &ast.ScalarSchemaType{Name: ast.Int64TypeName},
				NotNull: false,
			},
		},
		PrimaryKeys: []*ast.IndexKey{
			{
				Name: &ast.Ident{Name: "CATALOG_NAME"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "SCHEMA_NAME"},
				Dir:  ast.DirectionAsc,
			},
		},
	},
	{
		Name: &ast.Ident{Name: "__INFORMATION_SCHEMA__TABLES"},
		Columns: []*ast.ColumnDef{
			{
				Name:    &ast.Ident{Name: "TABLE_CATALOG"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_SCHEMA"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "PARENT_TABLE_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
			{
				Name:    &ast.Ident{Name: "ON_DELETE_ACTION"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
			{
				Name:    &ast.Ident{Name: "SPANNER_STATE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
		},
		PrimaryKeys: []*ast.IndexKey{
			{
				Name: &ast.Ident{Name: "TABLE_CATALOG"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_SCHEMA"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_NAME"},
				Dir:  ast.DirectionAsc,
			},
		},
	},
	{
		Name: &ast.Ident{Name: "__INFORMATION_SCHEMA__COLUMNS"},
		Columns: []*ast.ColumnDef{
			{
				Name:    &ast.Ident{Name: "TABLE_CATALOG"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_SCHEMA"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "COLUMN_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "ORDINAL_POSITION"},
				Type:    &ast.ScalarSchemaType{Name: ast.Int64TypeName},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "COLUMN_DEFAULT"},
				Type:    &ast.SizedSchemaType{Name: ast.BytesTypeName, Max: true},
				NotNull: false,
			},
			{
				Name:    &ast.Ident{Name: "DATA_TYPE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
			{
				Name:    &ast.Ident{Name: "IS_NULLABLE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
			{
				Name:    &ast.Ident{Name: "SPANNER_TYPE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
		},
		PrimaryKeys: []*ast.IndexKey{
			{
				Name: &ast.Ident{Name: "TABLE_CATALOG"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_SCHEMA"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_NAME"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "COLUMN_NAME"},
				Dir:  ast.DirectionAsc,
			},
		},
	},
	{
		Name: &ast.Ident{Name: "__INFORMATION_SCHEMA__INDEXES"},
		Columns: []*ast.ColumnDef{
			{
				Name:    &ast.Ident{Name: "TABLE_CATALOG"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_SCHEMA"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "INDEX_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "INDEX_TYPE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "PARENT_TABLE_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "IS_UNIQUE"},
				Type:    &ast.ScalarSchemaType{Name: ast.BoolTypeName},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "IS_NULL_FILTERED"},
				Type:    &ast.ScalarSchemaType{Name: ast.BoolTypeName},
				NotNull: true,
			},
			{
				Name: &ast.Ident{Name: "INDEX_STATE"},
				Type: &ast.SizedSchemaType{Name: ast.StringTypeName, Size: &ast.IntLiteral{Value: "100", Base: 10}},
				// NotNull: true,
				NotNull: false, // INFORMATION_SCHEMA.COLUMNS reports this column is non-nullable, but it returns NULL...
			},
			{
				Name:    &ast.Ident{Name: "SPANNER_IS_MANAGED"},
				Type:    &ast.ScalarSchemaType{Name: ast.BoolTypeName},
				NotNull: true,
			},
		},
		PrimaryKeys: []*ast.IndexKey{
			{
				Name: &ast.Ident{Name: "TABLE_CATALOG"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_SCHEMA"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_NAME"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "INDEX_NAME"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "INDEX_TYPE"},
				Dir:  ast.DirectionAsc,
			},
		},
	},
	{
		Name: &ast.Ident{Name: "__INFORMATION_SCHEMA__INDEX_COLUMNS"},
		Columns: []*ast.ColumnDef{
			{
				Name:    &ast.Ident{Name: "TABLE_CATALOG"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_SCHEMA"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "INDEX_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "INDEX_TYPE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "COLUMN_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "ORDINAL_POSITION"},
				Type:    &ast.ScalarSchemaType{Name: ast.Int64TypeName},
				NotNull: false,
			},
			{
				Name:    &ast.Ident{Name: "COLUMN_ORDERING"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
			{
				Name:    &ast.Ident{Name: "IS_NULLABLE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
			{
				Name:    &ast.Ident{Name: "SPANNER_TYPE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: false,
			},
		},
		PrimaryKeys: []*ast.IndexKey{
			{
				Name: &ast.Ident{Name: "TABLE_CATALOG"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_SCHEMA"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_NAME"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "INDEX_NAME"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "INDEX_TYPE"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "COLUMN_NAME"},
				Dir:  ast.DirectionAsc,
			},
		},
	},

	{
		Name: &ast.Ident{Name: "__INFORMATION_SCHEMA__COLUMN_OPTIONS"},
		Columns: []*ast.ColumnDef{
			{
				Name:    &ast.Ident{Name: "TABLE_CATALOG"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_SCHEMA"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "TABLE_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "COLUMN_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "OPTION_NAME"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "OPTION_TYPE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
			{
				Name:    &ast.Ident{Name: "OPTION_VALUE"},
				Type:    &ast.SizedSchemaType{Name: ast.StringTypeName, Max: true},
				NotNull: true,
			},
		},
		PrimaryKeys: []*ast.IndexKey{
			{
				Name: &ast.Ident{Name: "TABLE_CATALOG"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_SCHEMA"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "TABLE_NAME"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "COLUMN_NAME"},
				Dir:  ast.DirectionAsc,
			},
			{
				Name: &ast.Ident{Name: "OPTION_NAME"},
				Dir:  ast.DirectionAsc,
			},
		},
	},
}
