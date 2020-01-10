package server

import (
	"github.com/MakeNowJust/memefish/pkg/ast"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func FindParentDDL(stmt ast.DDL, ddls []ast.DDL) (*ast.CreateTable, error) {
	switch val := stmt.(type) {
	case *ast.CreateTable:
		if val.Cluster != nil {
			for _, subStmt := range ddls {
				switch subVal := subStmt.(type) {
				case *ast.CreateTable:
					if subVal.Name.Name == val.Cluster.TableName.Name {
						return subVal, nil
					}
				}
			}

			return nil, status.Error(codes.NotFound,"could not find parent table definition for interleave statement")
		}
		return nil, nil
	default:
		return nil, nil
	}
}
