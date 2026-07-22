package parser

import (
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

func Parse(sql string) (parser.Statements, error) {
	stmts, err := parser.Parse(sql); 
	if err != nil{
		return nil, err
	}

	return stmts, nil
}

func IsReturning(stmt parser.Statement) bool {
	if(stmt.AST.StatementType() == tree.Rows){
		return true
	}
	return false
}