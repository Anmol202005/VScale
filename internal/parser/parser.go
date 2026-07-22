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

func IsBegin(stmt parser.Statement) bool {
	if(stmt.AST.StatementTag() == "BEGIN"){
		return true
	}
	return false
}

func IsCommit(stmt parser.Statement) bool {
	if(stmt.AST.StatementTag() == "COMMIT"){
		return true
	}
	return false
}

func IsRollback(stmt parser.Statement) bool {
	if(stmt.AST.StatementTag() == "ROLLBACK"){
		return true
	}
	return false
}

func IsWrite(stmt parser.Statement) bool {
	return tree.CanWriteData(stmt.AST) || tree.CanModifySchema(stmt.AST)
}