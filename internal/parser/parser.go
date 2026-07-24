package parser

import (
	"fmt"
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

func GetTableName(stmt parser.Statement) (string, error) {
	switch s := stmt.AST.(type) {
	case *tree.Select:
		clause, ok := s.Select.(*tree.SelectClause)
		if !ok || len(clause.From.Tables) == 0 {
			return "", fmt.Errorf("no table in select")
		}
		if tbl, ok := clause.From.Tables[0].(*tree.AliasedTableExpr); ok {
			if name, ok := tbl.Expr.(*tree.TableName); ok {
				return name.Table(), nil
			}
		}
		return "", fmt.Errorf("unsupported from clause")

	case *tree.Insert:
		if name, ok := s.Table.(*tree.TableName); ok {
			return name.Table(), nil
		}
		return "", fmt.Errorf("unsupported insert target")

	case *tree.Update:
		if name, ok := s.Table.(*tree.TableName); ok {
			return name.Table(), nil
		}
		return "", fmt.Errorf("unsupported update target")

	case *tree.Delete:
		if name, ok := s.Table.(*tree.TableName); ok {
			return name.Table(), nil
		}
		return "", fmt.Errorf("unsupported delete target")

	default:
		return "", fmt.Errorf("unsupported statement type")
	}
}

func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

func equalityInExpr(expr tree.Expr, column string) (string, bool) {
	switch e := expr.(type) {
	case *tree.AndExpr:
		if val, ok := equalityInExpr(e.Left, column); ok {
			return val, true
		}
		return equalityInExpr(e.Right, column)

	case *tree.ComparisonExpr:
		if e.Operator != tree.EQ {
			return "", false
		}
		colName, ok := e.Left.(*tree.UnresolvedName)
		if !ok || colName.String() != column {
			return "", false
		}
		return stripQuotes(e.Right.String()), true

	default:
		return "", false
	}
}

func EqualityValue(stmt parser.Statement, column string) (string, bool) {
	switch s := stmt.AST.(type) {

	case *tree.Select:
		clause, ok := s.Select.(*tree.SelectClause)
		if !ok || clause.Where == nil {
			return "", false
		}
		return equalityInExpr(clause.Where.Expr, column)

	case *tree.Update:
		if s.Where == nil {
			return "", false
		}
		return equalityInExpr(s.Where.Expr, column)

	case *tree.Delete:
		if s.Where == nil {
			return "", false
		}
		return equalityInExpr(s.Where.Expr, column)

	case *tree.Insert:
		colIndex := -1
		for i, c := range s.Columns {
			if string(c) == column {
				colIndex = i
				break
			}
		}
		if colIndex == -1 {
			return "", false
		}
		valuesSel, ok := s.Rows.Select.(*tree.ValuesClause)
		if !ok || len(valuesSel.Rows) != 1 {
			return "", false
		}
		row := valuesSel.Rows[0]
		if colIndex >= len(row) {
			return "", false
		}
		return stripQuotes(row[colIndex].String()), true

	default:
		return "", false
	}
}
