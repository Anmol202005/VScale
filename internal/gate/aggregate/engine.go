package aggregate

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"

	pb "github.com/Anmol202005/VScale/proto/tablet"
)



type Direction int

const (
	Asc Direction = iota
	Desc
)

type OrderColumn struct {
	Name      string
	Direction Direction
}

type AggType int

const (
	AggCount AggType = iota
	AggSum
	AggMax
	AggMin
	AggAvg
)

type SelectExprInfo struct {
	IsAggregate bool
	AggType     AggType
	AggColName  string 
	Alias       string 
	RawExpr     string 
}

type QueryPlan struct {
	Distinct    bool
	OrderBy     []OrderColumn
	Limit       int64 
	Offset      int64
	Selects     []SelectExprInfo
	HasAgg      bool
	IsCountStar bool 
}



func (p *QueryPlan) NeedsShardRewrite() bool {
	return len(p.OrderBy) > 0 || p.Limit >= 0 || p.Offset > 0 || p.Distinct
}



func StripForScatter(sql string) (string, error) {
	stmts, err := parser.Parse(sql)
	if err != nil || len(stmts) == 0 {
		return sql, nil
	}
	stmt := stmts[0]

	sel, ok := stmt.AST.(*tree.Select)
	if !ok {
		return sql, nil
	}
	clause, ok := sel.Select.(*tree.SelectClause)
	if !ok {
		return sql, nil
	}

	strippedClause := &tree.SelectClause{
		Exprs:    make(tree.SelectExprs, len(clause.Exprs)),
		From:     clause.From,
		Where:    clause.Where,
		GroupBy:  clause.GroupBy,
		Having:   clause.Having,
		Window:   clause.Window,
	}
	for i, e := range clause.Exprs {
		strippedClause.Exprs[i] = e
	}

	stripped := &tree.Select{
		With:    sel.With,
		Select:  strippedClause,
		OrderBy: nil,
		Limit:   nil,
	}

	ctx := tree.NewFmtCtx(tree.FmtSimple)
	stripped.Format(ctx)
	return ctx.CloseAndGetString(), nil
}



func ExtractPlan(sql string) (*QueryPlan, error) {
	stmts, err := parser.Parse(sql)
	if err != nil || len(stmts) == 0 {
		return &QueryPlan{Limit: -1}, nil
	}
	stmt := stmts[0]

	sel, ok := stmt.AST.(*tree.Select)
	if !ok {
		return &QueryPlan{Limit: -1}, nil
	}

	plan := &QueryPlan{Limit: -1}

	clause, ok := sel.Select.(*tree.SelectClause)
	if !ok {
		return plan, nil
	}

	plan.Distinct = clause.Distinct

	plan.Selects = make([]SelectExprInfo, 0, len(clause.Exprs))
	for _, expr := range clause.Exprs {
		info := analyzeSelectExpr(expr)
		plan.Selects = append(plan.Selects, info)
		if info.IsAggregate {
			plan.HasAgg = true
			if info.AggType == AggCount && info.AggColName == "*" {
				plan.IsCountStar = true
			}
		}
	}

	for _, o := range sel.OrderBy {
		col := OrderColumn{
			Name:      o.Expr.String(),
			Direction: Asc,
		}
		if o.Direction == tree.Descending {
			col.Direction = Desc
		}
		plan.OrderBy = append(plan.OrderBy, col)
	}

	if sel.Limit != nil {
		if sel.Limit.Count != nil {
			plan.Limit = int64(evalIntExpr(sel.Limit.Count))
		}
		if sel.Limit.Offset != nil {
			plan.Offset = int64(evalIntExpr(sel.Limit.Offset))
		}
	}

	return plan, nil
}

func analyzeSelectExpr(se tree.SelectExpr) SelectExprInfo {
	info := SelectExprInfo{
		RawExpr: se.Expr.String(),
		Alias:   string(se.As),
	}

	funcExpr, ok := se.Expr.(*tree.FuncExpr)
	if !ok {
		return info
	}

	name := strings.ToUpper(funcExpr.Func.String())
	switch name {
	case "COUNT":
		info.IsAggregate = true
		info.AggType = AggCount
		info.AggColName = aggColumnName(funcExpr)
	case "SUM":
		info.IsAggregate = true
		info.AggType = AggSum
		info.AggColName = aggColumnName(funcExpr)
	case "MAX":
		info.IsAggregate = true
		info.AggType = AggMax
		info.AggColName = aggColumnName(funcExpr)
	case "MIN":
		info.IsAggregate = true
		info.AggType = AggMin
		info.AggColName = aggColumnName(funcExpr)
	case "AVG":
		info.IsAggregate = true
		info.AggType = AggAvg
		info.AggColName = aggColumnName(funcExpr)
	}

	return info
}

func aggColumnName(fe *tree.FuncExpr) string {
	if len(fe.Exprs) == 0 {
		return "*"
	}
	return fe.Exprs[0].String()
}

func evalIntExpr(e tree.Expr) int {
	switch v := e.(type) {
	case *tree.NumVal:
		n, _ := strconv.Atoi(v.OrigString())
		return n
	default:
		return 0
	}
}



func MergeScatterResponses(plan *QueryPlan, resp *pb.QueryResponse) (*pb.QueryResponse, error) {
	if plan == nil {
		plan = &QueryPlan{Limit: -1}
	}

	allRows, cols, err := flattenResults(resp)
	if err != nil {
		return nil, err
	}


	
	if plan.Distinct {
		allRows = applyDistinct(cols, allRows)
	}

	if plan.HasAgg {
		aggRow, err := applyAggregation(plan, cols, allRows)
		if err != nil {
			return nil, err
		}
		allRows = [][]interface{}{aggRow}
	}

	if len(plan.OrderBy) > 0 && len(allRows) > 1 {
		if err := sortRows(cols, allRows, plan.OrderBy); err != nil {
			return nil, err
		}
	}

	if plan.Limit >= 0 || plan.Offset > 0 {
		allRows = applyLimitOffset(allRows, plan.Offset, plan.Limit)
	}

	merged := []*pb.QueryResult{{
		Sql:     "",
		Columns: cols,
	}}
	for _, row := range allRows {
		pbRow := &pb.Row{}
		for _, v := range row {
			pbRow.Values = append(pbRow.Values, fmt.Sprintf("%v", v))
		}
		merged[0].Rows = append(merged[0].Rows, pbRow)
	}
	merged[0].RowsAffected = int64(len(allRows))

	return &pb.QueryResponse{Results: merged}, nil
}


func flattenResults(resp *pb.QueryResponse) (rows [][]interface{}, cols []string, err error) {
	if len(resp.Results) == 0 {
		return nil, nil, nil
	}
	cols = resp.Results[0].Columns
	for _, qr := range resp.Results {
		if len(qr.Columns) > 0 && len(cols) > 0 && !slicesEqual(qr.Columns, cols) {
			
		}
		for _, r := range qr.Rows {
			row := make([]interface{}, len(r.Values))
			for i, v := range r.Values {
				row[i] = inferType(v)
			}
			rows = append(rows, row)
		}
	}
	return rows, cols, nil
}

func inferType(s string) interface{} {
	if s == "NULL" || s == "null" || s == "" {
		return nil
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}
	return s
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}



func applyDistinct(cols []string, rows [][]interface{}) [][]interface{} {
	seen := make(map[string]bool)
	out := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		key := rowKey(row)
		if !seen[key] {
			seen[key] = true
			out = append(out, row)
		}
	}
	return out
}

func rowKey(row []interface{}) string {
	parts := make([]string, len(row))
	for i, v := range row {
		if v == nil {
			parts[i] = "\x00NULL\x00"
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return strings.Join(parts, "\x01")
}



func applyAggregation(plan *QueryPlan, cols []string, rows [][]interface{}) ([]interface{}, error) {
	aggRow := make([]interface{}, len(plan.Selects))

	

	for colIdx, sel := range plan.Selects {
		if !sel.IsAggregate {
			
			aggRow[colIdx] = sel.RawExpr
			continue
		}

		

		srcIdx := colIdx
		if srcIdx >= len(cols) {
			
			aggRow[colIdx] = nil
			continue
		}

		switch sel.AggType {
		case AggCount:

			aggRow[colIdx] = aggSumInt64(rows, srcIdx)
		case AggSum:
			aggRow[colIdx] = aggSum(rows, srcIdx)
		case AggMax:
			aggRow[colIdx] = aggMax(rows, srcIdx)
		case AggMin:
			aggRow[colIdx] = aggMin(rows, srcIdx)
		case AggAvg:

			

			aggRow[colIdx] = bestEffortAvg(rows, srcIdx)
		}
	}

	return aggRow, nil
}

func aggSumInt64(rows [][]interface{}, colIdx int) int64 {
	var sum int64
	for _, r := range rows {
		if colIdx < 0 || colIdx >= len(r) || r[colIdx] == nil {
			continue
		}
		switch v := r[colIdx].(type) {
		case int64:
			sum += v
		case int:
			sum += int64(v)
		case float64:
			sum += int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				sum += i
			}
		}
	}
	return sum
}

func aggSum(rows [][]interface{}, colIdx int) interface{} {
	var sum float64
	have := false
	for _, r := range rows {
		if colIdx < 0 || colIdx >= len(r) {
			continue
		}
		v := toFloat64(r[colIdx])
		if !math.IsNaN(v) {
			sum += v
			have = true
		}
	}
	if !have {
		return nil
	}
	return sum
}

func aggMax(rows [][]interface{}, colIdx int) interface{} {
	var max interface{}
	var maxComp comparableValue
	have := false
	for _, r := range rows {
		if colIdx < 0 || colIdx >= len(r) {
			continue
		}
		v := r[colIdx]
		if v == nil {
			continue
		}
		cv := newComparable(v)
		if !have || cv.greaterThan(maxComp) {
			max = v
			maxComp = cv
			have = true
		}
	}
	return max
}

func aggMin(rows [][]interface{}, colIdx int) interface{} {
	var min interface{}
	var minComp comparableValue
	have := false
	for _, r := range rows {
		if colIdx < 0 || colIdx >= len(r) {
			continue
		}
		v := r[colIdx]
		if v == nil {
			continue
		}
		cv := newComparable(v)
		if !have || cv.lessThan(minComp) {
			min = v
			minComp = cv
			have = true
		}
	}
	return min
}



func bestEffortAvg(rows [][]interface{}, colIdx int) interface{} {
	for _, r := range rows {
		if colIdx < 0 || colIdx >= len(r) || r[colIdx] == nil {
			continue
		}
		v := toFloat64(r[colIdx])
		if !math.IsNaN(v) {
			return v
		}
	}
	return nil
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int64:
		return float64(val)
	case float64:
		return val
	case int:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return math.NaN()
	}
}



type comparableValue struct {
	i  int64
	f  float64
	s  string
	nilVal bool
	kind int 
}

func newComparable(v interface{}) comparableValue {
	if v == nil {
		return comparableValue{nilVal: true, kind: 3}
	}
	switch val := v.(type) {
	case int64:
		return comparableValue{i: val, kind: 0}
	case int:
		return comparableValue{i: int64(val), kind: 0}
	case float64:
		return comparableValue{f: val, kind: 1}
	case string:
		return comparableValue{s: val, kind: 2}
	default:
		s := fmt.Sprintf("%v", val)
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return comparableValue{i: i, kind: 0}
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return comparableValue{f: f, kind: 1}
		}
		return comparableValue{s: s, kind: 2}
	}
}

func (c comparableValue) greaterThan(other comparableValue) bool {
	if c.nilVal {
		return false
	}
	if other.nilVal {
		return true
	}
	if c.kind != other.kind {
		
		if (c.kind == 0 || c.kind == 1) && (other.kind == 0 || other.kind == 1) {
			return c.asFloat() > other.asFloat()
		}
		
		return c.asString() > other.asString()
	}
	switch c.kind {
	case 0:
		return c.i > other.i
	case 1:
		return c.f > other.f
	case 2:
		return c.s > other.s
	}
	return false
}

func (c comparableValue) lessThan(other comparableValue) bool {
	if c.nilVal {
		return false
	}
	if other.nilVal {
		return true
	}
	if c.kind != other.kind {
		if (c.kind == 0 || c.kind == 1) && (other.kind == 0 || other.kind == 1) {
			return c.asFloat() < other.asFloat()
		}
		return c.asString() < other.asString()
	}
	switch c.kind {
	case 0:
		return c.i < other.i
	case 1:
		return c.f < other.f
	case 2:
		return c.s < other.s
	}
	return false
}

func (c comparableValue) equal(other comparableValue) bool {
	if c.nilVal && other.nilVal {
		return true
	}
	if c.nilVal || other.nilVal {
		return false
	}
	if c.kind != other.kind {
		if (c.kind == 0 || c.kind == 1) && (other.kind == 0 || other.kind == 1) {
			return c.asFloat() == other.asFloat()
		}
		return c.asString() == other.asString()
	}
	switch c.kind {
	case 0:
		return c.i == other.i
	case 1:
		return c.f == other.f
	case 2:
		return c.s == other.s
	}
	return false
}

func (c comparableValue) asFloat() float64 {
	if c.nilVal {
		return math.NaN()
	}
	switch c.kind {
	case 0:
		return float64(c.i)
	case 1:
		return c.f
	default:
		f, _ := strconv.ParseFloat(c.s, 64)
		return f
	}
}

func (c comparableValue) asString() string {
	if c.nilVal {
		return ""
	}
	return fmt.Sprintf("%v", c)
}

func (c comparableValue) String() string {
	if c.nilVal {
		return "NULL"
	}
	switch c.kind {
	case 0:
		return strconv.FormatInt(c.i, 10)
	case 1:
		return strconv.FormatFloat(c.f, 'f', -1, 64)
	case 2:
		return c.s
	}
	return ""
}



func sortRows(cols []string, rows [][]interface{}, orderBy []OrderColumn) error {
	
	colMap := make(map[string]int, len(cols))
	for i, c := range cols {
		colMap[strings.ToLower(c)] = i
	}

	orderIdx := make([]int, len(orderBy))
	for i, o := range orderBy {
		idx, ok := colMap[strings.ToLower(o.Name)]
		if !ok {
			
			idx, ok = colMap[o.Name]
			if !ok {
				return fmt.Errorf("aggregate: ORDER BY column %q not found in result set", o.Name)
			}
		}
		orderIdx[i] = idx
	}

	less := func(i, j int) bool {
		for k, o := range orderBy {
			idx := orderIdx[k]
			vi := newComparable(rows[i][idx])
			vj := newComparable(rows[j][idx])
			if !vi.equal(vj) {
				if o.Direction == Desc {
					return vj.lessThan(vi)
				}
				return vi.lessThan(vj)
			}
		}
		return false
	}

	quickSort(rows, less)
	return nil
}

func quickSort(rows [][]interface{}, less func(i, j int) bool) {
	if len(rows) < 2 {
		return
	}
	qSort(rows, less, 0, len(rows)-1)
}

func qSort(rows [][]interface{}, less func(i, j int) bool, lo, hi int) {
	if lo < hi {
		p := partition(rows, less, lo, hi)
		qSort(rows, less, lo, p-1)
		qSort(rows, less, p+1, hi)
	}
}

func partition(rows [][]interface{}, less func(i, j int) bool, lo, hi int) int {
	i := lo
	for j := lo; j < hi; j++ {
		if less(j, hi) {
			rows[i], rows[j] = rows[j], rows[i]
			i++
		}
	}
	rows[i], rows[hi] = rows[hi], rows[i]
	return i
}



func applyLimitOffset(rows [][]interface{}, offset, limit int64) [][]interface{} {
	n := int64(len(rows))
	start := offset
	if start > n {
		start = n
	}
	end := n
	if limit >= 0 {
		end = start + limit
		if end > n {
			end = n
		}
	}
	return rows[start:end]
}
