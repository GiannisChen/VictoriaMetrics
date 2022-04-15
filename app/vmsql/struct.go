package vmsql

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/metricsql"
	"strconv"
	"strings"
)

type (
	TagFilter struct {
		Key        string
		Value      string
		IsNegative bool
		IsRegexp   bool
	}

	Function struct {
		FuncName string
		Args     []string
	}

	OrderBy struct {
		SortingKey []string
		IsAsc      bool
	}

	MultiFilters struct {
		TimeFilter *TimeFilter
		AndFilters []*OtherFilter
		OrFilters  []*OtherFilter
	}

	TimeFilter struct {
		Start string
		End   string
		Step  string
	}

	OtherFilter struct {
		Key  string
		Op   string
		Args string
	}

	DeleteFilter struct {
		AndTagFilters []*TagFilter
		OrTagFilters  []*TagFilter
	}
)

// andJoinTagFilters used for delete-op-AND,
func andJoinTagFilters(a *DeleteFilter, b *TagFilter) *DeleteFilter {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		return &DeleteFilter{AndTagFilters: []*TagFilter{b}, OrTagFilters: nil}
	}
	if b == nil {
		return a
	}

	if a.AndTagFilters != nil {
		a.AndTagFilters = append(a.AndTagFilters, b)
	} else {
		a.AndTagFilters = []*TagFilter{b}
	}
	return a
}

// orJoinTagFilters used for delete-op-OR
func orJoinTagFilters(a *DeleteFilter, b *TagFilter) *DeleteFilter {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		return &DeleteFilter{AndTagFilters: []*TagFilter{b}, OrTagFilters: nil}
	}
	if b == nil {
		return a
	}

	if a.OrTagFilters != nil {
		a.OrTagFilters = append(a.OrTagFilters, b)
	} else {
		a.OrTagFilters = []*TagFilter{b}
	}
	return a
}

func andJoinMultiFilters(a *MultiFilters, b *OtherFilter) *MultiFilters {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		return &MultiFilters{TimeFilter: nil, AndFilters: []*OtherFilter{b}, OrFilters: nil}
	}
	if b == nil {
		return a
	}

	if a.AndFilters != nil {
		a.AndFilters = append(a.AndFilters, b)
	} else {
		a.AndFilters = []*OtherFilter{b}
	}
	return a
}

func orJoinMultiFilters(a *MultiFilters, b *OtherFilter) *MultiFilters {
	if a == nil {
		return &MultiFilters{TimeFilter: nil, OrFilters: []*OtherFilter{b}, AndFilters: nil}
	}
	if b == nil {
		return a
	}

	if a.OrFilters != nil {
		a.OrFilters = append(a.OrFilters, b)
	} else {
		a.OrFilters = []*OtherFilter{b}
	}
	return a
}

func TransSelectStatement(stmt *SelectStatement, t *Table) (bool, metricsql.Expr, error) {
	if t.ColMap == nil {
		return false, nil, errors.New("SELECT: empty table map, should contain table's column name")
	}

	columnTagMap := map[string][]*metricsql.LabelFilter{}
	columnValueMap := map[string]metricsql.Expr{}

	// traverse the where TAG-filter first:
	allLabelFilters := []metricsql.LabelFilter{{Label: "table", Value: t.TableName}}
	if stmt.WhereFilter != nil {
		if stmt.WhereFilter.AndFilters != nil && len(stmt.WhereFilter.AndFilters) != 0 {
			for _, filter := range stmt.WhereFilter.AndFilters {
				// TAG is one of the table's columns
				if c, ok := t.ColMap[filter.Key]; ok && c.Tag {
					if _, ok := columnTagMap[filter.Key]; ok {
						if lf := getLabelFilter(filter); lf != nil {
							columnTagMap[filter.Key] = append(columnTagMap[filter.Key], lf)
						} else {
							return false, nil, errors.New(fmt.Sprintf("SELECT: TAG not support %s ops", filter.Op))
						}
					} else {
						if lf := getLabelFilter(filter); lf != nil {
							columnTagMap[filter.Key] = []*metricsql.LabelFilter{lf}
						} else {
							return false, nil, errors.New(fmt.Sprintf("SELECT: TAG not support %s ops", filter.Op))
						}
					}
				} else if !ok {
					return false, nil, errors.New(fmt.Sprintf("SELECT: TAG %s is not in table %s", filter.Key, t.TableName))
				}
			}
		}
		if stmt.WhereFilter.AndFilters != nil && len(stmt.WhereFilter.OrFilters) != 0 {
			for _, filter := range stmt.WhereFilter.OrFilters {
				// TAG is one of the table's columns
				if c, ok := t.ColMap[filter.Key]; ok && c.Tag {
					if _, ok := columnTagMap[filter.Key]; ok {
						// if labelFilter can join with others, otherwise throw errors
						if lf := getLabelFilter(filter); lf != nil {
							for _, labelFilter := range columnTagMap[filter.Key] {
								if labelFilter.IsNegative == lf.IsNegative {
									labelFilter.IsRegexp = true
									labelFilter.Value = fmt.Sprintf("(%s)|(%s)", lf.Value, labelFilter.Value)
									lf = nil
									break
								}
							}
							if lf != nil {
								return false, nil, errors.New(fmt.Sprintf("SELECT: semantic error TAG %s has confused filters", filter.Key))
							}
						} else {
							return false, nil, errors.New(fmt.Sprintf("SELECT: TAG not support %s ops", filter.Op))
						}
					} else { // TAG appear only in OR, cause semantic errors
						return false, nil, errors.New(fmt.Sprintf("SELECT: semantic error TAG %s only appears in OR", filter.Key))
					}
				} else if !ok { // TAG not in this table
					return false, nil, errors.New(fmt.Sprintf("SELECT: TAG %s is not in table %s", filter.Key, t.TableName))
				}
			}
		}
	}

	// generate allLabelFilters
	if len(columnTagMap) != 0 {
		for _, filters := range columnTagMap {
			for _, filter := range filters {
				allLabelFilters = append(allLabelFilters, *filter)
			}
		}
	}
	// traverse the where VALUE-filter then:
	if stmt.WhereFilter != nil {
		if stmt.WhereFilter.AndFilters != nil && len(stmt.WhereFilter.AndFilters) != 0 {
			for _, filter := range stmt.WhereFilter.AndFilters {
				// VALUE is one of the table's columns
				if c, ok := t.ColMap[filter.Key]; ok && !c.Tag {
					if _, ok := columnValueMap[filter.Key]; !ok { // this VALUE never appears
						be, err := getBinaryExpr(filter, allLabelFilters)
						if err != nil {
							return false, nil, err
						}
						if be == nil {
							return false, nil, errors.New(fmt.Sprintf("SELECT: VALUE not support %s ops", filter.Op))
						}
						columnValueMap[filter.Key] = be
					} else { // VALUE already exists
						be, err := getBinaryExpr(filter, allLabelFilters)
						if err != nil {
							return false, nil, err
						}
						if be == nil {
							return false, nil, errors.New(fmt.Sprintf("SELECT: VALUE not support %s ops", filter.Op))
						}
						columnValueMap[filter.Key] = &metricsql.BinaryOpExpr{Op: "and", Left: columnValueMap[filter.Key], Right: be}
					}
				} else if !ok {
					return false, nil, errors.New(fmt.Sprintf("SELECT: VALUE %s is not in table %s", filter.Key, t.TableName))
				}
			}
		}
		if stmt.WhereFilter.OrFilters != nil && len(stmt.WhereFilter.OrFilters) != 0 {
			for _, filter := range stmt.WhereFilter.OrFilters {
				// VALUE is one of the table's columns
				if c, ok := t.ColMap[filter.Key]; ok && !c.Tag {
					if _, ok := columnValueMap[filter.Key]; !ok { // this VALUE never appears
						return false, nil, errors.New(fmt.Sprintf("SELECT: semantic error VALUE %s only appears in OR", filter.Key))
					} else { // VALUE already exists
						be, err := getBinaryExpr(filter, allLabelFilters)
						if err != nil {
							return false, nil, err
						}
						if be == nil {
							return false, nil, errors.New(fmt.Sprintf("SELECT: TAG not support %s ops", filter.Op))
						}
						columnValueMap[filter.Key] = &metricsql.BinaryOpExpr{Op: "or", Left: columnValueMap[filter.Key], Right: be}
					}
				} else if !ok { // TAG not in this table
					return false, nil, errors.New(fmt.Sprintf("SELECT: TAG %s is not in table %s", filter.Key, t.TableName))
				}
			}
		}
	}

	// traverse column
	if !stmt.IsStar && (stmt.Columns == nil || len(stmt.Columns) == 0) {
		return false, nil, errors.New("SELECT: SQL need '*' or column(s) but got nil")
	}
	// find if TAG-only
	isTag := false
	isValue := false
	for _, column := range stmt.Columns {
		if column == nil || len(column) == 0 {
			return false, nil, errors.New("SELECT: invalid column item")
		}
		if _, ok := t.ColMap[column[0].Args[0]]; !ok {
			return false, nil, errors.New(fmt.Sprintf("SELECT: %s is not in table %s", column[0].Args[0], t.TableName))
		}
		if t.ColMap[column[0].Args[0]].Tag && len(column) == 1 {
			isTag = true
		} else if t.ColMap[column[0].Args[0]].Tag && len(column) > 1 {
			return false, nil, errors.New(fmt.Sprintf("SELECT: semantic error on column %s", column[0].Args[0]))
		} else if !t.ColMap[column[0].Args[0]].Tag {
			isValue = true
		}
		if isValue && isTag {
			return false, nil, errors.New("SELECT: cannot select TAG, VALUE in one SQL in time series query")
		}
	}
	// TAG-only
	if isTag {
		var ms = &MetricsExpr{}
		var count int
		for _, column := range stmt.Columns {
			if item, ok := columnTagMap[column[0].Args[0]]; !ok {
				ms.Columns = append(ms.Columns, ColumnMetric{Name: column[0].Args[0], Metric: nil})
			} else {
				count++
				ms.Columns = append(ms.Columns, ColumnMetric{Name: column[0].Args[0], Metric: item})
			}
		}
		if len(columnTagMap) > count {
			return false, nil, fmt.Errorf("SELECT: unexcepted column in where but not selected")
		}

		return true, ms, nil
	}
	// VALUE-only
	// traverse group-by and limit
	if stmt.GroupBy != nil && len(stmt.GroupBy) == 0 {
		return false, nil, errors.New("SELECT: empty group by columns")
	}
	groupByExpr := metricsql.ModifierExpr{}
	if stmt.GroupBy != nil {
		for _, s := range stmt.GroupBy {
			if c, ok := t.ColMap[s]; !ok || !c.Tag {
				return false, nil, errors.New(fmt.Sprintf("SELECT: group by got not in table column %s", s))
			}
		}
		groupByExpr.Op = "by"
		groupByExpr.Args = stmt.GroupBy
	}
	var limit int64
	if len(stmt.Limit) != 0 {
		l, err := strconv.ParseInt(stmt.Limit, 10, 32)
		if err != nil {
			return false, nil, errors.New(fmt.Sprintf("SELECT: limit should get integer arg but got %s", stmt.Limit))
		}
		limit = l
	}
	// traverse stmt.Columns
	valueCount := len(columnValueMap)
	if stmt.IsStar && (stmt.Columns == nil || len(stmt.Columns) == 0) {
		stmt.Columns = [][]*Function{}
		for _, column := range t.Columns {
			if !column.Tag {
				stmt.Columns = append(stmt.Columns, []*Function{{Args: []string{column.ColumnName}}})
			}
		}
	}
	if stmt.Columns == nil || len(stmt.Columns) == 0 {
		return false, nil, errors.New("SELECT: none selected columns")
	}
	for _, column := range stmt.Columns {
		if _, ok := columnValueMap[column[0].Args[0]]; ok {
			valueCount--
		} else {
			columnValueMap[column[0].Args[0]] = &metricsql.MetricExpr{LabelFilters: append(allLabelFilters, metricsql.LabelFilter{
				Label: "__name__", Value: column[0].Args[0], IsNegative: false, IsRegexp: false})}
		}
		for i, function := range column[1:] {
			if isNormal(function.FuncName) { // Normal
				if len(function.Args) == 1 && function.Args[0] == "" { // normal function with only one arg
					columnValueMap[column[0].Args[0]] = &metricsql.FuncExpr{
						Name:            function.FuncName,
						Args:            []metricsql.Expr{columnValueMap[column[0].Args[0]]},
						KeepMetricNames: true,
					}
					continue
				} else if len(function.Args) == 2 && function.Args[0] == "" { // normal function with 2 Args,
					// such as clamp_max, clamp_min
					num, err := strconv.ParseFloat(function.Args[1], 64)
					if err != nil {
						return false, nil, errors.New(fmt.Sprintf("SELECT: need numerical arg but got %s", function.Args[1]))
					}
					columnValueMap[column[0].Args[0]] = &metricsql.FuncExpr{
						Name:            function.FuncName,
						Args:            []metricsql.Expr{columnValueMap[column[0].Args[0]], &metricsql.NumberExpr{N: num}},
						KeepMetricNames: true,
					}
					continue
				}
				return false, nil, errors.New(fmt.Sprintf("SELECT: illegal function args in column %s function %s",
					column[0].Args[0], function.FuncName))
			} else if isAggregation(function.FuncName) && i == len(column)-2 { // Aggregation
				if len(function.Args) == 1 && function.Args[0] == "" { // aggregation function with only one arg
					columnValueMap[column[0].Args[0]] = &metricsql.AggrFuncExpr{
						Name:     function.FuncName,
						Args:     []metricsql.Expr{columnValueMap[column[0].Args[0]]},
						Modifier: groupByExpr,
						Limit:    int(limit),
					}
					continue
				} else if len(function.Args) == 2 && function.Args[0] == "" { // aggregation function with 2 Args,
					// such as clamp_max, clamp_min
					num, err := strconv.ParseFloat(function.Args[1], 64)
					if err != nil {
						return false, nil, errors.New(fmt.Sprintf("SELECT: need numerical arg but got %s", function.Args[1]))
					}
					columnValueMap[column[0].Args[0]] = &metricsql.AggrFuncExpr{
						Name:     function.FuncName,
						Args:     []metricsql.Expr{&metricsql.NumberExpr{N: num}, columnValueMap[column[0].Args[0]]},
						Modifier: groupByExpr,
						Limit:    int(limit),
					}
					continue
				}
				return false, nil, errors.New(fmt.Sprintf("SELECT: illegal function args in column %s function %s",
					column[0].Args[0], function.FuncName))
			}
			return false, nil, errors.New(fmt.Sprintf("SELECT: illegal function %s", function.FuncName))
		}
	}
	// check if columnValueMap all used
	if valueCount != 0 {
		return false, nil, errors.New("SELECT: semantic error")
	}

	// union all in columnValueMap
	var q []metricsql.Expr
	for _, expr := range columnValueMap {
		q = append(q, expr)
	}
	// treat as export
	if stmt.WhereFilter == nil || stmt.WhereFilter.TimeFilter == nil || stmt.WhereFilter.TimeFilter.Step == "" {
		var m *metricsql.MetricExpr = nil
		k := 0
		for i := 0; i < len(q); i++ {
			switch q[i].(type) {
			case *metricsql.MetricExpr:
				if m == nil {
					m = q[i].(*metricsql.MetricExpr)
					for k = 0; k < len(m.LabelFilters); k++ {
						if m.LabelFilters[k].Label == "__name__" {
							break
						}
					}
				} else {
					j := 0
					for j = 0; j < len(q[i].(*metricsql.MetricExpr).LabelFilters); j++ {
						if q[i].(*metricsql.MetricExpr).LabelFilters[j].Label == "__name__" {
							m.LabelFilters[k].Value += "|" + q[i].(*metricsql.MetricExpr).LabelFilters[j].Value
							m.LabelFilters[k].IsRegexp = true
							break
						}
					}
				}
			default:
				return false, nil, fmt.Errorf("select real data doesn't support function method")
			}
		}
		return false, m, nil
	} else { // treat as query_range
		for len(q) > 1 {
			q = append(q, &metricsql.FuncExpr{
				Name:            "union",
				Args:            q[0:2],
				KeepMetricNames: false,
			})
			q = q[2:]
		}
		return false, q[0], nil
	}
}

func getLabelFilter(f *OtherFilter) *metricsql.LabelFilter {
	switch f.Op {
	case "=":
		return &metricsql.LabelFilter{Label: f.Key, Value: f.Args, IsNegative: false, IsRegexp: false}
	case "NE":
		return &metricsql.LabelFilter{Label: f.Key, Value: f.Args, IsNegative: true, IsRegexp: false}
	case "IN":
		return &metricsql.LabelFilter{Label: f.Key, Value: f.Args, IsNegative: false, IsRegexp: true}
	case "NOTIN":
		return &metricsql.LabelFilter{Label: f.Key, Value: f.Args, IsNegative: true, IsRegexp: true}
	case "LIKE":
		return &metricsql.LabelFilter{Label: f.Key, Value: f.Args, IsNegative: false, IsRegexp: true}
	case "NOTLIKE":
		return &metricsql.LabelFilter{Label: f.Key, Value: f.Args, IsNegative: true, IsRegexp: true}
	default:
		return nil
	}
}

func getBinaryExpr(f *OtherFilter, allLabelFilters []metricsql.LabelFilter) (*metricsql.BinaryOpExpr, error) {
	labelFilters := append([]metricsql.LabelFilter{{Label: "__name__", Value: f.Key, IsNegative: false, IsRegexp: false}}, allLabelFilters...)
	num, err := strconv.ParseFloat(f.Args, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("SELECT: cannot parse number, %s gets %s", f.Key, f.Args))
	}
	switch f.Op {
	case "=":
		return &metricsql.BinaryOpExpr{Op: "==", Bool: false, Left: &metricsql.MetricExpr{LabelFilters: labelFilters}, Right: &metricsql.NumberExpr{N: num}}, nil
	case "NE":
		return &metricsql.BinaryOpExpr{Op: "!=", Bool: false, Left: &metricsql.MetricExpr{LabelFilters: labelFilters}, Right: &metricsql.NumberExpr{N: num}}, nil
	case "GE":
		return &metricsql.BinaryOpExpr{Op: ">=", Bool: false, Left: &metricsql.MetricExpr{LabelFilters: labelFilters}, Right: &metricsql.NumberExpr{N: num}}, nil
	case "LE":
		return &metricsql.BinaryOpExpr{Op: "<=", Bool: false, Left: &metricsql.MetricExpr{LabelFilters: labelFilters}, Right: &metricsql.NumberExpr{N: num}}, nil
	case ">":
		return &metricsql.BinaryOpExpr{Op: ">", Bool: false, Left: &metricsql.MetricExpr{LabelFilters: labelFilters}, Right: &metricsql.NumberExpr{N: num}}, nil
	case "<":
		return &metricsql.BinaryOpExpr{Op: "<", Bool: false, Left: &metricsql.MetricExpr{LabelFilters: labelFilters}, Right: &metricsql.NumberExpr{N: num}}, nil
	default:
		return nil, nil
	}
}

var aggregationFunctionKeywords = []string{"sum", "max", "min", "avg", "stddev", "stdvar",
	"count", "count_values", "bottomk", "topk"}
var normalFunctionKeywords = []string{"abs", "ceil", "changes", "clamp_max", "clamp_min",
	"day_of_month", "day_of_week", "days_in_month", "delta", "deriv", "exp", "floor",
	"hour", "idelta", "increase", "irate", "ln", "log2", "log10", "minute", "month",
	"rate", "round", "sgn", "sort", "sort_desc", "sqrt", "timestamp", "year"}

func isAggregation(f string) bool {
	f = strings.ToLower(f)
	for i := 0; i < len(aggregationFunctionKeywords); i++ {
		if f == aggregationFunctionKeywords[i] {
			return true
		}
	}
	return false
}

func isNormal(f string) bool {
	f = strings.ToLower(f)
	for i := 0; i < len(normalFunctionKeywords); i++ {
		if f == normalFunctionKeywords[i] {
			return true
		}
	}
	return false
}
