package vmsql

import "github.com/VictoriaMetrics/metricsql"

// MetricsExpr use for labels query
type MetricsExpr struct {
	Columns []ColumnMetric
}

type ColumnMetric struct {
	Name   string
	Metric []*metricsql.LabelFilter
}

func (mse *MetricsExpr) AppendString(dst []byte) []byte {
	if mse.Columns == nil || len(mse.Columns) == 0 {
		dst = append(dst, []byte{'(', ')'}...)
		return dst
	}
	dst = append(dst, []byte("("+mse.Columns[0].Name+":")...)
	for _, filter := range mse.Columns[0].Metric {
		dst = filter.AppendString(dst)
	}
	for i := 1; i < len(mse.Columns); i++ {
		dst = append(dst, []byte(","+mse.Columns[0].Name+":")...)
		for _, filter := range mse.Columns[i].Metric {
			dst = filter.AppendString(dst)
		}
	}
	dst = append(dst, ')')
	return dst
}
