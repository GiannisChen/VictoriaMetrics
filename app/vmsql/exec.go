package vmsql

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/querystats"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/metricsql"
	"time"
)

func Exec(ec *promql.EvalConfig, expr metricsql.Expr) ([]netstorage.Result, error) {
	q := string(expr.AppendString(nil))
	if querystats.Enabled() {
		startTime := time.Now()
		defer querystats.RegisterQuery(q, ec.End-ec.Start, startTime)
	}

	ec.Validate()

	e, err := promql.ParsePromQLWithCache(q)
	if err != nil {
		return nil, err
	}

	qid := promql.ActiveQueriesV.Add(ec, q)
	rv, err := promql.EvalExpr(ec, e)
	promql.ActiveQueriesV.Remove(qid)
	if err != nil {
		return nil, err
	}

	maySort := promql.MaySortResults(e, rv)
	result, err := promql.TimeseriesToResult(rv, maySort)
	if err != nil {
		return nil, err
	}
	if n := ec.RoundDigits; n < 100 {
		for i := range result {
			values := result[i].Values
			for j, v := range values {
				values[j] = decimal.RoundToDecimalDigits(v, n)
			}
		}
	}
	return result, err
}
