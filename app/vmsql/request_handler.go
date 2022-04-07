package vmsql

import (
	"github.com/VictoriaMetrics/metrics"
	"net/http"
	"time"
)

var sqlDuration = metrics.NewSummary(`vm_request_duration_seconds{path="/api/v1/query_range"}`)

func requestHandler(startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	defer sqlDuration.UpdateDuration(startTime)

	//ct := startTime.UnixNano() / 1e6
	//sql := r.FormValue("sql")
	//if len(sql) == 0 {
	//	return fmt.Errorf("missing `sql` arg")
	//}
	//sql, _, err := SplitStatement(sql)
	//if err != nil {
	//	t.Errorf("EndOfStatementPosition(%s): ERROR: %v", tcase.sql, err)
	//	return
	//}
	//tokenizer := NewStringTokenizer(sql)
	//if yyParsePooled(tokenizer) != 0 {
	//	if tokenizer.partialDDL != nil {
	//		if typ, val := tokenizer.Scan(); typ != 0 {
	//			t.Errorf("extra characters encountered after end of DDL: '%s'", val)
	//		}
	//		t.Logf("ignoring error parsing DDL '%s': %v", sql, tokenizer.LastError)
	//		tokenizer.ParseTree = tokenizer.partialDDL
	//	}
	//	t.Log(tokenizer.LastError.Error())
	//}
	//if tokenizer.ParseTree == nil {
	//	t.Errorf("ParTree Empty.")
	//}
	//if tokenizer.ParseTree.Type() != tcase.t {
	//	t.Errorf("wrong statement type")
	//}
	return nil
}
