package vmsql

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"net/http"
	"strings"
)

func SQLInsertHandler(at *auth.Token, req *http.Request) error {
	sql, rem, err := vmsql.SplitStatement(req.FormValue("sql"))
	if err != nil {
		return err
	}
	if len(sql) == 0 {
		return fmt.Errorf("missing `sql` arg")
	}
	if len(strings.TrimSpace(rem)) != 0 {
		return fmt.Errorf("unsupported multi-sql")
	}
	tokenizer := vmsql.NewStringTokenizer(sql)
	if vmsql.YyParsePooled(tokenizer) != 0 {
		if tokenizer.PartialDDL != nil {
			if typ, val := tokenizer.Scan(); typ != 0 {
				return fmt.Errorf("extra characters encountered after end of DDL: '%s'", val)
			}
			tokenizer.ParseTree = tokenizer.PartialDDL
		}
	}
	if tokenizer.LastError != nil {
		return tokenizer.LastError
	}
	if tokenizer.ParseTree == nil {
		return fmt.Errorf("parTree empty")
	}

	switch tokenizer.ParseTree.Type() {
	case "INSERT":
		return insertHandler(at, tokenizer.ParseTree.(*vmsql.InsertStatement))
	default:
		return fmt.Errorf("unsupported sql type")
	}
}
