package vmsql

import (
	"reflect"
	"testing"
)

func TestTransSelectStatement(t *testing.T) {
	testcases := []struct {
		id  string
		sql string
	}{
		{id: "SELECT_01", sql: `select * from a`},
		{id: "SELECT_02", sql: `select * from a where city in ("nanjing", "beijing") AND voltage <= 1`},
		{id: "SELECT_03", sql: `select sum(voltage) from a where timestamp in ["0":"1":1000] AND city in ("nanjing", "beijing") AND voltage <= 1 GROUP by (city)`},
		{id: "SELECT_04", sql: `select exp(ln(clamp_min(voltage, 5))), max(ln(humidity)), temperature from a where city in ("nanjing", "beijing") AND voltage <= 1 AND area = "a" OR area = "b" GROUP by city limit 1`},
	}

	table := &Table{
		TableName: "a",
		Columns: []*Column{
			{"city", reflect.String, true, ""},
			{"area", reflect.String, true, ""},
			{"workshop", reflect.String, true, ""},
			{"machine", reflect.String, true, ""},
			{"voltage", reflect.Float64, false, ""},
			{"electricity", reflect.Float64, false, ""},
			{"humidity", reflect.Float64, false, ""},
			{"temperature", reflect.Float64, false, ""}},
		ColMap: map[string]*Column{},
	}
	for _, column := range table.Columns {
		table.ColMap[column.ColumnName] = column
	}

	for _, tcase := range testcases {
		t.Run(tcase.id, func(t *testing.T) {
			sql, _, err := SplitStatement(tcase.sql)
			if err != nil {
				t.Errorf("EndOfStatementPosition(%s): ERROR: %v", tcase.sql, err)
				return
			}
			tokenizer := NewStringTokenizer(sql)
			if yyParsePooled(tokenizer) != 0 {
				if tokenizer.partialDDL != nil {
					if typ, val := tokenizer.Scan(); typ != 0 {
						t.Errorf("extra characters encountered after end of DDL: '%s'", val)
					}
					t.Logf("ignoring error parsing DDL '%s': %v", sql, tokenizer.LastError)
					tokenizer.ParseTree = tokenizer.partialDDL
				}
				t.Log(tokenizer.LastError.Error())
			}
			if tokenizer.ParseTree == nil {
				t.Errorf("ParTree Empty.")
			}
			if tokenizer.ParseTree.Type() == "SELECT" {
				isTag, expr, err := TransSelectStatement(tokenizer.ParseTree.(*SelectStatement), table)
				if err != nil || expr == nil {
					t.Error(err, isTag)
				}
			}
		})
	}
}
