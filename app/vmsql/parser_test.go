package vmsql

import (
	"testing"
)

func TestParseSuccess(t *testing.T) {
	testcases := []struct {
		sql string
		t   string
	}{
		{sql: "CREATE TABLE a (test INT TAG NOT NULL, test2 FLOAT TAG, test3 FLOAT VALUE DEFAULT 1.0)", t: "CREATE"},
		{sql: "CREATE TABLE a (test INT TAG default 12, test2 STRING TAG default 'aab', test3 FLOAT VALUE NOT NULL);", t: "CREATE"},

		{sql: "INSERT INTO a VALUES (1,2),(6,7,7)", t: "INSERT"},
		{sql: "INSERT INTO a * VALUES (1,2),(6,7,7)", t: "INSERT"},
		{sql: "INSERT INTO a (timestamp,city,voltage) VALUES (1,2),(6,7,7)", t: "INSERT"},

		{sql: "DROP table a", t: "DROP"},

		{sql: "DELETE FROM a WHERE a = 'b' AND c IN (1.0,2)", t: "DELETE"},
		{sql: "DELETE FROM a WHERE a != 'b' OR c IN (1.0,2)", t: "DELETE"},
		{sql: "DELETE FROM a WHERE a = 'b' OR c NOT IN (1.0,2)", t: "DELETE"},
		{sql: "DELETE FROM a WHERE a = 'b' OR c LIKE 'a'", t: "DELETE"},
		{sql: "DELETE FROM a WHERE a = 'b' OR c NOT LIKE 'a'", t: "DELETE"},
		{sql: "DELETE FROM a WHERE a = 'b' OR a NOT LIKE 'a'", t: "DELETE"},

		{sql: "select * from a", t: "SELECT"},
		{sql: "select a,b, c, d from e where a='b'", t: "SELECT"},
		{sql: "select a,b, c, d from e where a='b' AND c in ('a','b','c')", t: "SELECT"},
		{sql: "select sum(ceil(a)),b, c, d from e where a='b' group by (a,b,c) order by (a,b) DESC limit 2", t: "SELECT"},
		{sql: `select sum(voltage) from a where timestamp in [0:1:'10s'] AND city in ("nanjing", "beijing") AND voltage <= 1 GROUP by (city)`, t: "SELECT"},

		{sql: `DESCRIBE TABLE a`, t: "DESCRIBE"},
	}
	for _, tcase := range testcases {
		t.Run(tcase.sql, func(t *testing.T) {
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
			if tokenizer.ParseTree.Type() != tcase.t {
				t.Errorf("wrong statement type")
			}
		})
	}
}

func TestParseError(t *testing.T) {
	testcases := []struct {
		sql string
	}{
		{"CREATE TABLE a (test INT TAG NULL, test2 FLOAT TAG NOT NULL, test3 FLOAT VALUE NOT NULL);"},
	}
	for _, tcase := range testcases {
		t.Run(tcase.sql, func(t *testing.T) {
			sql, _, err := SplitStatement(tcase.sql)
			if err != nil {
				t.Errorf("EndOfStatementPosition(%s): ERROR: %v", tcase.sql, err)
				return
			}
			tokenizer := NewStringTokenizer(sql)
			if yyParsePooled(tokenizer) != 1 {
				t.Error("must call error, but success.")
			}
			if tokenizer.ParseTree != nil || tokenizer.LastError == nil {
				t.Error("must call error, but success.")
			}
		})
	}
}
