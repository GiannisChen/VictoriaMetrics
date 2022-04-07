/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vmsql

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestLiteralID(t *testing.T) {
	testcases := []struct {
		in  string
		id  int
		out string
	}{{
		in:  "`aa`",
		id:  ID,
		out: "aa",
	}, {
		in:  "```a```",
		id:  ID,
		out: "`a`",
	}, {
		in:  "`a``b`",
		id:  ID,
		out: "a`b",
	}, {
		in:  "`a``b`c",
		id:  ID,
		out: "a`b",
	}, {
		in:  "`a``b",
		id:  LEX_ERROR,
		out: "a`b",
	}, {
		in:  "`a``b``",
		id:  LEX_ERROR,
		out: "a`b`",
	}, {
		in:  "``",
		id:  LEX_ERROR,
		out: "",
	}, {
		in:  "@x",
		id:  AT_ID,
		out: "x",
	}, {
		in:  "@@x",
		id:  AT_AT_ID,
		out: "x",
	}, {
		in:  "@@`x y`",
		id:  AT_AT_ID,
		out: "x y",
	}, {
		in:  "@@`@x @y`",
		id:  AT_AT_ID,
		out: "@x @y",
	}}

	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			tkn := NewStringTokenizer(tcase.in)
			id, out := tkn.Scan()
			require.Equal(t, tcase.id, id)
			require.Equal(t, tcase.out, string(out))
		})
	}
}

func tokenName(id int) string {
	if id == STRING {
		return "STRING"
	} else if id == LEX_ERROR {
		return "LEX_ERROR"
	}
	return fmt.Sprintf("%d", id)
}

func TestString(t *testing.T) {
	testcases := []struct {
		in   string
		id   int
		want string
	}{{
		in:   "''",
		id:   STRING_NUM,
		want: "",
	}, {
		in:   "''''",
		id:   STRING_NUM,
		want: "'",
	}, {
		in:   "'hello'",
		id:   STRING_NUM,
		want: "hello",
	}, {
		in:   "'\\n'",
		id:   STRING_NUM,
		want: "\n",
	}, {
		in:   "'\\nhello\\n'",
		id:   STRING_NUM,
		want: "\nhello\n",
	}, {
		in:   "'a''b'",
		id:   STRING_NUM,
		want: "a'b",
	}, {
		in:   "'a\\'b'",
		id:   STRING_NUM,
		want: "a'b",
	}, {
		in:   "'\\'",
		id:   LEX_ERROR,
		want: "'",
	}, {
		in:   "'",
		id:   LEX_ERROR,
		want: "",
	}, {
		in:   "'hello\\'",
		id:   LEX_ERROR,
		want: "hello'",
	}, {
		in:   "'hello",
		id:   LEX_ERROR,
		want: "hello",
	}, {
		in:   "'hello\\",
		id:   LEX_ERROR,
		want: "hello",
	}}

	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			id, got := NewStringTokenizer(tcase.in).Scan()
			require.Equal(t, tcase.id, id, "Scan(%q) = (%s), want (%s)", tcase.in, tokenName(id), tokenName(tcase.id))
			require.Equal(t, tcase.want, string(got))
		})
	}
}

func TestSplitStatement(t *testing.T) {
	testcases := []struct {
		in  string
		sql string
		rem string
	}{{
		in:  "select * from table",
		sql: "select * from table",
	}, {
		in:  "select * from table; ",
		sql: "select * from table",
		rem: " ",
	}, {
		in:  "select * from table; select * from table2;",
		sql: "select * from table",
		rem: " select * from table2;",
	}, {
		in:  "select * from /* comment */ table;",
		sql: "select * from /* comment */ table",
	}, {
		in:  "select * from /* comment ; */ table;",
		sql: "select * from /* comment ; */ table",
	}, {
		in:  "select * from table where semi = ';';",
		sql: "select * from table where semi = ';'",
	}, {
		in:  "-- select * from table",
		sql: "-- select * from table",
	}, {
		in:  " ",
		sql: " ",
	}, {
		in:  "",
		sql: "",
	}}

	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			sql, rem, err := SplitStatement(tcase.in)
			if err != nil {
				t.Errorf("EndOfStatementPosition(%s): ERROR: %v", tcase.in, err)
				return
			}

			if tcase.sql != sql {
				t.Errorf("EndOfStatementPosition(%s) got sql \"%s\" want \"%s\"", tcase.in, sql, tcase.sql)
			}

			if tcase.rem != rem {
				t.Errorf("EndOfStatementPosition(%s) got remainder \"%s\" want \"%s\"", tcase.in, rem, tcase.rem)
			}
		})
	}
}

func TestSplitStatementIsSpace(t *testing.T) {
	testcases := []struct {
		sql string
		is  bool
	}{{
		sql: "select * from table;  \t\n",
		is:  true,
	}, {
		sql: "select * from a; ; ;",
		is:  false,
	}}

	for _, testcase := range testcases {
		t.Run(testcase.sql, func(t *testing.T) {
			sql, rem, err := SplitStatement(testcase.sql)
			t.Log(sql)
			if err == nil && (len(rem) == 0 || len(strings.TrimSpace(sql)) == 0) {
				if !testcase.is {
					t.Error("should T but got F")
				}
			} else {
				if testcase.is {
					t.Error("should F but got T")
				}
			}
		})
	}
}

func TestIntegerAndID(t *testing.T) {
	testcases := []struct {
		in  string
		id  int
		out string
	}{{
		in: "334",
		id: INTEGER_NUM,
	}, {
		in: "33.4",
		id: FLOAT_NUM,
	}, {
		in: "0x33",
		id: INTEGER_NUM,
	}, {
		in: "33e4",
		id: FLOAT_NUM,
	}, {
		in: "33.4e-3",
		id: FLOAT_NUM,
	}, {
		in: "33t4",
		id: ID,
	}, {
		in: "0x2et3",
		id: ID,
	}, {
		in:  "3e2t3",
		id:  LEX_ERROR,
		out: "3e2",
	}, {
		in:  "3.2t",
		id:  LEX_ERROR,
		out: "3.2",
	}}

	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			tkn := NewStringTokenizer(tcase.in)
			id, out := tkn.Scan()
			require.Equal(t, tcase.id, id)
			expectedOut := tcase.out
			if expectedOut == "" {
				expectedOut = tcase.in
			}
			require.Equal(t, expectedOut, out)
		})
	}
}

func TestLexInSQL(t *testing.T) {
	testcases := []struct {
		sql string
		id  []int
	}{
		// CREATE
		{
			sql: "create table a (test int tag, test2 float tag, test3 float value)",
			id:  []int{CREATE, TABLE, ID, '(', ID, INT, TAG, ',', ID, FLOAT, TAG, ',', ID, FLOAT, VALUE, ')'},
		},
		{
			sql: "create table a (test int tag, test2 float tag, test3 float value)",
			id:  []int{CREATE, TABLE, ID, '(', ID, INT, TAG, ',', ID, FLOAT, TAG, ',', ID, FLOAT, VALUE, ')'},
		},
		{
			sql: "CREATE TABLE a (test INT TAG NOT NULL, test2 FLOAT TAG, test3 FLOAT VALUE DEFAULT 1.0)",
			id:  []int{CREATE, TABLE, ID, '(', ID, INT, TAG, NOT, NULL, ',', ID, FLOAT, TAG, ',', ID, FLOAT, VALUE, DEFAULT, FLOAT_NUM},
		},
		{
			sql: "CREATE TABLE a (test INT TAG default 12, test2 STRING TAG default 'aab', test3 FLOAT VALUE NOT NULL);",
			id:  []int{CREATE, TABLE, ID, '(', ID, INT, TAG, DEFAULT, INTEGER_NUM, ',', ID, STRING, TAG, DEFAULT, STRING_NUM, ',', ID, FLOAT, VALUE, NOT, NULL},
		},
		// INSERT
		{
			sql: "INSERT INTO  a VALUES (1,2),(6,7,7)",
			id:  []int{INSERT, INTO, ID, VALUES, '(', INTEGER_NUM, ',', INTEGER_NUM, ')', ',', '(', INTEGER_NUM, ',', INTEGER_NUM, ',', INTEGER_NUM, ')'},
		},
		{
			sql: "INSERT INTO  a * VALUES (1,2),(6,7,7)",
			id:  []int{INSERT, INTO, ID, '*', VALUES, '(', INTEGER_NUM, ',', INTEGER_NUM, ')', ',', '(', INTEGER_NUM, ',', INTEGER_NUM, ',', INTEGER_NUM, ')'},
		},
		{
			sql: "INSERT INTO  a (timestamp,city,voltage) VALUES (1,2),(6,7,7)",
			id:  []int{INSERT, INTO, ID, '(', ID, ',', ID, ',', ID, ')', VALUES, '(', INTEGER_NUM, ',', INTEGER_NUM, ')', ',', '(', INTEGER_NUM, ',', INTEGER_NUM, ',', INTEGER_NUM, ')'},
		},
		//DROP
		{
			sql: "DROP table a",
			id:  []int{DROP, TABLE, ID},
		},
		//DELETE
		{
			sql: "DELETE * FROM a",
			id:  []int{DELETE, '*', FROM, ID},
		},
		{
			sql: "DELETE FROM a",
			id:  []int{DELETE, FROM, ID},
		},
		{
			sql: "DELETE FROM a WHERE a = 'b' AND c IN (1.0,2)",
			id:  []int{DELETE, FROM, ID, WHERE, ID, '=', STRING_NUM, AND, ID, IN, '(', FLOAT_NUM, ',', INTEGER_NUM, ')'},
		},
		{
			sql: "DELETE FROM a WHERE a = 'b' OR c IN (1.0,2)",
			id:  []int{DELETE, FROM, ID, WHERE, ID, '=', STRING_NUM, OR, ID, IN, '(', FLOAT_NUM, ',', INTEGER_NUM, ')'},
		},
		{
			sql: "DELETE FROM a WHERE a = 'b' OR c NOT IN (1.0,2)",
			id:  []int{DELETE, FROM, ID, WHERE, ID, '=', STRING_NUM, OR, ID, NOT, IN, '(', FLOAT_NUM, ',', INTEGER_NUM, ')'},
		},
		{
			sql: "DELETE FROM a WHERE a = 'b' OR c LIKE 'a'",
			id:  []int{DELETE, FROM, ID, WHERE, ID, '=', STRING_NUM, OR, ID, LIKE, STRING_NUM},
		},
		{
			sql: "DELETE FROM a WHERE a = 'b' OR c NOT LIKE 'a'",
			id:  []int{DELETE, FROM, ID, WHERE, ID, '=', STRING_NUM, OR, ID, NOT, LIKE, STRING_NUM},
		},
		{
			sql: "SELECT * FROM a WHERE timestamp in [1:10:'5s']",
			id:  []int{SELECT, '*', FROM, ID, WHERE, ID, IN, '[', INTEGER_NUM, ':', INTEGER_NUM, ':', STRING_NUM, ']'},
		},
	}
	for _, tcase := range testcases {
		t.Run(tcase.sql, func(t *testing.T) {
			tok := NewStringTokenizer(tcase.sql)
			for _, expectedID := range tcase.id {
				id, _ := tok.Scan()
				require.Equal(t, expectedID, id)
			}
		})
	}
}

func TestSmallPart(t *testing.T) {
	testcases := []struct {
		in  string
		id  int
		out string
	}{{
		in:  "!=",
		id:  NE,
		out: "",
	}, {
		in:  "'t?x_.*ta'",
		id:  STRING_NUM,
		out: "t?x_.*ta",
	}, {
		in:  ":",
		id:  ':',
		out: ":",
	}}

	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			tkn := NewStringTokenizer(tcase.in)
			id, out := tkn.Scan()
			require.Equal(t, tcase.id, id)
			expectedOut := tcase.out
			if expectedOut == "" && out != "" {
				expectedOut = tcase.in
			}
			require.Equal(t, expectedOut, out)
		})
	}
}
