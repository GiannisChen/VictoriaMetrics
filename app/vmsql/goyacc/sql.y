/* create */

%{
package vmsql
func setParseTree(yylex yyLexer, stmt Statement) {
  yylex.(*Tokenizer).ParseTree = stmt
}

func setDDL(yylex yyLexer, node Statement) {
  yylex.(*Tokenizer).partialDDL = node
}

func incNesting(yylex yyLexer) bool {
  yylex.(*Tokenizer).nesting++
  if yylex.(*Tokenizer).nesting == 200 {
    return true
  }
  return false
}

func decNesting(yylex yyLexer) {
  yylex.(*Tokenizer).nesting--
}

// skipToEnd forces the lexer to end prematurely. Not all SQL statements
// are supported by the Parser, thus calling skipToEnd will make the lexer
// return EOF early.
func skipToEnd(yylex yyLexer) {
  yylex.(*Tokenizer).SkipToEnd = true
}

func bindVariable(yylex yyLexer, bvar string) {
  yylex.(*Tokenizer).BindVars[bvar] = struct{}{}
}
%}

%union {
        columns         []*Column
        str             string
        statement       Statement
        column          *Column
        columnType      reflect.Kind
        tuples 		[][]string
        tuple 		[]string
        tagFilter      	*TagFilter
        deleteFilters	*DeleteFilter
        functions	[][]*Function
        function	[]*Function
        orderBy		*OrderBy
        whereFilters	*MultiFilters
        timeFilter	*TimeFilter
        otherFilter	*OtherFilter
}

// Statement
%token LEX_ERROR UNUSED
%left <str> '(' ',' ')'
%left <str> OR '|'
%left <str> XOR
%left <str> AND
%right <str> NOT '!'
%left <str> BETWEEN CASE WHEN THEN ELSE END
%left <str> '=' '<' '>' LE GE NE NULL_SAFE_EQUAL IS LIKE REGEXP IN
%left <str> '&'
%left <str> SHIFT_LEFT SHIFT_RIGHT
%left <str> '+' '-'
%left <str> '*' '/' DIV '%' MOD
%left <str> '^'
%right <str> '~' UNARY
%token <str> CREATE INSERT INTO VALUES DELETE DROP FROM WHERE SELECT LIMIT ORDER BY GROUP DESC ASC ':' '[' ']'
%type <statement> command create_statement insert_statemnet drop_statement delete_statement select_statement
%type <deleteFilters> delete_where_expression
%type <tagFilter> delete_where_item
%type <tuples> insert_data
%type <tuple> sql_id_columns literal_list group_by_expression
%type <columns> table_column_list
%type <column> column_definition
%type <columnType> column_type
%type <str> sql_id
%type <str> literal num_literal string_literal non_reserved_keyword limit_expression
%type <str> id_or_var
%type <functions> select_item_list
%type <function> select_item
%type <orderBy> order_by_expression
%type <whereFilters> select_where_expressions select_where_expression
%type <timeFilter> time_select_list select_where_time_expression
%type <otherFilter> select_where_other_expression
// type
%token <str> BOOL STRING INT32 UINT32 INT64 UINT64 FLOAT DOUBLE BOOLEAN BIT TINYINT SMALLINT INT INTEGER BIGINT
%token <str> STRING_NUM INTEGER_NUM FLOAT_NUM DECIMAL_NUM TRUE_NUM FALSE_NUM
%token <str> ID AT_ID AT_AT_ID HEX INTEGRAL DECIMAL HEXNUM BIT_LITERAL UTINYINT USMALLINT UINT UBIGINT REAL


// keywords
%token <str> IF EXISTS NULL DEFAULT

// ts-db keywords
%token <str> TAG VALUE

// DDL Tokens
%token <str> DATABASE TABLE

%start start_command

%%

start_command:
	command semicolon_opt { setParseTree(yylex, $1) }

semicolon_opt:
	{}
    	| ';' {}

command:
	create_statement { $$ = $1 }
	| insert_statemnet { $$ = $1 }
	| drop_statement { $$ = $1 }
	| delete_statement { $$ = $1 }
	| select_statement { $$ = $1 }

create_statement:
	CREATE TABLE sql_id '(' table_column_list ')'
	{
		$$ = &CreateStatement{CreateTable: &Table{TableName: $3, Columns: $5}, IfNotExists: false}
		setDDL(yylex, $$)
	}
	| CREATE TABLE IF NOT EXISTS sql_id '(' table_column_list ')'
	{
		$$ = &CreateStatement{CreateTable: &Table{TableName: $6, Columns: $8}, IfNotExists: true}
		setDDL(yylex, $$)
	}


table_column_list:
	column_definition { $$ = []*Column{$1} }
	| table_column_list ',' column_definition { $$ = append($1, $3) }

column_definition:
	sql_id column_type TAG
	{
		$$ = &Column{ ColumnName: $1, Type: $2, Tag: true, Nullable: true, Default: "" }
	}
	| sql_id column_type TAG NOT NULL
	{
		$$ = &Column{ ColumnName: $1, Type: $2, Tag: true, Nullable: false, Default: "" }
	}
	| sql_id column_type TAG DEFAULT literal
	{
		$$ = &Column{ ColumnName: $1, Type: $2, Tag: true, Nullable: true, Default: $5 }
	}
	| sql_id column_type VALUE
	{
		$$ = &Column{ ColumnName: $1, Type: $2, Tag: false, Nullable: true, Default: "" }
	}
	| sql_id column_type VALUE NOT NULL
	{
		$$ = &Column{ ColumnName: $1, Type: $2, Tag: false, Nullable: false, Default: "" }
	}
	| sql_id column_type VALUE DEFAULT literal
	{
		$$ = &Column{ ColumnName: $1, Type: $2, Tag: false, Nullable: true, Default: $5 }
	}

column_type:
	BIT { $$ = reflect.Bool }
	| BOOL { $$ = reflect.Bool }
	| BOOLEAN { $$ = reflect.Bool }
	| TINYINT { $$ = reflect.Int8 }
	| SMALLINT { $$ = reflect.Int16 }
	| INT { $$ = reflect.Int32 }
	| INTEGER { $$ = reflect.Int32 }
	| BIGINT { $$ = reflect.Int64 }
	| UTINYINT { $$ = reflect.Uint8 }
	| USMALLINT { $$ = reflect.Uint16 }
	| UINT { $$ = reflect.Uint32 }
	| UBIGINT { $$ = reflect.Uint64 }
	| REAL { $$ = reflect.Float32 }
	| DOUBLE { $$ = reflect.Float64 }
	| FLOAT { $$ = reflect.Float32 }
	| STRING { $$ = reflect.String }

literal:
	string_literal { $$ = $1 }
	| num_literal { $$ = $1 }

string_literal:
	STRING_NUM { $$ = $1 }

num_literal:
	INTEGER_NUM { $$ = $1 }
	| FLOAT_NUM { $$ = $1 }
	| TRUE_NUM { $$ = "true" }
	| FALSE_NUM { $$ = "false" }

sql_id:
	id_or_var { $$ = $1 }
	| non_reserved_keyword { $$ = string($1) }

id_or_var:
  	ID { $$ = string($1) }

non_reserved_keyword:
	 BOOL
	| BOOLEAN
	| DOUBLE
	| STRING_NUM
	| INTEGER_NUM
	| FLOAT_NUM
	| DECIMAL_NUM
	| TRUE_NUM
	| FALSE_NUM

insert_statemnet:
	INSERT INTO sql_id VALUES insert_data { $$ = &InsertStatement{IsStar: true, Columns: nil, TableName: $3, InsertData: $5} }
	| INSERT INTO sql_id '*' VALUES insert_data { $$ = &InsertStatement{IsStar: true, Columns: nil, TableName: $3, InsertData: $6} }
	| INSERT INTO sql_id '(' sql_id_columns ')' VALUES insert_data { $$ = &InsertStatement{IsStar: false, Columns: $5, TableName: $3, InsertData: $8} }

sql_id_columns:
	sql_id { $$ = []string{$1} }
	| sql_id_columns ',' sql_id { $$ = append($1, $3) }

insert_data:
	'(' literal_list ')' { $$ = [][]string{$2} }
	| insert_data ',' '(' literal_list ')' { $$ = append($1, $4) }

literal_list:
	literal { $$ = []string{$1} }
	| literal_list ',' literal { $$ = append($1, $3) }

drop_statement:
	DROP TABLE sql_id { $$ = &DropStatement{TableName: $3} }

delete_statement:
	DELETE FROM sql_id { $$ = &DeleteStatement{TableName: $3, IsStar: true, Filters: nil, HasWhere: false} }
	| DELETE '*' FROM sql_id { $$ = &DeleteStatement{TableName: $4, IsStar: true, Filters: nil, HasWhere: false} }
	| DELETE FROM sql_id WHERE delete_where_expression { $$ = &DeleteStatement{TableName: $3, IsStar: false, Filters: $5, HasWhere: true} }

delete_where_expression:
	delete_where_item { $$ = &DeleteFilter{AndTagFilters: []*TagFilter{$1}, OrTagFilters: nil} }
	| delete_where_expression AND delete_where_item %prec AND { $$ = andJoinTagFilters($1, $3) }
	| delete_where_expression OR delete_where_item %prec OR { $$ = orJoinTagFilters($1, $3) }

delete_where_item:
	sql_id IN '(' literal_list ')' { $$ = &TagFilter{Key: $1, Value: strings.Join($4, "|"), IsNegative: false, IsRegexp: true} }
	| sql_id NOT IN '(' literal_list ')' { $$ = &TagFilter{Key: $1, Value: strings.Join($5, "|"), IsNegative: true, IsRegexp: true} }
	| sql_id '=' literal { $$ = &TagFilter{Key: $1, Value: $3, IsNegative: false, IsRegexp: false} }
	| sql_id NE literal { $$ = &TagFilter{Key: $1, Value: $3, IsNegative: true, IsRegexp: false} }
	| sql_id LIKE string_literal { $$ = &TagFilter{Key: $1, Value: $3, IsNegative: false, IsRegexp: true} }
	| sql_id NOT LIKE string_literal { $$ = &TagFilter{Key: $1, Value: $4, IsNegative: true, IsRegexp: true} }

select_statement:
	SELECT '*' FROM sql_id select_where_expressions group_by_expression order_by_expression limit_expression
	{
		$$ = &SelectStatement{
			TableName: $4,
			IsStar: true,
			Columns: nil,
			WhereFilter: $5,
			GroupBy: $6,
			OrderBy: $7,
			Limit: $8,
		}
	}
	| SELECT select_item_list FROM sql_id select_where_expressions group_by_expression order_by_expression limit_expression
	{
		$$ = &SelectStatement{
			TableName: $4,
			IsStar: true,
			Columns: $2,
			WhereFilter: $5,
			GroupBy: $6,
			OrderBy: $7,
			Limit: $8,
		}
        }

select_item_list:
	select_item { $$ = [][]*Function{$1} }
	| select_item_list ',' select_item { $$ = append($1, $3) }

select_item:
	sql_id '(' select_item ')'
	{ $$ = append($3, &Function{FuncName: $1, Args: []string{""}}) }
	| sql_id '(' select_item ',' num_literal ')'
	{ $$ = append($3, &Function{FuncName: $1, Args: []string{"", $5}}) }
	| sql_id { $$ = []*Function{{FuncName: "", Args: []string{$1}}} }

limit_expression:
	{ $$ = "" }
	| LIMIT INTEGER_NUM { $$ = $2 }

order_by_expression:
	{ $$ = nil }
	| ORDER BY '(' sql_id_columns ')' { $$ = &OrderBy{SortingKey: $4, IsAsc: true} }
	| ORDER BY '(' sql_id_columns ')' DESC { $$ = &OrderBy{SortingKey: $4, IsAsc: false} }
	| ORDER BY '(' sql_id_columns ')' ASC { $$ = &OrderBy{SortingKey: $4, IsAsc: true} }

group_by_expression:
	{ $$ = nil }
	| GROUP BY sql_id_columns { $$ = $3 }
	| GROUP BY '(' sql_id_columns ')' { $$ = $4 }

select_where_expressions:
	{ $$ = nil }
	| WHERE select_where_expression { $$ = $2 }

select_where_expression:
	select_where_time_expression { $$ = &MultiFilters{TimeFilter: $1, AndFilters: nil} }
	| select_where_other_expression { $$ = &MultiFilters{TimeFilter: nil, AndFilters: []*OtherFilter{$1}} }
	| select_where_expression AND select_where_other_expression %prec AND { $$ = andJoinMultiFilters($1, $3) }
	| select_where_expression OR select_where_other_expression %prec OR { $$ = orJoinMultiFilters($1, $3) }

select_where_other_expression:
	sql_id IN '(' literal_list ')' { $$ = &OtherFilter{Key: $1, Op: "IN", Args: strings.Join($4, "|")} }
	| sql_id NOT IN '(' literal_list ')' { $$ = &OtherFilter{Key: $1, Op: "NOTIN", Args: strings.Join($5, "|")} }
	| sql_id '=' literal { $$ = &OtherFilter{Key: $1, Op: "=", Args: $3} }
	| sql_id '<' literal { $$ = &OtherFilter{Key: $1, Op: "<", Args: $3} }
	| sql_id '>' literal { $$ = &OtherFilter{Key: $1, Op: ">", Args: $3} }
	| sql_id LE literal { $$ = &OtherFilter{Key: $1, Op: "LE", Args: $3} }
	| sql_id GE literal { $$ = &OtherFilter{Key: $1, Op: "GE", Args: $3} }
	| sql_id NE literal { $$ = &OtherFilter{Key: $1, Op: "NE", Args: $3} }
	| sql_id LIKE string_literal { $$ = &OtherFilter{Key: $1, Op: "LIKE", Args: $3} }
	| sql_id NOT LIKE string_literal { $$ = &OtherFilter{Key: $1, Op: "NOTLIKE", Args: $4} }

select_where_time_expression:
	sql_id IN '[' time_select_list ']'
	{
		if strings.ToLower($1) == "timestamp" {
			$$ = $4
		} else {
			$$ = nil
		}
	}

time_select_list:
	string_literal { $$ = &TimeFilter{Start: "", End: "", Step: $1} }
	| num_literal ':' num_literal {$$ = &TimeFilter{Start: $1, End: $3, Step: ""} }
	| num_literal ':' num_literal ':' string_literal {$$ = &TimeFilter{Start: $1, End: $3, Step: $5} }
