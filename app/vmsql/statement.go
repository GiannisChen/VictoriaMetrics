package vmsql

// Statement represents a statement.
type Statement interface {
	Type() string
}

// Statements

type (
	CreateStatement struct {
		CreateTable *Table
		IfNotExists bool
	}

	InsertStatement struct {
		IsStar     bool
		Columns    []string
		TableName  string
		InsertData [][]string
	}

	DropStatement struct {
		TableName string
		IfExists  bool
	}

	DeleteStatement struct {
		TableName string
		IsStar    bool
		HasWhere  bool
		Filters   *DeleteFilter
	}

	DescribeStatement struct {
		TableName string
	}

	SelectStatement struct {
		TableName   string
		IsStar      bool
		Columns     [][]*Function
		WhereFilter *MultiFilters
		GroupBy     []string
		OrderBy     *OrderBy
		Limit       string
	}
)

func (c *CreateStatement) Type() string   { return "CREATE" }
func (i *InsertStatement) Type() string   { return "INSERT" }
func (d *DropStatement) Type() string     { return "DROP" }
func (d *DeleteStatement) Type() string   { return "DELETE" }
func (s *SelectStatement) Type() string   { return "SELECT" }
func (s *DescribeStatement) Type() string { return "DESCRIBE" }
