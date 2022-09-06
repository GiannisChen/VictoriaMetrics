package protocol

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
)

// CondType used for Condition type.
type CondType int

const (
	// COND_NORMAL enum.
	COND_NORMAL CondType = iota
	// COND_DELAY enum.
	COND_DELAY
	// COND_ERROR enum.
	COND_ERROR
	// COND_PANIC enum.
	COND_PANIC
	// COND_STREAM enum.
	COND_STREAM
)

// Cond presents a condition tuple.
type Cond struct {
	// Cond type.
	Type CondType

	// Panic or Not
	Panic bool

	Result Result

	// Return Error if Error is not nil
	Error error

	// Delay(ms) for results return
	Delay int
}

func (c *Cond) SetError(err error) *Cond {
	c.Error = err
	return c
}

func (c *Cond) SetResult(convert Result) *Cond {
	c.Result = convert
	return c
}

// ResultState enum.
type ResultState int

const (
	// RStateNone enum.
	RStateNone ResultState = iota
	// RStateFields enum.
	RStateFields
	// RStateRows enum.
	RStateRows
	// RStateFinished enum.
	RStateFinished
)

// Field describes a single column returned by a query
type Field struct {
	// name of the field as returned by mysql C API
	Name string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	// vitess-defined type. Conversion function is in sqltypes package.
	Type Type `protobuf:"varint,2,opt,name=type,enum=query.Type" json:"type,omitempty"`
	// Remaining fields from mysql C API.
	// These fields are only populated when ExecuteOptions.included_fields
	// is set to IncludedFields.ALL.
	Table    string `protobuf:"bytes,3,opt,name=table" json:"table,omitempty"`
	OrgTable string `protobuf:"bytes,4,opt,name=org_table,json=orgTable" json:"org_table,omitempty"`
	Database string `protobuf:"bytes,5,opt,name=database" json:"database,omitempty"`
	OrgName  string `protobuf:"bytes,6,opt,name=org_name,json=orgName" json:"org_name,omitempty"`
	// column_length is really a uint32. All 32 bits can be used.
	ColumnLength uint32 `protobuf:"varint,7,opt,name=column_length,json=columnLength" json:"column_length,omitempty"`
	// charset is actually a uint16. Only the lower 16 bits are used.
	Charset uint32 `protobuf:"varint,8,opt,name=charset" json:"charset,omitempty"`
	// decimals is actualy a uint8. Only the lower 8 bits are used.
	Decimals uint32 `protobuf:"varint,9,opt,name=decimals" json:"decimals,omitempty"`
	// flags is actually a uint16. Only the lower 16 bits are used.
	Flags uint32 `protobuf:"varint,10,opt,name=flags" json:"flags,omitempty"`
}

type Result interface {
	ColumnSize() int
	GetState() ResultState
	GetWarnings() uint16
	GetInsertID() uint64
	GetRowsAffected() uint64
	GetFields() []*Field
	AppendRow([][]byte)
}

type MySQLResult struct {
	Fields []*Field   `json:"fields"`
	Rows   [][][]byte `json:"rows"`

	RowsAffected uint64 `json:"rows_affected"`
	InsertID     uint64 `json:"insert_id"`
	Warnings     uint16 `json:"warnings"`
	State        ResultState
	Result
}

func (m *MySQLResult) ColumnSize() int {
	return len(m.Fields)
}

func (m *MySQLResult) GetState() ResultState {
	return m.State
}

func (m *MySQLResult) GetWarnings() uint16 {
	return m.Warnings
}

func (m *MySQLResult) GetInsertID() uint64 {
	return m.InsertID
}

func (m *MySQLResult) GetRowsAffected() uint64 {
	return m.RowsAffected
}

func (m *MySQLResult) GetFields() []*Field {
	return m.Fields
}

func (m *MySQLResult) AppendRow(bs [][]byte) {
	if len(m.Fields) != len(bs) {
		return
	}
	m.Rows = append(m.Rows, bs)
}

type TimeSeriesResult struct {
	TagFields           []*Field `json:"tag_fields"`
	TVFields            []*Field `json:"timeseries_fields"`
	TagFieldsToIndexMap map[string]int
	TVFieldsToIndexMap  map[string]int
	Blocks              []*Block

	RowsAffected uint64 `json:"rows_affected"`
	InsertID     uint64 `json:"insert_id"`
	Warnings     uint16 `json:"warnings"`
	State        ResultState
	Result
}

func (t *TimeSeriesResult) ColumnSize() int {
	return len(t.TagFields) + len(t.TVFields)
}

func (t *TimeSeriesResult) GetState() ResultState {
	return t.State
}

func (t *TimeSeriesResult) GetWarnings() uint16 {
	return t.Warnings
}

func (t *TimeSeriesResult) GetInsertID() uint64 {
	return t.InsertID
}

func (t *TimeSeriesResult) GetRowsAffected() uint64 {
	return t.RowsAffected
}

func (t *TimeSeriesResult) GetFields() (res []*Field) {
	res = make([]*Field, t.ColumnSize())
	copy(res, t.TagFields)
	copy(res[len(t.TagFields):], t.TVFields)
	return
}

func (t *TimeSeriesResult) AppendRow(_ [][]byte) {
}

type Block struct {
	Tags [][]byte
	Data []netstorage.Result
}
