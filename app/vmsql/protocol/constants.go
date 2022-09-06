/*
 * This code was derived from https://github.com/youtube/vitess.
 *
 * codes from go-mysqlstack
 * Giannis Chen
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package protocol

/***************************************************/
// https://dev.mysql.com/doc/internals/en/command-phase.html
// include/my_command.h
const (
	COM_SLEEP               = 0x00
	COM_QUIT                = 0x01
	COM_INIT_DB             = 0x02
	COM_QUERY               = 0x03
	COM_FIELD_LIST          = 0x04
	COM_CREATE_DB           = 0x05
	COM_DROP_DB             = 0x06
	COM_REFRESH             = 0x07
	COM_SHUTDOWN            = 0x08
	COM_STATISTICS          = 0x09
	COM_PROCESS_INFO        = 0x0a
	COM_CONNECT             = 0x0b
	COM_PROCESS_KILL        = 0x0c
	COM_DEBUG               = 0x0d
	COM_PING                = 0x0e
	COM_TIME                = 0x0f
	COM_DELAYED_INSERT      = 0x10
	COM_CHANGE_USER         = 0x11
	COM_BINLOG_DUMP         = 0x12
	COM_TABLE_DUMP          = 0x13
	COM_CONNECT_OUT         = 0x14
	COM_REGISTER_SLAVE      = 0x15
	COM_STMT_PREPARE        = 0x16
	COM_STMT_EXECUTE        = 0x17
	COM_STMT_SEND_LONG_DATA = 0x18
	COM_STMT_CLOSE          = 0x19
	COM_STMT_RESET          = 0x1a
	COM_SET_OPTION          = 0x1b
	COM_STMT_FETCH          = 0x1c
	COM_DAEMON              = 0x1d
	COM_BINLOG_DUMP_GTID    = 0x1e
	COM_RESET_CONNECTION    = 0x1f
)

// CommandString used for translate cmd to string.
func CommandString(cmd byte) string {
	switch cmd {
	case COM_SLEEP:
		return "COM_SLEEP"
	case COM_QUIT:
		return "COM_QUIT"
	case COM_INIT_DB:
		return "COM_INIT_DB"
	case COM_QUERY:
		return "COM_QUERY"
	case COM_FIELD_LIST:
		return "COM_FIELD_LIST"
	case COM_CREATE_DB:
		return "COM_CREATE_DB"
	case COM_DROP_DB:
		return "COM_DROP_DB"
	case COM_REFRESH:
		return "COM_REFRESH"
	case COM_SHUTDOWN:
		return "COM_SHUTDOWN"
	case COM_STATISTICS:
		return "COM_STATISTICS"
	case COM_PROCESS_INFO:
		return "COM_PROCESS_INFO"
	case COM_CONNECT:
		return "COM_CONNECT"
	case COM_PROCESS_KILL:
		return "COM_PROCESS_KILL"
	case COM_DEBUG:
		return "COM_DEBUG"
	case COM_PING:
		return "COM_PING"
	case COM_TIME:
		return "COM_TIME"
	case COM_DELAYED_INSERT:
		return "COM_DELAYED_INSERT"
	case COM_CHANGE_USER:
		return "COM_CHANGE_USER"
	case COM_BINLOG_DUMP:
		return "COM_BINLOG_DUMP"
	case COM_TABLE_DUMP:
		return "COM_TABLE_DUMP"
	case COM_CONNECT_OUT:
		return "COM_CONNECT_OUT"
	case COM_REGISTER_SLAVE:
		return "COM_REGISTER_SLAVE"
	case COM_STMT_PREPARE:
		return "COM_STMT_PREPARE"
	case COM_STMT_EXECUTE:
		return "COM_STMT_EXECUTE"
	case COM_STMT_SEND_LONG_DATA:
		return "COM_STMT_SEND_LONG_DATA"
	case COM_STMT_CLOSE:
		return "COM_STMT_CLOSE"
	case COM_STMT_RESET:
		return "COM_STMT_RESET"
	case COM_SET_OPTION:
		return "COM_SET_OPTION"
	case COM_STMT_FETCH:
		return "COM_STMT_FETCH"
	case COM_DAEMON:
		return "COM_DAEMON"
	case COM_BINLOG_DUMP_GTID:
		return "COM_BINLOG_DUMP_GTID"
	case COM_RESET_CONNECTION:
		return "COM_RESET_CONNECTION"
	}
	return "UNKNOWN"
}

// https://dev.mysql.com/doc/internals/en/capability-flags.html
// include/mysql_com.h
const (
	// new more secure password
	CLIENT_LONG_PASSWORD = 1

	// Found instead of affected rows
	CLIENT_FOUND_ROWS = uint32(1 << 1)

	// Get all column flags
	CLIENT_LONG_FLAG = uint32(1 << 2)

	// One can specify db on connect
	CLIENT_CONNECT_WITH_DB = uint32(1 << 3)

	// Don't allow database.table.column
	CLIENT_NO_SCHEMA = uint32(1 << 4)

	// Can use compression protocol
	CLIENT_COMPRESS = uint32(1 << 5)

	// Odbc client
	CLIENT_ODBC = uint32(1 << 6)

	// Can use LOAD DATA LOCAL
	CLIENT_LOCAL_FILES = uint32(1 << 7)

	// Ignore spaces before '('
	CLIENT_IGNORE_SPACE = uint32(1 << 8)

	// New 4.1 protocol
	CLIENT_PROTOCOL_41 = uint32(1 << 9)

	// This is an interactive client
	CLIENT_INTERACTIVE = uint32(1 << 10)

	// Switch to SSL after handshake
	CLIENT_SSL = uint32(1 << 11)

	// IGNORE sigpipes
	CLIENT_IGNORE_SIGPIPE = uint32(1 << 12)

	// Client knows about transactions
	CLIENT_TRANSACTIONS = uint32(1 << 13)

	// Old flag for 4.1 protocol
	CLIENT_RESERVED = uint32(1 << 14)

	// Old flag for 4.1 authentication
	CLIENT_SECURE_CONNECTION = uint32(1 << 15)

	// Enable/disable multi-stmt support
	CLIENT_MULTI_STATEMENTS = uint32(1 << 16)

	// Enable/disable multi-results
	CLIENT_MULTI_RESULTS = uint32(1 << 17)

	// Multi-results in PS-protocol
	CLIENT_PS_MULTI_RESULTS = uint32(1 << 18)

	// Client supports plugin authentication
	CLIENT_PLUGIN_AUTH = uint32(1 << 19)

	// Client supports connection attributes
	CLIENT_CONNECT_ATTRS = uint32(1 << 20)

	//  Enable authentication response packet to be larger than 255 bytes
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = uint32(1 << 21)

	// Don't close the connection for a connection with expired password
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = uint32(1 << 22)

	// Capable of handling server state change information. Its a hint to the
	// server to include the state change information in Ok packet.
	CLIENT_SESSION_TRACK = uint32(1 << 23)

	//Client no longer needs EOF packet
	CLIENT_DEPRECATE_EOF = uint32(1 << 24)
)

const (
	// SSUnknownSQLState is the default SQLState.
	SSUnknownSQLState = "HY000"
)

// Status flags. They are returned by the server in a few cases.
// Originally found in include/mysql/mysql_com.h
// See http://dev.mysql.com/doc/internals/en/status-flags.html
const (
	// SERVER_STATUS_AUTOCOMMIT is the default status of auto-commit.
	SERVER_STATUS_AUTOCOMMIT = 0x0002
)

// A few interesting character set values.
// See http://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
const (
	// CharacterSetUtf8 is for UTF8. We use this by default.
	CharacterSetUtf8 = 33

	// CharacterSetBinary is for binary. Use by integer fields for instance.
	CharacterSetBinary = 63
)

// CharacterSetMap maps the charset name (used in ConnParams) to the
// integer value.  Interesting ones have their own constant above.
var CharacterSetMap = map[string]uint8{
	"big5":     1,
	"dec8":     3,
	"cp850":    4,
	"hp8":      6,
	"koi8r":    7,
	"latin1":   8,
	"latin2":   9,
	"swe7":     10,
	"ascii":    11,
	"ujis":     12,
	"sjis":     13,
	"hebrew":   16,
	"tis620":   18,
	"euckr":    19,
	"koi8u":    22,
	"gb2312":   24,
	"greek":    25,
	"cp1250":   26,
	"gbk":      28,
	"latin5":   30,
	"armscii8": 32,
	"utf8":     CharacterSetUtf8,
	"ucs2":     35,
	"cp866":    36,
	"keybcs2":  37,
	"macce":    38,
	"macroman": 39,
	"cp852":    40,
	"latin7":   41,
	"utf8mb4":  45,
	"cp1251":   51,
	"utf16":    54,
	"utf16le":  56,
	"cp1256":   57,
	"cp1257":   59,
	"utf32":    60,
	"binary":   CharacterSetBinary,
	"geostd8":  92,
	"cp932":    95,
	"eucjpms":  97,
}

const (
	// DefaultAuthPluginName is the default plugin name.
	DefaultAuthPluginName = "mysql_native_password"

	// DefaultServerCapability is the default server capability.
	DefaultServerCapability = CLIENT_LONG_PASSWORD |
		CLIENT_LONG_FLAG |
		CLIENT_CONNECT_WITH_DB |
		CLIENT_PROTOCOL_41 |
		CLIENT_TRANSACTIONS |
		CLIENT_MULTI_STATEMENTS |
		CLIENT_PLUGIN_AUTH |
		CLIENT_DEPRECATE_EOF |
		CLIENT_SECURE_CONNECTION

	// DefaultClientCapability is the default client capability.
	DefaultClientCapability = CLIENT_LONG_PASSWORD |
		CLIENT_LONG_FLAG |
		CLIENT_PROTOCOL_41 |
		CLIENT_TRANSACTIONS |
		CLIENT_MULTI_STATEMENTS |
		CLIENT_PLUGIN_AUTH |
		CLIENT_DEPRECATE_EOF |
		CLIENT_SECURE_CONNECTION
)

var (
	// DefaultSalt is the default salt bytes.
	DefaultSalt = []byte{
		0x77, 0x63, 0x6a, 0x6d, 0x61, 0x22, 0x23, 0x27, // first part
		0x38, 0x26, 0x55, 0x58, 0x3b, 0x5d, 0x44, 0x78, 0x53, 0x73, 0x6b, 0x41}
)

// Vitess data types. These are idiomatically
// named synonyms for the Type values.
const (
	Null       = Type_NULL_TYPE
	Int8       = Type_INT8
	Uint8      = Type_UINT8
	Int16      = Type_INT16
	Uint16     = Type_UINT16
	Int24      = Type_INT24
	Uint24     = Type_UINT24
	Int32      = Type_INT32
	Uint32     = Type_UINT32
	Int64      = Type_INT64
	Uint64     = Type_UINT64
	Float32    = Type_FLOAT32
	Float64    = Type_FLOAT64
	Timestamp  = Type_TIMESTAMP
	Date       = Type_DATE
	Time       = Type_TIME
	Datetime   = Type_DATETIME
	Year       = Type_YEAR
	Decimal    = Type_DECIMAL
	Text       = Type_TEXT
	Blob       = Type_BLOB
	VarChar    = Type_VARCHAR
	VarBinary  = Type_VARBINARY
	Char       = Type_CHAR
	Binary     = Type_BINARY
	Bit        = Type_BIT
	Enum       = Type_ENUM
	Set        = Type_SET
	Tuple      = Type_TUPLE
	Geometry   = Type_GEOMETRY
	TypeJSON   = Type_JSON
	Expression = Type_EXPRESSION
)

// bit-shift the mysql flags by two byte so we
// can merge them with the mysql or vitess types.
const (
	mysqlUnsigned = 32
	mysqlBinary   = 128
	mysqlEnum     = 256
	mysqlSet      = 2048
)

var TypeToMySQL = map[Type]struct {
	Typ   int64
	Flags int64
}{
	Int8:      {Typ: 1},
	Uint8:     {Typ: 1, Flags: mysqlUnsigned},
	Int16:     {Typ: 2},
	Uint16:    {Typ: 2, Flags: mysqlUnsigned},
	Int32:     {Typ: 3},
	Uint32:    {Typ: 3, Flags: mysqlUnsigned},
	Float32:   {Typ: 4},
	Float64:   {Typ: 5},
	Null:      {Typ: 6, Flags: mysqlBinary},
	Timestamp: {Typ: 7},
	Int64:     {Typ: 8},
	Uint64:    {Typ: 8, Flags: mysqlUnsigned},
	Int24:     {Typ: 9},
	Uint24:    {Typ: 9, Flags: mysqlUnsigned},
	Date:      {Typ: 10, Flags: mysqlBinary},
	Time:      {Typ: 11, Flags: mysqlBinary},
	Datetime:  {Typ: 12, Flags: mysqlBinary},
	Year:      {Typ: 13, Flags: mysqlUnsigned},
	Bit:       {Typ: 16, Flags: mysqlUnsigned},
	TypeJSON:  {Typ: 245},
	Decimal:   {Typ: 246},
	Text:      {Typ: 252},
	Blob:      {Typ: 252, Flags: mysqlBinary},
	VarChar:   {Typ: 253},
	VarBinary: {Typ: 253, Flags: mysqlBinary},
	Char:      {Typ: 254},
	Binary:    {Typ: 254, Flags: mysqlBinary},
	Enum:      {Typ: 254, Flags: mysqlEnum},
	Set:       {Typ: 254, Flags: mysqlSet},
	Geometry:  {Typ: 255},
}
