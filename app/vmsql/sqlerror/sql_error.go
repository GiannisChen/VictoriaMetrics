package sqlerror

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
)

const (
	// SQLStateGeneral is the SQLSTATE value for "general error".
	SQLStateGeneral = "HY000"
)

// SQLError is the error structure returned from calling a db library function
type SQLError struct {
	Num     uint16
	State   string
	Message string
	Query   string
}

// NewSQLError creates new sql error.
func NewSQLError(number uint16, args ...interface{}) *SQLError {
	sqlErr := &SQLError{}
	err, ok := SQLErrors[number]
	if !ok {
		unknow := SQLErrors[ER_UNKNOWN_ERROR]
		sqlErr.Num = unknow.Num
		sqlErr.State = unknow.State
		err = unknow
	} else {
		sqlErr.Num = err.Num
		sqlErr.State = err.State
	}
	sqlErr.Message = fmt.Sprintf(err.Message, args...)
	return sqlErr
}

func NewSQLErrorf(number uint16, format string, args ...interface{}) *SQLError {
	sqlErr := &SQLError{}
	err, ok := SQLErrors[number]
	if !ok {
		unknow := SQLErrors[ER_UNKNOWN_ERROR]
		sqlErr.Num = unknow.Num
		sqlErr.State = unknow.State
	} else {
		sqlErr.Num = err.Num
		sqlErr.State = err.State
	}
	sqlErr.Message = fmt.Sprintf(format, args...)
	return sqlErr
}

// NewSQLErrorWithState creates new sql error with state.
func NewSQLErrorWithState(number uint16, state string, format string, args ...interface{}) *SQLError {
	return &SQLError{
		Num:     number,
		State:   state,
		Message: fmt.Sprintf(format, args...),
	}
}

var errExtract = regexp.MustCompile(`.*\(errno ([0-9]*)\) \(sqlstate ([0-9a-zA-Z]{5})\).*`)

func NewSQLErrorFromError(err error) error {
	if err == nil {
		return nil
	}
	if serr, ok := err.(*SQLError); ok {
		return serr
	}

	msg := err.Error()
	match := errExtract.FindStringSubmatch(msg)
	if len(match) < 2 {
		unknown := SQLErrors[ER_UNKNOWN_ERROR]
		return &SQLError{
			Num:     unknown.Num,
			State:   unknown.State,
			Message: msg,
		}
	}
	num, err := strconv.Atoi(match[1])
	if err != nil {
		unknown := SQLErrors[ER_UNKNOWN_ERROR]
		return &SQLError{
			Num:     unknown.Num,
			State:   unknown.State,
			Message: msg,
		}
	}

	return &SQLError{
		Num:     uint16(num),
		State:   match[2],
		Message: msg,
	}
}

// Error implements the error interface
func (se *SQLError) Error() string {
	buf := &bytes.Buffer{}
	buf.WriteString(se.Message)

	// Add MySQL errno and SQLSTATE in a format that we can later parse.
	// There's no avoiding string parsing because all errors
	// are converted to strings anyway at RPC boundaries.
	// See NewSQLErrorFromError.
	fmt.Fprintf(buf, " (errno %v) (sqlstate %v)", se.Num, se.State)

	if se.Query != "" {
		fmt.Fprintf(buf, " during query: %s", se.Query)
	}
	return buf.String()
}

const (
	// Error codes for server-side errors.
	// Originally found in include/mysqld_error.h

	// ER_ERROR_FIRST enum.
	ER_ERROR_FIRST uint16 = 1000

	// ER_CON_COUNT_ERROR enum.
	ER_CON_COUNT_ERROR uint16 = 1040

	// ER_DBACCESS_DENIED_ERROR enum.
	ER_DBACCESS_DENIED_ERROR = 1044

	// ER_ACCESS_DENIED_ERROR enum.
	ER_ACCESS_DENIED_ERROR = 1045

	// ER_NO_DB_ERROR enum.
	ER_NO_DB_ERROR = 1046

	// ER_BAD_DB_ERROR enum.
	ER_BAD_DB_ERROR = 1049

	// ER_BAD_DB_ERROR enum.
	ER_TABLE_EXISTS_ERROR = 1050

	// ER_TOO_LONG_IDENT enum
	ER_TOO_LONG_IDENT = 1059

	// ER_KILL_DENIED_ERROR enum
	ER_KILL_DENIED_ERROR = 1095

	// ER_UNKNOWN_ERROR enum.
	ER_UNKNOWN_ERROR = 1105

	// ER_HOST_NOT_PRIVILEGED enum.
	ER_HOST_NOT_PRIVILEGED = 1130

	// ER_NO_SUCH_TABLE enum.
	ER_NO_SUCH_TABLE = 1146

	// ER_SYNTAX_ERROR enum.
	ER_SYNTAX_ERROR = 1149

	// ER_SPECIFIC_ACCESS_DENIED_ERROR enum.
	ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227

	// ER_UNKNOWN_STORAGE_ENGINE enum.
	ER_UNKNOWN_STORAGE_ENGINE = 1286

	// ER_OPTION_PREVENTS_STATEMENT enum.
	ER_OPTION_PREVENTS_STATEMENT = 1290

	// ER_MALFORMED_PACKET enum.
	ER_MALFORMED_PACKET = 1835

	// Error codes for client-side errors.
	// Originally found in include/mysql/errmsg.h
	// Used when:
	// - the client cannot write an initial auth packet.
	// - the client cannot read an initial auth packet.
	// - the client cannot read a response from the server.

	// CR_SERVER_LOST enum.
	CR_SERVER_LOST = 2013

	// CR_VERSION_ERROR enum.
	// This is returned if the server versions don't match what we support.
	CR_VERSION_ERROR = 2007
)

// SQLErrors is the list of sql errors.
var SQLErrors = map[uint16]*SQLError{
	ER_CON_COUNT_ERROR:              &SQLError{Num: ER_CON_COUNT_ERROR, State: "08004", Message: "Too many connections"},
	ER_DBACCESS_DENIED_ERROR:        &SQLError{Num: ER_DBACCESS_DENIED_ERROR, State: "42000", Message: "Access denied for user '%-.48s'@'%' to database '%-.48s'"},
	ER_ACCESS_DENIED_ERROR:          &SQLError{Num: ER_ACCESS_DENIED_ERROR, State: "28000", Message: "Access denied for user '%-.48s'@'%-.64s' (using password: %s)"},
	ER_NO_DB_ERROR:                  &SQLError{Num: ER_NO_DB_ERROR, State: "3D000", Message: "No database selected"},
	ER_BAD_DB_ERROR:                 &SQLError{Num: ER_BAD_DB_ERROR, State: "42000", Message: "Unknown database '%-.192s'"},
	ER_TABLE_EXISTS_ERROR:           &SQLError{Num: ER_TABLE_EXISTS_ERROR, State: "42S01", Message: "Table '%s' already exists"},
	ER_TOO_LONG_IDENT:               &SQLError{Num: ER_TOO_LONG_IDENT, State: "42000", Message: "Identifier name '%-.100s' is too long"},
	ER_KILL_DENIED_ERROR:            &SQLError{Num: ER_KILL_DENIED_ERROR, State: "HY000", Message: "You are not owner of thread '%-.192s'"},
	ER_UNKNOWN_ERROR:                &SQLError{Num: ER_UNKNOWN_ERROR, State: "HY000", Message: "%v"},
	ER_HOST_NOT_PRIVILEGED:          &SQLError{Num: ER_HOST_NOT_PRIVILEGED, State: "HY000", Message: "Host '%-.64s' is not allowed to connect to this MySQL server"},
	ER_NO_SUCH_TABLE:                &SQLError{Num: ER_NO_SUCH_TABLE, State: "42S02", Message: "Table '%s' doesn't exist"},
	ER_SYNTAX_ERROR:                 &SQLError{Num: ER_SYNTAX_ERROR, State: "42000", Message: "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use, %s"},
	ER_SPECIFIC_ACCESS_DENIED_ERROR: &SQLError{Num: ER_SPECIFIC_ACCESS_DENIED_ERROR, State: "42000", Message: "Access denied; you need (at least one of) the %-.128s privilege(s) for this operation"},
	ER_UNKNOWN_STORAGE_ENGINE:       &SQLError{Num: ER_UNKNOWN_STORAGE_ENGINE, State: "42000", Message: "Unknown storage engine '%v', currently we only support InnoDB and TokuDB"},
	ER_OPTION_PREVENTS_STATEMENT:    &SQLError{Num: ER_OPTION_PREVENTS_STATEMENT, State: "42000", Message: "The MySQL server is running with the %s option so it cannot execute this statement"},
	ER_MALFORMED_PACKET:             &SQLError{Num: ER_MALFORMED_PACKET, State: "HY000", Message: "Malformed communication packet, err: %v"},
	CR_SERVER_LOST:                  &SQLError{Num: CR_SERVER_LOST, State: "HY000", Message: ""},
}
