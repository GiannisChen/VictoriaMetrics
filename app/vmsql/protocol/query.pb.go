package protocol

import "reflect"

// Type defines the various supported data types in bind vars
// and query results.
type Type int32

const (
	// NULL_TYPE specifies a NULL type.
	Type_NULL_TYPE Type = 0
	// INT8 specifies a TINYINT type.
	// Properties: 1, IsNumber.
	Type_INT8 Type = 257
	// UINT8 specifies a TINYINT UNSIGNED type.
	// Properties: 2, IsNumber, IsUnsigned.
	Type_UINT8 Type = 770
	// INT16 specifies a SMALLINT type.
	// Properties: 3, IsNumber.
	Type_INT16 Type = 259
	// UINT16 specifies a SMALLINT UNSIGNED type.
	// Properties: 4, IsNumber, IsUnsigned.
	Type_UINT16 Type = 772
	// INT24 specifies a MEDIUMINT type.
	// Properties: 5, IsNumber.
	Type_INT24 Type = 261
	// UINT24 specifies a MEDIUMINT UNSIGNED type.
	// Properties: 6, IsNumber, IsUnsigned.
	Type_UINT24 Type = 774
	// INT32 specifies a INTEGER type.
	// Properties: 7, IsNumber.
	Type_INT32 Type = 263
	// UINT32 specifies a INTEGER UNSIGNED type.
	// Properties: 8, IsNumber, IsUnsigned.
	Type_UINT32 Type = 776
	// INT64 specifies a BIGINT type.
	// Properties: 9, IsNumber.
	Type_INT64 Type = 265
	// UINT64 specifies a BIGINT UNSIGNED type.
	// Properties: 10, IsNumber, IsUnsigned.
	Type_UINT64 Type = 778
	// FLOAT32 specifies a FLOAT type.
	// Properties: 11, IsFloat.
	Type_FLOAT32 Type = 1035
	// FLOAT64 specifies a DOUBLE or REAL type.
	// Properties: 12, IsFloat.
	Type_FLOAT64 Type = 1036
	// TIMESTAMP specifies a TIMESTAMP type.
	// Properties: 13, IsQuoted.
	Type_TIMESTAMP Type = 2061
	// DATE specifies a DATE type.
	// Properties: 14, IsQuoted.
	Type_DATE Type = 2062
	// TIME specifies a TIME type.
	// Properties: 15, IsQuoted.
	Type_TIME Type = 2063
	// DATETIME specifies a DATETIME type.
	// Properties: 16, IsQuoted.
	Type_DATETIME Type = 2064
	// YEAR specifies a YEAR type.
	// Properties: 17, IsNumber, IsUnsigned.
	Type_YEAR Type = 785
	// DECIMAL specifies a DECIMAL or NUMERIC type.
	// Properties: 18, None.
	Type_DECIMAL Type = 18
	// TEXT specifies a TEXT type.
	// Properties: 19, IsQuoted, IsText.
	Type_TEXT Type = 6163
	// BLOB specifies a BLOB type.
	// Properties: 20, IsQuoted, IsBinary.
	Type_BLOB Type = 10260
	// VARCHAR specifies a VARCHAR type.
	// Properties: 21, IsQuoted, IsText.
	Type_VARCHAR Type = 6165
	// VARBINARY specifies a VARBINARY type.
	// Properties: 22, IsQuoted, IsBinary.
	Type_VARBINARY Type = 10262
	// CHAR specifies a CHAR type.
	// Properties: 23, IsQuoted, IsText.
	Type_CHAR Type = 6167
	// BINARY specifies a BINARY type.
	// Properties: 24, IsQuoted, IsBinary.
	Type_BINARY Type = 10264
	// BIT specifies a BIT type.
	// Properties: 25, IsQuoted.
	Type_BIT Type = 2073
	// ENUM specifies an ENUM type.
	// Properties: 26, IsQuoted.
	Type_ENUM Type = 2074
	// SET specifies a SET type.
	// Properties: 27, IsQuoted.
	Type_SET Type = 2075
	// TUPLE specifies a tuple. This cannot
	// be returned in a QueryResult, but it can
	// be sent as a bind var.
	// Properties: 28, None.
	Type_TUPLE Type = 28
	// GEOMETRY specifies a GEOMETRY type.
	// Properties: 29, IsQuoted.
	Type_GEOMETRY Type = 2077
	// JSON specifies a JSON type.
	// Properties: 30, IsQuoted.
	Type_JSON Type = 2078
	// EXPRESSION specifies a SQL expression.
	// This type is for internal use only.
	// Properties: 31, None.
	Type_EXPRESSION Type = 31
	// HEXNUM specifies a HEXNUM type (unquoted varbinary).
	// Properties: 32, IsText.
	Type_HEXNUM Type = 4128
	// HEXVAL specifies a HEXVAL type (unquoted varbinary).
	// Properties: 33, IsText.
	Type_HEXVAL Type = 4129
)

// Enum value maps for Type.
var (
	Type_name = map[int32]string{
		0:     "NULL_TYPE",
		257:   "INT8",
		770:   "UINT8",
		259:   "INT16",
		772:   "UINT16",
		261:   "INT24",
		774:   "UINT24",
		263:   "INT32",
		776:   "UINT32",
		265:   "INT64",
		778:   "UINT64",
		1035:  "FLOAT32",
		1036:  "FLOAT64",
		2061:  "TIMESTAMP",
		2062:  "DATE",
		2063:  "TIME",
		2064:  "DATETIME",
		785:   "YEAR",
		18:    "DECIMAL",
		6163:  "TEXT",
		10260: "BLOB",
		6165:  "VARCHAR",
		10262: "VARBINARY",
		6167:  "CHAR",
		10264: "BINARY",
		2073:  "BIT",
		2074:  "ENUM",
		2075:  "SET",
		28:    "TUPLE",
		2077:  "GEOMETRY",
		2078:  "JSON",
		31:    "EXPRESSION",
		4128:  "HEXNUM",
		4129:  "HEXVAL",
	}
	Type_value = map[string]int32{
		"NULL_TYPE":  0,
		"INT8":       257,
		"UINT8":      770,
		"INT16":      259,
		"UINT16":     772,
		"INT24":      261,
		"UINT24":     774,
		"INT32":      263,
		"UINT32":     776,
		"INT64":      265,
		"UINT64":     778,
		"FLOAT32":    1035,
		"FLOAT64":    1036,
		"TIMESTAMP":  2061,
		"DATE":       2062,
		"TIME":       2063,
		"DATETIME":   2064,
		"YEAR":       785,
		"DECIMAL":    18,
		"TEXT":       6163,
		"BLOB":       10260,
		"VARCHAR":    6165,
		"VARBINARY":  10262,
		"CHAR":       6167,
		"BINARY":     10264,
		"BIT":        2073,
		"ENUM":       2074,
		"SET":        2075,
		"TUPLE":      28,
		"GEOMETRY":   2077,
		"JSON":       2078,
		"EXPRESSION": 31,
		"HEXNUM":     4128,
		"HEXVAL":     4129,
	}
	Kind_value = map[reflect.Kind]Type{
		reflect.Bool:    Type_UINT8,
		reflect.Int8:    Type_INT8,
		reflect.Uint8:   Type_UINT8,
		reflect.Int16:   Type_INT16,
		reflect.Uint16:  Type_UINT16,
		reflect.Int32:   Type_INT32,
		reflect.Uint32:  Type_UINT32,
		reflect.Int64:   Type_INT64,
		reflect.Uint64:  Type_UINT64,
		reflect.Float32: Type_FLOAT32,
		reflect.Float64: Type_FLOAT64,
		reflect.String:  Type_VARCHAR,
	}
)
