package vmsql

import (
	"fmt"
	"strings"
)

type keyword struct {
	name string
	id   int
}

func (k *keyword) match(input []byte) bool {
	if len(input) != len(k.name) {
		return false
	}
	for i, c := range input {
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if k.name[i] != c {
			return false
		}
	}
	return true
}

func (k *keyword) matchStr(input string) bool {
	return keywordASCIIMatch(input, k.name)
}

func keywordASCIIMatch(input string, expected string) bool {
	if len(input) != len(expected) {
		return false
	}
	for i := 0; i < len(input); i++ {
		c := input[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if expected[i] != c {
			return false
		}
	}
	return true
}

// keywords is a table of mysql keywords that fall into two categories:
// 1) keywords considered reserved by MySQL
// 2) keywords for us to handle specially in sql.y
//
// Those marked as UNUSED are likely reserved keywords. We add them here so that
// when rewriting queries we can properly backtick quote them so they don't cause issues
//
// NOTE: If you add new keywords, add them also to the reserved_keywords or
// non_reserved_keywords grammar in sql.y -- this will allow the keyword to be used
// in identifiers. See the docs for each grammar to determine which one to put it into.
var keywords = []keyword{

	{"bit", BIT},
	{"bool", BOOL},
	{"boolean", BOOLEAN},
	{"tinyint", TINYINT},
	{"smallint", SMALLINT},
	{"int", INT},
	{"integer", INTEGER},
	{"bigint", BIGINT},
	{"utinyint", UTINYINT},
	{"usmallint", USMALLINT},
	{"uint", UINT},
	{"ubigint", UBIGINT},
	{"real", REAL},
	{"float", FLOAT},
	{"string", STRING},
	{"database", DATABASE},
	{"default", DEFAULT},
	{"double", DOUBLE},
	{"exists", EXISTS},
	{"if", IF},
	{"not", NOT},
	{"null", NULL},
	{"tag", TAG},
	{"table", TABLE},
	{"value", VALUE},
	{"create", CREATE},
	{"values", VALUES},
	{"insert", INSERT},
	{"into", INTO},
	{"drop", DROP},
	{"delete", DELETE},
	{"from", FROM},
	{"like", LIKE},
	{"in", IN},
	{"where", WHERE},
	{"and", AND},
	{"or", OR},
	{"select", SELECT},
	{"limit", LIMIT},
	{"by", BY},
	{"order", ORDER},
	{"group", GROUP},
	{"desc", DESC},
	{"asc", ASC},
	{"describe", DESCRIBE},
}

// keywordStrings contains the reverse mapping of token to keyword strings
var keywordStrings = map[int]string{}

// keywordLookupTable is a perfect hash map that maps **case insensitive** keyword names to their ids
var keywordLookupTable *caseInsensitiveTable

type caseInsensitiveTable struct {
	h map[uint64]keyword
}

func buildCaseInsensitiveTable(keywords []keyword) *caseInsensitiveTable {
	table := &caseInsensitiveTable{
		h: make(map[uint64]keyword, len(keywords)),
	}

	for _, kw := range keywords {
		hash := fnv1aIstr(offset64, kw.name)
		if _, exists := table.h[hash]; exists {
			panic("collision in caseInsensitiveTable")
		}
		table.h[hash] = kw
	}
	return table
}

func (cit *caseInsensitiveTable) LookupString(name string) (int, bool) {
	hash := fnv1aIstr(offset64, name)
	if candidate, ok := cit.h[hash]; ok {
		return candidate.id, candidate.matchStr(name)
	}
	return 0, false
}

func (cit *caseInsensitiveTable) Lookup(name []byte) (int, bool) {
	hash := fnv1aI(offset64, name)
	if candidate, ok := cit.h[hash]; ok {
		return candidate.id, candidate.match(name)
	}
	return 0, false
}

func init() {
	for _, kw := range keywords {
		if kw.id == UNUSED {
			continue
		}
		if kw.name != strings.ToLower(kw.name) {
			panic(fmt.Sprintf("keyword %q must be lowercase in table", kw.name))
		}
		keywordStrings[kw.id] = kw.name
	}

	keywordLookupTable = buildCaseInsensitiveTable(keywords)
}

// KeywordString returns the string corresponding to the given keyword
func KeywordString(id int) string {
	str, ok := keywordStrings[id]
	if !ok {
		return ""
	}
	return str
}

const offset64 = uint64(14695981039346656037)
const prime64 = uint64(1099511628211)

func fnv1aI(h uint64, s []byte) uint64 {
	for _, c := range s {
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		h = (h ^ uint64(c)) * prime64
	}
	return h
}

func fnv1aIstr(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		h = (h ^ uint64(c)) * prime64
	}
	return h
}
