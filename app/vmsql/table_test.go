package vmsql

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

var table = &Table{
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

func TestMarshal(t *testing.T) {
	fmt.Println(table.JsonString())
}

func TestGet(t *testing.T) {
	table, err := Get(context.Background(), "a")
	if err != nil {
		panic(err)
	}
	fmt.Println(table.JsonString())
	cMap, err := json.Marshal(table.ColMap)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(cMap))
}

func TestPut(t *testing.T) {
	_, err := Put(context.Background(), table.TableName, table.JsonString())
	if err != nil {
		panic(err)
	}
}
