package vmsql

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
)

type Table struct {
	TableName string             `json:"table_name"`
	Columns   []*Column          `json:"columns"`
	ColMap    map[string]*Column `json:"-"`
}

func (t *Table) JsonString() string {
	bs, _ := json.Marshal(t)
	return string(bs)
}

type Column struct {
	ColumnName string       `json:"column_name"`
	Type       reflect.Kind `json:"type"`
	Tag        bool         `json:"tag"`
	Default    string       `json:"default"`
}

func (c *Column) JsonString() string {
	bs, _ := json.Marshal(c)
	return string(bs)
}

func (c *Column) StringTag() string {
	if c.Tag {
		return "TAG"
	} else {
		return "VALUE"
	}
}

func Get(ctx context.Context, tableName string) (*Table, error) {
	// cli.Get -> []byte
	if tableName != "a" {
		return nil, fmt.Errorf("cannot find table: %s", tableName)
	}
	var table = &Table{ColMap: map[string]*Column{}}
	jsonString := `{"table_name":"a","columns":[{"column_name":"city","type":24,"tag":true,"default":""},{"column_name":"area","type":24,"tag":true,"default":""},{"column_name":"workshop","type":24,"tag":true,"default":""},{"column_name":"machine","type":24,"tag":true,"default":""},{"column_name":"voltage","type":14,"tag":false,"default":""},{"column_name":"electricity","type":14,"tag":false,"default":""},{"column_name":"humidity","type":14,"tag":false,"default":""},{"column_name":"temperature","type":14,"tag":false,"default":""}]}`
	err := json.Unmarshal([]byte(jsonString), &table)
	if err != nil {
		return nil, err
	}
	for _, column := range table.Columns {
		table.ColMap[column.ColumnName] = column
	}
	return table, nil
}

func Put(ctx context.Context, tableName string, table string) (any, error) {
	return nil, nil
}

func Delete(ctx context.Context, tableName string) error {
	return nil
}
