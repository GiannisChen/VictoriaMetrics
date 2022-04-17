package vmsql

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTable_Save(t *testing.T) {
	table1 := &Table{
		TableName: "test",
		Columns: []*Column{{ColumnName: "city", Type: reflect.String, Tag: true, Default: "beijing"},
			{ColumnName: "workshop", Type: reflect.String, Tag: true, Default: "f1"},
			{ColumnName: "status", Type: reflect.String, Tag: true, Default: "200"},
			{ColumnName: "voltage", Type: reflect.Float64, Tag: false, Default: "v"},
			{ColumnName: "electricity", Type: reflect.Float64, Tag: false, Default: "0.0"},
			{ColumnName: "energy", Type: reflect.Float64, Tag: false, Default: "0.0"}},
		ColMap: nil,
	}
	table2 := &Table{TableName: "test2",
		Columns: []*Column{{ColumnName: "city", Type: reflect.String, Tag: true, Default: "beijing"},
			{ColumnName: "workshop", Type: reflect.String, Tag: true, Default: "f1"},
			{ColumnName: "status", Type: reflect.String, Tag: true, Default: "200"},
			{ColumnName: "voltage", Type: reflect.Float64, Tag: false, Default: "v"},
			{ColumnName: "electricity", Type: reflect.Float64, Tag: false, Default: "0.0"},
			{ColumnName: "energy", Type: reflect.Float64, Tag: false, Default: "0.0"}},
		ColMap: nil}
	table3 := &Table{
		TableName: "test3",
	}

	if err := SaveTableToDisk(table1, "./test"); err != nil {
		t.Fatal(err)
	}

	if err := SaveTableToDisk(table2, "./test"); err != nil {
		t.Fatal(err)
	}

	if err := SaveTableToDisk(table3, "./test"); err != nil {
		t.Fatal(err)
	}
}

func TestTable_LoadOrNew(t *testing.T) {

	table, err := LoadTableFromDisk("test", "./test")
	if err != nil {
		t.Error(err)
	}
	table1, err := LoadTableFromDisk("test1", "./test")
	if err != nil {
		fmt.Println("must error called,", err)
	}
	table2, err := LoadTableFromDisk("test2", "./test")
	if err != nil {
		t.Error(err)
	}
	table3, err := LoadTableFromDisk("test3", "./test")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(table, table1, table2, table3)
	fmt.Printf("%d\n", table.Columns[1].Type)
	fmt.Println("finished")
}

func TestTable_JsonString(t *testing.T) {
	table1 := &Table{
		TableName: "test",
		Columns: []*Column{{ColumnName: "city", Type: reflect.String, Tag: true, Default: "beijing"},
			{ColumnName: "workshop", Type: reflect.String, Tag: true, Default: "f1"},
			{ColumnName: "status", Type: reflect.String, Tag: true, Default: "200"},
			{ColumnName: "voltage", Type: reflect.Float64, Tag: false, Default: "v"},
			{ColumnName: "electricity", Type: reflect.Float64, Tag: false, Default: "0.0"},
			{ColumnName: "energy", Type: reflect.Float64, Tag: false, Default: "0.0"}},
		ColMap: nil,
	}
	table2 := &Table{TableName: "test2",
		Columns: []*Column{{ColumnName: "city", Type: reflect.String, Tag: true, Default: "beijing"},
			{ColumnName: "workshop", Type: reflect.String, Tag: true, Default: "f1"},
			{ColumnName: "status", Type: reflect.String, Tag: true, Default: "200"},
			{ColumnName: "voltage", Type: reflect.Float64, Tag: false, Default: "v"},
			{ColumnName: "electricity", Type: reflect.Float64, Tag: false, Default: "0.0"},
			{ColumnName: "energy", Type: reflect.Float64, Tag: false, Default: "0.0"}},
		ColMap: nil}

	fmt.Println(table1.JsonString())
	fmt.Println(table2.JsonString())
}
