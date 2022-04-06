package vmsql

import (
	"reflect"
	"testing"
)

func TestLoadTableCacheFromFileOrNew(t *testing.T) {
	LoadTableCacheFromFileOrNew("./test")

	err := AddTable(&Table{
		TableName: "add1",
		Columns: []*Column{{ColumnName: "workshop", Type: reflect.String, Tag: true, Nullable: false, Default: "f1"},
			{ColumnName: "status", Type: reflect.String, Tag: true, Nullable: false, Default: "200"},
			{ColumnName: "voltage", Type: reflect.Float64, Tag: false, Nullable: false, Default: "v"},
			{ColumnName: "electricity", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"},
			{ColumnName: "energy", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"}},
		ColMap: nil,
	}, "./test")
	if err != nil {
		t.Fatal(err)
	}

	table, err := FindTable("add1", "./test")
	if err != nil || table == nil {
		t.Fatal("should found add1 but not.")
	}

	err = AddTable(&Table{
		TableName: "add2",
		Columns: []*Column{{ColumnName: "workshop", Type: reflect.String, Tag: true, Nullable: false, Default: "f1"},
			{ColumnName: "status", Type: reflect.String, Tag: true, Nullable: false, Default: "200"},
			{ColumnName: "voltage", Type: reflect.Float64, Tag: false, Nullable: false, Default: "v"},
			{ColumnName: "electricity", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"},
			{ColumnName: "energy", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"}},
		ColMap: nil,
	}, "./test")
	if err != nil {
		t.Fatal(err)
	}
	err = AlterTable(&Table{
		TableName: "add1",
		Columns: []*Column{{ColumnName: "workshop1", Type: reflect.String, Tag: true, Nullable: false, Default: "f1"},
			{ColumnName: "status1", Type: reflect.String, Tag: true, Nullable: false, Default: "200"},
			{ColumnName: "voltage1", Type: reflect.Float64, Tag: false, Nullable: false, Default: "v"},
			{ColumnName: "electricity1", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"},
			{ColumnName: "energy1", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"}},
		ColMap: nil,
	}, "./test")
	if err != nil {
		t.Fatal(err)
	}
	err = DeleteTable("add1", "./test")
	if err != nil {
		t.Fatal(err)
	}

	table, err = FindTable("add1", "./test")
	if err == nil || table != nil {
		t.Fatal("should not found add1 but found.")
	}

	MustCloseCache("./test")
}

func TestLoadTableCacheFromFileOrNewParallel(t *testing.T) {
	LoadTableCacheFromFileOrNew("./test")

	go func() {
		err := AddTable(&Table{
			TableName: "add1",
			Columns: []*Column{{ColumnName: "workshop", Type: reflect.String, Tag: true, Nullable: false, Default: "f1"},
				{ColumnName: "status", Type: reflect.String, Tag: true, Nullable: false, Default: "200"},
				{ColumnName: "voltage", Type: reflect.Float64, Tag: false, Nullable: false, Default: "v"},
				{ColumnName: "electricity", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"},
				{ColumnName: "energy", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"}},
			ColMap: nil,
		}, "./test")
		if err != nil {
			t.Fatal(err)
		}
	}()
	go func() {
		err := AddTable(&Table{
			TableName: "add2",
			Columns: []*Column{{ColumnName: "workshop", Type: reflect.String, Tag: true, Nullable: false, Default: "f1"},
				{ColumnName: "status", Type: reflect.String, Tag: true, Nullable: false, Default: "200"},
				{ColumnName: "voltage", Type: reflect.Float64, Tag: false, Nullable: false, Default: "v"},
				{ColumnName: "electricity", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"},
				{ColumnName: "energy", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"}},
			ColMap: nil,
		}, "./test")
		if err != nil {
			t.Fatal(err)
		}

	}()

	go func() {
		err := AlterTable(&Table{
			TableName: "add1",
			Columns: []*Column{{ColumnName: "workshop1", Type: reflect.String, Tag: true, Nullable: false, Default: "f1"},
				{ColumnName: "status1", Type: reflect.String, Tag: true, Nullable: false, Default: "200"},
				{ColumnName: "voltage1", Type: reflect.Float64, Tag: false, Nullable: false, Default: "v"},
				{ColumnName: "electricity1", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"},
				{ColumnName: "energy1", Type: reflect.Float64, Tag: false, Nullable: false, Default: "0.0"}},
			ColMap: nil,
		}, "./test")
		if err != nil {
			t.Fatal(err)

		}
	}()

	go func() {
		err := DeleteTable("add1", "./test")
		if err != nil {
			t.Fatal(err)
		}
	}()

	MustCloseCache("./test")
}

func TestLoadAndClose(t *testing.T) {
	LoadTableCacheFromFileOrNew("./test")

	MustCloseCache("./test")
}

func TestInit(t *testing.T) {
	Init("./test")
}
