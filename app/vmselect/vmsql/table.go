package vmsql

import (
	"encoding/gob"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"os"
	"reflect"
)

const (
	MaxTableCacheSize = 64
)

type Table struct {
	TableName string
	Columns   []*Column
	ColMap    map[string]*Column
}

func SaveTableToDisk(t *Table, tablePath string) error {
	w, err := filestream.Create(fmt.Sprintf("%s/%s.bin", tablePath, t.TableName), true)
	defer w.MustClose()
	if err != nil {
		return err
	}
	encoder := gob.NewEncoder(w)
	if err := encoder.Encode(len(t.Columns)); err != nil {
		return err
	}
	for _, column := range t.Columns {
		if err := encoder.Encode(column); err != nil {
			return err
		}
	}
	w.MustFlush(true)
	return nil
}

func LoadTableFromDisk(tableName string, tablePath string) (*Table, error) {
	r, err := filestream.Open(fmt.Sprintf("%s/%s.bin", tablePath, tableName), true)
	if err != nil {
		return nil, err
	}

	decoder := gob.NewDecoder(r)
	var length int
	if err := decoder.Decode(&length); err != nil {
		return nil, err
	}

	t := &Table{
		TableName: tableName,
		Columns:   nil,
		ColMap:    map[string]*Column{},
	}
	for i := 0; i < length; i++ {
		column := &Column{}
		if err := decoder.Decode(&column); err != nil {
			return nil, err
		}
		t.Columns = append(t.Columns, column)
		t.ColMap[column.ColumnName] = column
	}
	r.MustClose()
	return t, nil
}

func DeleteTableOnDisk(tableName string, tablePath string) error {
	return os.Remove(fmt.Sprintf("%s/%s.bin", tablePath, tableName))
}

type Column struct {
	ColumnName string
	Type       reflect.Kind
	Tag        bool
	Nullable   bool
	Default    string
}
