package vmsql

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"sync"
)

// TableCache caches the tables,
// reduce I/Os and improve the insert&select performance
type TableCache struct {
	sync.RWMutex
	C    map[string]*Table
	Size int
	wg   sync.WaitGroup
}

func loadOrNew(r *filestream.Reader, tablePath string) error {
	var wg sync.WaitGroup
	decoder := gob.NewDecoder(r)
	TableCacheV.Lock()
	defer TableCacheV.Unlock()
	if err := decoder.Decode(&TableCacheV.Size); err != nil {
		return err
	}
	var tableName string

	if TableCacheV.C == nil {
		TableCacheV.C = map[string]*Table{}
	}

	for i := 0; i < TableCacheV.Size; i++ {
		if err := decoder.Decode(&tableName); err != nil {
			return err
		}
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()
			table, err := LoadTableFromDisk(tableName, tablePath)
			if err != nil {
				logger.Errorf("sync error: %s may be deleted but still in cache.bin", tableName)
			}
			TableCacheV.C[tableName] = table
		}(tableName)
	}
	wg.Wait()
	return nil
}

func mustSave(w *filestream.Writer) error {
	encoder := gob.NewEncoder(w)
	TableCacheV.RLock()
	defer TableCacheV.RUnlock()
	if err := encoder.Encode(TableCacheV.Size); err != nil {
		return err
	}
	for k := range TableCacheV.C {
		if err := encoder.Encode(k); err != nil {
			return err
		}
	}
	w.MustFlush(false)
	return nil
}

var TableCacheV *TableCache

func LoadTableCacheFromFileOrNew(tablePath string) {
	TableCacheV = &TableCache{C: map[string]*Table{}, Size: 0}
	TableCacheV.wg.Add(1)
	if err := fs.MkdirAllIfNotExist(tablePath); err != nil {
		logger.Panicf("FATAL: cannot create %q: %s", tablePath, err)
	}
	r, err := filestream.Open(tablePath+"/cache.bin", true)

	if err != nil {
		w, err := filestream.Create(tablePath+"/cache.bin", true)
		if err != nil {
			logger.Panicf("%s/cache.bin cannot open, init empty tableCache, %s", tablePath, err)
		} else {
			logger.Infof("%s/cache.bin cannot open, init empty tableCache, %s", tablePath, err)
		}
		w.MustClose()
		TableCacheV.wg.Done()
		return
	}
	if err := loadOrNew(r, tablePath); err != nil {
		logger.Errorf("cannot load from existed table-cache in %s, use an empty cache.", tablePath)
	}
	TableCacheV.wg.Done()
	r.MustClose()
}

// AddTable use Write-Through Strategy
func AddTable(table *Table, tablePath string) error {
	if table == nil {
		return errors.New("want *Table by got nil")
	}

	table.ColMap = map[string]*Column{}
	for _, column := range table.Columns {
		table.ColMap[column.ColumnName] = column
	}

	TableCacheV.wg.Add(1)
	TableCacheV.Lock()
	if _, ok := TableCacheV.C[table.TableName]; ok {
		TableCacheV.C[table.TableName] = table
	} else {
		if TableCacheV.Size == MaxTableCacheSize {
			for k := range TableCacheV.C {
				delete(TableCacheV.C, k)
				break
			}
		} else {
			TableCacheV.Size++
		}
		TableCacheV.C[table.TableName] = table
	}
	TableCacheV.Unlock()
	TableCacheV.wg.Done()

	if err := SaveTableToDisk(table, tablePath); err != nil {
		return err
	}
	return nil
}

// AlterTable continues to Write Through
func AlterTable(table *Table, tablePath string) error {
	if table == nil {
		return errors.New("want *Table by got nil")
	}
	TableCacheV.wg.Add(1)
	defer TableCacheV.wg.Done()
	TableCacheV.RLock()
	if _, ok := TableCacheV.C[table.TableName]; !ok {
		TableCacheV.RUnlock()
		return errors.New(fmt.Sprintf("cannot find table %s", table.TableName))
	}
	TableCacheV.RUnlock()
	return AddTable(table, tablePath)
}

// DeleteTable continues to Write Through
func DeleteTable(tableName, tablePath string) error {
	if len(tableName) == 0 {
		return errors.New("want tableName by got \"\"")
	}
	TableCacheV.wg.Add(1)
	TableCacheV.Lock()
	if _, ok := TableCacheV.C[tableName]; ok {
		delete(TableCacheV.C, tableName)
		TableCacheV.Size--
	}
	TableCacheV.Unlock()
	TableCacheV.wg.Done()

	if err := DeleteTableOnDisk(tableName, tablePath); err != nil {
		return err
	}
	return nil
}

// FindTable return nil,err if not found.
func FindTable(tableName, tablePath string) (*Table, error) {
	if len(tableName) == 0 {
		return nil, errors.New("want tableName by got \"\"")
	}
	TableCacheV.wg.Add(1)
	TableCacheV.RLock()
	if t, ok := TableCacheV.C[tableName]; ok {
		TableCacheV.RUnlock()
		TableCacheV.wg.Done()
		return t, nil
	}
	TableCacheV.RUnlock()
	TableCacheV.wg.Done()

	table, err := LoadTableFromDisk(tableName, tablePath)
	if err != nil {
		return nil, fmt.Errorf("%s not exists", tableName)
	}

	TableCacheV.wg.Add(1)
	TableCacheV.Lock()
	if TableCacheV.Size == MaxTableCacheSize {
		for k := range TableCacheV.C {
			delete(TableCacheV.C, k)
			break
		}
	} else {
		TableCacheV.Size++
	}
	TableCacheV.C[table.TableName] = table
	TableCacheV.RUnlock()
	TableCacheV.wg.Done()

	return table, nil
}

func MustCloseCache(tablePath string) {
	TableCacheV.wg.Wait()
	if err := fs.MkdirAllIfNotExist(tablePath); err != nil {
		logger.Panicf("FATAL: cannot create %q: %s", tablePath, err)
	}
	w, err := filestream.Create(tablePath+"/cache.bin", true)
	if err != nil {
		logger.Panicf("FATAL: cannot create %q/cache.bin: %s", tablePath, err)
		return
	}
	if err := mustSave(w); err != nil {
		logger.Panicf("FATAL: cannot save TableCache to %q/cache.bin, %s", tablePath, err)
	}
	w.MustFlush(true)
	w.MustClose()
	return
}
