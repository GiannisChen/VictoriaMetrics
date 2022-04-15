package vmsql

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/writeconcurrencylimiter"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastjson/fastfloat"
	"strings"
	"time"
)

type ColumnDescriptor struct {
	ParseTimestamp func(s string) (int64, error)
	TagName        string
	MetricName     string
}

type Row struct {
	Metric    string
	Tags      []Tag
	Value     float64
	Timestamp int64
}

// Tag represents metric tag
type Tag struct {
	Key   string
	Value string
}

type Metric struct {
	Metric string
	Value  float64
}

var (
	rowsInserted  = metrics.NewCounter(`vm_rows_inserted_total{type="vmsql"}`)
	rowsPerInsert = metrics.NewHistogram(`vm_rows_per_insert{type="vmsql"}`)
)

// insertHandler processes /api/v1/sql INSERT part.
func insertHandler(stmt *InsertStatement, table *Table) error {
	return writeconcurrencylimiter.Do(func() error {
		return ParseStmt(stmt, table, func(rows []Row) error {
			return insertRows(rows)
		})
	})
}

func ParseStmt(stmt *InsertStatement, table *Table, callback func(rows []Row) error) error {
	cds, err := ParseColumnDescriptors(stmt, table)
	if err != nil {
		return fmt.Errorf("cannot parse the provided sql format: %w", err)
	}
	var rows []Row
	if stmt.InsertData != nil && len(stmt.InsertData) != 0 {
		for _, datum := range stmt.InsertData {
			var ms []Metric
			var tags []Tag
			var ts int64
			if datum == nil || len(datum) > len(cds) {
				return fmt.Errorf("cannot format values")
			}
			for i, s := range datum {
				if cds[i].ParseTimestamp != nil {
					if t, err := cds[i].ParseTimestamp(s); err != nil {
						return err
					} else {
						ts = t
					}
				} else if cds[i].TagName != "" && cds[i].MetricName == "" {
					tags = append(tags, Tag{Key: cds[i].TagName, Value: s})
				} else if cds[i].TagName == "" && cds[i].MetricName != "" {
					if f, err := fastfloat.Parse(s); err != nil {
						return err
					} else {
						ms = append(ms, Metric{Metric: cds[i].MetricName, Value: f})
					}
				} else {
					return fmt.Errorf("synatx error on column %d", i)
				}
			}
			if len(datum) < len(cds) {
				for i := len(datum); i < len(cds); i++ {
					if cds[i].TagName != "" {
						tags = append(tags, Tag{Key: cds[i].TagName, Value: cds[i].MetricName})
					} else {
						return fmt.Errorf("synatx error")
					}
				}
			}
			if len(tags) == 0 || len(ms) == 0 {
				return fmt.Errorf("empty insert item")
			}
			tags = append([]Tag{{Key: "table", Value: table.TableName}}, tags...)
			for _, m := range ms {
				rows = append(rows, Row{Metric: m.Metric, Tags: tags, Value: m.Value, Timestamp: ts})
			}
		}
		return callback(rows)
	}
	return fmt.Errorf("empty insert item")
}

func ParseColumnDescriptors(stmt *InsertStatement, table *Table) ([]ColumnDescriptor, error) {
	if stmt.IsStar && (stmt.Columns == nil || len(stmt.Columns) == 0) {
		var cds []ColumnDescriptor
		cds = append(cds, ColumnDescriptor{ParseTimestamp: parseTimestamp})
		if table.Columns == nil || len(table.Columns) == 0 {
			return nil, fmt.Errorf("empty clomun table, table %s", table.TableName)
		}
		for _, column := range table.Columns {
			if column.Tag {
				cds = append(cds, ColumnDescriptor{TagName: column.ColumnName})
			} else {
				cds = append(cds, ColumnDescriptor{MetricName: column.ColumnName})
			}
		}
		return cds, nil
	} else if !stmt.IsStar && (stmt.Columns != nil && len(stmt.Columns) != 0) {
		used := make(map[string]bool)
		hasValue, hasTag, hasTime := false, false, 0
		var cds []ColumnDescriptor
		for _, column := range stmt.Columns {
			if strings.ToLower(column) == "timestamp" {
				hasTime++
				cds = append(cds, ColumnDescriptor{ParseTimestamp: parseTimestamp})
				continue
			}
			if strings.ToLower(column) == "datetime" {
				hasTime++
				cds = append(cds, ColumnDescriptor{ParseTimestamp: parseDateTime})
				continue
			}
			if _, ok := table.ColMap[column]; !ok {
				return nil, fmt.Errorf("cannot find the column %s in table %s", column, table.TableName)
			}
			if table.ColMap[column].Tag {
				hasTag = true
				used[column] = true
				cds = append(cds, ColumnDescriptor{TagName: column})
			} else {
				hasValue = true
				used[column] = true
				cds = append(cds, ColumnDescriptor{MetricName: column})
			}
		}
		for _, column := range table.Columns {
			if _, ok := used[column.ColumnName]; !ok {
				if column.Tag && !column.Nullable && column.Default == "" {
					return nil, fmt.Errorf("cannot ignore not null column %s", column.ColumnName)
				} else if column.Tag && !column.Nullable && column.Default != "" {
					hasTag = true
					cds = append(cds, ColumnDescriptor{TagName: column.ColumnName, MetricName: column.Default})
				}
			}
		}
		if hasValue && hasTag && hasTime == 1 {
			return cds, nil
		}
		return nil, fmt.Errorf("sql missing column(s), must contains TIMESTAMP, TAG, VALUE")
	}
	return nil, fmt.Errorf("sql synatx error")
}

func insertRows(rows []Row) error {
	ctx := common.GetInsertCtx()
	defer common.PutInsertCtx(ctx)

	ctx.Reset(len(rows))
	hasRelabeling := relabel.HasRelabeling()
	for i := range rows {
		r := &rows[i]
		ctx.Labels = ctx.Labels[:0]
		ctx.AddLabel("", r.Metric)
		for j := range r.Tags {
			tag := &r.Tags[j]
			ctx.AddLabel(tag.Key, tag.Value)
		}
		if hasRelabeling {
			ctx.ApplyRelabeling()
		}
		if len(ctx.Labels) == 0 {
			// Skip metric without labels.
			continue
		}
		ctx.SortLabelsIfNeeded()
		if err := ctx.WriteDataPoint(nil, ctx.Labels, r.Timestamp, r.Value); err != nil {
			return err
		}
	}
	rowsInserted.Add(len(rows))
	rowsPerInsert.Update(float64(len(rows)))
	return ctx.FlushBufs()
}

func parseTimestamp(s string) (int64, error) {
	n, err := fastfloat.ParseInt64(s)
	if err != nil {
		return 0, fmt.Errorf("cannot parse timestamp milliseconds from %q: %w", s, err)
	}
	return n, nil
}

func parseDateTime(s string) (int64, error) {
	n, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("cannot parse timestamp milliseconds from %q: %w", s, err)
	}
	return n.UnixMilli(), nil
}
