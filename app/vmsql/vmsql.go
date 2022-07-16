package vmsql

import (
	"context"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/bufferedwriter"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/metricsql"
	"github.com/valyala/fastjson/fastfloat"
	"github.com/valyala/quicktemplate"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	sqlDeleteDuration   = metrics.NewSummary(`vm_request_duration_seconds{path="sql/{}/api/v1/sql<delete>"}`)
	sqlSelectDuration   = metrics.NewSummary(`vm_request_duration_seconds{path="sql/{}/api/v1/sql<select>"}`)
	sqlDropDuration     = metrics.NewSummary(`vm_request_duration_seconds{path="sql/{}/api/v1/sql<drop>"}`)
	sqlCreateDuration   = metrics.NewSummary(`vm_request_duration_seconds{path="sql/{}/api/v1/sql<create>"}`)
	sqlDescribeDuration = metrics.NewSummary(`vm_request_duration_seconds{path="sql/{}/api/v1/sql<describe>"}`)
)

// Default step used if not set.
const defaultStep = 5 * 60 * 1000

func SQLSelectHandler(startTime time.Time, at *auth.Token, w http.ResponseWriter, r *http.Request) error {
	defer sqlSelectDuration.UpdateDuration(startTime)

	sql, rem, err := SplitStatement(r.FormValue("sql"))
	if err != nil {
		return err
	}
	if len(sql) == 0 {
		return fmt.Errorf("missing `sql` arg")
	}
	if len(strings.TrimSpace(rem)) != 0 {
		return fmt.Errorf("unsupported multi-sql")
	}
	tokenizer := NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 {
		if tokenizer.partialDDL != nil {
			if typ, val := tokenizer.Scan(); typ != 0 {
				return fmt.Errorf("extra characters encountered after end of DDL: '%s'", val)
			}
			tokenizer.ParseTree = tokenizer.partialDDL
		}
	}
	if tokenizer.LastError != nil {
		return tokenizer.LastError
	}
	if tokenizer.ParseTree == nil {
		return fmt.Errorf("parTree empty")
	}

	switch tokenizer.ParseTree.Type() {
	case "SELECT":
		return selectHandler(tokenizer.ParseTree.(*SelectStatement), startTime, w, r, at)
	case "CREATE":
		return createHandler(tokenizer.ParseTree.(*CreateStatement), startTime, w, r, at)
	case "DELETE":
		return deleteHandler(tokenizer.ParseTree.(*DeleteStatement), startTime, w, r, at)
	case "DROP":
		return dropHandler(tokenizer.ParseTree.(*DropStatement), startTime, w, r, at)
	case "DESCRIBE":
		return describeHandler(tokenizer.ParseTree.(*DescribeStatement), startTime, w, r, at)
	default:
		return fmt.Errorf("unsupported sql type")
	}
}

func selectHandler(stmt *SelectStatement, startTime time.Time, w http.ResponseWriter, r *http.Request, at *auth.Token) error {
	defer sqlSelectDuration.UpdateDuration(startTime)
	table, err := Get(context.Background(), stmt.TableName)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}

	isTag, expr, err := TransSelectStatement(stmt, table)
	if err != nil {
		return err
	}

	// isTag use `label/.../value`-like strategy.
	if isTag {
		return selectTagHandler(expr, stmt, w, r, startTime, at)
	}

	expr = metricsql.Optimize(expr)
	logger.Infof(fmt.Sprintf("vmsql working, equals to %s", expr.AppendString(nil)))

	// stmt doesn't contain timeFilter or step, use `export`-like strategy.
	if stmt.WhereFilter == nil || stmt.WhereFilter.TimeFilter == nil || stmt.WhereFilter.TimeFilter.Step == "" {
		start := int64(0)
		end := startTime.UnixMilli()
		if stmt.WhereFilter != nil && stmt.WhereFilter.TimeFilter != nil {
			if s, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.Start); err != nil {
				return err
			} else {
				start = s.UnixMilli()
			}
			if e, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.End); err != nil {
				return err
			} else {
				end = e.UnixMilli()
			}
		}
		deadline := searchutils.GetDeadlineForExport(r, startTime)

		writeResponseFunc := prometheus.WriteExportStdResponse
		writeLineFunc := func(xb *prometheus.ExportBlock, resultsCh chan<- *quicktemplate.ByteBuffer) {
			bb := quicktemplate.AcquireByteBuffer()
			prometheus.WriteExportJSONLine(bb, xb)
			resultsCh <- bb
		}
		contentType := "application/stream+json; charset=utf-8"

		format := r.FormValue("format")
		maxRowsPerLine := int(fastfloat.ParseInt64BestEffort(r.FormValue("max_rows_per_line")))
		reduceMemUsage := searchutils.GetBool(r, "reduce_mem_usage")
		if format == "prometheus" {
			contentType = "text/plain; charset=utf-8"
			writeLineFunc = func(xb *prometheus.ExportBlock, resultsCh chan<- *quicktemplate.ByteBuffer) {
				bb := quicktemplate.AcquireByteBuffer()
				prometheus.WriteExportPrometheusLine(bb, xb)
				resultsCh <- bb
			}
		} else if format == "promapi" {
			writeResponseFunc = prometheus.WriteExportPromAPIResponse
			writeLineFunc = func(xb *prometheus.ExportBlock, resultsCh chan<- *quicktemplate.ByteBuffer) {
				bb := quicktemplate.AcquireByteBuffer()
				prometheus.WriteExportPromAPILine(bb, xb)
				resultsCh <- bb
			}
		}
		if maxRowsPerLine > 0 {
			writeLineFuncOrig := writeLineFunc
			writeLineFunc = func(xb *prometheus.ExportBlock, resultsCh chan<- *quicktemplate.ByteBuffer) {
				valuesOrig := xb.Values
				timestampsOrig := xb.Timestamps
				values := valuesOrig
				timestamps := timestampsOrig
				for len(values) > 0 {
					var valuesChunk []float64
					var timestampsChunk []int64
					if len(values) > maxRowsPerLine {
						valuesChunk = values[:maxRowsPerLine]
						timestampsChunk = timestamps[:maxRowsPerLine]
						values = values[maxRowsPerLine:]
						timestamps = timestamps[maxRowsPerLine:]
					} else {
						valuesChunk = values
						timestampsChunk = timestamps
						values = nil
						timestamps = nil
					}
					xb.Values = valuesChunk
					xb.Timestamps = timestampsChunk
					writeLineFuncOrig(xb, resultsCh)
				}
				xb.Values = valuesOrig
				xb.Timestamps = timestampsOrig
			}
		}

		switch expr.(type) {
		case *metricsql.MetricExpr:
		default:
			return fmt.Errorf("SELECT: cannot parse where filter")
		}
		tagFilterss, err := getTagFilterssFromExpr(expr.(*metricsql.MetricExpr))
		sq := storage.NewSearchQuery(at.AccountID, at.ProjectID, start, end, tagFilterss)
		w.Header().Set("Content-Type", contentType)
		bw := bufferedwriter.Get(w)
		defer bufferedwriter.Put(bw)

		resultsCh := make(chan *quicktemplate.ByteBuffer, cgroup.AvailableCPUs())
		doneCh := make(chan error)
		if !reduceMemUsage {
			// Unconditionally deny partial response for the exported data,
			// since users usually expect that the exported data is full.
			denyPartialResponse := true
			rss, _, err := netstorage.ProcessSearchQuery(at, denyPartialResponse, sq, true, deadline)
			if err != nil {
				return fmt.Errorf("cannot fetch data for %q: %w", sq, err)
			}
			go func() {
				err := rss.RunParallel(func(rs *netstorage.Result, workerID uint) error {
					if err := bw.Error(); err != nil {
						return err
					}
					xb := prometheus.ExportBlockPool.Get().(*prometheus.ExportBlock)
					xb.Mn = &rs.MetricName
					xb.Timestamps = rs.Timestamps
					xb.Values = rs.Values
					writeLineFunc(xb, resultsCh)
					xb.Reset()
					prometheus.ExportBlockPool.Put(xb)
					return nil
				})
				close(resultsCh)
				doneCh <- err
			}()
		} else {
			go func() {
				err := netstorage.ExportBlocks(at, sq, deadline, func(mn *storage.MetricName, b *storage.Block, tr storage.TimeRange) error {
					if err := bw.Error(); err != nil {
						return err
					}
					if err := b.UnmarshalData(); err != nil {
						return fmt.Errorf("cannot unmarshal block during export: %s", err)
					}
					xb := prometheus.ExportBlockPool.Get().(*prometheus.ExportBlock)
					xb.Mn = mn
					xb.Timestamps, xb.Values = b.AppendRowsWithTimeRangeFilter(xb.Timestamps[:0], xb.Values[:0], tr)
					if len(xb.Timestamps) > 0 {
						writeLineFunc(xb, resultsCh)
					}
					xb.Reset()
					prometheus.ExportBlockPool.Put(xb)
					return nil
				})
				close(resultsCh)
				doneCh <- err
			}()
		}

		// writeResponseFunc must consume all the data from resultsCh.
		writeResponseFunc(bw, resultsCh)
		if err := bw.Flush(); err != nil {
			return err
		}
		err = <-doneCh
		if err != nil {
			return fmt.Errorf("error during sending the data to remote client: %w", err)
		}
		return nil
	}

	// stmt contains timeFilter, use `query_range`-like strategy.
	evalConfig, err := getEvalConfig(stmt, r, startTime, at)
	if err != nil {
		return err
	}
	result, err := promql.Exec(evalConfig, string(expr.AppendString(nil)), false)
	if err != nil {
		return fmt.Errorf("cannot execute query: %w", err)
	}
	if evalConfig.Step < prometheus.MaxStepForPointsAdjustment.Milliseconds() {
		queryOffset := getLatencyOffsetMilliseconds()
		if (startTime.UnixNano()/1e6)-queryOffset < evalConfig.End {
			result = prometheus.AdjustLastPoints(result, startTime.UnixNano()/1e6-queryOffset,
				startTime.UnixNano()/1e6+evalConfig.Step)
		}
	}
	// Remove NaN values as Prometheus does.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/153
	result = prometheus.RemoveEmptyValuesAndTimeseries(result)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	bw := bufferedwriter.Get(w)
	defer bufferedwriter.Put(bw)
	prometheus.WriteQueryRangeResponse(bw, evalConfig.IsPartialResponse, result)
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("cannot send query range response to remote client: %w", err)
	}
	return nil
}

func createHandler(stmt *CreateStatement, startTime time.Time, w http.ResponseWriter, r *http.Request, at *auth.Token) error {
	defer sqlCreateDuration.UpdateDuration(startTime)

	if _, err := Get(context.Background(), stmt.CreateTable.TableName); err == nil {
		if stmt.IfNotExists {
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(`{"message":"create success"}`)); err != nil {
				return err
			}
			return nil
		} else {
			return fmt.Errorf("table already exists")
		}
	}
	if _, err := Put(context.Background(), stmt.CreateTable.TableName, ""); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(`{"message":"create success"}`)); err != nil {
		return err
	}
	return nil
}

func deleteHandler(stmt *DeleteStatement, startTime time.Time, w http.ResponseWriter, r *http.Request, at *auth.Token) error {
	defer sqlDeleteDuration.UpdateDuration(startTime)

	table, err := Get(context.Background(), stmt.TableName)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}

	defer sqlDeleteDuration.UpdateDuration(startTime)

	deadline := searchutils.GetDeadlineForQuery(r, startTime)
	ct := startTime.UnixNano() / 1e6

	var tagFilterss [][]storage.TagFilter
	if stmt.IsStar && !stmt.HasWhere && stmt.Filters == nil {
		tagFilterss, err = getTagFilterssFromTable(table)
		if err != nil {
			return err
		}
	} else if !stmt.IsStar && stmt.HasWhere && stmt.Filters != nil {
		tagFilterss, err = getTagFilterssFromDeleteStmt(stmt, table)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("")
	}

	tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{Key: []byte("table"), Value: []byte(table.TableName)})
	sq := storage.NewSearchQuery(at.AccountID, at.ProjectID, 0, ct, tagFilterss)
	deletedCount, err := netstorage.DeleteSeries(at, sq, deadline)
	if err != nil {
		return fmt.Errorf("cannot delete time series: %w", err)
	}
	if deletedCount > 0 {
		promql.ResetRollupResultCache()
	}
	return nil
}

func dropHandler(stmt *DropStatement, startTime time.Time, w http.ResponseWriter, r *http.Request, at *auth.Token) error {
	defer sqlDropDuration.UpdateDuration(startTime)
	table, err := Get(context.Background(), stmt.TableName)
	if err != nil || table == nil {
		if stmt.IfExists {
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(`{"message":"drop success"}`)); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}
	defer sqlDeleteDuration.UpdateDuration(startTime)

	deadline := searchutils.GetDeadlineForQuery(r, startTime)
	ct := startTime.UnixNano() / 1e6
	tagFilterss, err := getTagFilterssFromTable(table)
	if err != nil {
		return err
	}
	tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{Key: []byte("table"), Value: []byte(table.TableName)})
	sq := storage.NewSearchQuery(at.AccountID, at.ProjectID, 0, ct, tagFilterss)
	deletedCount, err := netstorage.DeleteSeries(at, sq, deadline)
	if err != nil {
		return fmt.Errorf("cannot delete time series: %w", err)
	}
	if deletedCount > 0 {
		promql.ResetRollupResultCache()
	}
	if err := Delete(context.Background(), stmt.TableName); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	return nil
}

func describeHandler(stmt *DescribeStatement, startTime time.Time, w http.ResponseWriter, r *http.Request, at *auth.Token) error {
	defer sqlDescribeDuration.UpdateDuration(startTime)

	table, err := Get(context.Background(), stmt.TableName)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}

	contentType := "application/stream+json; charset=utf-8"
	w.Header().Set("Content-Type", contentType)
	if _, err := w.Write([]byte(table.JsonString())); err != nil {
		return err
	}
	return nil
}

func selectTagHandler(expr metricsql.Expr, stmt *SelectStatement, w http.ResponseWriter, r *http.Request, startTime time.Time, at *auth.Token) error {
	switch expr.(type) {
	case *MetricsExpr:
		if expr.(*MetricsExpr) == nil {
			return fmt.Errorf("SELECT: synatx error")
		}
		deadline := searchutils.GetDeadlineForExport(r, startTime)

		if stmt.WhereFilter == nil || stmt.WhereFilter.TimeFilter == nil || stmt.WhereFilter.TimeFilter.Step == "" {
			w.Header().Set("Content-Type", "application/json")
			bw := bufferedwriter.Get(w)
			defer bufferedwriter.Put(bw)

			start := int64(0)
			end := startTime.UnixMilli()
			if stmt.WhereFilter != nil && stmt.WhereFilter.TimeFilter != nil {

				if s, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.Start); err != nil {
					return err
				} else {
					start = s.UnixMilli()
				}
				if e, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.End); err != nil {
					return err
				} else {
					end = e.UnixMilli()
				}
			}
			tr := storage.TimeRange{
				MinTimestamp: start,
				MaxTimestamp: end,
			}

			for i, column := range expr.(*MetricsExpr).Columns {
				if i != 0 {
					if _, err := bw.Write([]byte(",\n")); err != nil {
						return err
					}
				}
				var labelValues []string
				var err error
				var isPartial bool

				if column.Metric == nil {
					labelValues, isPartial, err = netstorage.GetLabelValuesOnTimeRange(at, false, column.Name, tr, deadline)
				} else {
					labelValues, isPartial, err = prometheus.LabelValuesWithMatches(at, false,
						column.Name, []string{}, getTagFilterssFromLabelFilters(column.Metric), start, end, deadline)
				}
				if err != nil {
					return fmt.Errorf(`cannot obtain label values on time range for %q: %w`, column.Name, err)
				}

				prometheus.WriteLabelValuesResponse(bw, isPartial, labelValues)
				if err := bw.Flush(); err != nil {
					return fmt.Errorf("canot flush label values to remote client: %w", err)
				}
			}
			return nil
		} else {
			return fmt.Errorf("SELECT: label select doesn't support step")
		}
	default:
		return fmt.Errorf("SELECT: synatx error")
	}
}

func getEvalConfig(stmt *SelectStatement, r *http.Request, startTime time.Time, at *auth.Token) (*promql.EvalConfig, error) {
	deadline := searchutils.GetDeadlineForQuery(r, startTime)
	mayCache := !searchutils.GetBool(r, "nocache")
	lookbackDelta, err := getMaxLookback(r)
	if err != nil {
		return nil, err
	}

	// Validate input args.
	start, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.Start)
	if err != nil {
		return nil, err
	}
	startTimestamp := start.UnixMilli()

	end, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.End)
	if err != nil {
		return nil, err
	}
	endTimestamp := end.UnixMilli()

	step, err := parseInt64(stmt.WhereFilter.TimeFilter.Step, defaultStep)
	if err != nil {
		return nil, err
	}

	if startTimestamp > endTimestamp {
		endTimestamp = startTimestamp + defaultStep
	}
	if err := promql.ValidateMaxPointsPerTimeseries(startTimestamp, endTimestamp, step); err != nil {
		return nil, err
	}
	if mayCache {
		startTimestamp, endTimestamp = promql.AdjustStartEnd(startTimestamp, endTimestamp, step)
	}
	ec := &promql.EvalConfig{
		AuthToken:        at,
		QuotedRemoteAddr: httpserver.GetQuotedRemoteAddr(r),
		Deadline:         deadline,
		MayCache:         mayCache,
		LookbackDelta:    lookbackDelta,
		RoundDigits:      getRoundDigits(r),
		Start:            startTimestamp,
		End:              endTimestamp,
		Step:             step,
	}
	return ec, nil
}

func getMaxLookback(r *http.Request) (int64, error) {
	d := prometheus.MaxLookback.Milliseconds()
	if d == 0 {
		d = prometheus.MaxStalenessInterval.Milliseconds()
	}
	return searchutils.GetDuration(r, "max_lookback", d)
}

func getRoundDigits(r *http.Request) int {
	s := r.FormValue("round_digits")
	if len(s) == 0 {
		return 100
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 100
	}
	return n
}

func parseInt64(s string, de int64) (int64, error) {
	if s != "" {
		if parseInt, err := strconv.ParseInt(s, 10, 64); err != nil {
			return 0, err
		} else {
			return parseInt, nil
		}
	} else {
		return de, nil
	}
}

func getLatencyOffsetMilliseconds() int64 {
	d := prometheus.LatencyOffset.Milliseconds()
	if d <= 1000 {
		d = 1000
	}
	return d
}

func getTagFilterssFromExpr(expr *metricsql.MetricExpr) ([][]storage.TagFilter, error) {
	tagFilterss := make([][]storage.TagFilter, 1)
	if expr == nil || expr.LabelFilters == nil || len(expr.LabelFilters) == 0 {
		return nil, fmt.Errorf("empty filter")
	}
	for _, lf := range expr.LabelFilters {
		if lf.Label == "__name__" {
			tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{
				Key:        nil,
				Value:      []byte(lf.Value),
				IsNegative: lf.IsNegative,
				IsRegexp:   lf.IsRegexp,
			})
		} else {
			tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{
				Key:        []byte(lf.Label),
				Value:      []byte(lf.Value),
				IsNegative: lf.IsNegative,
				IsRegexp:   lf.IsRegexp,
			})
		}

	}
	return tagFilterss, nil
}

func getTagFilterssFromLabelFilters(lfs []*metricsql.LabelFilter) []storage.TagFilter {
	var tagFilterss []storage.TagFilter
	for _, lf := range lfs {
		tagFilterss = append(tagFilterss, storage.TagFilter{
			Key:        []byte(lf.Label),
			Value:      []byte(lf.Value),
			IsNegative: lf.IsNegative,
			IsRegexp:   lf.IsRegexp,
		})
	}
	return tagFilterss
}

func getTagFilterssFromTable(table *Table) ([][]storage.TagFilter, error) {
	metricTagFilter := storage.TagFilter{}
	tagFilterss := make([][]storage.TagFilter, 1)
	for _, column := range table.Columns {
		if column.Tag {
			tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{
				Key:        []byte(column.ColumnName),
				Value:      []byte(""),
				IsNegative: true,
				IsRegexp:   false,
			})
		} else {
			if metricTagFilter.Value == nil {
				metricTagFilter.Value = []byte(column.ColumnName)
				metricTagFilter.IsRegexp = true
			} else {
				metricTagFilter.Value = append(metricTagFilter.Value, []byte("|"+column.ColumnName)...)
			}
		}
	}
	if metricTagFilter.Value == nil {
		return nil, fmt.Errorf("DELETE: cannot transition none-VALUE table")
	} else {
		tagFilterss[0] = append(tagFilterss[0], metricTagFilter)
		return tagFilterss, nil
	}
}

func getTagFilterssFromDeleteStmt(stmt *DeleteStatement, table *Table) ([][]storage.TagFilter, error) {

	metricFilter := storage.TagFilter{}
	columnTagMap := make(map[string][]*storage.TagFilter)
	for _, column := range table.Columns {
		if column.Tag {
			if _, ok := columnTagMap[column.ColumnName]; !ok {
				columnTagMap[column.ColumnName] = []*storage.TagFilter{{
					Key:        []byte(column.ColumnName),
					Value:      []byte(""),
					IsNegative: true,
					IsRegexp:   false}}
			} else {
				columnTagMap[column.ColumnName] = append(columnTagMap[column.ColumnName], &storage.TagFilter{
					Key:        []byte(column.ColumnName),
					Value:      []byte(""),
					IsNegative: true,
					IsRegexp:   false})
			}
		} else {
			if metricFilter.Value == nil {
				metricFilter.Value = []byte(column.ColumnName)
			} else {
				metricFilter.IsRegexp = true
				metricFilter.Value = append(metricFilter.Value, []byte("|"+column.ColumnName)...)
			}
		}
	}

	if stmt.Filters.AndTagFilters == nil || len(stmt.Filters.AndTagFilters) == 0 {
		return nil, fmt.Errorf("DELETE: synatx error")
	}

	for _, filter := range stmt.Filters.AndTagFilters {
		if _, ok := columnTagMap[filter.Key]; !ok {
			return nil, fmt.Errorf("DELETE: no such column %s in table %s", filter.Key, table.TableName)
		} else {
			columnTagMap[filter.Key] = append(columnTagMap[filter.Key], &storage.TagFilter{
				Key:        []byte(filter.Key),
				Value:      []byte(filter.Value),
				IsNegative: filter.IsNegative,
				IsRegexp:   filter.IsRegexp,
			})
		}
	}

	for _, filter := range stmt.Filters.OrTagFilters {
		if fs, ok := columnTagMap[filter.Key]; !ok {
			return nil, fmt.Errorf("DELETE: no such column %s in table %s", filter.Key, table.TableName)
		} else {
			isIn := false
			for _, f := range fs {
				if filter.IsNegative == f.IsNegative {
					f.IsRegexp = true
					f.Value = append(f.Value, []byte("|"+filter.Value)...)
					isIn = true
					break
				}
			}
			if !isIn {
				return nil, fmt.Errorf("DELETE: synatx error")
			}
		}
	}
	tagFilterss := make([][]storage.TagFilter, 1)
	tagFilterss[0] = append(tagFilterss[0], metricFilter)
	for _, filters := range columnTagMap {
		for _, filter := range filters {
			tagFilterss[0] = append(tagFilterss[0], *filter)
		}
	}
	return tagFilterss, nil
}
