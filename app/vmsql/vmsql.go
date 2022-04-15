package vmsql

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/bufferedwriter"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/metricsql"
	"github.com/valyala/quicktemplate"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var sqlDuration = metrics.NewSummary(`vm_request_duration_seconds{path="/api/v1/sql"}`)

const defaultStep int64 = 300000

func requestHandler(startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	defer sqlDuration.UpdateDuration(startTime)

	sql, rem, err := SplitStatement(r.FormValue("sql"))
	if err != nil {
		return err
	}
	if len(sql) == 0 {
		return fmt.Errorf("missing `sql` arg")
	}
	if len(strings.TrimSpace(rem)) != 0 {
		return fmt.Errorf("supported multi sql")
	}
	if len(sql) > prometheus.MaxQueryLen.N {
		return fmt.Errorf("too long query; got %d bytes; mustn't exceed `-search.maxQueryLen=%d` bytes", len(sql), prometheus.MaxQueryLen.N)
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
		return SelectHandler(tokenizer.ParseTree.(*SelectStatement), startTime, w, r)
	case "INSERT":
		return InsertHandler(tokenizer.ParseTree.(*InsertStatement), startTime, w, r)
	case "CREATE":
		return CreateHandler(tokenizer.ParseTree.(*CreateStatement), startTime, w, r)
	case "DELETE":
		return DeleteHandler(tokenizer.ParseTree.(*DeleteStatement), startTime, w, r)
	case "DROP":
		return DropHandler(tokenizer.ParseTree.(*DropStatement), startTime, w, r)
	case "DESCRIBE":
		return DescribeHandler(tokenizer.ParseTree.(*DescribeStatement), startTime, w, r)
	default:
		return fmt.Errorf("unsupported sql type")
	}
}

func SelectHandler(stmt *SelectStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	tableDirPath := *vmstorage.DataPath + "/table"
	table, err := FindTable(stmt.TableName, tableDirPath)
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
		return selectTagHandler(expr, stmt, w, r, startTime)
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

		switch expr.(type) {
		case *metricsql.MetricExpr:
		default:
			return fmt.Errorf("SELECT: cannot parse where filter")
		}
		tagFilterss, err := getTagFilterssFromExpr(expr.(*metricsql.MetricExpr))
		sq := storage.NewSearchQuery(start, end, tagFilterss)
		w.Header().Set("Content-Type", contentType)
		bw := bufferedwriter.Get(w)
		defer bufferedwriter.Put(bw)

		resultsCh := make(chan *quicktemplate.ByteBuffer, cgroup.AvailableCPUs())
		doneCh := make(chan error, 1)

		rss, err := netstorage.ProcessSearchQuery(sq, true, deadline)
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
				xb.Mn.RemoveTag("table")
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
	evalConfig, err := getEvalConfig(stmt, r, startTime)
	if err != nil {
		return err
	}
	result, err := Exec(evalConfig, expr)
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

	w.Header().Set("Content-Type", "application/json")
	bw := bufferedwriter.Get(w)
	defer bufferedwriter.Put(bw)
	prometheus.WriteSelectStepValueResponse(bw, result)
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("cannot send query range response to remote client: %w", err)
	}
	return nil
}

func InsertHandler(stmt *InsertStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	tableDirPath := *vmstorage.DataPath + "/table"
	table, err := FindTable(stmt.TableName, tableDirPath)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}
	if err := insertHandler(stmt, table); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write([]byte(`{"message":"insert success"}`)); err != nil {
		return err
	}
	return nil
}

func CreateHandler(stmt *CreateStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	tableName := *vmstorage.DataPath + "/table"
	if _, err := FindTable(stmt.CreateTable.TableName, tableName); err == nil {
		if stmt.IfNotExists {
			return nil
		} else {
			return fmt.Errorf("table already exists")
		}
	}
	if err := AddTable(stmt.CreateTable, tableName); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(`{"message":"create success"}`)); err != nil {
		return err
	}
	return nil
}

func DeleteHandler(stmt *DeleteStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	tableDirPath := *vmstorage.DataPath + "/table"
	table, err := FindTable(stmt.TableName, tableDirPath)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}

	defer deleteDuration.UpdateDuration(startTime)

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
	sq := storage.NewSearchQuery(0, ct, tagFilterss)
	deletedCount, err := netstorage.DeleteSeries(sq, deadline)
	if err != nil {
		return fmt.Errorf("cannot delete time series: %w", err)
	}
	if deletedCount > 0 {
		promql.ResetRollupResultCache()
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write([]byte(`{"message":"delete success"}`)); err != nil {
		return err
	}
	return nil
}

func DropHandler(stmt *DropStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	tableDirPath := *vmstorage.DataPath + "/table"
	table, err := FindTable(stmt.TableName, tableDirPath)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}
	defer deleteDuration.UpdateDuration(startTime)

	deadline := searchutils.GetDeadlineForQuery(r, startTime)
	ct := startTime.UnixNano() / 1e6
	tagFilterss, err := getTagFilterssFromTable(table)
	if err != nil {
		return err
	}
	tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{Key: []byte("table"), Value: []byte(table.TableName)})
	sq := storage.NewSearchQuery(0, ct, tagFilterss)
	deletedCount, err := netstorage.DeleteSeries(sq, deadline)
	if err != nil {
		return fmt.Errorf("cannot delete time series: %w", err)
	}
	if deletedCount > 0 {
		promql.ResetRollupResultCache()
	}
	if err := DeleteTable(stmt.TableName, tableDirPath); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(`{"message":"drop success"}`)); err != nil {
		return err
	}
	return nil
}

func DescribeHandler(stmt *DescribeStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	tableDirPath := *vmstorage.DataPath + "/table"
	table, err := FindTable(stmt.TableName, tableDirPath)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}
	if _, err := w.Write([]byte(table.JsonString())); err != nil {
		return err
	}
	return nil
}

var labelValuesDuration = metrics.NewSummary(`vm_request_duration_seconds{path="/api/v1/sql(label)"}`)

var deleteDuration = metrics.NewSummary(`vm_request_duration_seconds{path="/api/v1/sql(drop)"}`)

func selectTagHandler(expr metricsql.Expr, stmt *SelectStatement, w http.ResponseWriter, r *http.Request, startTime time.Time) error {
	switch expr.(type) {
	case *MetricsExpr:
		if expr.(*MetricsExpr) == nil {
			return fmt.Errorf("SELECT: synatx error")
		}
		deadline := searchutils.GetDeadlineForExport(r, startTime)
		defer labelValuesDuration.UpdateDuration(startTime)

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

			if _, err := bw.Write([]byte{'{'}); err != nil {
				return err
			}
			for i, column := range expr.(*MetricsExpr).Columns {
				if i != 0 {
					if _, err := bw.Write([]byte(",\n")); err != nil {
						return err
					}
				}
				var labelValues []string
				var err error
				if column.Metric == nil {
					labelValues, err = netstorage.GetLabelValuesOnTimeRange(column.Name, tr, deadline)
				} else {
					labelValues, err = prometheus.LabelValuesWithMatches(
						column.Name, []string{}, getTagFilterssFromLabelFilters(column.Metric), start, end, deadline)
				}
				if err != nil {
					return err
				}
				prometheus.WriteMultiLabelsValuesResponse(bw, column.Name, labelValues)
				if err := bw.Flush(); err != nil {
					return fmt.Errorf("canot flush label values to remote client: %w", err)
				}
			}
			if _, err := bw.Write([]byte{'}'}); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return fmt.Errorf("canot flush label values to remote client: %w", err)
			}
			return nil
		} else {
			return fmt.Errorf("SELECT: label select doesn't support step")
		}
	default:
		return fmt.Errorf("SELECT: synatx error")
	}
}

func getEvalConfig(stmt *SelectStatement, r *http.Request, startTime time.Time) (*promql.EvalConfig, error) {
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

func getTagFilterssFromLabelFilters(lfs []*metricsql.LabelFilter) [][]storage.TagFilter {
	tagFilterss := make([][]storage.TagFilter, 1)
	for _, lf := range lfs {
		tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{
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
