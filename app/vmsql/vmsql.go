package vmsql

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/bufferedwriter"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/metrics"
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
		return selectHandler(tokenizer.ParseTree.(*SelectStatement), startTime, w, r)
	case "INSERT":
		return insertHandler(tokenizer.ParseTree.(*InsertStatement), startTime, w, r)
	case "CREATE":
		return createHandler(tokenizer.ParseTree.(*CreateStatement), startTime, w, r)
	case "DELETE":
		return nil
	case "DROP":
		return nil
	default:
		return fmt.Errorf("unsupported sql type")
	}
}

func selectHandler(stmt *SelectStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	tableDirPath := *vmstorage.DataPath + "/table"
	table, err := FindTable(stmt.TableName, tableDirPath)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}

	evalConfig, err := getEvalConfig(stmt, r, startTime)
	if err != nil {
		return err
	}

	isTag, expr, err := TransSelectStatement(stmt, table)
	if err != nil {
		return err
	}

	if !isTag {
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
		prometheus.WriteQueryRangeResponse(bw, result)
		if err := bw.Flush(); err != nil {
			return fmt.Errorf("cannot send query range response to remote client: %w", err)
		}
		return nil
	}
	return nil
}

func insertHandler(stmt *InsertStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	tableDirPath := *vmstorage.DataPath + "/table"
	table, err := FindTable(stmt.TableName, tableDirPath)
	if err != nil {
		return err
	}
	if table == nil {
		return fmt.Errorf("cannot find table %s", stmt.TableName)
	}
	return InsertHandler(stmt, table)
}

func createHandler(stmt *CreateStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
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
	_, err := w.Write([]byte("create success"))
	if err != nil {
		return err
	}
	return nil
}

func deleteHandler(stmt *DeleteStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	return nil
}

func dropHandler(stmt *DropStatement, startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	return nil
}

func getEvalConfig(stmt *SelectStatement, r *http.Request, startTime time.Time) (*promql.EvalConfig, error) {
	deadline := searchutils.GetDeadlineForQuery(r, startTime)
	mayCache := !searchutils.GetBool(r, "nocache")
	lookbackDelta, err := getMaxLookback(r)
	if err != nil {
		return nil, err
	}

	end := startTime.UnixNano() / 1e6
	start := end - defaultStep
	step := defaultStep
	// Validate input args.
	if stmt.WhereFilter != nil && stmt.WhereFilter.TimeFilter != nil {
		start, err = parseInt64(stmt.WhereFilter.TimeFilter.Start, startTime.UnixNano()/1e6-defaultStep)
		if err != nil {
			return nil, err
		}
		end, err = parseInt64(stmt.WhereFilter.TimeFilter.End, startTime.UnixNano()/1e6)
		if err != nil {
			return nil, err
		}
		step, err = parseInt64(stmt.WhereFilter.TimeFilter.Step, defaultStep)
		if err != nil {
			return nil, err
		}
	}
	if start > end {
		end = start + defaultStep
	}
	if err := promql.ValidateMaxPointsPerTimeseries(start, end, step); err != nil {
		return nil, err
	}
	if mayCache {
		start, end = promql.AdjustStartEnd(start, end, step)
	}
	ec := &promql.EvalConfig{
		QuotedRemoteAddr: httpserver.GetQuotedRemoteAddr(r),
		Deadline:         deadline,
		MayCache:         mayCache,
		LookbackDelta:    lookbackDelta,
		RoundDigits:      getRoundDigits(r),
		Start:            start,
		End:              end,
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
