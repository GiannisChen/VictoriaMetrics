package vmsqlserver

import (
	"context"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/promql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/protocol"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmsql/sqlerror"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metricsql"
	"github.com/valyala/quicktemplate"
	"regexp"
	"strings"
	"time"
)

// Default step used if not set.
const defaultStep = 5 * 60 * 1000

// AuthCheck checks the auth by D.I.Y rule.
func (s *Server) AuthCheck(session *Session) error {
	user := session.User()
	if user != "root" {
		return sqlerror.NewSQLErrorf(sqlerror.ER_ACCESS_DENIED_ERROR, "Access denied for user '%v'", user)
	}
	// todo: Auth.authResponse represents password
	return nil
}

func parserComQuery(data []byte) string {
	data = data[1:]
	tmp := common.BytesToString(data)
	if re, err := regexp.Compile(`\s*/\*.*\*/\s*`); err != nil {
		return tmp
	} else {
		return re.ReplaceAllString(tmp, "")
	}
}

func (s *Server) ComQuery(session *Session, query string, callback func(qr protocol.Result) error) error {
	var cond *protocol.Cond
	if cond = handshake(query); cond != nil {
		return callback(cond.Result)
	}
	s.mu.RLock()
	session = s.ss[session.id]
	s.mu.RUnlock()

	sql, rem, err := vmsql.SplitStatement(query)
	if err != nil {
		return err
	}
	if len(sql) == 0 {
		return fmt.Errorf("missing `sql` arg")
	}
	if len(strings.TrimSpace(rem)) != 0 {
		return fmt.Errorf("unsupported multi-sql")
	}
	tokenizer := vmsql.NewStringTokenizer(sql)
	if vmsql.YyParsePooled(tokenizer) != 0 {
		if tokenizer.PartialDDL != nil {
			if typ, val := tokenizer.Scan(); typ != 0 {
				return fmt.Errorf("extra characters encountered after end of DDL: '%s'", val)
			}
			tokenizer.ParseTree = tokenizer.PartialDDL
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
		cond = s.querySelect(tokenizer.ParseTree.(*vmsql.SelectStatement))
	case "CREATE":
		cond = s.queryCreate(tokenizer.ParseTree.(*vmsql.CreateStatement))
	case "DELETE":
		cond = s.queryDelete(tokenizer.ParseTree.(*vmsql.DeleteStatement))
	case "DROP":
		cond = s.queryDrop(tokenizer.ParseTree.(*vmsql.DropStatement))
	case "DESCRIBE":
		cond = s.queryDescribe(tokenizer.ParseTree.(*vmsql.DescribeStatement))
	default:
		cond = &protocol.Cond{Type: protocol.COND_ERROR, Error: fmt.Errorf("unsupported sql type")}
	}

	if cond != nil {
		switch cond.Type {
		case protocol.COND_DELAY:
			select {
			case <-session.killed:
				session.closed = true
				return fmt.Errorf("vmsql session[%v] query[%s] was killed", s.connectionID, query)
			case <-time.After(time.Millisecond * time.Duration(cond.Delay)):
				logger.Infof("vmsql handler delay done...")
			}
			return callback(cond.Result)
		case protocol.COND_ERROR:
			return cond.Error
		case protocol.COND_PANIC:
			logger.Panicf("vmsql handler panic....")
		case protocol.COND_NORMAL:
			return callback(cond.Result)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return fmt.Errorf("vmsql query[%v] error", query)
}

func (s *Server) querySelect(stmt *vmsql.SelectStatement) *protocol.Cond {
	startTime := time.Now()
	at := &auth.Token{ProjectID: 0, AccountID: 0}
	cond := &protocol.Cond{Type: protocol.COND_NORMAL}
	table, err := vmsql.Get(context.Background(), stmt.TableName)
	if err != nil {
		return cond.SetError(err)
	}
	if table == nil {
		return cond.SetError(fmt.Errorf("cannot find table %s", stmt.TableName))
	}

	isTag, expr, err := vmsql.TransSelectStatement(stmt, table)
	if err != nil {
		return cond.SetError(err)
	}

	// isTag use `label/.../value`-like strategy.
	if isTag {
		switch expr.(type) {
		case *vmsql.MetricsExpr:
			if expr.(*vmsql.MetricsExpr) == nil {
				return cond.SetError(fmt.Errorf("SELECT: synatx error"))
			}
			deadline := searchutils.NewDeadline(startTime, time.Minute*5, "-search.maxExportDuration")

			if stmt.WhereFilter == nil || stmt.WhereFilter.TimeFilter == nil || stmt.WhereFilter.TimeFilter.Step == "" {
				start := int64(0)
				end := startTime.UnixMilli()
				if stmt.WhereFilter != nil && stmt.WhereFilter.TimeFilter != nil {
					if s, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.Start); err != nil {
						return cond.SetError(err)
					} else {
						start = s.UnixMilli()
					}
					if e, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.End); err != nil {
						return cond.SetError(err)
					} else {
						end = e.UnixMilli()
					}
				}
				tr := storage.TimeRange{
					MinTimestamp: start,
					MaxTimestamp: end,
				}

				for _, column := range expr.(*vmsql.MetricsExpr).Columns {
					var labelValues []string
					var err error

					if column.Metric == nil {
						labelValues, _, err = netstorage.GetLabelValuesOnTimeRange(at, false, column.Name, tr, deadline)
					} else {
						labelValues, _, err = prometheus.LabelValuesWithMatches(at, false,
							column.Name, []string{}, common.GetTagFilterssFromLabelFilters(column.Metric), start, end, deadline)
					}
					if err != nil {
						return cond.SetError(fmt.Errorf(`cannot obtain label values on time range for %q: %w`, column.Name, err))
					}
					cond.Result = &protocol.MySQLResult{Fields: []*protocol.Field{
						{Name: column.Name, Type: protocol.Type_VARCHAR},
					}}
					bytes := make([][]byte, 0)
					for _, value := range labelValues {
						bytes = append(bytes, []byte(value))
					}
					cond.Result.AppendRow(bytes)
				}
				return nil
			} else {
				return cond.SetError(fmt.Errorf("SELECT: label select doesn't support step"))
			}
		default:
			return cond.SetError(fmt.Errorf("SELECT: synatx error"))
		}
	}

	expr = metricsql.Optimize(expr)
	logger.Infof(fmt.Sprintf("vmsql working, equals to %s", expr.AppendString(nil)))

	// stmt doesn't contain timeFilter or step, use `export`-like strategy.
	if stmt.WhereFilter == nil || stmt.WhereFilter.TimeFilter == nil || stmt.WhereFilter.TimeFilter.Step == "" {
		start := int64(0)
		end := startTime.UnixMilli()
		if stmt.WhereFilter != nil && stmt.WhereFilter.TimeFilter != nil {
			if s, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.Start); err != nil {
				return cond.SetError(err)
			} else {
				start = s.UnixMilli()
			}
			if e, err := time.Parse(time.RFC3339Nano, stmt.WhereFilter.TimeFilter.End); err != nil {
				return cond.SetError(err)
			} else {
				end = e.UnixMilli()
			}
		}
		deadline := searchutils.NewDeadline(startTime, time.Minute*30, "-search.maxExportDuration")

		switch expr.(type) {
		case *metricsql.MetricExpr:
		default:
			return cond.SetError(fmt.Errorf("SELECT: cannot parse where filter"))
		}
		tagFilterss, err := common.GetTagFilterssFromExpr(expr.(*metricsql.MetricExpr))
		sq := storage.NewSearchQuery(at.AccountID, at.ProjectID, start, end, tagFilterss)

		resultsCh := make(chan *quicktemplate.ByteBuffer, cgroup.AvailableCPUs())
		doneCh := make(chan error)
		// Unconditionally deny partial response for the exported data,
		// since users usually expect that the exported data is full.
		rss, _, err2 := netstorage.ProcessSearchQuery(at, true, sq, true, deadline)
		if err2 != nil {
			return cond.SetError(fmt.Errorf("cannot fetch data for %q: %w", sq, err2))
		}
		go func() {
			err := rss.RunParallel(func(rs *netstorage.Result, workerID uint) error {
				xb := prometheus.ExportBlockPool.Get().(*prometheus.ExportBlock)
				xb.Mn = &rs.MetricName
				xb.Timestamps = rs.Timestamps
				xb.Values = rs.Values
				xb.Reset()
				prometheus.ExportBlockPool.Put(xb)
				return nil
			})
			close(resultsCh)
			doneCh <- err
		}()
		err = <-doneCh
		if err != nil {
			return cond.SetError(fmt.Errorf("error during sending the data to remote client: %w", err))
		}
		return cond
	}

	// stmt contains timeFilter, use `query_range`-like strategy.
	evalConfig, err := getEvalConfig(stmt, startTime, at)
	if err != nil {
		return cond.SetError(err)
	}
	result, err := promql.Exec(evalConfig, string(expr.AppendString(nil)), false)
	if err != nil {
		return cond.SetError(fmt.Errorf("cannot execute query: %w", err))
	}
	if evalConfig.Step < prometheus.MaxStepForPointsAdjustment.Milliseconds() {
		queryOffset := time.Second.Milliseconds() * 30
		if (startTime.UnixNano()/1e6)-queryOffset < evalConfig.End {
			result = prometheus.AdjustLastPoints(result, startTime.UnixNano()/1e6-queryOffset,
				startTime.UnixNano()/1e6+evalConfig.Step)
		}
	}
	// Remove NaN values as Prometheus does.
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/153
	result = prometheus.RemoveEmptyValuesAndTimeseries(result)
	return cond.SetResult(ResultConvert(stmt, table, result))
}

func (s *Server) queryCreate(statement *vmsql.CreateStatement) *protocol.Cond {
	return nil
}

func (s *Server) queryDelete(statement *vmsql.DeleteStatement) *protocol.Cond {
	return nil
}

func (s *Server) queryDrop(statement *vmsql.DropStatement) *protocol.Cond {
	return nil
}

func (s *Server) queryDescribe(statement *vmsql.DescribeStatement) *protocol.Cond {
	return nil
}

func getEvalConfig(stmt *vmsql.SelectStatement, startTime time.Time, at *auth.Token) (*promql.EvalConfig, error) {
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

	step, err := common.ParseInt64(stmt.WhereFilter.TimeFilter.Step, defaultStep)
	if err != nil {
		return nil, err
	}

	if startTimestamp > endTimestamp {
		endTimestamp = startTimestamp + defaultStep
	}
	if err := promql.ValidateMaxPointsPerTimeseries(startTimestamp, endTimestamp, step); err != nil {
		return nil, err
	}
	startTimestamp, endTimestamp = promql.AdjustStartEnd(startTimestamp, endTimestamp, step)

	ec := &promql.EvalConfig{
		AuthToken:        at,
		QuotedRemoteAddr: "",
		Deadline:         searchutils.NewDeadline(startTime, time.Second*30, "-search.maxQueryDuration"),
		MayCache:         true,
		LookbackDelta:    0,
		RoundDigits:      100,
		Start:            startTimestamp,
		End:              endTimestamp,
		Step:             step,
	}
	return ec, nil
}

func ResultConvert(stmt *vmsql.SelectStatement, table *vmsql.Table, res []netstorage.Result) protocol.Result {
	r := &protocol.TimeSeriesResult{
		TagFieldsToIndexMap: make(map[string]int, 0),
		TVFieldsToIndexMap:  make(map[string]int, 0),
	}
	r.TVFieldsToIndexMap["timestamp"] = 0
	r.TVFields = append(r.TVFields, &protocol.Field{
		Name: "timestamp",
		Type: protocol.Type_TIMESTAMP,
	})
	if stmt.IsStar {
		for _, column := range table.Columns {
			if column.Tag {
				r.TagFieldsToIndexMap[column.ColumnName] = len(r.TagFields)
				r.TagFields = append(r.TagFields, &protocol.Field{
					Name: column.ColumnName,
					Type: protocol.Kind_value[column.Type],
				})
			} else {
				r.TVFieldsToIndexMap[column.ColumnName] = len(r.TVFields)
				r.TVFields = append(r.TVFields, &protocol.Field{
					Name: column.ColumnName,
					Type: protocol.Kind_value[column.Type],
				})
			}
		}
	} else {
		for _, column := range stmt.Columns {
			tmp := table.ColMap[column[0].Args[0]]
			if tmp.Tag {
				r.TagFieldsToIndexMap[tmp.ColumnName] = len(r.TagFields)
				r.TagFields = append(r.TagFields, &protocol.Field{
					Name: tmp.ColumnName,
					Type: protocol.Kind_value[tmp.Type],
				})
			} else {
				r.TVFieldsToIndexMap[tmp.ColumnName] = len(r.TVFields)
				r.TVFields = append(r.TVFields, &protocol.Field{
					Name: tmp.ColumnName,
					Type: protocol.Kind_value[tmp.Type],
				})
			}
		}
	}

	m := map[string]int{}
	var block *protocol.Block
	for _, re := range res {
		key := re.MetricName.TagsOnlyString()
		if idx, ok := m[key]; !ok {
			block = &protocol.Block{
				Tags: make([][]byte, len(r.TagFields)),
				Data: make([]netstorage.Result, len(r.TVFields)),
			}
			for _, tag := range re.MetricName.Tags {
				if i, ok := r.TagFieldsToIndexMap[string(tag.Key)]; ok {
					block.Tags[i] = tag.Value
				}
			}
			m[key] = len(r.Blocks)
			r.Blocks = append(r.Blocks, block)
		} else {
			block = r.Blocks[idx]
		}
		block.Data[r.TVFieldsToIndexMap[string(re.MetricName.MetricGroup)]] = re
	}

	return r
}

func handshake(query string) *protocol.Cond {
	query = strings.ToLower(query)
	if idx := strings.IndexByte(query, ';'); idx >= 0 {
		query = query[:idx]
	}
	// msg changes.
	if query == "show variables" {
		return &protocol.Cond{Type: protocol.COND_NORMAL, Result: &protocol.MySQLResult{
			Fields: []*protocol.Field{
				{Name: "Variable_name", Type: protocol.Type_VARCHAR},
				{Name: "Value", Type: protocol.Type_VARCHAR},
			},
			Rows: [][][]byte{
				{[]byte("wait_timeout"), []byte("28800")},
				{[]byte("autocommit"), []byte("ON")},
			},
		}}
	}
	if strings.HasPrefix(query, "set") {
		return &protocol.Cond{Type: protocol.COND_NORMAL, Result: &protocol.MySQLResult{}}
	}

	return nil
}
