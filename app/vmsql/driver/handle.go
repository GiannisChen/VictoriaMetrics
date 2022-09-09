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

var variables = map[string]string{
	"wait_timeout":           "28800",
	"autocommit":             "ON",
	"sql_mode":               "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
	"lower_case_table_names": "2",
	"version":                "5.7.25-FakeDB",
	"version_comment":        "TiDB Server (Apache License 2.0) Community Edition, MySQL 5.7 compatible",
	"interactive_timeout":    "28800",
	"offline_mode":           "OFF",
	"version_compile_os":     "windows",
	"Ssl_cipher":             "",
}

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
	if cond = s.handshake(query); cond != nil {
		return callback(cond.Result)
	}
	s.mu.RLock()
	session = s.ss[session.id]
	s.mu.RUnlock()

	sql, rem, err := vmsql.SplitStatement(query)
	if err != nil {
		return sqlerror.NewSQLError(sqlerror.ER_SYNTAX_ERROR, err)
	}
	if len(sql) == 0 {
		return sqlerror.NewSQLError(sqlerror.ER_SYNTAX_ERROR, "empty SQL")
	}
	if len(strings.TrimSpace(rem)) != 0 {
		return sqlerror.NewSQLError(sqlerror.ER_SYNTAX_ERROR, "too many SQLs, must contain only one")
	}
	tokenizer := vmsql.NewStringTokenizer(sql)
	if vmsql.YyParsePooled(tokenizer) != 0 {
		if tokenizer.PartialDDL != nil {
			if typ, val := tokenizer.Scan(); typ != 0 {
				return sqlerror.NewSQLError(sqlerror.ER_SYNTAX_ERROR, "extra characters encountered after end of DDL: '%s'", val)
			}
			tokenizer.ParseTree = tokenizer.PartialDDL
		}
	}
	if tokenizer.LastError != nil {
		return sqlerror.NewSQLError(sqlerror.ER_SYNTAX_ERROR, tokenizer.LastError)
	}
	if tokenizer.ParseTree == nil {
		return sqlerror.NewSQLError(sqlerror.ER_SYNTAX_ERROR, "syntax error")
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
		cond = &protocol.Cond{
			Type:  protocol.COND_ERROR,
			Error: sqlerror.NewSQLError(sqlerror.ER_SYNTAX_ERROR, "unsupported sql type"),
		}
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

func (s *Server) queryCreate(stmt *vmsql.CreateStatement) *protocol.Cond {
	if _, err := vmsql.Get(context.Background(), stmt.CreateTable.TableName); err == nil {
		if stmt.IfNotExists {
			return &protocol.Cond{}
		} else {
			return &protocol.Cond{
				Type:  protocol.COND_ERROR,
				Error: fmt.Errorf("table %q already exists", stmt.CreateTable.TableName),
			}
		}
	}
	if _, err := vmsql.Put(context.Background(), stmt.CreateTable.TableName, ""); err != nil {
		return &protocol.Cond{
			Type:  protocol.COND_ERROR,
			Error: err,
		}
	}
	return &protocol.Cond{}
}

func (s *Server) queryDelete(stmt *vmsql.DeleteStatement) *protocol.Cond {
	cond := &protocol.Cond{}
	table, err := vmsql.Get(context.Background(), stmt.TableName)
	if err != nil {
		return cond.SetError(err)
	}
	if table == nil {
		return cond.SetError(fmt.Errorf("cannot find table %s", stmt.TableName))
	}

	startTime := time.Now()
	deadline := searchutils.NewDeadline(startTime, time.Minute*30, "-search.maxQueryDuration")
	ct := startTime.UnixNano() / 1e6

	var tagFilterss [][]storage.TagFilter
	if stmt.IsStar && !stmt.HasWhere && stmt.Filters == nil {
		tagFilterss, err = getTagFilterssFromTable(table)
		if err != nil {
			return cond.SetError(err)
		}
	} else if !stmt.IsStar && stmt.HasWhere && stmt.Filters != nil {
		tagFilterss, err = getTagFilterssFromDeleteStmt(stmt, table)
		if err != nil {
			return cond.SetError(err)
		}
	} else {
		return cond.SetError(fmt.Errorf("syntax error"))
	}

	at := &auth.Token{}
	tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{Key: []byte("table"), Value: []byte(table.TableName)})
	sq := storage.NewSearchQuery(at.AccountID, at.ProjectID, 0, ct, tagFilterss)
	deletedCount, err := netstorage.DeleteSeries(at, sq, deadline)
	if err != nil {
		return cond.SetError(fmt.Errorf("cannot delete time series: %w", err))
	}
	if deletedCount > 0 {
		promql.ResetRollupResultCache()
	}
	return nil
}

func (s *Server) queryDrop(stmt *vmsql.DropStatement) *protocol.Cond {
	cond := &protocol.Cond{}
	table, err := vmsql.Get(context.Background(), stmt.TableName)
	if err != nil || table == nil {
		if stmt.IfExists {
			return &protocol.Cond{}
		}
		return cond.SetError(fmt.Errorf("cannot find table %s", stmt.TableName))
	}

	startTime := time.Now()
	deadline := searchutils.NewDeadline(startTime, time.Minute*30, "-search.maxQueryDuration")
	ct := startTime.UnixNano() / 1e6
	tagFilterss, err := getTagFilterssFromTable(table)
	if err != nil {
		return cond.SetError(err)
	}
	tagFilterss[0] = append(tagFilterss[0], storage.TagFilter{Key: []byte("table"), Value: []byte(table.TableName)})
	at := &auth.Token{}
	sq := storage.NewSearchQuery(at.AccountID, at.ProjectID, 0, ct, tagFilterss)
	deletedCount, err := netstorage.DeleteSeries(at, sq, deadline)
	if err != nil {
		return cond.SetError(fmt.Errorf("cannot delete time series: %w", err))
	}
	if deletedCount > 0 {
		promql.ResetRollupResultCache()
	}
	if err := vmsql.Delete(context.Background(), stmt.TableName); err != nil {
		return cond.SetError(err)
	}
	return &protocol.Cond{}
}

func (s *Server) queryDescribe(stmt *vmsql.DescribeStatement) *protocol.Cond {
	table, err := vmsql.Get(context.Background(), stmt.TableName)
	if err != nil {
		return &protocol.Cond{
			Type:  protocol.COND_ERROR,
			Error: err,
		}
	}
	if table == nil {
		return &protocol.Cond{
			Type:  protocol.COND_ERROR,
			Error: fmt.Errorf("cannot find table %s", stmt.TableName),
		}
	}

	fields := []*protocol.Field{
		{Name: "Field", Type: protocol.Type_VARCHAR},
		{Name: "Type", Type: protocol.Type_VARCHAR},
		{Name: "Null", Type: protocol.Type_VARCHAR},
		{Name: "Key", Type: protocol.Type_VARCHAR},
		{Name: "Default", Type: protocol.Type_VARCHAR},
		{Name: "Extra", Type: protocol.Type_VARCHAR},
	}
	cond := &protocol.Cond{
		Result: &protocol.MySQLResult{Fields: fields},
	}
	cond.Result.AppendRow([][]byte{
		[]byte("timestamp"),
		[]byte("INT64"),
		[]byte("NO"),
		[]byte("PK"),
		[]byte("0"),
		[]byte("Transparent"),
	})
	for _, column := range table.Columns {
		cond.Result.AppendRow([][]byte{
			[]byte(column.ColumnName),
			[]byte(protocol.Type_name[int32(protocol.Kind_value[column.Type])]),
			[]byte("NO"),
			[]byte(""),
			[]byte(column.Default),
			[]byte(column.StringTag()),
		})
	}
	return cond
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

func (s *Server) handshake(query string) *protocol.Cond {
	query = strings.ToLower(query)
	if idx := strings.IndexByte(query, ';'); idx >= 0 {
		query = query[:idx]
	}
	// msg changes.
	if strings.HasPrefix(query, "set") {
		return &protocol.Cond{}
	} else if strings.HasPrefix(query, "show session") && !strings.Contains(query, "from") {
		item := query[strings.Index(query, `'`)+1 : strings.LastIndex(query, `'`)]
		value := ""
		if _, ok := variables[item]; ok {
			value = variables[item]
		}
		return &protocol.Cond{
			Type: protocol.COND_NORMAL, Result: &protocol.MySQLResult{
				Fields: []*protocol.Field{
					{Name: "Variable_name", Type: protocol.Type_VARCHAR},
					{Name: "Value", Type: protocol.Type_VARCHAR},
				},
				Rows: [][][]byte{
					{[]byte(item), []byte(value)},
				},
			},
		}
	} else if strings.HasPrefix(query, "use") {
		return &protocol.Cond{}
	} else {
		switch query {
		case "show variables":
			return &protocol.Cond{
				Type: protocol.COND_NORMAL, Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{
						{Name: "Variable_name", Type: protocol.Type_VARCHAR},
						{Name: "Value", Type: protocol.Type_VARCHAR},
					},
					Rows: [][][]byte{
						{[]byte("wait_timeout"), []byte("28800")},
						{[]byte("autocommit"), []byte("ON")},
					},
				},
			}
		case "select current_user()":
			return &protocol.Cond{
				Type: protocol.COND_NORMAL,
				Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{{Name: "current_user()", Type: protocol.Type_VARCHAR}},
					Rows:   [][][]byte{{[]byte(`root@%`)}},
				},
			}
		case "select connection_id()":
			return &protocol.Cond{
				Type: protocol.COND_NORMAL,
				Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{{Name: "connection_id()", Type: protocol.Type_VARCHAR}},
					Rows:   [][][]byte{{[]byte(`0`)}},
				},
			}
		case `show character set where charset = 'utf8mb4'`:
			return &protocol.Cond{
				Type: protocol.COND_NORMAL,
				Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{
						{Name: "Charset", Type: protocol.Type_VARCHAR},
						{Name: "Description", Type: protocol.Type_VARCHAR},
						{Name: "Default collation", Type: protocol.Type_VARCHAR},
						{Name: "Maxlen", Type: protocol.Type_INT32},
					},
					Rows: [][][]byte{
						{[]byte(`utf8mb4`), []byte("UTF-8 Unicode"), []byte("utf8mb4_bin"), []byte("4")},
					},
				},
			}
		case `show engines`:
			return &protocol.Cond{
				Type: protocol.COND_NORMAL,
				Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{
						{Name: "Engine", Type: protocol.Type_VARCHAR},
						{Name: "Support", Type: protocol.Type_VARCHAR},
						{Name: "Comment", Type: protocol.Type_VARCHAR},
						{Name: "Transactions", Type: protocol.Type_VARCHAR},
						{Name: "XA", Type: protocol.Type_VARCHAR},
						{Name: "Savepoints", Type: protocol.Type_VARCHAR},
					},
					Rows: [][][]byte{
						{
							[]byte("InnoDB"),
							[]byte("DEFAULT"),
							[]byte("Supports transactions, row-level locking, and foreign keys"),
							[]byte("YES"),
							[]byte("YES"),
							[]byte("YES"),
						},
					},
				}}
		case `show charset`:
			return &protocol.Cond{
				Type: protocol.COND_NORMAL,
				Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{
						{Name: "Charset", Type: protocol.Type_VARCHAR},
						{Name: "Description", Type: protocol.Type_VARCHAR},
						{Name: "Default collation", Type: protocol.Type_VARCHAR},
						{Name: "Maxlen", Type: protocol.Type_INT32},
					},
					Rows: [][][]byte{
						{
							[]byte("utf8mb4"),
							[]byte("UTF-8 Unicode"),
							[]byte("utf8mb4_bin"),
							[]byte("4"),
						},
					},
				},
			}
		case `show collation`:
			return &protocol.Cond{
				Type: protocol.COND_NORMAL,
				Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{
						{Name: "Collation", Type: protocol.Type_VARCHAR},
						{Name: "Charset", Type: protocol.Type_VARCHAR},
						{Name: "Id", Type: protocol.Type_INT32},
						{Name: "Default", Type: protocol.Type_VARCHAR},
						{Name: "Compiled", Type: protocol.Type_VARCHAR},
						{Name: "Sortlen", Type: protocol.Type_INT32},
					},
					Rows: [][][]byte{
						{
							[]byte("ascii_bin"),
							[]byte("ascii"),
							[]byte("65"),
							[]byte("Yes"),
							[]byte("Yes"),
							[]byte("1"),
						},
						{
							[]byte("binary"),
							[]byte("binary"),
							[]byte("63"),
							[]byte("Yes"),
							[]byte("Yes"),
							[]byte("1"),
						},
						{
							[]byte("gbk_bin"),
							[]byte("gbk"),
							[]byte("87"),
							[]byte(""),
							[]byte("Yes"),
							[]byte("1"),
						},
						{
							[]byte("gbk_chinese_ci"),
							[]byte("gbk"),
							[]byte("28"),
							[]byte("Yes"),
							[]byte("Yes"),
							[]byte("1"),
						},
						{
							[]byte("latin1_bin"),
							[]byte("latin1"),
							[]byte("47"),
							[]byte("Yes"),
							[]byte("Yes"),
							[]byte("1"),
						},
						{
							[]byte("utf8_bin"),
							[]byte("utf8"),
							[]byte("83"),
							[]byte("Yes"),
							[]byte("Yes"),
							[]byte("1"),
						},
						{
							[]byte("utf8_general_ci"),
							[]byte("utf8"),
							[]byte("33"),
							[]byte(""),
							[]byte("Yes"),
							[]byte("1"),
						}, {
							[]byte("utf8_unicode_ci"),
							[]byte("utf8"),
							[]byte("192"),
							[]byte(""),
							[]byte("Yes"),
							[]byte("1"),
						}, {
							[]byte("utf8mb4_bin"),
							[]byte("utf8mb4"),
							[]byte("46"),
							[]byte("Yes"),
							[]byte("Yes"),
							[]byte("1"),
						}, {
							[]byte("utf8mb4_general_ci"),
							[]byte("utf8mb4"),
							[]byte("45"),
							[]byte(""),
							[]byte("Yes"),
							[]byte("1"),
						}, {
							[]byte("utf8mb4_unicode_ci"),
							[]byte("utf8mb4"),
							[]byte("224"),
							[]byte(""),
							[]byte("Yes"),
							[]byte("1"),
						},
					},
				},
			}
		case `show databases`:
			return &protocol.Cond{
				Type: protocol.COND_NORMAL,
				Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{
						{Name: "Database", Type: protocol.Type_VARCHAR},
					},
				},
			}
		case `select version()`:
			return &protocol.Cond{
				Type: protocol.COND_NORMAL,
				Result: &protocol.MySQLResult{
					Fields: []*protocol.Field{{Name: "version()", Type: protocol.Type_VARCHAR}},
					Rows:   [][][]byte{{[]byte(s.version)}},
				},
			}
		default:
			return nil
		}
	}
}

func getTagFilterssFromTable(table *vmsql.Table) ([][]storage.TagFilter, error) {
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

func getTagFilterssFromDeleteStmt(stmt *vmsql.DeleteStatement, table *vmsql.Table) ([][]storage.TagFilter, error) {

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
