package vmsql

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/prometheus"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/searchutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/metrics"
	"net/http"
	"strings"
	"time"
)

var slowQueries = metrics.NewCounter(`vm_slow_sql_queries_total`)

func getDefaultMaxConcurrentRequests() int {
	n := cgroup.AvailableCPUs()
	if n <= 4 {
		n *= 2
	}
	if n > 16 {
		// A single request can saturate all the CPU cores, so there is no sense
		// in allowing higher number of concurrent requests - they will just contend
		// for unavailable CPU time.
		n = 16
	}
	return n
}

func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
	startTime := time.Now()
	defer requestDuration.UpdateDuration(startTime)

	// Limit the number of concurrent queries.
	select {
	case vmselect.ConcurrencyCh <- struct{}{}:
		defer func() { <-vmselect.ConcurrencyCh }()
	default:
		// Sleep for a while until giving up. This should resolve short bursts in requests.
		concurrencyLimitReached.Inc()
		d := searchutils.GetMaxQueryDuration(r)
		if d > *vmselect.MaxQueueDuration {
			d = *vmselect.MaxQueueDuration
		}
		t := timerpool.Get(d)
		select {
		case vmselect.ConcurrencyCh <- struct{}{}:
			timerpool.Put(t)
			defer func() { <-vmselect.ConcurrencyCh }()
		case <-t.C:
			timerpool.Put(t)
			concurrencyLimitTimeout.Inc()
			err := &httpserver.ErrorWithStatusCode{
				Err: fmt.Errorf("cannot handle more than %d concurrent search requests during %s; possible solutions: "+
					"increase `-search.maxQueueDuration`; increase `-search.maxQueryDuration`; increase `-search.maxConcurrentRequests`; "+
					"increase server capacity",
					*vmselect.MaxConcurrentRequests, d),
				StatusCode: http.StatusServiceUnavailable,
			}
			httpserver.Errorf(w, r, "%s", err)
			return true
		}
	}

	if *vmselect.LogSlowQueryDuration > 0 {
		actualStartTime := time.Now()
		defer func() {
			d := time.Since(actualStartTime)
			if d >= *vmselect.LogSlowQueryDuration {
				remoteAddr := httpserver.GetQuotedRemoteAddr(r)
				requestURI := httpserver.GetRequestURI(r)
				logger.Warnf("slow query according to -search.logSlowQueryDuration=%s: remoteAddr=%s, duration=%.3f seconds; requestURI: %q",
					*vmselect.LogSlowQueryDuration, remoteAddr, d.Seconds(), requestURI)
				slowQueries.Inc()
			}
		}()
	}

	path := strings.Replace(r.URL.Path, "//", "/", -1)
	switch path {
	case "/api/v1/sql":
		sqlRequests.Inc()
		httpserver.EnableCORS(w, r)
		if err := requestHandler(startTime, w, r); err != nil {
			sqlErrors.Inc()
			sendSQLError(w, r, err)
			return true
		}
		return true
	default:
		return false
	}
}

func Init() {
	LoadTableCacheFromFileOrNew(*vmstorage.DataPath + "/table")
}

func Stop() {
	MustCloseCache(*vmstorage.DataPath + "/table")
	logger.Infof("vmsql cache flushed")
}

func sendSQLError(w http.ResponseWriter, r *http.Request, err error) {
	logger.Warnf("error in %q: %s", httpserver.GetRequestURI(r), err)
	w.Header().Set("Content-Type", "application/json")
	statusCode := http.StatusUnprocessableEntity
	var esc *httpserver.ErrorWithStatusCode
	if errors.As(err, &esc) {
		statusCode = esc.StatusCode
	}
	w.WriteHeader(statusCode)
	prometheus.WriteErrorResponse(w, statusCode, err)
}

var (
	concurrencyLimitReached = metrics.NewCounter(`vm_concurrent_sql_limit_reached_total`)
	concurrencyLimitTimeout = metrics.NewCounter(`vm_concurrent_sql_limit_timeout_total`)

	_ = metrics.NewGauge(`vm_concurrent_sql_capacity`, func() float64 {
		return float64(cap(vmselect.ConcurrencyCh))
	})
	_ = metrics.NewGauge(`vm_concurrent_sql_current`, func() float64 {
		return float64(len(vmselect.ConcurrencyCh))
	})
)

var (
	requestDuration = metrics.NewHistogram(`vmsql_request_duration_seconds`)
	sqlRequests     = metrics.NewCounter(`vm_http_requests_total{path="/api/v1/sql"}`)
	sqlErrors       = metrics.NewCounter(`vm_http_request_errors_total{path="/api/v1/sql"}`)
)
