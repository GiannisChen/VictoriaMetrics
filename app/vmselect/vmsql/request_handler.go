package vmsql

import (
	"github.com/VictoriaMetrics/metrics"
	"net/http"
	"time"
)

var sqlDuration = metrics.NewSummary(`vm_request_duration_seconds{path="/api/v1/sql"}`)

// SqlHandler parse and execute standard SQL from request /api/v1/sql.
func SqlHandler(startTime time.Time, w http.ResponseWriter, r *http.Request) error {
	defer sqlDuration.UpdateDuration(startTime)
	return nil
}
