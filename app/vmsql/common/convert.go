package common

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/metricsql"
	"strconv"
)

func GetTagFilterssFromLabelFilters(lfs []*metricsql.LabelFilter) []storage.TagFilter {
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

func ParseInt64(s string, de int64) (int64, error) {
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

func GetTagFilterssFromExpr(expr *metricsql.MetricExpr) ([][]storage.TagFilter, error) {
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
