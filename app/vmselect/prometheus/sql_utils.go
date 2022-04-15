package prometheus

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	qt422016 "github.com/valyala/quicktemplate"
	qtio422016 "io"
)

func StreamMultiLabelsValuesResponse(qw422016 *qt422016.Writer, columnName string, labelValues []string) {
	qw422016.N().Q(columnName)
	qw422016.N().S(`:[`)
	for i, labelValue := range labelValues {
		qw422016.N().Q(labelValue)
		if i+1 < len(labelValues) {
			qw422016.N().S(`,`)
		}
	}
	qw422016.N().S(`]`)
}

func WriteMultiLabelsValuesResponse(qq422016 qtio422016.Writer, columnName string, labelValues []string) {
	qw422016 := qt422016.AcquireWriter(qq422016)
	StreamMultiLabelsValuesResponse(qw422016, columnName, labelValues)
	qt422016.ReleaseWriter(qw422016)
}

func WriteSelectStepValueResponse(qq422016 qtio422016.Writer, rs []netstorage.Result) {
	qw422016 := qt422016.AcquireWriter(qq422016)
	StreamSelectStepValueResponse(qw422016, rs)
	qt422016.ReleaseWriter(qw422016)
}

func StreamSelectStepValueResponse(qw422016 *qt422016.Writer, rs []netstorage.Result) {
	qw422016.N().S(`{"status":"success","data":{"resultType":"matrix","result":[`)
	if len(rs) > 0 {
		streamSelectStepValueLine(qw422016, &rs[0])
		rs = rs[1:]

		for i := range rs {
			qw422016.N().S(`,`)
			streamSelectStepValueLine(qw422016, &rs[i])
		}
	}
	qw422016.N().S(`]}}`)
}

func streamSelectStepValueLine(qw422016 *qt422016.Writer, r *netstorage.Result) {
	qw422016.N().S(`{"metric":`)
	streamMetricNameObjectWithoutTable(qw422016, &r.MetricName)
	qw422016.N().S(`,"values":`)
	streamvaluesWithTimestamps(qw422016, r.Values, r.Timestamps)
	qw422016.N().S(`}`)
}

func streamMetricNameObjectWithoutTable(qw422016 *qt422016.Writer, mn *storage.MetricName) {
	mn.RemoveTag("table")
	qw422016.N().S(`{`)
	if len(mn.MetricGroup) > 0 {
		qw422016.N().S(`"__name__":`)
		qw422016.N().QZ(mn.MetricGroup)
		if len(mn.Tags) > 0 {
			qw422016.N().S(`,`)
		}
	}
	for j := range mn.Tags {
		tag := &mn.Tags[j]
		qw422016.N().QZ(tag.Key)
		qw422016.N().S(`:`)
		qw422016.N().QZ(tag.Value)
		if j+1 < len(mn.Tags) {
			qw422016.N().S(`,`)
		}
	}
	qw422016.N().S(`}`)
}
