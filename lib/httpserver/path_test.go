package httpserver

import "testing"

func TestParsePath(t *testing.T) {
	t.Helper()

	request := []string{
		"/insert/0/prometheus/api/v1/import",
		"/insert/0:0/prometheus/api/v1/import",
		"/select/0/prometheus/api/v1/query",
		"/select/0:0/graphite/render",
	}

	for _, str := range request {
		p, err := ParsePath(str)
		if err != nil {
			t.Error()
		}
		t.Logf("\nprefix: %s\nauthToken: %s\nsuffix: %s\n", p.Prefix, p.AuthToken, p.Suffix)
	}
}
