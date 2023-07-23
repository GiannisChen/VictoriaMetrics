package gorillazplus

import (
	"bufio"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/gorillaz"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/testutils"
	"os"
	"testing"
)

func TestCompressDecompressInts(t *testing.T) {
	t.Helper()
	files, err := testutils.ReadFileName("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/index.txt", 36)
	if err != nil {
		t.Log(err)
	}
	logs, err := os.OpenFile("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/gorilla.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()

	w := bufio.NewWriter(logs)

	for _, file := range files {
		float64s, err := testutils.ReadAllFloat64File("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/" + file)
		if err != nil {
			t.Fatal(err)
		}
		values, _ := decimal.AppendFloatToInt64(nil, float64s)

		if len(values) < 8192 {
			bytes, _, err := Compress(make([]byte, 0), values)
			if err != nil {
				t.Fatal(err)
			}
			l1 := float64(len(bytes)) / float64(len(values)*8)

			bytes = gorillaz.Compress(make([]byte, 0), values)
			l2 := float64(len(bytes)) / float64(len(values)*8)
			if _, err := w.WriteString(fmt.Sprintf("%.8f,%.8f\n", l1, l2)); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("flush error.")
			}
			continue
		}

		for i := 0; i < len(values)-8191; i += 8192 {
			bytes, _, err := Compress(make([]byte, 0), values[i:i+8192])
			if err != nil {
				t.Fatal(err)
			}
			l1 := float64(len(bytes)) / (8192.0 * 8)

			bytes = gorillaz.Compress(make([]byte, 0), values[i:i+8192])
			l2 := float64(len(bytes)) / (8192.0 * 8)
			if _, err := w.WriteString(fmt.Sprintf("%.8f,%.8f\n", l1, l2)); err != nil {
				t.Fatal(err)
			}
		}
		bytes, _, err := Compress(make([]byte, 0), values[len(values)-8192:])
		if err != nil {
			t.Fatal(err)
		}
		l1 := float64(len(bytes)) / (8192.0 * 8)

		bytes = gorillaz.Compress(make([]byte, 0), values[len(values)-8192:])
		l2 := float64(len(bytes)) / (8192.0 * 8)
		if _, err := w.WriteString(fmt.Sprintf("%.8f,%.8f\n", l1, l2)); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("flush error.")
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush error.")
	}
}
