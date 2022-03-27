package encoding

import (
	"bufio"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/gorillaz"
	"github.com/bkaradzic/go-lz4"
	"github.com/golang/snappy"
	"os"
	"testing"
	"time"
)

type Result struct {
	SetName string
	Label   string
	Ratio   float64
	CSpeed  float64
	DSpeed  float64
}

func (r *Result) toString() string {
	return fmt.Sprintf("%s,%s,%.8f,%f,%f\n", r.SetName, r.Label, r.Ratio, r.CSpeed, r.DSpeed)
}

type Item struct {
	Label      string
	compress   func([]int64) (int64, []byte)
	decompress func(int64, []byte, int)
}

var items = []Item{
	{"ZSTDDelta2", func(src []int64) (int64, []byte) {
		var fv int64
		bb := bbPool.Get()
		bb.B, fv = marshalInt64NearestDelta2(bb.B[:0], src, 64)
		compressLevel := getCompressLevel(len(src))
		dst := CompressZSTDLevel(nil, bb.B, compressLevel)
		bbPool.Put(bb)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		var err error
		bb := bbPool.Get()
		bb.B, err = DecompressZSTD(bb.B[:0], src)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		_, err = unmarshalInt64NearestDelta2(nil, bb.B, fv, l)
		bbPool.Put(bb)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		return
	}},
	{"ZSTDDelta", func(src []int64) (int64, []byte) {
		var fv int64
		bb := bbPool.Get()
		bb.B, fv = marshalInt64NearestDelta(bb.B[:0], src, 64)
		compressLevel := getCompressLevel(len(src))
		dst := CompressZSTDLevel(nil, bb.B, compressLevel)
		bbPool.Put(bb)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		var err error
		bb := bbPool.Get()
		bb.B, err = DecompressZSTD(bb.B[:0], src)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		_, err = unmarshalInt64NearestDelta(nil, bb.B, fv, l)
		bbPool.Put(bb)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		return
	}},
	{"Delta2", func(src []int64) (int64, []byte) {
		dst, fv := marshalInt64NearestDelta2(nil, src, 64)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		_, err := unmarshalInt64NearestDelta(nil, src, fv, l)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
	}},
	{"Delta", func(src []int64) (int64, []byte) {
		dst, fv := marshalInt64NearestDelta(nil, src, 64)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		_, err := unmarshalInt64NearestDelta(nil, src, fv, l)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
	}},
	{"ZSTD", func(src []int64) (int64, []byte) {
		bb := bbPool.Get()
		fv := src[0]
		bb.B = MarshalVarInt64s(bb.B[:0], src)
		compressLevel := getCompressLevel(len(src))
		dst := CompressZSTDLevel(nil, bb.B, compressLevel)
		bbPool.Put(bb)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		var err error
		bb := bbPool.Get()
		bb.B, err = DecompressZSTD(bb.B[:0], src)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		dst := make([]int64, l)
		_, err = UnmarshalVarInt64s(dst, bb.B)
		bbPool.Put(bb)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
	}},
	{"GorillaZ", func(src []int64) (int64, []byte) {
		dst, fv, _ := gorillaz.Compress(make([]byte, 0), src)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		_, err := gorillaz.Decompress(make([]int64, l), src)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
	}},
	{"DeltaSnappy", func(src []int64) (int64, []byte) {
		var fv int64
		bb := bbPool.Get()
		bb.B, fv = marshalInt64NearestDelta(bb.B[:0], src, 64)
		dst := snappy.Encode(nil, bb.B)
		bbPool.Put(bb)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		var err error
		bb := bbPool.Get()
		bb.B, err = snappy.Decode(bb.B[:0], src)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		_, err = unmarshalInt64NearestDelta(nil, bb.B, fv, l)
		bbPool.Put(bb)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		return
	}},
	{"DeltaLZ4", func(src []int64) (int64, []byte) {
		var fv int64
		bb := bbPool.Get()
		bb.B, fv = marshalInt64NearestDelta(bb.B[:0], src, 64)
		dst, _ := lz4.Encode(nil, bb.B)
		bbPool.Put(bb)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		var err error
		bb := bbPool.Get()
		bb.B, err = lz4.Decode(bb.B[:0], src)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		_, err = unmarshalInt64NearestDelta(nil, bb.B, fv, l)
		bbPool.Put(bb)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		return
	}},
	{"Delta2Snappy", func(src []int64) (int64, []byte) {
		var fv int64
		bb := bbPool.Get()
		bb.B, fv = marshalInt64NearestDelta2(bb.B[:0], src, 64)
		dst := snappy.Encode(nil, bb.B)
		bbPool.Put(bb)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		var err error
		bb := bbPool.Get()
		bb.B, err = snappy.Decode(bb.B[:0], src)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		_, err = unmarshalInt64NearestDelta2(nil, bb.B, fv, l)
		bbPool.Put(bb)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		return
	}},
	{"Delta2LZ4", func(src []int64) (int64, []byte) {
		var fv int64
		bb := bbPool.Get()
		bb.B, fv = marshalInt64NearestDelta2(bb.B[:0], src, 64)
		dst, _ := lz4.Encode(nil, bb.B)
		bbPool.Put(bb)
		return fv, dst
	}, func(fv int64, src []byte, l int) {
		var err error
		bb := bbPool.Get()
		bb.B, err = lz4.Decode(bb.B[:0], src)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		_, err = unmarshalInt64NearestDelta2(nil, bb.B, fv, l)
		bbPool.Put(bb)
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		return
	}},
}

func generateData(t *testing.T, label string, compress func([]int64) (int64, []byte), decompress func(int64, []byte, int)) (ress []Result) {
	files, err := ReadFileName("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/index.txt", 36)
	if err != nil {
		t.Log(err)
	}

	for _, file := range files {
		float64s, err := ReadAllFloat64File("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/" + file)
		if err != nil {
			t.Fatal(err)
		}
		values, _ := decimal.AppendFloatToInt64(nil, float64s)

		res := Result{SetName: file[:len(file)-4], Label: label}
		if len(values) < 8192 {
			start := time.Now()
			firstValue, dst := compress(values)
			res.CSpeed = float64(time.Now().UnixNano() - start.UnixNano())
			res.Ratio = float64((len(values) * 8)) / float64(len(dst))
			start = time.Now()
			decompress(firstValue, dst, len(values))
			res.DSpeed = float64(time.Now().UnixNano() - start.UnixNano())
			ress = append(ress, res)
			continue
		}

		var cpd, dcpd float64
		for i := 0; i < len(values)-8191; i += 8192 {
			start := time.Now()
			firstValue, dst := compress(values[i : i+8192])
			res.CSpeed += float64(time.Now().UnixNano() - start.UnixNano())
			cpd += float64(len(dst))
			dcpd += float64(len(values[i:i+8192]) * 8)
			start = time.Now()
			decompress(firstValue, dst, 8192)
			res.DSpeed += float64(time.Now().UnixNano() - start.UnixNano())
		}
		start := time.Now()
		firstValue, dst := compress(values[len(values)-8192:])
		res.CSpeed += float64(time.Now().UnixNano() - start.UnixNano())
		cpd += float64(len(dst))
		dcpd += float64(len(values[len(values)-8192:]) * 8)
		start = time.Now()
		decompress(firstValue, dst, 8192)
		res.DSpeed += float64(time.Now().UnixNano() - start.UnixNano())
		res.Ratio = dcpd / cpd
		ress = append(ress, res)
	}

	return
}

func TestCharts(t *testing.T) {
	logs, err := os.OpenFile("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/charts.csv", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()

	w := bufio.NewWriter(logs)
	for _, item := range items {
		ress := generateData(t, item.Label, item.compress, item.decompress)
		for _, result := range ress {
			w.WriteString(result.toString())
		}
		w.Flush()
	}
	return
}
