package encoding

import (
	"bufio"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/statistics"
	"io"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"
)

func TestCompressDecompressValues(t *testing.T) {
	var values1, values2, values3 []int64
	var tmpValues2, tmpValues3 []float64
	v := int64(0)
	for i := 0; i < 8*1024; i++ {
		v += int64(rand.NormFloat64() * 1e2)
		values1 = append(values1, v)
	}

	u := float64(0)
	for i := 0; i < 8*1024; i++ {
		u += rand.NormFloat64() * 1e2
		tmpValues2 = append(tmpValues2, u)
	}
	values2, _ = decimal.AppendFloatToInt64(values2, tmpValues2)

	for i := 0; i < 8*1024; i++ {
		tmpValues3 = append(tmpValues3, rand.NormFloat64()*1e2)
	}
	values3, _ = decimal.AppendFloatToInt64(values3, tmpValues3)

	testCompressDecompressValues(t, values1)
	testCompressDecompressValues(t, values2)
	testCompressDecompressValues(t, values3)
}

func testCompressDecompressValues(t *testing.T, values []int64) {
	start := time.Now()
	result, mt, firstValue := marshalInt64s(nil, values, 0)
	values2, err := unmarshalInt64s(nil, result, mt, firstValue, len(values))
	t.Logf("NEW(%d):%d %d %.8f\n", mt, time.Now().UnixNano()-start.UnixNano(), len(result), float64(len(result))/float64(8*len(values)))

	if err != nil {
		t.Fatalf("cannot unmarshal values: %s", err)
	}
	if len(values) != len(values2) {
		t.Fatalf("unmarshal length does not match\n")
	}
	for i := 0; i < len(values); i++ {
		if values[i] != values2[i] {
			t.Fatalf("unmarshal items does not match, values want: %d, but values2 got %d\n",
				values[i], values2[i])
		}
	}
	start = time.Now()
	result, mt, firstValue = MarshalValues(nil, values, 64)
	values2, err = UnmarshalValues(nil, result, mt, firstValue, len(values))
	t.Logf("OLD(%d):%d %d %.8f\n", mt, time.Now().UnixNano()-start.UnixNano(), len(result), float64(len(result))/float64(8*len(values)))
	if err != nil {
		t.Fatalf("cannot unmarshal values: %s", err)
	}
	if err := checkPrecisionBits(values, values2, 64); err != nil {
		t.Fatalf("too low precision for values: %s", err)
	}
}

func TestRealData(t *testing.T) {
	files, err := ReadFileName("/home/giannischen/go/src/giannischen@nuaa.edu.cn/encoding/generatedata/data/real_world_data/index.txt", 3117)
	if err != nil {
		t.Log(err)
	}

	logs, err := os.OpenFile("./data/real_data_result_xor.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()

	w := bufio.NewWriter(logs)

	var newTotal, oldTotal Res
	var news, olds []Res

	for _, file := range files {
		float64s, err := readFloat64File("/home/giannischen/go/src/giannischen@nuaa.edu.cn/encoding/generatedata/data/real_world_data/"+file, 8000)
		if err != nil {
			t.Fatal(err)
		}
		values, _ := decimal.AppendFloatToInt64(nil, float64s)
		n, o := testRealData(t, values)
		newTotal.add(n)
		oldTotal.add(o)

		n.hd = statistics.ShannonEntropy(values)
		o.hd = n.hd
		news = append(news, n)
		olds = append(olds, o)
	}
	sort.Slice(news, func(i, j int) bool {
		return news[i].hd < news[j].hd
	})
	sort.Slice(olds, func(i, j int) bool {
		return olds[i].hd < olds[j].hd
	})
	t.Log(len(news), len(olds))
	for i := 0; i < len(news); i++ {
		if _, err := w.WriteString(fmt.Sprintf("NEW(%.8f)--  ", news[i].hd) + news[i].String()); err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteString(fmt.Sprintf("OLD(%.8f)--  ", olds[i].hd) + olds[i].String() + "\n"); err != nil {
			t.Fatal(err)

		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.WriteString("total:\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("NEW--  " + newTotal.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("OLD--  " + oldTotal.String()); err != nil {
		t.Fatal(err)
	}
	w.Flush()
}

func TestRealDataSolarPower(t *testing.T) {
	logs, err := os.OpenFile("./data/us_solar_power_result_xor.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	w := bufio.NewWriter(logs)
	var newTotal, oldTotal Res

	for i := 0; i < 15000; i++ {
		float64s, err := ReadAllFloat64File(fmt.Sprintf("/home/giannischen/dataSet/solarPower/data/us_solar_power_%d.csv", i))
		if err != nil {
			t.Log(err)
		}
		for j := 0; j < len(float64s)-8191; j += 8192 {
			values, _ := decimal.AppendFloatToInt64(nil, float64s[j:j+8192])
			n, o := testRealData(t, values)
			newTotal.add(n)
			oldTotal.add(o)
			hd := statistics.HammingDistance(values)
			if _, err := w.WriteString(fmt.Sprintf("NEW(%.8f)--  ", hd) + n.String()); err != nil {
				t.Fatal(err)
			}
			if _, err := w.WriteString(fmt.Sprintf("OLD(%.8f)--  ", hd) + o.String() + "\n"); err != nil {
				t.Fatal(err)

			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if len(float64s) >= 8192 {
			values, _ := decimal.AppendFloatToInt64(nil, float64s[len(float64s)-8192:len(float64s)])
			n, o := testRealData(t, values)
			newTotal.add(n)
			oldTotal.add(o)
			hd := statistics.HammingDistance(values)
			if _, err := w.WriteString(fmt.Sprintf("NEW(%.8f)--  ", hd) + n.String()); err != nil {
				t.Fatal(err)
			}
			if _, err := w.WriteString(fmt.Sprintf("OLD(%.8f)--  ", hd) + o.String() + "\n"); err != nil {
				t.Fatal(err)

			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}

	}

	if _, err := w.WriteString("total:\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("NEW--  " + newTotal.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("OLD--  " + oldTotal.String()); err != nil {
		t.Fatal(err)
	}
	w.Flush()
}

func TestRealDataSwitching(t *testing.T) {
	logs, err := os.OpenFile("./data/switching_result_xor.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	w := bufio.NewWriter(logs)
	var newTotal, oldTotal Res

	float64s, err := ReadAllFloat64File("/home/giannischen/dataSet/switching/switching.csv")
	if err != nil {
		t.Log(err)
	}
	for j := 0; j < len(float64s)-8191; j += 8192 {
		values, _ := decimal.AppendFloatToInt64(nil, float64s[j:j+8192])
		n, o := testRealData(t, values)
		newTotal.add(n)
		oldTotal.add(o)
		hd := statistics.HammingDistance(values)
		if _, err := w.WriteString(fmt.Sprintf("NEW(%.8f)--  ", hd) + n.String()); err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteString(fmt.Sprintf("OLD(%.8f)--  ", hd) + o.String() + "\n"); err != nil {
			t.Fatal(err)

		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if len(float64s) >= 8192 {
		values, _ := decimal.AppendFloatToInt64(nil, float64s[len(float64s)-8192:len(float64s)])
		n, o := testRealData(t, values)
		newTotal.add(n)
		oldTotal.add(o)
		hd := statistics.HammingDistance(values)
		if _, err := w.WriteString(fmt.Sprintf("NEW(%.8f)--  ", hd) + n.String()); err != nil {
			t.Fatal(err)
		}
		if _, err := w.WriteString(fmt.Sprintf("OLD(%.8f)--  ", hd) + o.String() + "\n"); err != nil {
			t.Fatal(err)

		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := w.WriteString("total:\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("NEW--  " + newTotal.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("OLD--  " + oldTotal.String()); err != nil {
		t.Fatal(err)
	}
	w.Flush()
}

func TestRealDataStock(t *testing.T) {
	logs, err := os.OpenFile("./data/stock_result_xor.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	w := bufio.NewWriter(logs)

	for _, i := range []int{0} {
		for _, j := range []int{2, 3, 4, 5, 7, 8, 9} {
			var newTotal, oldTotal Res

			float64s, err := ReadAllFloat64File(fmt.Sprintf("/home/giannischen/dataSet/stock2/%d_%d", i, j))
			if err != nil {
				t.Log(err)
			}
			for k := 0; k < len(float64s)-8191; k += 8192 {
				values, _ := decimal.AppendFloatToInt64(nil, float64s[k:k+8192])
				n, o := testRealData(t, values)
				newTotal.add(n)
				oldTotal.add(o)
				//hd := statistics.HammingDistance(values)
				//if _, err := w.WriteString(fmt.Sprintf("NEW(%.8f)--  ", hd) + n.String()); err != nil {
				//	t.Fatal(err)
				//}
				//if _, err := w.WriteString(fmt.Sprintf("OLD(%.8f)--  ", hd) + o.String() + "\n"); err != nil {
				//	t.Fatal(err)
				//
				//}
				//if err := w.Flush(); err != nil {
				//	t.Fatal(err)
				//}
			}
			if len(float64s) >= 8192 {
				values, _ := decimal.AppendFloatToInt64(nil, float64s[len(float64s)-8192:])
				n, o := testRealData(t, values)
				newTotal.add(n)
				oldTotal.add(o)
				//hd := statistics.HammingDistance(values)
				//if _, err := w.WriteString(fmt.Sprintf("NEW(%.8f)--  ", hd) + n.String()); err != nil {
				//	t.Fatal(err)
				//}
				//if _, err := w.WriteString(fmt.Sprintf("OLD(%.8f)--  ", hd) + o.String() + "\n"); err != nil {
				//	t.Fatal(err)
				//
				//}
				//if err := w.Flush(); err != nil {
				//	t.Fatal(err)
				//}
			}
			if _, err := w.WriteString(fmt.Sprintf("total%d_%d:\n", i, j)); err != nil {
				t.Fatal(err)
			}
			if _, err := w.WriteString("NEW--  " + newTotal.String()); err != nil {
				t.Fatal(err)
			}
			if _, err := w.WriteString("OLD--  " + oldTotal.String()); err != nil {
				t.Fatal(err)
			}
			t.Logf("NEW--  " + newTotal.String())
			t.Logf("OLD--  " + oldTotal.String())
			w.Flush()
		}

	}

}

type Res struct {
	ratio           float64
	size            int
	marshalType     MarshalType
	compressSpeed   int64
	decompressSpeed int64
	hd              float64
}

func (r *Res) add(e Res) {
	r.compressSpeed += e.compressSpeed
	r.decompressSpeed += e.decompressSpeed
	r.size += e.size
}

func (r *Res) String() string {
	return fmt.Sprintf("mt: %v  ratio: %.8f  size: %d  c-speed: %d  d-speed: %d\n",
		r.marshalType, r.ratio, r.size, r.compressSpeed, r.decompressSpeed)
}

func testRealData(t *testing.T, values []int64) (n, o Res) {
	n, o = Res{}, Res{}
	start1 := time.Now()
	result1, mt1, firstValue1 := marshalInt64s(nil, values, 0)
	mid1 := time.Now()
	values21, err := unmarshalInt64s(nil, result1, mt1, firstValue1, len(values))

	n.decompressSpeed = time.Now().UnixNano() - mid1.UnixNano()
	n.compressSpeed = mid1.UnixNano() - start1.UnixNano()
	n.size = len(result1)
	n.ratio = float64(len(result1)) / float64(8*len(values))
	n.marshalType = mt1

	if err != nil {
		t.Fatalf("cannot unmarshal values: %s", err)
	}
	if len(values) != len(values21) {
		t.Fatalf("unmarshal length does not match\n")
	}
	for i := 0; i < len(values); i++ {
		if values[i] != values21[i] {
			t.Fatalf("unmarshal items does not match, values want: %d, but values2 got %d\n",
				values[i], values21[i])
		}
	}

	start2 := time.Now()
	result2, mt2, firstValue2 := MarshalValues(nil, values, 64)
	mid2 := time.Now()
	values22, err := UnmarshalValues(nil, result2, mt2, firstValue2, len(values))

	o.decompressSpeed = time.Now().UnixNano() - mid2.UnixNano()
	o.compressSpeed = mid2.UnixNano() - start2.UnixNano()
	o.size = len(result2)
	o.ratio = float64(len(result2)) / float64(8*len(values))
	o.marshalType = mt2

	if err != nil {
		t.Fatalf("cannot unmarshal values: %s", err)
	}
	if err := checkPrecisionBits(values, values22, 64); err != nil {
		t.Fatalf("too low precision for values: %s", err)
	}
	return
}

func readFloat64File(file string, length uint) ([]float64, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	dst := make([]float64, length)
	r := bufio.NewReader(f)
	for i := uint(0); i < length; i++ {
		var num float64
		if n, err := fmt.Fscanf(r, "%f\n", &num); n == 0 || (err != nil && err != io.EOF) {
			return nil, err
		} else {
			dst[i] = num
		}
	}
	return dst, nil
}

func ReadAllFloat64File(file string) ([]float64, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	dst := make([]float64, 0)
	r := bufio.NewReader(f)
	for true {
		var num float64
		if n, err := fmt.Fscanf(r, "%f\n", &num); n == 0 || (err != nil && err != io.EOF) {
			if err == io.EOF {
				return dst, nil
			}
			return dst, nil
		} else {
			dst = append(dst, num)
			if err == io.EOF {
				return dst, nil
			}
		}
	}
	return dst, nil
}

func ReadFileName(file string, length uint) ([]string, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	dst := make([]string, length)
	r := bufio.NewReader(f)
	for i := uint(0); i < length; i++ {
		var name string
		if n, err := fmt.Fscanf(r, "%s\n", &name); n == 0 || (err != nil && err != io.EOF) {
			return nil, err
		} else {
			dst[i] = name
		}
	}
	return dst, nil
}
