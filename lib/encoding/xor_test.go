package encoding

import (
	"bufio"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/statistics"
	"github.com/valyala/gozstd"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestMarshalUnmarshalInt64DeltaXor(t *testing.T) {
	testMarshalUnmarshalInt64DeltaXor(t, []int64{0}, 4)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{0, 0}, 4)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{1, -3}, 4)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{255, 255}, 4)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{0, 1, 2, 3, 4, 5}, 4)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{5, 4, 3, 2, 1, 0}, 4)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{-5e12, -6e12, -7e12, -8e12, -8.9e12}, 1)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{-5e12, -6e12, -7e12, -8e12, -8.9e12}, 2)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{-5e12, -6e12, -7e12, -8e12, -8.9e12}, 3)
	testMarshalUnmarshalInt64DeltaXor(t, []int64{-5e12, -5.6e12, -7e12, -8e12, -8.9e12}, 4)

	// Verify constant encoding.
	va := []int64{}
	for i := 0; i < 1024; i++ {
		va = append(va, 9876543210123)
	}
	testMarshalUnmarshalInt64DeltaXor(t, va, 4)
	testMarshalUnmarshalInt64DeltaXor(t, va, 63)

	// Verify encoding for monotonically incremented va.
	v := int64(-35)
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += 8
		va = append(va, v)
	}
	testMarshalUnmarshalInt64DeltaXor(t, va, 4)
	testMarshalUnmarshalInt64DeltaXor(t, va, 63)

	// Verify encoding for monotonically decremented va.
	v = 793
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v -= 16
		va = append(va, v)
	}
	testMarshalUnmarshalInt64DeltaXor(t, va, 4)
	testMarshalUnmarshalInt64DeltaXor(t, va, 63)

	// Verify encoding for quadratically incremented va.
	v = -1234567
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += 32 + int64(i)
		va = append(va, v)
	}
	testMarshalUnmarshalInt64DeltaXor(t, va, 4)

	// Verify encoding for decremented va with norm-float noise.
	v = 787933
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v -= 25 + int64(rand.NormFloat64()*2)
		va = append(va, v)
	}
	testMarshalUnmarshalInt64DeltaXor(t, va, 4)

	// Verify encoding for incremented va with random noise.
	v = 943854
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += 30 + rand.Int63n(5)
		va = append(va, v)
	}
	testMarshalUnmarshalInt64DeltaXor(t, va, 4)

	// Verify encoding for constant va with norm-float noise.
	v = -12345
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += int64(rand.NormFloat64() * 10)
		va = append(va, v)
	}
	testMarshalUnmarshalInt64DeltaXor(t, va, 4)

	// Verify encoding for constant va with random noise.
	v = -12345
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += rand.Int63n(15) - 1
		va = append(va, v)
	}
	testMarshalUnmarshalInt64DeltaXor(t, va, 4)
}

func testMarshalUnmarshalInt64DeltaXor(t *testing.T, va []int64, precisionBits uint8) {
	t.Helper()

	b, firstValue := marshalInt64DeltaXor(nil, va, precisionBits)
	vaNew, err := unmarshalInt64DeltaXor(nil, b, firstValue, len(va))
	if err != nil {
		t.Fatalf("cannot unmarshal data for va=%d, precisionBits=%d from b=%x: %s", va, precisionBits, b, err)
	}
	if err = checkPrecisionBits(vaNew, va, precisionBits); err != nil {
		t.Fatalf("too small precisionBits for va=%d, precisionBits=%d: %s, vaNew=\n%d", va, precisionBits, err, vaNew)
	}

	vaPrefix := []int64{1, 2, 3, 4}
	vaNew, err = unmarshalInt64DeltaXor(vaPrefix, b, firstValue, len(va))
	if err != nil {
		t.Fatalf("cannot unmarshal prefixed data for va=%d, precisionBits=%d from b=%x: %s", va, precisionBits, b, err)
	}
	if !reflect.DeepEqual(vaNew[:len(vaPrefix)], vaPrefix) {
		t.Fatalf("unexpected prefix for va=%d, precisionBits=%d: got\n%d; expecting\n%d", va, precisionBits, vaNew[:len(vaPrefix)], vaPrefix)
	}
	if err = checkPrecisionBits(vaNew[len(vaPrefix):], va, precisionBits); err != nil {
		t.Fatalf("too small precisionBits for prefixed va=%d, precisionBits=%d: %s, vaNew=\n%d", va, precisionBits, err, vaNew[len(vaPrefix):])
	}
}

func TestMarshalUnmarshalInt64XorDelta(t *testing.T) {
	testMarshalUnmarshalInt64XorDelta(t, []int64{0}, 4)
	testMarshalUnmarshalInt64XorDelta(t, []int64{0, 0}, 4)
	testMarshalUnmarshalInt64XorDelta(t, []int64{1, -3}, 4)
	testMarshalUnmarshalInt64XorDelta(t, []int64{255, 255}, 4)
	testMarshalUnmarshalInt64XorDelta(t, []int64{0, 1, 2, 3, 4, 5}, 4)
	testMarshalUnmarshalInt64XorDelta(t, []int64{5, 4, 3, 2, 1, 0}, 4)
	testMarshalUnmarshalInt64XorDelta(t, []int64{-5e12, -6e12, -7e12, -8e12, -8.9e12}, 1)
	testMarshalUnmarshalInt64XorDelta(t, []int64{-5e12, -6e12, -7e12, -8e12, -8.9e12}, 2)
	testMarshalUnmarshalInt64XorDelta(t, []int64{-5e12, -6e12, -7e12, -8e12, -8.9e12}, 3)
	testMarshalUnmarshalInt64XorDelta(t, []int64{-5e12, -5.6e12, -7e12, -8e12, -8.9e12}, 4)

	// Verify constant encoding.
	va := []int64{}
	for i := 0; i < 1024; i++ {
		va = append(va, 9876543210123)
	}
	testMarshalUnmarshalInt64XorDelta(t, va, 4)
	testMarshalUnmarshalInt64XorDelta(t, va, 63)

	// Verify encoding for monotonically incremented va.
	v := int64(-35)
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += 8
		va = append(va, v)
	}
	testMarshalUnmarshalInt64XorDelta(t, va, 4)
	testMarshalUnmarshalInt64XorDelta(t, va, 63)

	// Verify encoding for monotonically decremented va.
	v = 793
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v -= 16
		va = append(va, v)
	}
	testMarshalUnmarshalInt64XorDelta(t, va, 4)
	testMarshalUnmarshalInt64XorDelta(t, va, 63)

	// Verify encoding for quadratically incremented va.
	v = -1234567
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += 32 + int64(i)
		va = append(va, v)
	}
	testMarshalUnmarshalInt64XorDelta(t, va, 4)

	// Verify encoding for decremented va with norm-float noise.
	v = 787933
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v -= 25 + int64(rand.NormFloat64()*2)
		va = append(va, v)
	}
	testMarshalUnmarshalInt64XorDelta(t, va, 4)

	// Verify encoding for incremented va with random noise.
	v = 943854
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += 30 + rand.Int63n(5)
		va = append(va, v)
	}
	testMarshalUnmarshalInt64XorDelta(t, va, 4)

	// Verify encoding for constant va with norm-float noise.
	v = -12345
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += int64(rand.NormFloat64() * 10)
		va = append(va, v)
	}
	testMarshalUnmarshalInt64XorDelta(t, va, 4)

	// Verify encoding for constant va with random noise.
	v = -12345
	va = []int64{}
	for i := 0; i < 1024; i++ {
		v += rand.Int63n(15) - 1
		va = append(va, v)
	}
	testMarshalUnmarshalInt64XorDelta(t, va, 4)
}

func testMarshalUnmarshalInt64XorDelta(t *testing.T, va []int64, precisionBits uint8) {
	t.Helper()

	b, firstValue := marshalInt64DeltaXor(nil, va, precisionBits)
	vaNew, err := unmarshalInt64DeltaXor(nil, b, firstValue, len(va))
	if err != nil {
		t.Fatalf("cannot unmarshal data for va=%d, precisionBits=%d from b=%x: %s", va, precisionBits, b, err)
	}
	if err = checkPrecisionBits(vaNew, va, precisionBits); err != nil {
		t.Fatalf("too small precisionBits for va=%d, precisionBits=%d: %s, vaNew=\n%d", va, precisionBits, err, vaNew)
	}

	vaPrefix := []int64{1, 2, 3, 4}
	vaNew, err = unmarshalInt64DeltaXor(vaPrefix, b, firstValue, len(va))
	if err != nil {
		t.Fatalf("cannot unmarshal prefixed data for va=%d, precisionBits=%d from b=%x: %s", va, precisionBits, b, err)
	}
	if !reflect.DeepEqual(vaNew[:len(vaPrefix)], vaPrefix) {
		t.Fatalf("unexpected prefix for va=%d, precisionBits=%d: got\n%d; expecting\n%d", va, precisionBits, vaNew[:len(vaPrefix)], vaPrefix)
	}
	if err = checkPrecisionBits(vaNew[len(vaPrefix):], va, precisionBits); err != nil {
		t.Fatalf("too small precisionBits for prefixed va=%d, precisionBits=%d: %s, vaNew=\n%d", va, precisionBits, err, vaNew[len(vaPrefix):])
	}
}

func TestMarshalUnMarshalCompared2(t *testing.T) {
	logs, err := os.OpenFile("./data/xor_test_result_10000.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	w := bufio.NewWriter(logs)

	var (
		totalCompressedXor, totalCompressed2Xor, totalUncompressedXor          int64
		totalCompressedDelta, totalCompressed2Delta, totalUncompressedDelta    int64
		totalCompressedDelta2, totalCompressed2Delta2, totalUncompressedDelta2 int64
	)

	for i := 0; i < 10000; i++ {
		va := benchInt64XorArray
		hd, _ := statistics.ComplexHammingDistance(va)
		compressedSize, compressed2Size, uncompressedSize := testMarshalUnMarshalCompared(t, va,
			marshalInt64DeltaXor, unmarshalInt64DeltaXor)
		totalCompressedXor += compressedSize
		totalCompressed2Xor += compressed2Size
		totalUncompressedXor += uncompressedSize
		if _, err := w.WriteString(fmt.Sprintf("Xor    %.8f \t%d \t%d \t%d\n",
			hd, compressedSize, compressed2Size, uncompressedSize)); err != nil {
			t.Fatal(err)
		}

		compressedSize, compressed2Size, uncompressedSize = testMarshalUnMarshalCompared(t, va,
			marshalInt64NearestDelta, unmarshalInt64NearestDelta)
		totalCompressedDelta += compressedSize
		totalCompressed2Delta += compressed2Size
		totalUncompressedDelta += uncompressedSize
		if _, err := w.WriteString(fmt.Sprintf("Delta  %.8f \t%d \t%d \t%d\n",
			hd, compressedSize, compressed2Size, uncompressedSize)); err != nil {
			t.Fatal(err)
		}

		compressedSize, compressed2Size, uncompressedSize = testMarshalUnMarshalCompared(t, va,
			marshalInt64NearestDelta2, unmarshalInt64NearestDelta2)
		totalCompressedDelta2 += compressedSize
		totalCompressed2Delta2 += compressed2Size
		totalUncompressedDelta2 += uncompressedSize
		if _, err := w.WriteString(fmt.Sprintf("Delta2 %.8f \t%d \t%d \t%d\n",
			hd, compressedSize, compressed2Size, uncompressedSize)); err != nil {
			t.Fatal(err)
		}
		w.WriteByte('\n')
		w.Flush()
	}

	t.Logf("Xor   : %d / %d / %d , ratio: %.8f", totalCompressed2Xor,
		totalCompressedXor, totalUncompressedXor, float64(totalCompressed2Xor)/float64(totalUncompressedXor))
	t.Logf("Delta : %d / %d / %d , ratio: %.8f", totalCompressed2Delta,
		totalCompressedDelta, totalUncompressedDelta, float64(totalCompressed2Delta)/float64(totalUncompressedDelta))
	t.Logf("Delta2: %d / %d / %d , ratio: %.8f", totalCompressed2Delta2,
		totalCompressedDelta2, totalUncompressedDelta2, float64(totalCompressed2Delta2)/float64(totalUncompressedDelta2))
}

func TestMarshalUnmarshalComparedRealData(t *testing.T) {
	logs, err := os.OpenFile("./data/stock_xor_test.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	w := bufio.NewWriter(logs)

	for _, i := range []int{1} {
		for _, j := range []int{2, 3, 4, 5} {
			var newTotal, oldTotal Res

			float64s, err := ReadAllFloat64File(fmt.Sprintf("/home/giannischen/dataSet/stock2/%d_%d", i, j))
			if err != nil {
				t.Log(err)
			}
			for k := 0; k < len(float64s)-8191; k += 8192 {
				values, _ := decimal.AppendFloatToInt64(nil, float64s[k:k+8192])
				n, o := testMarshalRealData(t, values)
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
				values, _ := decimal.AppendFloatToInt64(nil, float64s[len(float64s)-8192:])
				n, o := testMarshalRealData(t, values)
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

func testMarshalRealData(t *testing.T, values []int64) (n, o Res) {
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
	result2, firstValue2 := marshalInt64XorDelta(nil, values, 64)
	result21 := gozstd.CompressLevel(nil, result2, 5)
	mid2 := time.Now()
	values12, err := gozstd.Decompress(nil, result21)
	values22, err := unmarshalInt64XorDelta(nil, values12, firstValue2, len(values))

	o.decompressSpeed = time.Now().UnixNano() - mid2.UnixNano()
	o.compressSpeed = mid2.UnixNano() - start2.UnixNano()
	o.size = len(result21)
	o.ratio = float64(len(result21)) / float64(8*len(values))
	o.marshalType = MarshalTypeDeltaXorZSTD

	if err != nil {
		t.Fatalf("cannot unmarshal values: %s", err)
	}
	if err := checkPrecisionBits(values, values22, 64); err != nil {
		t.Fatalf("too low precision for values: %s", err)
	}
	return
}

func TestRealDataXor(t *testing.T) {
	files, err := ReadFileName("/home/giannischen/go/src/giannischen@nuaa.edu.cn/encoding/generatedata/data/real_world_data/index.txt", 3117)
	if err != nil {
		t.Log(err)
	}

	logs, err := os.OpenFile("./data/real_data_test_xor_delta.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
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
		n, o := testMarshalRealData(t, values)
		newTotal.add(n)
		oldTotal.add(o)

		n.hd = statistics.HammingDistance(values)
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
		if _, err := w.WriteString(fmt.Sprintf("XOR(%.8f)--  ", olds[i].hd) + olds[i].String() + "\n"); err != nil {
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
	if _, err := w.WriteString("XOR--  " + oldTotal.String()); err != nil {
		t.Fatal(err)
	}
	w.Flush()
}

var benchInt64XorArray = func() []int64 {
	var a []int64
	var v int64
	for i := 0; i < 8*1024; i++ {
		v += 30e3 + int64(rand.Int63n(1e2))
		a = append(a, v)
	}
	return a
}()
