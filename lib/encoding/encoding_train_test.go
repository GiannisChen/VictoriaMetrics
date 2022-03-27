package encoding

import (
	"bufio"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/gorillaz"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/statistics"
	"github.com/bkaradzic/go-lz4"
	"github.com/golang/snappy"
	"os"
	"testing"
)

func TestML(t *testing.T) {
	logs, err := os.OpenFile("./data/us_solar_power_result_xor.csv", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	w := bufio.NewWriter(logs)

	for i := 1; i < 8398; i++ {
		float64s, err := ReadAllFloat64File(fmt.Sprintf("/home/giannischen/dataSet/pre-data/%d.csv", i))
		if err != nil {
			t.Log(err)
		}
		for j := 0; j < len(float64s)-8191; j += 8192 {
			values, _ := decimal.AppendFloatToInt64(nil, float64s[j:j+8192])
			ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt := testMarshal(t, values)

			if _, err := w.WriteString(fmt.Sprintf("%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%d\n", ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt)); err != nil {
				t.Fatal(err)
			}
		}
		if len(float64s) > 8192 {
			values, _ := decimal.AppendFloatToInt64(nil, float64s[len(float64s)-8192:])
			ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt := testMarshal(t, values)

			if _, err := w.WriteString(fmt.Sprintf("%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%d\n", ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestMLValidation(t *testing.T) {
	files, err := ReadFileName("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/index.txt", 36)
	if err != nil {
		t.Log(err)
	}

	logs, err := os.OpenFile("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/validation.csv", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()

	w := bufio.NewWriter(logs)

	for _, file := range files {
		float64s, err := ReadAllFloat64File("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/" + file)
		if err != nil {
			t.Fatal(err)
		}
		values, _ := decimal.AppendFloatToInt64(nil, float64s)

		if len(values) < 8192 {
			ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt := testMarshal(t, values)
			if _, err := w.WriteString(fmt.Sprintf("%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%d\n", ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt)); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("flush error.")
			}
			continue
		}

		for i := 0; i < len(values)-8191; i += 8192 {
			ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt := testMarshal(t, values[i:i+8192])
			if _, err := w.WriteString(fmt.Sprintf("%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%d\n", ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt)); err != nil {
				t.Fatal(err)
			}
		}
		ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt := testMarshal(t, values[len(values)-8192:])
		if _, err := w.WriteString(fmt.Sprintf("%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%d\n", ahd, arc, adhd, alz, atz, mhd, mdhd, mlz, mtz, mt)); err != nil {
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

func TestValidation(t *testing.T) {
	files, err := ReadFileName("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/index.txt", 36)
	if err != nil {
		t.Log(err)
	}

	logs, err := os.OpenFile("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/validation.csv", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()

	w := bufio.NewWriter(logs)
	hash := map[int]float64{}
	count := float64(0)
	for _, file := range files {
		float64s, err := ReadAllFloat64File("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/" + file)
		if err != nil {
			t.Fatal(err)
		}
		values, _ := decimal.AppendFloatToInt64(nil, float64s)

		if len(values) < 8192 {
			for i := 1; i < len(values); i++ {
				count++
				xor := values[i] ^ values[i-1]
				for j := 0; j < 64; j += 1 {
					if xor&(1<<j) != 0 {
						hash[j]++
					}
				}
			}
			continue
		}

		for i := 0; i < len(values)-8191; i += 8192 {
			tmp := values[i : i+8192]
			for k := 1; k < len(tmp); k++ {
				count++
				xor := tmp[k] ^ tmp[k-1]
				for j := 0; j < 64; j += 1 {
					if xor&(1<<j) != 0 {
						hash[j]++
					}
				}
			}
		}
		tmp := values[len(values)-8192:]
		for k := 1; k < len(tmp); k++ {
			count++
			xor := tmp[k] ^ tmp[k-1]
			for j := 0; j < 64; j += 1 {
				if xor&(1<<j) != 0 {
					hash[j]++
				}
			}
		}
	}

	for i := 0; i < 64; i++ {
		w.WriteString(fmt.Sprintf("%d\t%f\n", i, hash[i]))
	}
	w.WriteString(fmt.Sprintf("%f\n", count))
	w.Flush()
}

func testMarshal(t *testing.T, values []int64) (
	avgHammingDistance, repeatingZero, avgDeltaHammingDistance,
	avgDeltaLeadingZero, avgDeltaTrailingZero, meanHammingDistance,
	meanDeltaHammingDistance, meanDeltaLeadingZero, meanDeltaTrailingZero float64, mt MarshalType) {

	avgHammingDistance, repeatingZero, avgDeltaHammingDistance,
		avgDeltaLeadingZero, avgDeltaTrailingZero, meanHammingDistance,
		meanDeltaHammingDistance, meanDeltaLeadingZero, meanDeltaTrailingZero = statistics.ComplexHammingDistance2(values)
	if avgHammingDistance == 0 {
		return avgHammingDistance, repeatingZero, avgDeltaHammingDistance,
			avgDeltaLeadingZero, avgDeltaTrailingZero, meanHammingDistance,
			meanDeltaHammingDistance, meanDeltaLeadingZero, meanDeltaTrailingZero, MarshalTypeConst
	}
	minSize := len(values) * 8

	bb := bbPool.Get()
	bb.B, _ = marshalInt64NearestDelta2(bb.B[:0], values, 64)
	compressLevel := getCompressLevel(len(values))
	dst := CompressZSTDLevel(nil, bb.B, compressLevel)
	bbPool.Put(bb)
	if len(dst) < minSize {
		mt = MarshalTypeZSTDNearestDelta2
		minSize = len(dst)
	}

	bb = bbPool.Get()
	bb.B, _ = marshalInt64NearestDelta(bb.B[:0], values, 64)
	dst = CompressZSTDLevel(nil, bb.B, compressLevel)
	bbPool.Put(bb)
	if len(dst) < minSize {
		mt = MarshalTypeZSTDNearestDelta
		minSize = len(dst)
	}

	bb = bbPool.Get()
	bb.B = MarshalVarInt64s(bb.B[:0], values)
	dst = CompressZSTDLevel(nil, bb.B, compressLevel)
	bbPool.Put(bb)
	if len(dst) < minSize {
		mt = MarshalTypeZSTD
		minSize = len(dst)
	}

	if repeatingZero > 0.6 {
		dst, _ = MarshalRepeatEliminate(nil, values, 0)
		if len(dst) < minSize {
			mt = MarshalTypeSwitching
			minSize = len(dst)
		}
	}

	dst, _ = marshalInt64NearestDelta(nil, values, 64)
	if len(dst) < minSize {
		mt = MarshalTypeNearestDelta
		minSize = len(dst)
	}

	dst, _ = marshalInt64NearestDelta2(nil, values, 64)
	if len(dst) < minSize {
		mt = MarshalTypeNearestDelta2
		minSize = len(dst)
	}

	dst, _, _ = gorillaz.Compress(nil, values)
	if len(dst) < minSize {
		mt = MarshalTypeGorillaZ
		minSize = len(dst)
	}

	bb = bbPool.Get()
	bb.B, _ = marshalInt64NearestDelta(bb.B[:0], values, 64)
	dst = snappy.Encode(nil, bb.B)
	bbPool.Put(bb)
	if len(dst) < minSize {
		mt = MarshalTypeDeltaSnappy
		minSize = len(dst)
	}

	bb = bbPool.Get()
	bb.B, _ = marshalInt64NearestDelta(bb.B[:0], values, 64)
	dst, err := lz4.Encode(nil, bb.B)
	if err != nil {
		t.Fatal(err)
	}
	bbPool.Put(bb)
	if len(dst) < minSize {
		mt = MarshalTypeDeltaLZ4
		minSize = len(dst)
	}

	//bb = bbPool.Get()
	//bb.B, _ = marshalInt64NearestDelta(bb.B[:0], values, 64)
	//buffer := &bytes.Buffer{}
	//w := brotli.NewWriter(buffer)
	//_, err = w.Write(bb.B)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//err = w.Flush()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//bbPool.Put(bb)
	//if len(buffer.Bytes()) < minSize {
	//	mt = MarshalTypeDeltaBrotli
	//	minSize = len(buffer.Bytes())
	//}
	//w.Close()

	bb = bbPool.Get()
	bb.B, _ = marshalInt64NearestDelta2(bb.B[:0], values, 64)
	dst = snappy.Encode(nil, bb.B)
	bbPool.Put(bb)
	if len(dst) < minSize {
		mt = MarshalTypeDelta2Snappy
		minSize = len(dst)
	}

	bb = bbPool.Get()
	bb.B, _ = marshalInt64NearestDelta2(bb.B[:0], values, 64)
	dst, err = lz4.Encode(nil, bb.B)
	if err != nil {
		t.Fatal(err)
	}
	bbPool.Put(bb)
	if len(dst) < minSize {
		mt = MarshalTypeDelta2LZ4
		minSize = len(dst)
	}

	//bb = bbPool.Get()
	//bb.B, _ = marshalInt64NearestDelta2(bb.B[:0], values, 64)
	//buffer = &bytes.Buffer{}
	//w = brotli.NewWriter(buffer)
	//_, err = w.Write(bb.B)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//err = w.Flush()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//bbPool.Put(bb)
	//if len(buffer.Bytes()) < minSize {
	//	mt = MarshalTypeDelta2Brotli
	//	minSize = len(buffer.Bytes())
	//}
	//w.Close()

	//h := huffman.GetHuff()
	//dst = h.Compress(make([]byte, 0), values, values[0])
	//huffman.PutHuff(h)
	//if len(dst) < minSize {
	//	mt = MarshalTypeXorHuffman
	//	minSize = len(dst)
	//}

	return avgHammingDistance, repeatingZero, avgDeltaHammingDistance,
		avgDeltaLeadingZero, avgDeltaTrailingZero, meanHammingDistance,
		meanDeltaHammingDistance, meanDeltaLeadingZero, meanDeltaTrailingZero, mt
}

func TestGenerateDict(t *testing.T) {
	hash := map[uint8]uint64{}
	logs, err := os.OpenFile("./data/dict.csv", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	w := bufio.NewWriter(logs)

	for i := 1; i < 8398; i++ {
		float64s, err := ReadAllFloat64File(fmt.Sprintf("/home/giannischen/dataSet/pre-data/%d.csv", i))
		if err != nil {
			t.Log(err)
		}
		for j := 0; j < len(float64s)-8191; j += 8192 {
			values, _ := decimal.AppendFloatToInt64(nil, float64s[j:j+8192])
			prev := values[0]
			xor := uint64(0)
			for k := 1; k < len(values); k++ {
				xor = uint64(prev ^ values[k])
				for m := 0; m < 64; m += 4 {
					hash[uint8(xor>>m)&0x0F]++
				}
			}
		}
		if len(float64s) > 8192 {
			values, _ := decimal.AppendFloatToInt64(nil, float64s[len(float64s)-8192:])
			prev := values[0]
			xor := uint64(0)
			for k := 1; k < len(values); k++ {
				xor = uint64(prev ^ values[k])
				for m := 0; m < 64; m += 4 {
					hash[uint8(xor>>m)&0x0F]++
				}
			}
		}

	}

	for i := 0; i < 256; i++ {
		if _, ok := hash[uint8(i)]; ok {
			w.WriteString(fmt.Sprintf("%d,%d\n", i, hash[uint8(i)]))
		} else {
			w.WriteString(fmt.Sprintf("%d,%d\n", i, 0))
		}
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}
