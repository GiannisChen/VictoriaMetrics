package encoding

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/statistics"
	"github.com/valyala/gozstd"
	"reflect"
	"testing"
)

func TestMarshalUnMarshalRepeatEliminate(t *testing.T) {
	testMarshalUnMarshalRepeatEliminate(t, []int64{0}, 4)
	testMarshalUnMarshalRepeatEliminate(t, []int64{0, 0}, 4)
	testMarshalUnMarshalRepeatEliminate(t, []int64{1, 1, 1, 1, 1, 0, 0}, 4)
	testMarshalUnMarshalRepeatEliminate(t, []int64{255, 255}, 4)
	testMarshalUnMarshalRepeatEliminate(t, []int64{0, 1, 0, 0, 1, 0}, 4)
	testMarshalUnMarshalRepeatEliminate(t, []int64{5, 5, 3, 3, 3, 0}, 4)
	testMarshalUnMarshalRepeatEliminate(t, []int64{-5e12, -5e12, -5e12, -8e12, -8e12}, 1)
	testMarshalUnMarshalRepeatEliminate(t, []int64{-5e12, -8e12, -5e12, -8e12, -8e12}, 2)

	// Verify constant encoding.
}

func testMarshalUnMarshalRepeatEliminate(t *testing.T, va []int64, precisionBits uint8) {
	t.Helper()

	b, firstValue := MarshalRepeatEliminate(nil, va, precisionBits)
	lenb := len(b)
	vaNew, err := UnmarshalRepeatEliminate(nil, b, firstValue, len(va))
	if err != nil {
		t.Fatalf("cannot unmarshal data for va=%d, precisionBits=%d from b=%x: %s", va, precisionBits, b, err)
	}
	if len(vaNew) != len(va) {
		t.Fatalf("va & vNew length does not match.")
	}
	for i, v := range vaNew {
		if v != va[i] {
			t.Fatalf("va & vaNew does not match.")
		}
	}
	t.Logf("before: %d, after: %d\n", len(va)*8, lenb)

	vaPrefix := []int64{1, 2, 3, 4}
	vaNew, err = UnmarshalRepeatEliminate(vaPrefix, b, firstValue, len(va))
	if err != nil {
		t.Fatalf("cannot unmarshal prefixed data for va=%d, precisionBits=%d from b=%x: %s", va, precisionBits, b, err)
	}
	if !reflect.DeepEqual(vaNew[:len(vaPrefix)], vaPrefix) {
		t.Fatalf("unexpected prefix for va=%d, precisionBits=%d: got\n%d; expecting\n%d", va, precisionBits, vaNew[:len(vaPrefix)], vaPrefix)
	}
}

func TestMarshalUnMarshalCompared(t *testing.T) {
	var (
		totalCompressedRE, totalCompressed2RE, totalUncompressedRE             int64
		totalCompressedDelta, totalCompressed2Delta, totalUncompressedDelta    int64
		totalCompressedDelta2, totalCompressed2Delta2, totalUncompressedDelta2 int64
	)

	for i := 0; i < 10000; i++ {
		va := benchRepeatInt64Array
		compressedSize, compressed2Size, uncompressedSize := testMarshalUnMarshalCompared(t, va,
			MarshalRepeatEliminate, UnmarshalRepeatEliminate)
		totalCompressedRE += compressedSize
		totalCompressed2RE += compressed2Size
		totalUncompressedRE += uncompressedSize

		compressedSize, compressed2Size, uncompressedSize = testMarshalUnMarshalCompared(t, va,
			marshalInt64NearestDelta, unmarshalInt64NearestDelta)
		totalCompressedDelta += compressedSize
		totalCompressed2Delta += compressed2Size
		totalUncompressedDelta += uncompressedSize

		compressedSize, compressed2Size, uncompressedSize = testMarshalUnMarshalCompared(t, va,
			marshalInt64NearestDelta2, unmarshalInt64NearestDelta2)
		totalCompressedDelta2 += compressedSize
		totalCompressed2Delta2 += compressed2Size
		totalUncompressedDelta2 += uncompressedSize
	}

	t.Logf("RE    : %d / %d / %d , ratio: %.8f", totalCompressed2RE,
		totalCompressedRE, totalUncompressedRE, float64(totalCompressed2RE)/float64(totalUncompressedRE))
	t.Logf("Delta : %d / %d / %d , ratio: %.8f", totalCompressed2Delta,
		totalCompressedDelta, totalUncompressedDelta, float64(totalCompressed2Delta)/float64(totalUncompressedDelta))
	t.Logf("Delta2: %d / %d / %d , ratio: %.8f", totalCompressed2Delta2,
		totalCompressedDelta2, totalUncompressedDelta2, float64(totalCompressed2Delta2)/float64(totalUncompressedDelta2))
}

func testMarshalUnMarshalCompared(t *testing.T, va []int64,
	marshal func([]byte, []int64, uint8) ([]byte, int64),
	unmarshal func([]int64, []byte, int64, int) ([]int64, error)) (compressedSize int64, compressed2Size int64, uncompressedSize int64) {
	t.Helper()

	b, firstValue := marshal(nil, va, 64)
	compressedSize = int64(len(b))

	b2 := gozstd.CompressLevel(nil, b, 5)
	compressed2Size = int64(len(b2))

	b3, err := gozstd.Decompress(nil, b2)
	if err != nil {
		t.Fatalf("cannot unmarshal data for va=%d, precisionBits=%d from b=%x: %s", va, 64, b3, err)
	}

	vaNew, err := unmarshal(nil, b3, firstValue, len(va))
	if err != nil {
		t.Fatalf("cannot unmarshal data for va=%d, precisionBits=%d from b=%x: %s", va, 64, b, err)
	}
	if len(vaNew) != len(va) {
		t.Fatalf("va & vNew length does not match.")
	}
	for i, v := range vaNew {
		if v != va[i] {
			t.Fatalf("va & vaNew does not match.")
		}
	}
	uncompressedSize = int64(len(va) * 8)

	vaPrefix := []int64{1, 2, 3, 4}
	vaNew, err = unmarshal(vaPrefix, b, firstValue, len(va))
	if err != nil {
		t.Fatalf("cannot unmarshal prefixed data for va=%d, precisionBits=%d from b=%x: \n%s", va, 64, b, err)
	}
	if !reflect.DeepEqual(vaNew[:len(vaPrefix)], vaPrefix) {
		t.Fatalf("unexpected prefix for va=%d, precisionBits=%d: got\n%d; expecting\n%d", va, 64, vaNew[:len(vaPrefix)], vaPrefix)
	}
	return
}

func TestBenchRepeatInt64Array(t *testing.T) {
	testMarshalUnMarshalRepeatEliminate(t, benchRepeatInt64Array, 0)
}

func TestRepeatInt64Array(t *testing.T) {
	hd, repeat := statistics.ComplexHammingDistance(benchRepeatInt64Array)
	t.Logf("%.8f %v\n", hd, repeat)
}
