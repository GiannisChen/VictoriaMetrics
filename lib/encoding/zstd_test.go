package encoding

import (
	"math"
	"math/rand"
	"testing"
)

func TestCompressDecompress(t *testing.T) {
	var values []int64
	v := float64(0)
	for i := 0; i < 8*1024; i++ {
		v += rand.NormFloat64() * 1e2
		values = append(values, int64(math.Float64bits(v)))
	}
	testCandD(t, values, CompressZstd, DecompressZstd)
	testCandD(t, values, CompressDeltaZstd, DecompressDeltaZstd)
	testCandD(t, values, CompressXorZstd, DecompressXorZstd)
	testCandD(t, values, CompressDelta2Zstd, DecompressDelta2Zstd)
}

func TestCompressXorZstd(t *testing.T) {
	values := []int64{1, 2, 3, 4, 5, 6}
	testCandD(t, values, CompressXorZstd, DecompressXorZstd)
}

func TestCompressDelta2Zstd(t *testing.T) {
	values := []int64{1, 2, 3, 4, 5, 6}
	testCandD(t, values, CompressDelta2Zstd, DecompressDelta2Zstd)
}

func testCandD(t *testing.T, values []int64, compress func([]byte, []int64, int64) []byte,
	decompress func([]int64, []byte, int64, int) ([]int64, error)) {
	t.Helper()
	result := compress(nil, values, values[0])
	values2, err := decompress(nil, result, values[0], len(values))
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
}
