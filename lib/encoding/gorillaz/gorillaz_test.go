package gorillaz

import (
	"encoding/binary"
	"fmt"
	"github.com/valyala/gozstd"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestCompressDecompressInts(t *testing.T) {
	t.Helper()
	int64s := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
	var compressedByte []byte
	compressedByte, firstValue, err := Compress(compressedByte, int64s)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("first value: ", firstValue)
	fmt.Println("compressed len: ", len(compressedByte))
	var decompressedInt64s = make([]int64, 10)
	decompressedInt64s, err = Decompress(decompressedInt64s, compressedByte)
	if err != nil {
		t.Error(err)
	}
	if len(decompressedInt64s) != len(int64s) {
		t.Error("de-compress error")
	}
	for i := 0; i < len(decompressedInt64s); i++ {
		if decompressedInt64s[i] != int64s[i] {
			t.Error("de-compress error")
		}
	}
}

func TestCompressDecompressFloats(t *testing.T) {
	t.Helper()
	var float64s []int64
	for i := 0; i < 8000; i++ {
		float64s = append(float64s, int64(math.Float64bits(rand.Float64())))
	}
	var compressedByte []byte
	compressedByte, firstValue, err := Compress(nil, float64s)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("first value: ", firstValue)
	fmt.Println("compressed len: ", len(compressedByte))
	var decompressedFloat64s []int64 = make([]int64, 8000)
	decompressedFloat64s, err = Decompress(decompressedFloat64s, compressedByte)
	if err != nil {
		t.Error(err)
	}
	if len(decompressedFloat64s) != len(float64s) {
		t.Error("de-compress error")
	}
	for i := 0; i < len(decompressedFloat64s); i++ {
		if decompressedFloat64s[i] != float64s[i] {
			t.Error("de-compress error")
		}
	}
}

func BenchmarkCompressFloat64s(b *testing.B) {
	var float64s []int64
	rand.Seed(22)
	for i := 0; i < 8000; i++ {
		float64s = append(float64s, int64(math.Float64bits(rand.Float64())))
	}
	b.ResetTimer()
	var compressedByte []byte
	compressedByte, _, err := Compress(compressedByte, float64s)
	if err != nil {
		b.Error(err)
	}
	//fmt.Println("compressed ratio: ", float64(len(compressedByte))/8*8000)
	var decompressedFloat64s []int64 = make([]int64, 8000)
	decompressedFloat64s, err = Decompress(decompressedFloat64s, compressedByte)
	if err != nil {
		b.Error(err)
	}
	b.StopTimer()
	if len(decompressedFloat64s) != len(float64s) {
		b.Error("de-compress error")
	}
	for i := 0; i < len(decompressedFloat64s); i++ {
		if decompressedFloat64s[i] != float64s[i] {
			b.Error("de-compress error")
		}
	}
}

func BenchmarkCompressFloat64sBaselineZstd(b *testing.B) {
	var float64s []byte
	rand.Seed(22)
	for i := 0; i < 8000; i++ {
		tmp := make([]byte, 8)
		binary.LittleEndian.PutUint64(tmp, math.Float64bits(rand.Float64()))
		float64s = append(float64s, tmp...)
	}
	b.ResetTimer()
	var compressedByte []byte
	compressedByte = gozstd.Compress(compressedByte, float64s)
	//fmt.Println("compressed ratio: ", float64(len(compressedByte))/8*8000)
	var decompressedFloat64s []byte
	decompressedFloat64s, err := gozstd.Decompress(decompressedFloat64s, compressedByte)
	if err != nil {
		b.Error(err)
	}
	b.StopTimer()
}

func TestCompareG(t *testing.T) {
	t.Helper()
	compressCost, uncompressCost, compressSize := float64(0), float64(0), float64(0)
	for i := 0; i < 10000; i++ {
		var float64s []int64
		for i := 0; i < 8000; i++ {
			float64s = append(float64s, int64(math.Float64bits(rand.Float64())))
		}
		start := time.Now()
		var compressedByte []byte
		compressedByte, _, err := Compress(compressedByte, float64s)
		if err != nil {
			t.Error(err)
		}
		l := len(compressedByte)
		mid := time.Now()
		var decompressedFloat64s []int64 = make([]int64, 8000)
		decompressedFloat64s, err = Decompress(decompressedFloat64s, compressedByte)
		if err != nil {
			t.Error(err)
		}
		end := time.Now()
		compressCost += float64(mid.UnixMicro() - start.UnixMicro())
		uncompressCost += float64(end.UnixMicro() - mid.UnixMicro())
		compressSize += float64(l)
	}
	t.Logf("gorilla:\ncompress cost: %f\nuncompress cost: %f\n compress size: %f\n",
		compressCost, uncompressCost, compressSize)
}

func TestCompareZ(t *testing.T) {
	t.Helper()
	compressCost, uncompressCost, compressSize := float64(0), float64(0), float64(0)
	for i := 0; i < 10000; i++ {
		var float64bytes []byte
		for i := 0; i < 8000; i++ {
			tmp := make([]byte, 8)
			binary.LittleEndian.PutUint64(tmp, math.Float64bits(rand.Float64()))
			float64bytes = append(float64bytes, tmp...)
		}
		start := time.Now()
		var compressedByte []byte
		compressedByte = gozstd.Compress(compressedByte, float64bytes)

		l := len(compressedByte)
		mid := time.Now()
		var decompressedFloat64s []byte
		decompressedFloat64s, err := gozstd.Decompress(decompressedFloat64s, compressedByte)
		if err != nil {
			t.Error(err)
		}
		end := time.Now()
		compressCost += float64(mid.UnixMicro() - start.UnixMicro())
		uncompressCost += float64(end.UnixMicro() - mid.UnixMicro())
		compressSize += float64(l)
	}
	t.Logf("gorilla:\ncompress cost: %f\nuncompress cost: %f\n compress size: %f\n",
		compressCost, uncompressCost, compressSize)
}
