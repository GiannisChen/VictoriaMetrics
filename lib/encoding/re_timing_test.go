package encoding

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
)

func BenchmarkMarshalRepeatEliminate(b *testing.B) {
	b.Run("re", func(b *testing.B) {
		benchmarkMarshalRepeatEliminate(b, 64)
	})

}

func benchmarkMarshalRepeatEliminate(b *testing.B, precisionBits uint8) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchRepeatInt64Array)))
	b.RunParallel(func(pb *testing.PB) {
		var dst []byte
		for pb.Next() {
			dst, _ = MarshalRepeatEliminate(dst[:0], benchRepeatInt64Array, precisionBits)
			atomic.AddUint64(&Sink, uint64(len(dst)))
		}
	})
}

func BenchmarkUnMarshalRepeatEliminate(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchRepeatInt64Array)))
	b.RunParallel(func(pb *testing.PB) {
		var dst []int64
		var err error
		for pb.Next() {
			dst, err = UnmarshalRepeatEliminate(dst[:0], benchInt64RepeatEliminateData, 0, len(benchRepeatInt64Array))
			if err != nil {
				panic(fmt.Errorf("unexpected error: %w", err))
			}
			atomic.AddUint64(&Sink, uint64(len(dst)))
		}
	})
}

var benchInt64RepeatEliminateData = func() []byte {
	data, _ := MarshalRepeatEliminate(nil, benchRepeatInt64Array, 4)
	return data
}()

var benchRepeatInt64Array = func() []int64 {
	rand.Seed(21)
	var a []int64
	var v int64 = 0
	var turningPoint []int
	p := 0
	for {
		p += rand.Intn(200)
		if p >= 8*1024 {
			break
		}
		turningPoint = append(turningPoint, p)
	}

	for i := 0; i < 8*1024; i++ {
		if len(turningPoint) != 0 && i == turningPoint[0] {
			v ^= 1
			turningPoint = turningPoint[1:]
		}
		a = append(a, v)
	}
	return a
}()
