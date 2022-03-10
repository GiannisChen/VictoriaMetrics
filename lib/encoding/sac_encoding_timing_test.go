package encoding

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkSACMarshalInt64Array(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchInt64Array)))
	b.RunParallel(func(pb *testing.PB) {
		var dst []byte
		var mt MarshalType
		for pb.Next() {
			dst, mt, _ = marshalInt64s(dst[:0], benchInt64Array, 4)
			if mt != benchSACMarshalType {
				panic(fmt.Errorf("unexpected marshal type; got %d; expecting %d", mt, benchSACMarshalType))
			}
			atomic.AddUint64(&Sink, uint64(len(dst)))
		}
	})
}

func BenchmarkSACUnmarshalInt64Array(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchInt64Array)))
	b.RunParallel(func(pb *testing.PB) {
		var dst []int64
		var err error
		for pb.Next() {
			dst, err = unmarshalInt64s(dst[:0], benchMarshaledInt64s, benchSACMarshalType, benchInt64Array[0], len(benchInt64Array))
			if err != nil {
				panic(fmt.Errorf("cannot unmarshal int64 array: %w", err))
			}
			atomic.AddUint64(&Sink, uint64(len(dst)))
		}
	})
}

var benchMarshaledInt64s = func() []byte {
	b, _, _ := marshalInt64s(nil, benchInt64Array, 4)
	return b
}()

var benchSACMarshalType = func() MarshalType {
	_, mt, _ := marshalInt64s(nil, benchInt64Array, 4)
	return mt
}()
