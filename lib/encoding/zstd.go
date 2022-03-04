package encoding

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/gozstd"
	"sync"
)

func CompressZstd(dst []byte, src []int64, _ int64) []byte {
	bs := GetBytes((len(src) - 1) * 8)
	defer PutBytes(bs)
	for i := 1; i < len(src); i++ {
		binary.LittleEndian.PutUint64(bs.B[(i-1)*8:i*8], uint64(src[i]))
	}
	return gozstd.Compress(dst, bs.B[:(len(src)-1)*8])
}

func CompressDeltaZstd(dst []byte, src []int64, _ int64) []byte {
	if len(src) < 1 {
		logger.Panicf("BUG: src must contain at least 1 item; got %d items", len(src))
	}
	v := src[0]
	src = src[1:]
	is := GetInt64s(len(src))

	// Fast path.
	for i, next := range src {
		d := next - v
		v += d
		is.A[i] = d
	}
	bb := bbPool.Get()
	bb.B = MarshalVarInt64s(bb.B[:0], is.A)
	dst = gozstd.Compress(dst, bb.B)
	bbPool.Put(bb)
	PutInt64s(is)
	return dst
}

func CompressDelta2Zstd(dst []byte, src []int64, _ int64) []byte {
	if len(src) < 2 {
		logger.Panicf("BUG: src must contain at least 2 items; got %d items", len(src))
	}
	bb := bbPool.Get()
	d1 := src[1] - src[0]
	bb.B = MarshalVarInt64(bb.B[:0], d1)
	v := src[1]
	src = src[2:]
	is := GetInt64s(len(src))
	for i, next := range src {
		d2 := next - v - d1
		d1 += d2
		v += d1
		is.A[i] = d2
	}
	bb.B = MarshalVarInt64s(bb.B, is.A)
	dst = gozstd.Compress(dst, bb.B)
	bbPool.Put(bb)
	PutInt64s(is)
	return dst
}

func CompressXorZstd(dst []byte, src []int64, _ int64) []byte {
	if len(src) < 1 {
		logger.Panicf("BUG: src must contain at least 1 item; got %d items", len(src))
	}
	v := src[0]
	src = src[1:]
	is := GetInt64s(len(src))

	// Fast path.
	for i, next := range src {
		d := next ^ v
		v ^= d
		is.A[i] = d
	}
	bb := bbPool.Get()
	bb.B = MarshalVarInt64s(bb.B[:0], is.A)
	dst = gozstd.Compress(dst, bb.B)
	bbPool.Put(bb)
	PutInt64s(is)
	return dst
}

func DecompressZstd(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	bb := bbPool.Get()
	var err error
	bb.B, err = gozstd.Decompress(bb.B[:0], src)
	if err != nil {
		return nil, err
	}
	if len(bb.B)%8 != 0 || itemsCount-1 > len(bb.B)/8 {
		return nil, errors.New("abnormal data block")
	}
	dst = append(dst, firstValue)
	for i := 1; i < itemsCount; i++ {
		dst = append(dst, int64(binary.LittleEndian.Uint64(bb.B[(i-1)*8:i*8])))
	}
	bbPool.Put(bb)
	return dst, nil
}

func DecompressDeltaZstd(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	if itemsCount < 1 {
		logger.Panicf("BUG: itemsCount must be greater than 0; got %d", itemsCount)
	}

	bb := bbPool.Get()
	var err error
	bb.B, err = gozstd.Decompress(bb.B[:0], src)
	if err != nil {
		return nil, err
	}

	is := GetInt64s(itemsCount - 1)
	defer PutInt64s(is)

	tail, err := UnmarshalVarInt64s(is.A, bb.B)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal nearest delta from %d bytes; src=%X: %w", len(src), src, err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after unmarshaling %d items from %d bytes; tail size=%d; src=%X; tail=%X", itemsCount, len(src), len(tail), src, tail)
	}

	v := firstValue
	dst = append(dst, v)
	for _, d := range is.A {
		v += d
		dst = append(dst, v)
	}

	bbPool.Put(bb)
	return dst, nil
}

func DecompressDelta2Zstd(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	if itemsCount < 2 {
		logger.Panicf("BUG: itemsCount must be greater than 1; got %d", itemsCount)
	}

	bb := bbPool.Get()
	var err error
	bb.B, err = gozstd.Decompress(bb.B[:0], src)
	if err != nil {
		return nil, err
	}

	is := GetInt64s(itemsCount - 1)
	defer PutInt64s(is)

	tail, err := UnmarshalVarInt64s(is.A, bb.B)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal nearest delta from %d bytes; src=%X: %w", len(src), src, err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after unmarshaling %d items from %d bytes; tail size=%d; src=%X; tail=%X", itemsCount, len(src), len(tail), src, tail)
	}

	v := firstValue
	d1 := is.A[0]
	dst = append(dst, v)
	v += d1
	dst = append(dst, v)
	for _, d2 := range is.A[1:] {
		d1 += d2
		v += d1
		dst = append(dst, v)
	}

	bbPool.Put(bb)
	return dst, nil
}

func DecompressXorZstd(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	if itemsCount < 1 {
		logger.Panicf("BUG: itemsCount must be greater than 0; got %d", itemsCount)
	}

	bb := bbPool.Get()
	var err error
	bb.B, err = gozstd.Decompress(bb.B[:0], src)
	if err != nil {
		return nil, err
	}

	is := GetInt64s(itemsCount - 1)
	defer PutInt64s(is)

	tail, err := UnmarshalVarInt64s(is.A, bb.B)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal nearest delta from %d bytes; src=%X: %w", len(src), src, err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after unmarshaling %d items from %d bytes; tail size=%d; src=%X; tail=%X", itemsCount, len(src), len(tail), src, tail)
	}

	v := firstValue
	dst = append(dst, v)
	for _, d := range is.A {
		v ^= d
		dst = append(dst, v)
	}

	bbPool.Put(bb)
	return dst, nil
}

func GetBytes(size int) *Bytes {
	v := bytesPool.Get()
	if v == nil {
		return &Bytes{B: make([]byte, size)}
	}
	bs := v.(*Bytes)
	if n := size - cap(bs.B); n > 0 {
		bs.B = append(bs.B[:cap(bs.B)], make([]byte, n)...)
	}
	bs.B = bs.B[:size]
	return bs
}

func PutBytes(bs *Bytes) {
	bytesPool.Put(bs)
}

type Bytes struct {
	B []byte
}

var bytesPool sync.Pool
