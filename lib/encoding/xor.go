package encoding

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// marshalInt64DeltaXor encodes src using `delta xor` encoding
func marshalInt64DeltaXor(dst []byte, src []int64, _ uint8) (result []byte, firstValue int64) {
	if len(src) < 1 {
		logger.Panicf("BUG: src must contain at least 1 item; got %d items", len(src))
	}

	firstValue = src[0]
	v := src[0]
	var prex int64
	src = src[1:]
	is := GetInt64s(len(src))

	for i, next := range src {
		prex ^= next - v
		v = next
		is.A[i] = prex
	}

	dst = MarshalVarInt64s(dst, is.A)
	PutInt64s(is)
	return dst, firstValue
}

// unmarshalInt64DeltaXor decodes src using `delta xor` encoding,
// appends the result to dst and returns the appended result.
//
// The firstValue must be the value returned from unmarshalInt64DeltaXor.
func unmarshalInt64DeltaXor(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	if itemsCount < 1 {
		logger.Panicf("BUG: itemsCount must be greater than 0; got %d", itemsCount)
	}

	is := GetInt64s(itemsCount - 1)
	defer PutInt64s(is)

	tail, err := UnmarshalVarInt64s(is.A, src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal nearest delta from %d bytes; src=%X: %w", len(src), src, err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after unmarshaling %d items from %d bytes; tail size=%d; src=%X; tail=%X", itemsCount, len(src), len(tail), src, tail)
	}

	v := firstValue
	var prex int64
	dst = append(dst, v)
	for _, x := range is.A {
		v += prex ^ x
		prex = x
		dst = append(dst, v)
	}
	return dst, nil
}

// marshalInt64XorDelta encodes src using `delta xor` encoding
func marshalInt64XorDelta(dst []byte, src []int64, _ uint8) (result []byte, firstValue int64) {
	if len(src) < 1 {
		logger.Panicf("BUG: src must contain at least 1 item; got %d items", len(src))
	}

	firstValue = src[0]
	v := src[0]
	var prex int64
	src = src[1:]
	is := GetInt64s(len(src))

	for i, next := range src {
		prex = (next ^ v) - prex
		v = next
		is.A[i] = prex
	}

	dst = MarshalVarInt64s(dst, is.A)
	PutInt64s(is)
	return dst, firstValue
}

// unmarshalInt64XorDelta decodes src using `delta xor` encoding,
// appends the result to dst and returns the appended result.
//
// The firstValue must be the value returned from unmarshalInt64XorDelta.
func unmarshalInt64XorDelta(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	if itemsCount < 1 {
		logger.Panicf("BUG: itemsCount must be greater than 0; got %d", itemsCount)
	}

	is := GetInt64s(itemsCount - 1)
	defer PutInt64s(is)

	tail, err := UnmarshalVarInt64s(is.A, src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal nearest delta from %d bytes; src=%X: %w", len(src), src, err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after unmarshaling %d items from %d bytes; tail size=%d; src=%X; tail=%X", itemsCount, len(src), len(tail), src, tail)
	}

	v := firstValue
	var pred int64
	dst = append(dst, v)
	for _, d := range is.A {
		v ^= pred + d
		pred = d
		dst = append(dst, v)
	}
	return dst, nil
}
