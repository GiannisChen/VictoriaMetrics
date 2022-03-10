package encoding

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/statistics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fastnum"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// Self-Adaptive Compression Method
func marshalInt64s(dst []byte, a []int64, _ uint8) (result []byte, mt MarshalType, firstValue int64) {
	if len(a) == 0 {
		logger.Panicf("BUG: a must contain at least one item")
	}
	mt = GetMarshalType(a)
	switch mt {
	case MarshalTypeConst:
		firstValue = a[0]

	case MarshalTypeDeltaConst:
		firstValue = a[0]
		dst = MarshalVarInt64(dst, a[1]-a[0])

	case MarshalTypeZSTDNearestDelta2:
		bb := bbPool.Get()
		bb.B, firstValue = marshalInt64NearestDelta2(bb.B[:0], a, 64)
		compressLevel := getCompressLevel(len(a))
		dst = CompressZSTDLevel(dst, bb.B, compressLevel)
		bbPool.Put(bb)

	case MarshalTypeZSTDNearestDelta:
		bb := bbPool.Get()
		bb.B, firstValue = marshalInt64NearestDelta(bb.B[:0], a, 64)
		compressLevel := getCompressLevel(len(a))
		dst = CompressZSTDLevel(dst, bb.B, compressLevel)
		bbPool.Put(bb)

	case MarshalTypeZSTD:
		bb := bbPool.Get()
		firstValue = a[0]
		bb.B = MarshalVarInt64s(bb.B[:0], a)
		compressLevel := getCompressLevel(len(a))
		dst = CompressZSTDLevel(dst, bb.B, compressLevel)
		bbPool.Put(bb)

	case MarshalTypeSwitching:
		dst, firstValue = MarshalRepeatEliminate(dst, a, 0)

	default:
		logger.Panicf("BUG: unexpected mt=%d", mt)
	}
	return dst, mt, firstValue
}

func unmarshalInt64s(dst []int64, src []byte, mt MarshalType, firstValue int64, itemsCount int) ([]int64, error) {
	// Extend dst capacity in order to eliminate memory allocations below.
	dst = decimal.ExtendInt64sCapacity(dst, itemsCount)

	var err error
	switch mt {
	case MarshalTypeZSTDNearestDelta:
		bb := bbPool.Get()
		bb.B, err = DecompressZSTD(bb.B[:0], src)
		if err != nil {
			return nil, fmt.Errorf("cannot decompress zstd data: %w", err)
		}
		dst, err = unmarshalInt64NearestDelta(dst, bb.B, firstValue, itemsCount)
		bbPool.Put(bb)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal nearest delta data after zstd decompression: %w; src_zstd=%X", err, src)
		}
		return dst, nil

	case MarshalTypeZSTDNearestDelta2:
		bb := bbPool.Get()
		bb.B, err = DecompressZSTD(bb.B[:0], src)
		if err != nil {
			return nil, fmt.Errorf("cannot decompress zstd data: %w", err)
		}
		dst, err = unmarshalInt64NearestDelta2(dst, bb.B, firstValue, itemsCount)
		bbPool.Put(bb)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal nearest delta2 data after zstd decompression: %w; src_zstd=%X", err, src)
		}
		return dst, nil

	case MarshalTypeNearestDelta:
		dst, err = unmarshalInt64NearestDelta(dst, src, firstValue, itemsCount)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal nearest delta data: %w", err)
		}
		return dst, nil

	case MarshalTypeNearestDelta2:
		dst, err = unmarshalInt64NearestDelta2(dst, src, firstValue, itemsCount)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal nearest delta2 data: %w", err)
		}
		return dst, nil

	case MarshalTypeConst:
		if len(src) > 0 {
			return nil, fmt.Errorf("unexpected data left in const encoding: %d bytes", len(src))
		}
		if firstValue == 0 {
			dst = fastnum.AppendInt64Zeros(dst, itemsCount)
			return dst, nil
		}
		if firstValue == 1 {
			dst = fastnum.AppendInt64Ones(dst, itemsCount)
			return dst, nil
		}
		for itemsCount > 0 {
			dst = append(dst, firstValue)
			itemsCount--
		}
		return dst, nil

	case MarshalTypeDeltaConst:
		v := firstValue
		tail, d, err := UnmarshalVarInt64(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal delta value for delta const: %w", err)
		}
		if len(tail) > 0 {
			return nil, fmt.Errorf("unexpected trailing data after delta const (d=%d): %d bytes", d, len(tail))
		}
		for itemsCount > 0 {
			dst = append(dst, v)
			itemsCount--
			v += d
		}
		return dst, nil

	case MarshalTypeZSTD:
		bb := bbPool.Get()
		bb.B, err = DecompressZSTD(bb.B[:0], src)
		if err != nil {
			return nil, fmt.Errorf("cannot decompress zstd data: %w", err)
		}
		dst = append(dst, make([]int64, itemsCount)...)
		_, err = UnmarshalVarInt64s(dst, bb.B)
		bbPool.Put(bb)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal data after zstd decompression: %w; src_zstd=%X", err, src)
		}
		return dst, nil

	case MarshalTypeSwitching:
		dst, err = UnmarshalRepeatEliminate(dst, src, firstValue, itemsCount)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal repeat-eliminate data: %w; src_zstd=%X", err, src)
		}
		return dst, nil

	default:
		return nil, fmt.Errorf("unknown MarshalType=%d", mt)
	}
}

// Self-Adaptive Compression
// GetMarshalType return the marshal type
func GetMarshalType(int64s []int64) MarshalType {

	if len(int64s) <= 1 {
		return MarshalTypeConst
	}
	if len(int64s) <= 2 {
		return MarshalTypeDeltaConst
	}
	distance, isRepeat := statistics.ComplexHammingDistance(int64s)
	if distance == 0 {
		return MarshalTypeConst
	}
	if isRepeat {
		return MarshalTypeSwitching
	}
	if distance < 1 {
		return MarshalTypeZSTDNearestDelta2
	}
	if distance < 20.5 {
		return MarshalTypeZSTDNearestDelta
	}

	return MarshalTypeZSTD
}
