package encoding

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/statistics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fastnum"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// Self-Adaptive Compression Method
func marshalInt64s(dst []byte, a []int64, _ uint8) ([]byte, MarshalType, int64) {
	if len(a) == 0 {
		logger.Panicf("BUG: a must contain at least one item")
	}
	mt := GetMarshalType(a)
	firstValue := a[0]
	switch mt {
	case MarshalConst:
		// do nothing
	case MarshalDelta2Zstd:
		dst = CompressDelta2Zstd(dst, a, firstValue)

	case MarshalDeltaZstd:
		dst = CompressDeltaZstd(dst, a, firstValue)

	case MarshalZstd:
		dst = CompressZstd(dst, a, firstValue)
	}
	return dst, mt, firstValue
}

func unmarshalInt64s(dst []int64, src []byte, mt MarshalType, firstValue int64, itemsCount int) ([]int64, error) {
	// Extend dst capacity in order to eliminate memory allocations below.
	dst = decimal.ExtendInt64sCapacity(dst, itemsCount)

	var err error
	switch mt {
	case MarshalConst:
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

	case MarshalZstd:
		dst, err = DecompressZstd(dst, src, firstValue, itemsCount)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal data after zstd decompression: %w; src_zstd=%X", err, src)
		}
		return dst, nil

	case MarshalDeltaZstd:
		dst, err = DecompressDeltaZstd(dst, src, firstValue, itemsCount)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal data after delta-zstd decompression: %w; src_delta_zstd=%X", err, src)
		}
		return dst, nil

	case MarshalDelta2Zstd:
		dst, err = DecompressDelta2Zstd(dst, src, firstValue, itemsCount)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal data after brotli decompression: %w; src_brotli=%X", err, src)
		}
		return dst, nil

	default:
		return nil, fmt.Errorf("unknown MarshalType=%d", mt)
	}
}

// Self-Adaptive Compression
// GetMarshalType return the marshal type
func GetMarshalType(int64s []int64) MarshalType {

	distance := statistics.HammingDistance(int64s)
	if distance == 0 {
		return MarshalConst
	}
	if distance < 1 {
		return MarshalDelta2Zstd
	}
	if distance >= 22 {
		return MarshalZstd
	}
	return MarshalDeltaZstd
}
