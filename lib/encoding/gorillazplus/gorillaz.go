package gorillazplus

import (
	"errors"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/utils"
	"io"
	"math/bits"
)

// Compress
func Compress(dst []byte, src []int64) (result []byte, firstValue int64, err error) {
	if len(src) < 1 {
		return nil, 0, fmt.Errorf("BUG: src must contain at least 1 item; got %d items", len(src))
	}
	firstValue = src[0]
	v := uint64(firstValue)
	bs := &utils.ByteWrapper{Stream: &dst, Count: 0}
	bs.AppendBits(v, 64) // append first value without any compression
	src = src[1:]
	prevLeadingZeros, prevTrailingZeros := ^uint8(0), uint8(0)
	sigbits := uint8(0)
	prev := v
	for _, num := range src {
		v = uint64(num) ^ prev
		if v == 0 {
			bs.AppendBit(utils.Zero)
		} else {
			bs.AppendBit(utils.One)
			leadingZeros, trailingZeros := uint8(bits.LeadingZeros64(v)), uint8(bits.TrailingZeros64(v))
			// clamp number of leading zeros to avoid overflow when encoding
			if leadingZeros >= 64 {
				leadingZeros = 63
			}
			if prevLeadingZeros != ^uint8(0) && leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros {
				bs.AppendBit(utils.Zero)
				bs.AppendBit(utils.Zero)
				bs.AppendBits(v>>prevTrailingZeros, 64-int(prevLeadingZeros)-int(prevTrailingZeros))
			} else if prevLeadingZeros != ^uint8(0) && trailingZeros >= prevTrailingZeros && leadingZeros+trailingZeros >= prevLeadingZeros+prevTrailingZeros {
				bs.AppendBit(utils.Zero)
				bs.AppendBit(utils.One)
				bs.AppendBits(uint64(trailingZeros), 6)
				bs.AppendBits(v>>trailingZeros, 64-int(prevLeadingZeros)-int(prevTrailingZeros))
			} else {
				prevLeadingZeros, prevTrailingZeros = leadingZeros, trailingZeros
				bs.AppendBit(utils.One)
				bs.AppendBits(uint64(leadingZeros), 6)
				sigbits = 64 - leadingZeros - trailingZeros
				bs.AppendBits(uint64(sigbits), 6)
				bs.AppendBits(v>>trailingZeros, int(sigbits))
			}
		}
		prev = uint64(num)
	}
	bs.Finish()
	return dst, firstValue, nil
}

// Decompress append data to dst and return the appended dst
func Decompress(dst []int64, src []byte) ([]int64, error) {
	if len(dst) == 0 {
		return nil, errors.New("dst cap is zero")
	}

	bs := &utils.ByteWrapper{Stream: &src, Count: 8}
	firstValue, err := bs.ReadBits(64)
	if err != nil {
		return nil, err
	}
	dst[0] = int64(firstValue)
	prev := firstValue

	prevLeadingZeros, prevTrailingZeros := uint8(0), uint8(0)
	for i := 1; i < len(dst); i++ {
		b, err := bs.ReadBit()
		if err != nil {
			return nil, err
		}
		if b == utils.Zero {
			dst[i] = int64(prev)
			continue
		} else {
			b, err = bs.ReadBit()
			if err != nil {
				return nil, err
			}
			leadingZeros, trailingZeros := prevLeadingZeros, prevTrailingZeros
			if b == utils.One {
				bts, err := bs.ReadBits(6) // read leading zeros' length
				if err != nil {
					return nil, err
				}
				leadingZeros = uint8(bts)
				bts, err = bs.ReadBits(6) // read sig's length
				if err != nil {
					return nil, err
				}
				midLen := uint8(bts)
				if midLen == 0 {
					midLen = 64
				}
				if midLen+leadingZeros > 64 {
					if b, err = bs.ReadBit(); b == utils.Zero {
						return nil, io.EOF
					}
					return nil, errors.New("invalid bits")
				}
				trailingZeros = 64 - leadingZeros - midLen
				prevLeadingZeros, prevTrailingZeros = leadingZeros, trailingZeros
			}
			bts, err := bs.ReadBits(int(64 - leadingZeros - trailingZeros))
			if err != nil {
				return nil, err
			}
			v := prev
			v ^= bts << trailingZeros
			dst[i] = int64(v)
			prev = v
		}
	}
	return dst, nil
}
