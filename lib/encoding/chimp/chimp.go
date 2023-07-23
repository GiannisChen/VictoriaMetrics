package chimp

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/gorillaz"
	"math/bits"
)

func Compress(dst []byte, src []int64) []byte {
	bs := &gorillaz.ByteWrapper{Stream: &dst, Count: 0}
	bs.AppendBits(uint64(len(src)), 14)
	bs.AppendBits(uint64(src[0]), 64)
	lastLeading := 0
	lastValue := uint64(src[0])
	for i := 1; i < len(src); i++ {
		xor := uint64(src[i]) ^ lastValue
		leading := bits.LeadingZeros64(xor)
		if leading > 15 {
			leading = 15
		}
		if leading%2 == 1 {
			leading -= 1
		}
		trailing := bits.TrailingZeros64(xor)
		if trailing > 6 {
			bs.AppendBit(gorillaz.Zero)
			if xor == 0 {
				bs.AppendBit(gorillaz.Zero)
			} else {
				bs.AppendBit(gorillaz.One)
				bs.AppendBits(uint64(leading/2), 3)
				centerBitsCounts := 64 - leading - trailing
				bs.AppendBits(uint64(centerBitsCounts), 6)
				bs.AppendBits(uint64(xor>>trailing), centerBitsCounts)
			}
		} else {
			bs.AppendBit(gorillaz.One)
			if lastLeading == leading {
				bs.AppendBit(gorillaz.Zero)
				bs.AppendBits(xor, 64-leading)
			} else {
				bs.AppendBit(gorillaz.One)
				bs.AppendBits(uint64(leading/2), 3)
				bs.AppendBits(xor, 64-leading)
			}
		}
		lastLeading = leading
		lastValue = uint64(src[i])
	}
	return dst
}

func Decompress(dst []int64, src []byte) ([]int64, error) {
	bs := &gorillaz.ByteWrapper{Stream: &src, Count: 8}
	length, err := bs.ReadBits(14)
	if err != nil {
		return nil, err
	}
	firstValue, err := bs.ReadBits(64)
	if err != nil {
		return nil, err
	}
	dst = append(dst, int64(firstValue))
	lastLeading := 0
	lastValue := firstValue
	curValue := uint64(0)
	for i := uint64(1); i < length; i++ {
		bit, err := bs.ReadBit()
		if err != nil {
			return nil, err
		}
		if !bit {
			bit, err = bs.ReadBit()
			if err != nil {
				return nil, err
			}
			if !bit {
				curValue = lastValue
			} else {
				leading, err := bs.ReadBits(3)
				if err != nil {
					return nil, err
				}
				leading *= 2
				centerCount, err := bs.ReadBits(6)
				if err != nil {
					return nil, err
				}
				xored, err := bs.ReadBits(int(centerCount))
				xored <<= 64 - leading - centerCount
				curValue = lastValue ^ xored
			}
		} else {
			bit, err = bs.ReadBit()
			if err != nil {
				return nil, err
			}
			if !bit {
				xored, err := bs.ReadBits(64 - lastLeading)
				if err != nil {
					return nil, err
				}
				curValue = xored ^ lastValue
			} else {
				leading, err := bs.ReadBits(3)
				if err != nil {
					return nil, err
				}
				leading *= 2
				xored, err := bs.ReadBits(64 - int(leading))
				curValue = xored ^ lastValue
			}
		}
		dst = append(dst, int64(curValue))
		leading := bits.LeadingZeros64(curValue ^ lastValue)
		if leading > 15 {
			leading = 15
		}
		if leading%2 == 1 {
			leading -= 1
		}
		lastLeading = leading
		lastValue = curValue
	}

	return dst, nil
}
