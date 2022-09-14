package pdfcm

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/gorillaz"
	"math/bits"
)

const SIZE = 8

var (
	previousCounts          = float64(0)
	windowValueCounts       = float64(0)
	windowResidualCounts    = float64(0)
	predictorResidualCounts = float64(0)
	exceptionCount          = float64(0)
)

var (
	windowResidualLength    = float64(0)
	predictorResidualLength = float64(0)
)

var (
	alpha = uint64(0)
	beta  = uint64(0)
)

func PrintMetrics() {
	fmt.Printf("previousCounts:      %.1f\n", previousCounts)
	fmt.Printf("windowValueCounts:     %.1f\n", windowValueCounts)
	fmt.Printf("windowResidualCounts:           %.1f\n", windowResidualCounts)
	fmt.Printf("predictorResidualCounts: %.1f\n", predictorResidualCounts)
	fmt.Printf("exceptionCount:     %.1f\n", exceptionCount)
	fmt.Println()
	fmt.Printf("windowResidualLength:     %.1f\n", windowResidualLength)
	fmt.Printf("predictorResidualLength:     %.1f\n", predictorResidualLength)
}

func Reset() {
	previousCounts = 0
	windowValueCounts = 0
	windowResidualCounts = 0
	predictorResidualCounts = 0
	exceptionCount = 0
}

type compressor struct {
	dictSize     int
	dictIndex    int
	dictMaxZeros int
	dictTmpZeros int
	xfcm         *PdfcmPredictor
}

func (c *compressor) update(value uint64) {
	c.xfcm.Update(value)
	if c.dictSize < SIZE {
		c.dictSize++
	}
}

func Compress(dst []byte, src []int64) []byte {
	bs := &gorillaz.ByteWrapper{Stream: &dst, Count: 0}
	c := compressor{xfcm: NewPdfcmPredictor(16)}
	bs.AppendBits(uint64(src[0]), 64)
	bs.AppendBits(uint64(len(src)), 14)
	c.update(uint64(src[0]))
	var isReference, isXfcm bool
	for i := 1; i < len(src); i++ {
		if src[i] == src[i-1] {
			bs.AppendBit(gorillaz.Zero)
			c.update(uint64(src[i]))
			previousCounts++
			continue
		}
		isReference, isXfcm = false, false
		c.dictIndex = -1
		c.dictMaxZeros = 1
		for j := 0; j < c.dictSize; j++ {
			if src[i-j-1] == src[i] {
				bs.AppendBits(0b10000|uint64(j), 5)
				isReference = true
				windowValueCounts++
				break
			}
			if xor := uint64(src[i-j-1] ^ src[i]); bits.LeadingZeros64(xor)/8+bits.TrailingZeros64(xor)/8 > c.dictMaxZeros {
				c.dictIndex = j
				c.dictMaxZeros = bits.LeadingZeros64(xor)/8 + bits.TrailingZeros64(xor)/8
			}
		}
		if !isReference {
			if xor := c.xfcm.PredictNext() ^ uint64(src[i]); bits.LeadingZeros64(xor)/8+bits.TrailingZeros64(xor)/8 >= c.dictMaxZeros {
				c.dictMaxZeros = bits.LeadingZeros64(xor)/8 + bits.TrailingZeros64(xor)/8
				isXfcm = true
			}
			if c.dictMaxZeros > 8 {
				c.dictMaxZeros = 8
			}
			if isXfcm {
				bs.AppendBits(0b01110, 4)
				xor := c.xfcm.PredictNext() ^ uint64(src[i])
				tz := uint64(bits.TrailingZeros64(xor) / 8)
				bs.AppendBits(tz, 3)
				bs.AppendBits(uint64(8-c.dictMaxZeros), 3)
				bs.AppendBits(xor>>(tz*8), (8-c.dictMaxZeros)*8)
				predictorResidualLength += float64(8 - c.dictMaxZeros)
				predictorResidualCounts++
			} else {
				if c.dictIndex == -1 {
					bs.AppendBits(0b01111, 4)
					bs.AppendBits(uint64(src[i]), 64)
					exceptionCount++
				} else {
					bs.AppendBits(0b0110, 3)
					bs.AppendBits(uint64(c.dictIndex), 3)
					xor := src[i-c.dictIndex-1] ^ src[i]
					tz := uint64(bits.TrailingZeros64(uint64(xor)) / 8)
					bs.AppendBits(tz, 3)
					bs.AppendBits(uint64(8-c.dictMaxZeros), 3)
					bs.AppendBits(uint64(xor>>(tz*8)), (8-c.dictMaxZeros)*8)
					windowResidualLength += float64(8 - c.dictMaxZeros)
					windowResidualCounts++
				}
			}
		}
		c.update(uint64(src[i]))
	}
	return dst
}

func Decompress(dst []int64, src []byte) ([]int64, error) {
	bs := &gorillaz.ByteWrapper{Stream: &src, Count: 8}
	c := compressor{xfcm: NewPdfcmPredictor(16)}
	firstValue, err := bs.ReadBits(64)
	if err != nil {
		return nil, err
	}
	dst = append(dst, int64(firstValue))
	c.update(firstValue)
	length, err := bs.ReadBits(14)
	if err != nil {
		return nil, err
	}
	for i := uint64(1); i < length; i++ {
		bit, err := bs.ReadBit()
		if err != nil {
			return nil, err
		}
		if !bit { // 0'
			dst = append(dst, dst[i-1])
		} else {
			bit, err = bs.ReadBit()
			if err != nil {
				return nil, err
			}
			if !bit { // 10'
				offset, err := bs.ReadBits(3)
				if err != nil {
					return nil, err
				}
				dst = append(dst, dst[i-offset-1])
			} else {
				bit, err = bs.ReadBit()
				if err != nil {
					return nil, err
				}
				if !bit { // 110'
					offset, err := bs.ReadBits(3)
					if err != nil {
						return nil, err
					}
					tz, err := bs.ReadBits(3)
					if err != nil {
						return nil, err
					}
					l, err := bs.ReadBits(3)
					if err != nil {
						return nil, err
					}
					xor, err := bs.ReadBits(int(l * 8))
					if err != nil {
						return nil, err
					}
					xor <<= tz * 8
					xor ^= uint64(dst[i-offset-1])
					dst = append(dst, int64(xor))
				} else {
					bit, err = bs.ReadBit()
					if err != nil {
						return nil, err
					}
					if !bit { //1110'
						if err != nil {
							return nil, err
						}
						tz, err := bs.ReadBits(3)
						if err != nil {
							return nil, err
						}
						l, err := bs.ReadBits(3)
						if err != nil {
							return nil, err
						}
						xor, err := bs.ReadBits(int(l * 8))
						if err != nil {
							return nil, err
						}
						xor <<= tz * 8
						xor ^= c.xfcm.PredictNext()
						dst = append(dst, int64(xor))
					} else { //1111'
						exception, err := bs.ReadBits(64)
						if err != nil {
							return nil, err
						}
						dst = append(dst, int64(exception))
					}
				}
			}
		}
		c.update(uint64(dst[i]))
	}
	return dst, nil
}
