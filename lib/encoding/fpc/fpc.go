package fpc

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/gorillaz"
	"math/bits"
)

func Compress(dst []byte, src []int64) []byte {
	bs := &gorillaz.ByteWrapper{Stream: &dst, Count: 0}
	bs.AppendBits(uint64(len(src)), 14)
	dfcm := NewDfcmPredictor(16)
	fcm := NewFcmPredictor(16)
	var dtmp, ftmp, cnt, xor uint64
	for _, v := range src {
		u := uint64(v)
		dtmp = dfcm.PredictNext() ^ u
		ftmp = fcm.PredictNext() ^ u
		if bits.LeadingZeros64(dtmp)/8 > bits.LeadingZeros64(ftmp)/8 {
			cnt = uint64(bits.LeadingZeros64(dtmp) / 8)
			if cnt == 8 {
				cnt -= 1
			}
			xor = dtmp
			bs.AppendBits(cnt|0b1000, 4)
		} else {
			cnt = uint64(bits.LeadingZeros64(ftmp) / 8)
			if cnt == 8 {
				cnt -= 1
			}
			xor = ftmp
			bs.AppendBits(cnt|0b0000, 4)
		}
		bs.AppendBits(xor, int((8-cnt)*8))
		dfcm.Update(u)
		fcm.Update(u)
	}
	return dst
}

func Decompress(dst []int64, src []byte) ([]int64, error) {
	bs := &gorillaz.ByteWrapper{Stream: &src, Count: 8}
	size, err := bs.ReadBits(14)
	if err != nil {
		return nil, err
	}
	dfcm := NewDfcmPredictor(16)
	fcm := NewFcmPredictor(16)
	var pred, xor, cnt uint64
	for i := uint64(0); i < size; i++ {
		if bit, err := bs.ReadBit(); err != nil {
			return nil, err
		} else {
			cnt, err = bs.ReadBits(3)
			if err != nil {
				return nil, err
			}
			if bit {
				pred = dfcm.PredictNext()
			} else {
				pred = fcm.PredictNext()
			}
			xor, err = bs.ReadBits(int(8 * (8 - cnt)))
			if err != nil {
				return nil, err
			}
			xor ^= pred
			dst = append(dst, int64(xor))
			dfcm.Update(xor)
			fcm.Update(xor)
		}
	}
	return dst, nil
}
