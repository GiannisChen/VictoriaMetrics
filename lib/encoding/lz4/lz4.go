package lz4

import (
	"encoding/binary"
	"github.com/bkaradzic/go-lz4"
)

func Compress(dst []byte, src []int64) []byte {
	uncb := make([]byte, len(src)*8)
	for i, u := range src {
		binary.LittleEndian.PutUint64(uncb[i*8:(i+1)*8], uint64(u))
	}
	dst, _ = lz4.Encode(dst, uncb)
	return dst
}

func Decompress(dst []int64, src []byte) ([]int64, error) {
	var uncb []byte
	uncb, _ = lz4.Decode(uncb, src)
	for i := 0; i < len(uncb)/8; i++ {
		dst = append(dst, int64(binary.LittleEndian.Uint64(uncb[i*8:(i+1)*8])))
	}
	return dst, nil
}
