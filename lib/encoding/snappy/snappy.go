package snappy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/klauspost/compress/snappy"
	"io/ioutil"
)

func Compress(dst []byte, src []int64) []byte {
	uncb := make([]byte, len(src)*8)
	for i, u := range src {
		binary.LittleEndian.PutUint64(uncb[i*8:(i+1)*8], uint64(u))
	}
	bw := &bytes.Buffer{}
	snappyEncoder := snappy.NewBufferedWriter(bw)
	_, err := snappyEncoder.Write(uncb)
	if err != nil {
		fmt.Println(err)
	}
	err = snappyEncoder.Flush()
	if err != nil {
		fmt.Println(err)
	}
	err = snappyEncoder.Close()
	if err != nil {
		fmt.Println(err)
	}
	return append(dst, bw.Bytes()...)
}

func Decompress(dst []int64, src []byte) ([]int64, error) {
	snappyDecoder := snappy.NewReader(bytes.NewReader(src))
	uncb, err := ioutil.ReadAll(snappyDecoder)
	if err != nil {
		fmt.Println(err)
	}
	for i := 0; i < len(uncb)/8; i++ {
		dst = append(dst, int64(binary.LittleEndian.Uint64(uncb[i*8:(i+1)*8])))
	}
	return dst, nil
}
