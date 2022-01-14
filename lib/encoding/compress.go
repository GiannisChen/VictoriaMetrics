package encoding

import (
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/lz4"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/metrics"
)

// CompressZSTDLevel appends compressed src to dst and returns
// the appended dst.
//
// The given compressLevel is used for the compression.
func CompressZSTDLevel(dst, src []byte, compressLevel int) []byte {
	ZstdCompressCalls.Inc()
	ZstdOriginalBytes.Add(len(src))
	dstLen := len(dst)
	dst = zstd.CompressLevel(dst, src, compressLevel)
	ZstdCompressedBytes.Add(len(dst) - dstLen)
	return dst
}

// DecompressZSTD decompresses src, appends the result to dst and returns
// the appended dst.
func DecompressZSTD(dst, src []byte) ([]byte, error) {
	ZstdDecompressCalls.Inc()
	b, err := zstd.Decompress(dst, src)
	if err != nil {
		return b, fmt.Errorf("cannot decompress zstd block with len=%d to a buffer with len=%d: %w; block data (hex): %X", len(src), len(dst), err, src)
	}
	return b, nil
}

func CompressLZ4Level(dst, src []byte, compressLevel int) ([]byte, error) {
	Lz4CompressCalls.Inc()
	Lz4OriginalBytes.Add(len(src))

	compressedLen := 0
	if compressLevel == 0 {
		if n, err := lz4.CompressLZ4(src, dst); err == nil {
			compressedLen = n
		} else {
			return nil, err
		}
	} else {
		// todo: fix out what is depth in lz4-HC and how does depth take effect.
		if n, err := lz4.CompressLZ4HC(src, dst, 10); err == nil {
			compressedLen = n
		} else {
			return nil, err
		}
	}

	Lz4CompressedBytes.Add(compressedLen)
	return dst, nil
}

func DecompressLZ4(dst, src []byte) ([]byte, error) {
	Lz4DecompressCalls.Inc()
	if _, err := lz4.UncompressBlock(src, dst, nil); err == nil {
		return dst, nil
	} else {
		return nil, err
	}
}

var (
	ZstdCompressCalls   = metrics.NewCounter(`vm_zstd_block_compress_calls_total`)
	ZstdDecompressCalls = metrics.NewCounter(`vm_zstd_block_decompress_calls_total`)

	ZstdOriginalBytes   = metrics.NewCounter(`vm_zstd_block_original_bytes_total`)
	ZstdCompressedBytes = metrics.NewCounter(`vm_zstd_block_compressed_bytes_total`)

	Lz4CompressCalls   = metrics.NewCounter(`vm_lz4_block_compress_call_total`)
	Lz4DecompressCalls = metrics.NewCounter(`vm_lz4_block_decompress_calls_total`)

	Lz4OriginalBytes   = metrics.NewCounter(`vm_lz4_block_original_bytes_total`)
	Lz4CompressedBytes = metrics.NewCounter(`vm_lz4_block_compressed_bytes_total`)
)
