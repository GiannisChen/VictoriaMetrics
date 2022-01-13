package lz4

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"testing"
)

type testcase struct {
	file string
	src  []byte
}

var rawFiles = []testcase{
	{"./testdata/e.txt", nil},
	{"./testdata/bzImage_lz4_isolated", nil},
	{"./testdata/e.txt", nil},
	{"./testdata/gettysburg.txt", nil},
	{"./testdata/issue102.data", nil},
	{"./testdata/issue43.data", nil},
	{"./testdata/issue51.data", nil},
	{"./testdata/Mark.Twain-Tom.Sawyer.txt", nil},
	{"./testdata/Mark.Twain-Tom.Sawyer_long.txt", nil},
	{"./testdata/pg1661.txt", nil},
	{"./testdata/pg_control.tar", nil},
	{"./testdata/pi.txt", nil},
	{"./testdata/random.data", nil},
	{"./testdata/repeat.txt", nil},
	{"./testdata/upperbound.data", nil},
	{"./testdata/vmlinux_LZ4_19377", nil},
}

func TestCompressUncompress(t *testing.T) {
	type compressor func(s, d []byte) (int, error)

	run := func(t *testing.T, tc testcase, compress compressor) int {
		t.Helper()
		src := tc.src

		// Compress the data.
		zbuf := make([]byte, CompressBlockBound(len(src)))
		n, err := compress(src, zbuf)
		if err != nil {
			t.Error(err)
			return 0
		}
		zbuf = zbuf[:n]

		if n == 0 {
			t.Errorf("data not compressed: %d/%d", n, len(src))
			return 0
		}

		// Uncompress the data.
		buf := make([]byte, len(src))
		n, err = UncompressBlock(zbuf, buf, nil)
		if err != nil {
			t.Fatal(err)
		} else if n < 0 || n > len(buf) {
			t.Fatalf("returned written bytes > len(buf): n=%d available=%d", n, len(buf))
		} else if n != len(src) {
			t.Errorf("expected to decompress into %d bytes got %d", len(src), n)
		}

		buf = buf[:n]
		if !bytes.Equal(src, buf) {
			var c int
			for i, b := range buf {
				if c > 10 {
					break
				}
				if src[i] != b {
					t.Errorf("%d: exp(%x) != got(%x)", i, src[i], buf[i])
					c++
				}
			}
			t.Fatal("uncompressed compressed data not matching initial input")
			return 0
		}

		return len(zbuf)
	}

	t.Logf("%-41s: %8s / %8s / %8s / %8s / %8s\n", "file-name", "N-len", "N-ratio", "HC-len", "HC-ratio", "src-len")
	for _, tc := range rawFiles {
		src, err := ioutil.ReadFile(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		tc.src = src

		var n, nhc int
		t.Run("", func(t *testing.T) {
			tc := tc
			t.Run(tc.file, func(t *testing.T) {
				n = run(t, tc, func(src, dst []byte) (int, error) {
					return CompressLZ4(src, dst)
				})
			})
			t.Run(fmt.Sprintf("%s HC", tc.file), func(t *testing.T) {
				nhc = run(t, tc, func(src, dst []byte) (int, error) {
					return CompressLZ4HC(src, dst, 10)
				})
			})
		})
		if !t.Failed() {
			l := len(src)
			t.Logf("%-40s: %8d / %8f / %8d / %8f / %8d\n", tc.file, n, float64(n)/float64(l), nhc, float64(nhc)/float64(l), l)
		}
	}
}

func TestCompressUncompressDigit(t *testing.T) {
	type compressor func(s, d []byte) (int, error)

	rand.Seed(22)
	num := rand.Uint64()
	list := make([][]uint64, 10)
	for i := 0; i < 10; i++ {
		list[i] = make([]uint64, 8000)
		for j := 0; j < 8000; j++ {
			list[i][j] = num
		}
	}

	run := func(t *testing.T, digits []uint64, compress compressor) (int, int) {
		t.Helper()
		src := new(bytes.Buffer)
		if err := binary.Write(src, binary.LittleEndian, digits); err != nil {
			t.Fatalf("cannot converse []uint64 to []byte")
		}

		// Compress the data.
		zbuf := make([]byte, CompressBlockBound(src.Len()))
		n, err := compress(src.Bytes(), zbuf)
		if err != nil {
			t.Error(err)
			return 0, 0
		}
		zbuf = zbuf[:n]

		if n == 0 {
			t.Errorf("data not compressed: %d/%d", n, src.Len())
			return 0, 0
		}

		// Uncompress the data.
		buf := make([]byte, src.Len())
		n, err = UncompressBlock(zbuf, buf, nil)
		if err != nil {
			t.Fatal(err)
		} else if n < 0 || n > len(buf) {
			t.Fatalf("returned written bytes > len(buf): n=%d available=%d", n, len(buf))
		} else if n != src.Len() {
			t.Errorf("expected to decompress into %d bytes got %d", src.Len(), n)
		}

		buf = buf[:n]
		if !bytes.Equal(src.Bytes(), buf) {
			var c int
			for i, b := range buf {
				if c > 10 {
					break
				}
				if src.Bytes()[i] != b {
					t.Errorf("%d: exp(%x) != got(%x)", i, src.Bytes()[i], buf[i])
					c++
				}
			}
			t.Fatal("uncompressed compressed data not matching initial input")
			return 0, 0
		}

		return len(zbuf), src.Len()
	}

	t.Logf("%-41s: %8s / %8s / %8s / %8s / %8s\n", "list", "N-len", "N-ratio", "HC-len", "HC-ratio", "src-len")

	var n, nhc, l int
	t.Run("", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			t.Run("list"+strconv.Itoa(i), func(t *testing.T) {
				n, l = run(t, list[i], func(src, dst []byte) (int, error) {
					return CompressLZ4(src, dst)
				})
			})
			t.Run(fmt.Sprintf("%s HC", "list"+strconv.Itoa(i)), func(t *testing.T) {
				nhc, l = run(t, list[i], func(src, dst []byte) (int, error) {
					return CompressLZ4HC(src, dst, 10)
				})
			})
			t.Logf("%-40s: %8d / %8f / %8d / %8f / %8d\n", "list", n, float64(n)/float64(l), nhc, float64(nhc)/float64(l), l)
		}
	})
}
