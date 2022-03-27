package huffman

import (
	"bufio"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/decimal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/gorillaz"
	"io"
	"math"
	"math/rand"
	"os"
	"testing"
)

func TestHuffmanCompressor(t *testing.T) {
	t.Helper()
	h := GetHuff()
	defer PutHuff(h)

	values := []int64{1, 2, 3}

	dst := h.Compress(make([]byte, 0), values, values[0])

	newValues, err := h.Decompress(make([]int64, 0), dst, values[0], len(values))
	if err != nil {
		t.Fatal(err)
	}

	if len(newValues) != len(values) {
		t.Fatalf("length does not match.")
	}

	for i := 0; i < len(values); i++ {
		if values[i] != newValues[i] {
			t.Fatalf("items does not match. where: %d\n", i)
		}
	}
}

func TestHuffmanCompressorDecompress(t *testing.T) {
	t.Helper()
	h := GetHuff()
	defer PutHuff(h)

	values := make([]int64, 8192)
	for i := 0; i < len(values); i++ {
		values[i] = int64(math.Float64bits(30e3 + rand.NormFloat64()*1e2))
	}

	//for i := 1; i < len(values); i++ {
	//	fmt.Printf("%.64b\n", values[i]^values[i-1])
	//}

	dst := h.Compress(make([]byte, 0), values, values[0])
	size := len(dst)

	newValues, err := h.Decompress(make([]int64, 0), dst, values[0], len(values))
	if err != nil {
		t.Fatal(err)
	}

	if len(newValues) != len(values) {
		t.Fatalf("length does not match.")
	}

	for i := 0; i < len(values); i++ {
		if values[i] != newValues[i] {
			t.Fatalf("items does not match. where: %d\n", i)
		}
	}

	t.Logf("%d %.8f\n", size, float64(size)/float64(len(values)*8))
}

func TestBuild(t *testing.T) {
	t.Helper()

	leaves := []*Node{ // From "this is an example of a huffman tree"
		{Value: 0, Count: 2149144473},
		{Value: 1, Count: 7017311},
		{Value: 2, Count: 5970369},
		{Value: 3, Count: 6030184},
		{Value: 4, Count: 5573481},
		{Value: 5, Count: 4780470},
		{Value: 6, Count: 5786580},
		{Value: 7, Count: 5174340},
		{Value: 8, Count: 4723360},
		{Value: 9, Count: 3712278},
		{Value: 10, Count: 6859939},
		{Value: 11, Count: 3807160},
		{Value: 12, Count: 8076332},
		{Value: 13, Count: 4260195},
		{Value: 14, Count: 4380499},
		{Value: 15, Count: 4168961},
		{Value: 16, Count: 5268087},
		{Value: 17, Count: 4031858},
		{Value: 18, Count: 5162303},
		{Value: 19, Count: 5007177},
		{Value: 20, Count: 8121456},
		{Value: 21, Count: 5566697},
		{Value: 22, Count: 5687720},
		{Value: 23, Count: 4041580},
		{Value: 24, Count: 5250048},
		{Value: 25, Count: 15280955},
		{Value: 26, Count: 5256216},
		{Value: 27, Count: 4334954},
		{Value: 28, Count: 5335141},
		{Value: 29, Count: 4023778},
		{Value: 30, Count: 7358495},
		{Value: 31, Count: 4586977},
		{Value: 32, Count: 10608132},
		{Value: 33, Count: 5968286},
		{Value: 34, Count: 5907454},
		{Value: 35, Count: 5768526},
		{Value: 36, Count: 5573134},
		{Value: 37, Count: 5482270},
		{Value: 38, Count: 8696065},
		{Value: 39, Count: 5402130},
		{Value: 40, Count: 7074748},
		{Value: 41, Count: 5780193},
		{Value: 42, Count: 9330556},
		{Value: 43, Count: 5163206},
		{Value: 44, Count: 5667685},
		{Value: 45, Count: 4949947},
		{Value: 46, Count: 5028620},
		{Value: 47, Count: 5022508},
		{Value: 48, Count: 8643391},
		{Value: 49, Count: 8219446},
		{Value: 50, Count: 7851642},
		{Value: 51, Count: 354848945},
		{Value: 52, Count: 7198777},
		{Value: 53, Count: 7257592},
		{Value: 54, Count: 6576604},
		{Value: 55, Count: 6620252},
		{Value: 56, Count: 5510719},
		{Value: 57, Count: 5701234},
		{Value: 58, Count: 4720051},
		{Value: 59, Count: 4242612},
		{Value: 60, Count: 4297753},
		{Value: 61, Count: 6539317},
		{Value: 62, Count: 4064741},
		{Value: 63, Count: 36018793},
		{Value: 64, Count: 261514630},
		{Value: 65, Count: 4620436},
		{Value: 66, Count: 4513306},
		{Value: 67, Count: 3797832},
		{Value: 68, Count: 3173202},
		{Value: 69, Count: 2990236},
		{Value: 70, Count: 3421616},
		{Value: 71, Count: 4947717},
		{Value: 72, Count: 3063298},
		{Value: 73, Count: 2487966},
		{Value: 74, Count: 2319697},
		{Value: 75, Count: 1867827},
		{Value: 76, Count: 12613971},
		{Value: 77, Count: 2188706},
		{Value: 78, Count: 1900325},
		{Value: 79, Count: 2005359},
		{Value: 80, Count: 3314102},
		{Value: 81, Count: 4871321},
		{Value: 82, Count: 2958601},
		{Value: 83, Count: 3055144},
		{Value: 84, Count: 1885859},
		{Value: 85, Count: 94673047},
		{Value: 86, Count: 1935062},
		{Value: 87, Count: 11273713},
		{Value: 88, Count: 1635317},
		{Value: 89, Count: 4705286},
		{Value: 90, Count: 1349290},
		{Value: 91, Count: 1398951},
		{Value: 92, Count: 3318794},
		{Value: 93, Count: 1044724},
		{Value: 94, Count: 1027478},
		{Value: 95, Count: 1229586},
		{Value: 96, Count: 6308982},
		{Value: 97, Count: 1382508},
		{Value: 98, Count: 1420314},
		{Value: 99, Count: 945282},
		{Value: 100, Count: 1222438},
		{Value: 101, Count: 837743},
		{Value: 102, Count: 338466450},
		{Value: 103, Count: 768098},
		{Value: 104, Count: 895681},
		{Value: 105, Count: 862838},
		{Value: 106, Count: 2214587},
		{Value: 107, Count: 810782},
		{Value: 108, Count: 1740649},
		{Value: 109, Count: 805664},
		{Value: 110, Count: 1182911},
		{Value: 111, Count: 1113863},
		{Value: 112, Count: 2970947},
		{Value: 113, Count: 1205736},
		{Value: 114, Count: 1075257},
		{Value: 115, Count: 3963011},
		{Value: 116, Count: 1283255},
		{Value: 117, Count: 1165613},
		{Value: 118, Count: 989845},
		{Value: 119, Count: 857550},
		{Value: 120, Count: 1103395},
		{Value: 121, Count: 1704040},
		{Value: 122, Count: 3541779},
		{Value: 123, Count: 1838570},
		{Value: 124, Count: 1019851},
		{Value: 125, Count: 913358},
		{Value: 126, Count: 1082380},
		{Value: 127, Count: 25745238},
		{Value: 128, Count: 25118875},
		{Value: 129, Count: 411902},
		{Value: 130, Count: 289001},
		{Value: 131, Count: 389105},
		{Value: 132, Count: 482183},
		{Value: 133, Count: 3104003},
		{Value: 134, Count: 1026886},
		{Value: 135, Count: 559398},
		{Value: 136, Count: 395135},
		{Value: 137, Count: 494583},
		{Value: 138, Count: 667104},
		{Value: 139, Count: 687573},
		{Value: 140, Count: 3433772},
		{Value: 141, Count: 668800},
		{Value: 142, Count: 379513},
		{Value: 143, Count: 2768327},
		{Value: 144, Count: 665479},
		{Value: 145, Count: 744837},
		{Value: 146, Count: 347844},
		{Value: 147, Count: 1384747},
		{Value: 148, Count: 453414},
		{Value: 149, Count: 1556566},
		{Value: 150, Count: 367304},
		{Value: 151, Count: 455672},
		{Value: 152, Count: 377273},
		{Value: 153, Count: 331561654},
		{Value: 154, Count: 70631927},
		{Value: 155, Count: 692031},
		{Value: 156, Count: 416001},
		{Value: 157, Count: 532730},
		{Value: 158, Count: 540461},
		{Value: 159, Count: 701633},
		{Value: 160, Count: 5015832},
		{Value: 161, Count: 403491},
		{Value: 162, Count: 340968},
		{Value: 163, Count: 2250319},
		{Value: 164, Count: 1093013},
		{Value: 165, Count: 415565},
		{Value: 166, Count: 3387269},
		{Value: 167, Count: 490256},
		{Value: 168, Count: 346622},
		{Value: 169, Count: 10993983},
		{Value: 170, Count: 84990095},
		{Value: 171, Count: 8492545},
		{Value: 172, Count: 1235298},
		{Value: 173, Count: 466078},
		{Value: 174, Count: 3168895},
		{Value: 175, Count: 349369},
		{Value: 176, Count: 617278},
		{Value: 177, Count: 542302},
		{Value: 178, Count: 641793},
		{Value: 179, Count: 11164528},
		{Value: 180, Count: 424808},
		{Value: 181, Count: 583002},
		{Value: 182, Count: 564431},
		{Value: 183, Count: 466045},
		{Value: 184, Count: 3215987},
		{Value: 185, Count: 6343541},
		{Value: 186, Count: 544016},
		{Value: 187, Count: 313135},
		{Value: 188, Count: 649280},
		{Value: 189, Count: 461284},
		{Value: 190, Count: 425523},
		{Value: 191, Count: 1465715},
		{Value: 192, Count: 12810818},
		{Value: 193, Count: 519087},
		{Value: 194, Count: 2504173},
		{Value: 195, Count: 1035592},
		{Value: 196, Count: 540878},
		{Value: 197, Count: 670600},
		{Value: 198, Count: 1193464},
		{Value: 199, Count: 506614},
		{Value: 200, Count: 968208},
		{Value: 201, Count: 3219588},
		{Value: 202, Count: 742814},
		{Value: 203, Count: 464728},
		{Value: 204, Count: 273148018},
		{Value: 205, Count: 58945384},
		{Value: 206, Count: 509532},
		{Value: 207, Count: 445891},
		{Value: 208, Count: 559217},
		{Value: 209, Count: 523398},
		{Value: 210, Count: 803961},
		{Value: 211, Count: 3031019},
		{Value: 212, Count: 658228},
		{Value: 213, Count: 4545040},
		{Value: 214, Count: 622680},
		{Value: 215, Count: 2757740},
		{Value: 216, Count: 616629},
		{Value: 217, Count: 5383112},
		{Value: 218, Count: 494483},
		{Value: 219, Count: 523941},
		{Value: 220, Count: 632162},
		{Value: 221, Count: 650521},
		{Value: 222, Count: 790375},
		{Value: 223, Count: 874893},
		{Value: 224, Count: 6488138},
		{Value: 225, Count: 3473712},
		{Value: 226, Count: 534383},
		{Value: 227, Count: 1950858},
		{Value: 228, Count: 774526},
		{Value: 229, Count: 603018},
		{Value: 230, Count: 11929029},
		{Value: 231, Count: 561502},
		{Value: 232, Count: 626030},
		{Value: 233, Count: 1987823},
		{Value: 234, Count: 1908072},
		{Value: 235, Count: 2878984},
		{Value: 236, Count: 2447276},
		{Value: 237, Count: 563474},
		{Value: 238, Count: 532089},
		{Value: 239, Count: 545222},
		{Value: 240, Count: 1572580},
		{Value: 241, Count: 1813299},
		{Value: 242, Count: 761029},
		{Value: 243, Count: 4359442},
		{Value: 244, Count: 1433147},
		{Value: 245, Count: 3016443},
		{Value: 246, Count: 1714765},
		{Value: 247, Count: 648935},
		{Value: 248, Count: 1457162},
		{Value: 249, Count: 2058426},
		{Value: 250, Count: 735771},
		{Value: 251, Count: 1393144},
		{Value: 252, Count: 11428097},
		{Value: 253, Count: 645303},
		{Value: 254, Count: 9767383},
		{Value: 255, Count: 85575994},
	}
	root := Build(leaves)
	if root == nil {
		t.Errorf("Got: %v, want: not nil", root)
	}
	t.Log(root)
}

func TestNewHuff(t *testing.T) {
	Print(newHuffmanCompressor().root)
}

func TestStatistics(t *testing.T) {
	h := newHuffmanCompressor()
	count := 0
	totalBits := float64(0)
	totalNums := float64(0)
	Print(h.root)
	for _, leaf := range h.leaves {
		count++
		code := leaf.Code()
		totalBits += float64(code.length) * float64(leaf.Count)
		totalNums += float64(leaf.Count)
		fmt.Println(leaf.Value)
	}
	fmt.Printf("%.8f\n", totalBits/totalNums)
	fmt.Println(len(h.leaves))
}

func TestRealData(t *testing.T) {
	files, err := ReadFileName("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/index.txt", 36)
	if err != nil {
		t.Log(err)
	}

	logs, err := os.OpenFile("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/real_data_xor_huffman.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()

	w := bufio.NewWriter(logs)

	for _, file := range files {
		float64s, err := ReadAllFloat64File("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/" + file)
		if err != nil {
			t.Fatal(err)
		}
		values, _ := decimal.AppendFloatToInt64(nil, float64s)

		if len(values) < 8192 {
			h := GetHuff()
			dst := h.Compress(make([]byte, 0), values, values[0])
			if _, err := w.WriteString(fmt.Sprintf("%s %.8f\n", file, float64(len(dst))/float64(8*len(values)))); err != nil {
				t.Fatalf("write error.")
			}
			PutHuff(h)
			continue
		}

		var compressedBytes, ordinaryBytes int64
		for i := 0; i < len(values)-8191; i += 8192 {
			h := GetHuff()
			dst := h.Compress(make([]byte, 0), values[i:i+8192], values[0])
			compressedBytes += int64(len(dst))
			ordinaryBytes += int64(8192 * 8)
			PutHuff(h)
		}
		h := GetHuff()
		dst := h.Compress(make([]byte, 0), values[len(values)-8192:], values[0])
		compressedBytes += int64(len(dst))
		ordinaryBytes += int64(8192 * 8)
		PutHuff(h)

		if _, err := w.WriteString(fmt.Sprintf("%s %.8f\n", file, float64(compressedBytes)/float64(ordinaryBytes))); err != nil {
			t.Fatalf("write error.")
		}
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("flush error.")
	}
}

func TestRealDataGorilla(t *testing.T) {
	files, err := ReadFileName("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/index.txt", 36)
	if err != nil {
		t.Log(err)
	}

	logs, err := os.OpenFile("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/real_data_xor_gorilla.log", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()

	w := bufio.NewWriter(logs)

	for _, file := range files {
		float64s, err := ReadAllFloat64File("/home/giannischen/go/src/giannischen@nuaa.edu.cn/VictoriaMetrics/lib/encoding/data/ammmmo/" + file)
		if err != nil {
			t.Fatal(err)
		}
		values, _ := decimal.AppendFloatToInt64(nil, float64s)

		if len(values) < 8192 {
			h := GetHuff()
			dst, _, _ := gorillaz.Compress(make([]byte, 0), values)
			if _, err := w.WriteString(fmt.Sprintf("%s %.8f\n", file, float64(len(dst))/float64(8*len(values)))); err != nil {
				t.Fatalf("write error.")
			}
			PutHuff(h)
			continue
		}

		var compressedBytes, ordinaryBytes int64
		for i := 0; i < len(values)-8191; i += 8192 {
			h := GetHuff()
			dst, _, _ := gorillaz.Compress(make([]byte, 0), values[i:i+8192])
			compressedBytes += int64(len(dst))
			ordinaryBytes += int64(8192 * 8)
			PutHuff(h)
		}
		h := GetHuff()
		dst, _, _ := gorillaz.Compress(make([]byte, 0), values[len(values)-8192:])
		compressedBytes += int64(len(dst))
		ordinaryBytes += int64(8192 * 8)
		PutHuff(h)

		if _, err := w.WriteString(fmt.Sprintf("%s %.8f\n", file, float64(compressedBytes)/float64(ordinaryBytes))); err != nil {
			t.Fatalf("write error.")
		}
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("flush error.")
	}
}

func ReadAllFloat64File(file string) ([]float64, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	dst := make([]float64, 0)
	r := bufio.NewReader(f)
	for true {
		var num float64
		if n, err := fmt.Fscanf(r, "%f\n", &num); n == 0 || (err != nil && err != io.EOF) {
			if err == io.EOF {
				return dst, nil
			}
			return dst, nil
		} else {
			dst = append(dst, num)
			if err == io.EOF {
				return dst, nil
			}
		}
	}
	return dst, nil
}

func ReadFileName(file string, length uint) ([]string, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	dst := make([]string, length)
	r := bufio.NewReader(f)
	for i := uint(0); i < length; i++ {
		var name string
		if n, err := fmt.Fscanf(r, "%s\n", &name); n == 0 || (err != nil && err != io.EOF) {
			return nil, err
		} else {
			dst[i] = name
		}
	}
	return dst, nil
}
