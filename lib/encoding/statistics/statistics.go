package statistics

import (
	"github.com/steakknife/hamming"
	"math"
	"math/bits"
)

func HammingDistance(int64s []int64) float64 {
	if int64s == nil || len(int64s) <= 1 {
		return 0
	}
	distance := float64(0)
	for i := 1; i < len(int64s); i++ {
		distance += float64(hamming.Int64(int64s[i-1], int64s[i]))
	}
	return distance / float64(len(int64s))
}

func ComplexHammingDistance(int64s []int64) (float64, bool) {
	if int64s == nil || len(int64s) <= 1 {
		return 0, false
	}

	var (
		xor    int64
		repeat int64
		averHd float64
	)

	for i := 1; i < len(int64s); i++ {
		xor = int64s[i-1] ^ int64s[i]
		if xor == 0 {
			repeat++
		} else {
			averHd += float64(hamming.CountBitsInt64(xor))
		}
	}
	return averHd / float64(len(int64s)), float64(repeat) > float64(len(int64s))*0.9
}

func ComplexHammingDistance2(int64s []int64) (float64, float64, float64, float64, float64, float64, float64, float64, float64) {
	if int64s == nil || len(int64s) <= 1 {
		return 0, 0, 0, 0, 0, 0, 0, 0, 0
	}

	var (
		repeat            int64
		averHd            float64
		averDeltaHd       float64
		deltaLeadingZero  float64
		deltaTrailingZero float64

		meanHd      float64
		meanDeltaHd float64
		meanDeltaLZ float64
		meanDeltaTZ float64

		xor       int64
		delta     int64
		prevDelta int64
	)

	for i := 1; i < len(int64s); i++ {
		xor = int64s[i-1] ^ int64s[i]
		delta = int64s[i] - int64s[i-1]
		if xor == 0 {
			repeat++
		} else {
			averHd += float64(hamming.CountBitsInt64(xor))
		}
		averDeltaHd += float64(hamming.Int64(delta, prevDelta))
		deltaLeadingZero += float64(bits.LeadingZeros64(uint64(delta)))
		deltaTrailingZero += float64(bits.TrailingZeros64(uint64(delta)))
		prevDelta = delta
	}
	averHd /= float64(len(int64s) - 1)
	averDeltaHd /= float64(len(int64s) - 1)
	deltaLeadingZero /= float64(len(int64s) - 1)
	deltaTrailingZero /= float64(len(int64s) - 1)

	var hd, dhd, dlz, dtz float64
	for i := 1; i < len(int64s); i++ {
		xor = int64s[i-1] ^ int64s[i]
		delta = int64s[i] - int64s[i-1]
		hd = float64(hamming.CountBitsInt64(xor)) - averHd
		dhd = float64(hamming.Int64(delta, prevDelta)) - averDeltaHd
		dlz = float64(bits.LeadingZeros64(uint64(delta))) - deltaLeadingZero
		dtz = float64(bits.TrailingZeros64(uint64(delta))) - deltaTrailingZero
		meanHd = hd * hd
		meanDeltaHd = dhd * dhd
		meanDeltaLZ = dlz * dlz
		meanDeltaTZ = dtz * dtz
		prevDelta = delta
	}

	return averHd,
		float64(repeat) / float64(len(int64s)-1),
		averDeltaHd, deltaLeadingZero, deltaTrailingZero, meanHd, meanDeltaHd, meanDeltaLZ, meanDeltaTZ

}

func DeltaHammingDistance(int64s []int64) float64 {
	if int64s == nil || len(int64s) <= 1 {
		return 0
	}

	length := len(int64s)
	delta := (int64s[length-1] - int64s[0]) / int64(length-1)
	standard := int64s[1]
	distance := float64(0)
	for i := 1; i < len(int64s); i++ {
		distance += float64(hamming.Int64(standard, int64s[i]))
		standard += delta
	}
	return distance / float64(len(int64s))
}

func Delta2HammingDistance(int64s []int64) float64 {
	if int64s == nil || len(int64s) <= 1 {
		return 0
	}
	if len(int64s) == 2 {
		return float64(hamming.Int64(int64s[0], int64s[1]))
	}

	length := len(int64s)
	deltaLongTerm := int64s[length-1] - int64s[length-2]
	deltaShortTerm := int64s[1] - int64s[0]
	delta := deltaShortTerm
	delta2 := int64(0)
	if deltaLongTerm != deltaShortTerm {
		delta2 = (deltaLongTerm - deltaShortTerm) / int64(length-2)
	}
	standard := int64s[0]
	distance := float64(0)
	for i := 0; i < len(int64s); i++ {
		distance += float64(hamming.Int64(standard, int64s[i]))
		standard += delta
		delta += delta2
	}
	return distance / float64(len(int64s))
}

func DeltaDistance(int64s []int64) float64 {
	if int64s == nil || len(int64s) <= 1 {
		return 0
	}
	distance := float64(0)
	for i := 1; i < len(int64s); i++ {
		distance += float64(int64s[i] - int64s[i-1])
	}
	return distance / float64(len(int64s))
}

func RepeatCounter(int64s []int64) int64 {
	if int64s == nil || len(int64s) <= 1 {
		return 0
	}
	counter := int64(0)
	for i := 1; i < len(int64s); i++ {
		if int64s[i-1] == int64s[i] {
			counter++
		}
	}
	return counter
}

func ShannonEntropy(int64s []int64) (e float64) {
	for _, v := range int64s {
		if v != 0 { // Entropy needs 0 * log(0) == 0.
			e -= float64(v) * math.Log(float64(v))
		}
	}
	return
}
