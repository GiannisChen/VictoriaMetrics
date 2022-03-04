package statistics

import "github.com/steakknife/hamming"

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
