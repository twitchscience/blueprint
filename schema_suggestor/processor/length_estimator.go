package processor

import "sort"

type LengthEstimator struct {
	Lengths []int
}

func NewLengthEstimator() LengthEstimator {
	return LengthEstimator{
		Lengths: make([]int, 0),
	}
}

func (l *LengthEstimator) Increment(size int) {
	l.Lengths = append(l.Lengths, size)
}

func (l *LengthEstimator) Estimate() int {
	return int(Percentile(l.Lengths, 99))
}

// using http://en.wikipedia.org/wiki/Percentile#Alternative_methods
func Percentile(nums []int, percentile int) float64 {
	sort.Ints(nums)

	rank := float64(percentile)/100.0*(float64(len(nums))-1.0) + 1.0

	integerPart := int(rank)
	decimalPart := rank - float64(integerPart)

	switch integerPart {
	case 1:
		return float64(nums[integerPart-1])
	case len(nums):
		return float64(nums[integerPart-1])
	default:
		return float64(nums[integerPart-1]) + decimalPart*float64(nums[integerPart]-nums[integerPart-1])
	}
}
