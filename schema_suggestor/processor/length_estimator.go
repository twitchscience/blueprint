package processor

import "sort"

// LengthEstimator computes the 99th percentile of a set of integers.
type LengthEstimator struct {
	Lengths []int
}

// Increment adds ann integer to the set of integers.
func (l *LengthEstimator) Increment(size int) {
	l.Lengths = append(l.Lengths, size)
}

// Estimate returns the 99the percentile of the set of integers.
func (l *LengthEstimator) Estimate() int {
	return int(Percentile(l.Lengths, 99))
}

// Percentile computes the percentile of a set of integers using http://en.wikipedia.org/wiki/Percentile#Alternative_methods
func Percentile(nums []int, percentile int) float64 {
	if len(nums) == 0 {
		return 0.0
	}

	sort.Ints(nums)

	rank := float64(percentile)/100.0*(float64(len(nums))-1.0) + 1.0 // .99 * (5-1) + 1 = 4.9999

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
