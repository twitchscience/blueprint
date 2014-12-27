package processor

import "testing"

func TestLengthEstimator(t *testing.T) {
	e := LengthEstimator{}
	for _, num := range []int{15, 40, 50, 20, 35} {
		e.Increment(num)
	}
	if e.Estimate() != 49 {
		t.Fatalf("thought we would get 50 but got %d\n", e.Estimate())
	}
}
