package reporter

import (
	"time"

	"github.com/cactus/go-statsd-client/statsd"

	"github.com/twitchscience/aws_utils/logger"
)

// CactusStatterWrapper is a wrapper for a statsd.Statter with a rate for all stats.
type CactusStatterWrapper struct {
	statter statsd.Statter
	rate    float32
}

// WrapCactusStatter returns a CactusStatterWrapper of the statter at a given rate.
func WrapCactusStatter(statter statsd.Statter, rate float32) *CactusStatterWrapper {
	return &CactusStatterWrapper{
		statter: statter,
		rate:    rate,
	}
}

// Timing reports the timing to the wrapped statter at our rate.
func (c *CactusStatterWrapper) Timing(stat string, t time.Duration) {
	if err := c.statter.Timing(stat, int64(t), c.rate); err != nil {
		logger.WithError(err).WithField("stat", stat).Error("Failed to report timing")
	}
}

// IncrBy reports the stat increment to the wrapped statter at our rate.
func (c *CactusStatterWrapper) IncrBy(stat string, value int) {
	if err := c.statter.Inc(stat, int64(value), c.rate); err != nil {
		logger.WithError(err).WithField("stat", stat).Error("Failed to report IncrBy")
	}
}
