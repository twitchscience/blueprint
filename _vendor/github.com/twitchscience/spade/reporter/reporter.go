package reporter

import (
	"fmt"
	"time"

	"github.com/twitchscience/aws_utils/logger"
)

const reportBuffer = 400000

// StatsLogger is an interface to receive statsd stats.
type StatsLogger interface {
	Timing(string, time.Duration)
	IncrBy(string, int)
}

// Reporter Records Results and returns stats on them.
type Reporter interface {
	Record(*Result)
	Report() map[string]int
}

// Tracker Tracks Results in some external system.
type Tracker interface {
	Track(*Result)
}

// Result tracks an input record with identifiers, timing information, and failures.
type Result struct {
	Duration   time.Duration
	FinishedAt time.Time
	Failure    FailMode
	UUID       string
	Line       string
	Category   string
}

// SpadeReporter is a Reporter that also sends stats to external Trackers.
// It is NOT thread safe.
type SpadeReporter struct {
	Trackers []Tracker
	stats    map[string]int
	record   chan *Result
	report   chan chan map[string]int
	reset    chan bool
}

// SpadeStatsdTracker is a Tracker that reports to statsd.
type SpadeStatsdTracker struct {
	Stats StatsLogger
}

// BuildSpadeReporter builds a SpadeReporter on the given Trackers and starts its goroutine.
func BuildSpadeReporter(trackers []Tracker) Reporter {
	r := &SpadeReporter{
		Trackers: trackers,
		stats:    make(map[string]int),
		record:   make(chan *Result, reportBuffer),
		report:   make(chan chan map[string]int),
		reset:    make(chan bool),
	}
	logger.Go(r.crank)
	return r
}

func (r *SpadeReporter) crank() {
	for {
		select {
		case result := <-r.record:
			for _, t := range r.Trackers {
				t.Track(result)
			}
			r.stats[result.Failure.String()]++
		case responseChan := <-r.report:
			c := make(map[string]int, len(r.stats))
			for k, v := range r.stats {
				c[k] = v
			}
			responseChan <- c
		}
	}
}

// Record sends the given result to all trackers and our stats report.
func (r *SpadeReporter) Record(result *Result) {
	r.record <- result
}

// Report returns the current stats report.
func (r *SpadeReporter) Report() map[string]int {
	responseChan := make(chan map[string]int)
	defer close(responseChan)
	r.report <- responseChan
	return <-responseChan
}

// Track sends the given result to statsd.
func (s *SpadeStatsdTracker) Track(result *Result) {
	if result.Failure == None || result.Failure == SkippedColumn {
		s.Stats.IncrBy(fmt.Sprintf("tracking.%s.success", result.Category), 1)
	} else {
		s.Stats.IncrBy(fmt.Sprintf("tracking.%s.fail", result.Category), 1)
	}
	s.Stats.Timing(fmt.Sprintf("%d", result.Failure), result.Duration)
}
