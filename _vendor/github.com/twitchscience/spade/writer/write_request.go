package writer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/twitchscience/spade/reporter"
)

// WriteRequest is a processed event with metadata, ready for writing to an output.
type WriteRequest struct {
	Category string
	Version  int
	// Line is the transformed data in tsv format
	Line string
	// Record is the transformed data in a key/value map
	Record map[string]string
	UUID   string
	// Keep the source around for logging
	Source  json.RawMessage
	Failure reporter.FailMode
	Pstart  time.Time
}

// GetStartTime returns when procesing of the event started.
func (r *WriteRequest) GetStartTime() time.Time {
	return r.Pstart
}

// GetCategory returns the event type.
func (r *WriteRequest) GetCategory() string {
	return fmt.Sprintf("%s.v%d", r.Category, r.Version)
}

// GetMessage returns the raw JSON of the event.
func (r *WriteRequest) GetMessage() string {
	return string(r.Source)
}

// GetResult returns timing and metadata of the event.
func (r *WriteRequest) GetResult() *reporter.Result {
	return &reporter.Result{
		Failure:    r.Failure,
		UUID:       r.UUID,
		Line:       r.Line,
		Category:   r.Category,
		FinishedAt: time.Now(),
		Duration:   time.Since(r.Pstart),
	}
}
