package reporter

// FailMode is an enum of the types of failure modes we report.
type FailMode int

// See String() for a more verbose explanation of each.
const (
	None FailMode = iota
	UnableToParseData
	NonTrackingEvent
	BadColumnConversion
	FailedWrite
	EmptyRequest
	SkippedColumn
	UnknownError
	PanickedInProcessing
	FailedTransport
)

var failMessages = map[FailMode]string{
	None:                 "Success",
	SkippedColumn:        "Missing One or More Columns",
	UnableToParseData:    "Malformed Data",
	NonTrackingEvent:     "Untracked Event",
	BadColumnConversion:  "Badly Typed Columns",
	FailedWrite:          "Failed To Write",
	EmptyRequest:         "Empty Request",
	UnknownError:         "Unknown Failure",
	PanickedInProcessing: "Panicked in Processing",
	FailedTransport:      "Failed in Transport",
}

// Return a human-readable string describing the given FailMode.
func (m FailMode) String() string {
	msg, ok := failMessages[m]
	if !ok {
		return "Unknown Failure"
	}
	return msg
}
