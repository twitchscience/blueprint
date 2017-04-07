package parser

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/twitchscience/spade/reporter"
)

// MixpanelEvent is a decoded Mixpanel Event with Properties/source information.
type MixpanelEvent struct {
	Pstart     time.Time         // the time that we started processing
	EventTime  json.Number       // the time that the server recieved the event
	UUID       string            // UUID of the event as assigned by the edge
	ClientIP   string            // the ipv4 of the client
	Event      string            // the type of the event
	UserAgent  string            // the user agent from an edge request
	Properties json.RawMessage   // the raw bytes of the json properties sub object
	Failure    reporter.FailMode // a flag for failure modes
}

// MakePanickedEvent returns an event inidicating a panic happened while parsing the event.
func MakePanickedEvent(line Parseable) *MixpanelEvent {
	return &MixpanelEvent{
		Pstart:     line.StartTime(),
		EventTime:  json.Number("0"),
		UUID:       "error",
		ClientIP:   "",
		Event:      "Unknown",
		Properties: json.RawMessage(line.Data()),
		Failure:    reporter.PanickedInProcessing,
	}
}

// MakeErrorEvent returns an event indicating an error happened while parsing the event.
func MakeErrorEvent(line Parseable, uuid string, when string) *MixpanelEvent {
	if uuid == "" || len(uuid) > 64 {
		uuid = "error"
	}
	if when == "" {
		when = "0"
	}
	if _, err := strconv.Atoi(when); err != nil {
		when = "0"
	}
	return &MixpanelEvent{
		Pstart:     line.StartTime(),
		EventTime:  json.Number(when),
		UUID:       uuid,
		ClientIP:   "",
		Event:      "Unknown",
		Properties: json.RawMessage{},
		Failure:    reporter.UnableToParseData,
	}
}
