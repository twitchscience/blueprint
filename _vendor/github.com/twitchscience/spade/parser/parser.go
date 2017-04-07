package parser

import (
	"fmt"
	"time"
)

type parserEntry struct {
	name string
	p    Parser
}

// TODO: Don't use a global?
var parsers = []parserEntry{}

// Register makes Parsers available to parse lines. Each Parser should
// provide a mechanism to register themselves with this Registry. The
// order in which parsers are registered are the order in which
// they'll be called when attempting to parse a Parseable
func Register(name string, p Parser) error {
	if p == nil {
		return fmt.Errorf("parser: Register parser is nil")
	}
	for _, k := range parsers {
		if k.name == name {
			return fmt.Errorf("parser: Register called twice for parser: %s", name)
		}
	}
	parsers = append(parsers, parserEntry{
		name: name,
		p:    p,
	})
	return nil
}

type fanoutParser struct{}

func (f *fanoutParser) Parse(line Parseable) (events []MixpanelEvent, err error) {
	numParsers := len(parsers)
	if numParsers == 0 {
		return nil, fmt.Errorf("parser: no parsers registered")
	}

	for _, entry := range parsers {
		if mes, e := entry.p.Parse(line); e != nil && events == nil {
			events = mes
			err = e
		} else {
			events = mes
			err = e
			// return the first successful parse
			break
		}
	}
	return
}

// BuildSpadeParser returns a fanoutParser that uses the global parsers list.
func BuildSpadeParser() Parser {
	return &fanoutParser{}
}

// Parseable is a byte stream to be parsed associated with a time.
type Parseable interface {
	Data() []byte
	StartTime() time.Time
}

// Parser is an interface for turning Parseables into one or more MixpanelEvents.
type Parser interface {
	Parse(Parseable) ([]MixpanelEvent, error)
}

// URLEscaper is an interface for unescape URL query encoding in a byte stream.
type URLEscaper interface {
	QueryUnescape([]byte) ([]byte, error)
}

// ParseResult is a base 64-encoded byte array with a UUID and time attached.
type ParseResult interface {
	Data() []byte
	UUID() string
	Time() string
}
