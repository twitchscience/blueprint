package globber

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twitchscience/aws_utils/logger"
)

var (
	prefix         = '['
	separator      = ','
	postfix        = ']'
	version   byte = 1
)

// Complete is the type of a function that Globber will
// call for every completed glob
type Complete func([]byte)

// Config is used to configure a globber instance
type Config struct {
	// MaxSize is the max size per glob before compression
	MaxSize int

	// MaxAge is the max age of the oldest entry in the glob
	MaxAge string

	// BufferLength is the length of the channel where newly
	// submitted entries are stored, decreasing the size of this
	// buffer can cause stalls, and increasing the size can increase
	// shutdown time
	BufferLength int
}

// Validate returns an error if the config is invalid, nil otherwise.
func (c *Config) Validate() error {
	maxAge, err := time.ParseDuration(c.MaxAge)
	if err != nil {
		return err
	}

	if maxAge <= 0 {
		return errors.New("MaxAge must be a positive value")
	}

	if c.MaxSize <= 0 {
		return errors.New("MaxSize must be a positive value")
	}

	if c.BufferLength == 0 {
		return errors.New("BufferLength must be a positive value")
	}

	return nil
}

// A Globber is an object that will combine a bunch of json marshallable
// objects into compressed json array
type Globber struct {
	config     Config
	completor  Complete
	compressor *flate.Writer
	incoming   chan []byte
	pending    bytes.Buffer
	timer      *time.Timer
	maxAge     time.Duration

	sync.WaitGroup
}

// New returns a newly created instance of a Globber
func New(config Config, completor Complete) (*Globber, error) {
	err := config.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %s", err)
	}
	maxAge, err := time.ParseDuration(config.MaxAge)
	if err != nil {
		return nil, fmt.Errorf("config MaxAge failed parsing as a duration: %s", err)
	}

	g := &Globber{
		config:    config,
		completor: completor,
		maxAge:    maxAge,
		timer:     time.NewTimer(maxAge),
		incoming:  make(chan []byte, config.BufferLength),
	}

	g.Add(1)
	logger.Go(g.worker)
	return g, nil
}

// Submit submits an object for globbing
func (g *Globber) Submit(entry interface{}) error {
	e, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %v", err)
	}

	g.incoming <- e
	return nil
}

// Close stops the globbing process. Will return after all
// entries are flushed
func (g *Globber) Close() {
	close(g.incoming)
	g.Wait()
}

/* #nosec */
func (g *Globber) add(entry []byte) error {
	s := len(entry) + g.pending.Len()
	if s > g.config.MaxSize {
		if err := g.complete(); err != nil {
			return fmt.Errorf("error completing glob: %s", err)
		}
	}

	if g.pending.Len() == 0 {
		g.timer.Reset(g.maxAge)
		_, _ = g.pending.WriteRune(prefix)
	} else {
		_, _ = g.pending.WriteRune(separator)
	}
	_, _ = g.pending.Write(entry)
	return nil
}

func (g *Globber) complete() error {
	if g.pending.Len() == 0 {
		return nil
	}

	/* #nosec */
	_, _ = g.pending.WriteRune(postfix)
	err := g._complete()
	if err != nil {
		return fmt.Errorf("error compressing glob: %s", err)
	}
	return nil
}

func (g *Globber) _complete() error {
	var compressed bytes.Buffer
	var err error

	/* #nosec */
	_ = compressed.WriteByte(version)

	if g.compressor == nil {
		if g.compressor, err = flate.NewWriter(&compressed, flate.BestSpeed); err != nil {
			return err
		}
	} else {
		g.compressor.Reset(&compressed)
	}
	if _, err = g.compressor.Write(g.pending.Bytes()); err != nil {
		return err
	}

	if err = g.compressor.Close(); err != nil {
		return err
	}

	g.completor(compressed.Bytes())
	g.pending.Reset()
	return nil
}

// TODO: propagate errors here back to main thread so we can exit?
func (g *Globber) worker() {
	defer g.Done()
	defer func() {
		if err := g.complete(); err != nil {
			logger.WithError(err).Error("Failed to complete glob")
		}
	}()
	for {
		select {
		case <-g.timer.C:
			if err := g.complete(); err != nil {
				logger.WithError(err).Error("Failed to complete glob")
			}
		case e, ok := <-g.incoming:
			if !ok {
				return
			}
			if err := g.add(e); err != nil {
				logger.WithError(err).Error("Failed to add to glob")
			}
		}
	}
}
