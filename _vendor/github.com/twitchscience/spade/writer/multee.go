package writer

import (
	"sync"

	"github.com/twitchscience/aws_utils/logger"
)

// Multee implements the `SpadeWriter` interface and forwards all calls
// to a slice of targets.
type Multee struct {
	// targets is the spadewriters we will Multee events to
	targets []SpadeWriter
	sync.RWMutex
}

// Add adds a new writer to the slice
func (t *Multee) Add(w SpadeWriter) {
	t.Lock()
	defer t.Unlock()
	t.targets = append(t.targets, w)
}

// AddMany adds muliple writers to the slice
func (t *Multee) AddMany(ws []SpadeWriter) {
	t.Lock()
	defer t.Unlock()
	t.targets = append(t.targets, ws...)
}

// Write forwards a writerequest to multiple targets
func (t *Multee) Write(r *WriteRequest) {
	t.RLock()
	defer t.RUnlock()

	for _, writer := range t.targets {
		writer.Write(r)
	}
}

// Rotate forwards a rotation request to multiple targets
func (t *Multee) Rotate() (bool, error) {
	t.RLock()
	defer t.RUnlock()

	allDone := true
	for i, writer := range t.targets {
		// losing errors here. Alternatives are to
		// not rotate writers further down the
		// chain, or to return an arbitrary error
		// out of all possible ones that occured
		done, err := writer.Rotate()
		if err != nil {
			logger.WithError(err).WithField("writer_index", i).Error("Failed to forward rotation request")
			allDone = false
		} else {
			allDone = allDone && done
		}
	}
	return allDone, nil
}

// Close closes all the target writers, it does this asynchronously
func (t *Multee) Close() error {
	t.Lock()
	defer t.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(t.targets))

	for idx, writer := range t.targets {
		w := writer
		i := idx
		// losing errors here. Alternative is to
		// return an arbitrary error out of all
		// possible ones that occured
		logger.Go(func() {
			defer wg.Done()
			err := w.Close()
			if err != nil {
				logger.WithError(err).
					WithField("writer_index", i).
					Error("Failed to forward rotation request")
			}
		})
	}

	wg.Wait()
	return nil
}
