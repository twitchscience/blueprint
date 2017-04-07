package uploader

import (
	"log"
	"os"
	"sync"
)

var (
	debug = os.Getenv("debug")
)

type ErrorNotifierHarness interface {
	SendError(error)
}

type NotifierHarness interface {
	SendMessage(*UploadReceipt) error
}

type UploadRequest struct {
	Filename string
	FileType FileTypeHeader
	retry    int64
}

type UploadReceipt struct {
	Path    string
	KeyName string
	retry   int64
}

type UploaderPool struct {
	Pool          []Uploader
	Notifier      NotifierHarness
	ErrorNotifier ErrorNotifierHarness

	finishedUploading chan bool
	in                chan *UploadRequest
	out               chan *UploadReceipt
}

const UPLOAD_BUFFER_SIZE = 100

func StartUploaderPool(
	numWorkers int,
	errorNotifier ErrorNotifierHarness,
	notifier NotifierHarness,
	builder Factory,
) *UploaderPool {
	workers := make([]Uploader, numWorkers)
	in := make(chan *UploadRequest, UPLOAD_BUFFER_SIZE)
	out := make(chan *UploadReceipt, UPLOAD_BUFFER_SIZE)
	for i := 0; i < numWorkers; i++ {
		workers[i] = builder.NewUploader()
	}
	pool := &UploaderPool{
		Pool:              workers,
		Notifier:          notifier,
		ErrorNotifier:     errorNotifier,
		in:                in,
		out:               out,
		finishedUploading: make(chan bool),
	}
	go pool.Crank()
	return pool
}

func (p *UploaderPool) Upload(req *UploadRequest) {
	p.in <- req
}

func (p *UploaderPool) Close() {
	close(p.in)
	<-p.finishedUploading
}

func (p *UploaderPool) Crank() {
	w := &sync.WaitGroup{}
	log.Println("DEBUG=" + debug)
	for _, worker := range p.Pool {
		w.Add(1)

		worker := worker

		go func() {
			// Close() should cause the loggers to close thier channels
			for request := range p.in {
				if debug != "" {
					log.Println(request)
				}
				reciept, err := worker.Upload(request)
				if err != nil {
					p.ErrorNotifier.SendError(err)
				} else {
					p.out <- reciept
				}
			}

			// which will then decrement the job counter
			w.Done()
		}()
	}
	go func() {
		for reciept := range p.out {
			if debug != "" {
				log.Println(reciept)
			}
			err := p.Notifier.SendMessage(reciept)
			if err != nil {
				p.ErrorNotifier.SendError(err)
			}
		}
		// once the uploaders are drained tell the outside world
		p.finishedUploading <- true
	}()
	// when the jobs are finished then close the notify channel
	// this should cause the drain of the uploaders to be cleaned up appropriately.
	w.Wait()
	close(p.out)
}
