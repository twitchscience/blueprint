package gzpool

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"io"

	"github.com/twitchscience/aws_utils/logger"
)

// GZPool is a pool of gzip.Writers you can Get and then Put back.
type GZPool struct {
	size int
	free *list.List
	get  chan chan *gzip.Writer
	put  chan *gzip.Writer
}

var emptyWriter bytes.Buffer

func (g *GZPool) crank() {
	for {
		select {
		case give := <-g.get:
			maybeFront := g.free.Front()
			if maybeFront == nil {
				g.size *= 2
				g.init(g.size)
				maybeFront = g.free.Front()
			}
			give <- g.free.Remove(maybeFront).(*gzip.Writer)
		case finished := <-g.put:
			finished.Reset(&emptyWriter)
			g.free.PushBack(finished)
		}
	}
}

// New returns a GZPool starting with the given size.
func New(size int) *GZPool {
	pool := &GZPool{
		size: size,
		free: list.New(),
		get:  make(chan chan *gzip.Writer),
		put:  make(chan *gzip.Writer),
	}
	pool.init(size)
	logger.Go(pool.crank)
	return pool
}

// Not thread safe. Be careful in usage
func (g *GZPool) init(size int) {
	for i := 0; i < size; i++ {
		g.free.PushBack(gzip.NewWriter(&emptyWriter))
	}
}

// Get returns a gzip.Writer from the GZPool.
func (g *GZPool) Get(w io.Writer) *gzip.Writer {
	receive := make(chan *gzip.Writer)
	g.get <- receive
	gzWriter := <-receive
	gzWriter.Reset(w)
	return gzWriter
}

// Put places the gzip.Writer back in the GZPool.
func (g *GZPool) Put(w *gzip.Writer) {
	g.put <- w
}
