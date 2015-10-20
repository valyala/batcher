// Package batcher groups items in batches
// and calls the user-specified function on these batches.
package batcher

import (
	"time"
)

// Batcher groups items in batches and calls BatcherFunc on them.
type Batcher struct {
	// Func is called by Batcher when batch is ready to be processed.
	Func BatcherFunc

	// Maximum batch size that will be passed to BatcherFunc.
	MaxBatchSize int

	// Maximum delay between Push() and BatcherFunc call.
	MaxDelay time.Duration

	// Maximum unprocessed items' queue size.
	QueueSize int

	ch     chan interface{}
	doneCh chan struct{}
}

// BatcherFunc is called by Batcher when batch is ready to be processed.
//
// BatcherFunc must process the given batch before returning.
// It must not hold references to the batch after returning.
type BatcherFunc func(batch []interface{})

// Start starts batch processing.
func (b *Batcher) Start() {
	if b.ch != nil {
		panic("batcher already started")
	}
	if b.Func == nil {
		panic("Batcher.Func must be set")
	}

	if b.QueueSize <= 0 {
		b.QueueSize = 8 * 1024
	}
	if b.MaxBatchSize <= 0 {
		b.MaxBatchSize = 64 * 1024
	}
	if b.MaxDelay <= 0 {
		b.MaxDelay = time.Millisecond
	}

	b.ch = make(chan interface{}, b.QueueSize)
	b.doneCh = make(chan struct{})
	go func() {
		processBatches(b.Func, b.ch, b.MaxBatchSize, b.MaxDelay)
		close(b.doneCh)
	}()
}

// Stop stops batch processing.
func (b *Batcher) Stop() {
	if b.ch == nil {
		panic("BUG: forgot calling Batcher.Start()?")
	}
	close(b.ch)
	<-b.doneCh
	b.ch = nil
	b.doneCh = nil
}

// Push pushes new item into the batcher.
//
// Don't forget calling Start() before pushing items into the batcher.
func (b *Batcher) Push(x interface{}) bool {
	if b.ch == nil {
		panic("BUG: forgot calling Batcher.Start()?")
	}
	select {
	case b.ch <- x:
		return true
	default:
		return false
	}
}

func processBatches(f BatcherFunc, ch <-chan interface{}, maxBatchSize int, maxDelay time.Duration) {
	var batch []interface{}
	var x interface{}
	var ok bool
	lastPushTime := time.Now()
	for {
		select {
		case x, ok = <-ch:
			if !ok {
				call(f, batch)
				return
			}
			batch = append(batch, x)
		default:
			if len(batch) == 0 {
				x, ok = <-ch
				if !ok {
					call(f, batch)
					return
				}
				batch = append(batch, x)
			} else {
				if delay := maxDelay - time.Since(lastPushTime); delay > 0 {
					t := acquireTimer(delay)
					select {
					case x, ok = <-ch:
						if !ok {
							call(f, batch)
							return
						}
						batch = append(batch, x)
					case <-t.C:
					}
					releaseTimer(t)
				}
			}
		}

		if len(batch) >= maxBatchSize || time.Since(lastPushTime) > maxDelay {
			lastPushTime = time.Now()
			call(f, batch)
			batch = batch[:0]
		}
	}
}

func call(f BatcherFunc, batch []interface{}) {
	if len(batch) > 0 {
		f(batch)
	}
}
