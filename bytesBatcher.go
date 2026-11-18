package batcher

import (
	"sync"
	"time"
)

// BytesBatcher consructs a byte slice on every Push call and calls BatchFunc
// on every MaxBatchSize Push calls or MaxDelay interval.
//
// See also Batcher.
type BytesBatcher struct {
	// BatchFunc is called when either MaxBatchSize or MaxDelay is reached.
	//
	//   * b contains a byte slice constructed when Push is called.
	//   * items contains the number of Push calls used for constructing b.
	//
	// BytesBatcher prevents calling BatchFunc from concurrently running
	// goroutines.
	//
	// b mustn't be accessed after returning from BatchFunc.
	BatchFunc func(b []byte, items int)

	// HeaderFunc is called before starting new batch.
	//
	// HeaderFunc must append header data to dst and return the resulting
	// byte slice.
	//
	// dst mustn't be accessed after returning from HeaderFunc.
	//
	// HeaderFunc may be nil.
	HeaderFunc func(dst []byte) []byte

	// FooterFunc is called before the batch is passed to BatchFunc.
	//
	// FooterFunc must append footer data to dst and return the resulting
	// byte slice.
	//
	// dst mustn't be accessed after returning from FooterFunc.
	//
	// FooterFunc may be nil.
	FooterFunc func(dst []byte) []byte

	// MaxBatchSize the maximum batch size.
	MaxBatchSize int

	// MaxBatchBytesSize specifies the maximum size in bytes of a batch passed to
	// BatchFunc, header and footer included. A row that would take the batch
	// past it starts the next batch instead; a single row larger than the limit
	// is sent as a batch of its own. While the previous batch is still in
	// BatchFunc the current one cannot be sent and grows past the limit rather
	// than lose rows. Zero means no limit.
	MaxBatchBytesSize int

	// MaxDelay is the maximum duration before BatchFunc is called
	// unless MaxBatchSize is reached.
	MaxDelay time.Duration

	stopped bool
	once    sync.Once
	lock    sync.Mutex

	// b is the batch being collected. pendingB is the batch BatchFunc is
	// processing while pending is set, and the spare buffer the next batch
	// starts in once it is not: the two buffers swap on every send, so a
	// batch is never copied - under the lock or anywhere else.
	b        []byte
	pendingB []byte

	// pending is set while a batch sent in its own goroutine is in BatchFunc.
	// It is a flag of its own rather than len(pendingB) > 0, since an empty
	// batch is still being processed.
	pending bool

	// overflowB holds a row moved to the next batch while the current one is
	// sent; a buffer of its own, since the footer appended to the batch would
	// otherwise write over it.
	overflowB []byte

	items        int
	lastExecTime time.Time

	// inFlight counts the BatchFunc calls running in their own goroutines, so
	// Stop can wait for them.
	inFlight sync.WaitGroup
}

// Stop sends the batch collected so far and refuses every later Push. It waits
// for a batch still in BatchFunc first, so the last one is not lost to it, and
// returns once BatchFunc has returned for every batch.
func (b *BytesBatcher) Stop() {
	b.lock.Lock()
	b.stopped = true
	b.lock.Unlock()

	// No new asynchronous send can start once stopped is set: Push refuses,
	// and the delay timer exits.
	b.inFlight.Wait()

	b.lock.Lock()
	b.execNolock(false)
	b.lock.Unlock()
}

// Push calls appendFunc on a byte slice.
//
// appendFunc must append data to dst and return the resulting byte slice.
// dst mustn't be accessed after returning from appendFunc.
//
// The function returns false if the batch reached MaxBatchSize and BatchFunc
// isn't returned yet.
func (b *BytesBatcher) Push(appendFunc func(dst []byte, rows int) []byte) bool {
	b.once.Do(b.init)
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.stopped {
		return false
	}
	if b.items >= b.MaxBatchSize && !b.execNolock(true) {
		return false
	}
	if b.items == 0 {
		b.writeHeader()
	}

	sizeBefore := len(b.b)
	b.b = appendFunc(b.b, b.items)
	b.items++

	if b.MaxBatchBytesSize > 0 {
		b.limitBytesNolock(sizeBefore)
		return true
	}

	if b.items >= b.MaxBatchSize {
		b.execNolockNocheck()
	}

	return true
}

// limitBytesNolock keeps the batch within MaxBatchBytesSize after a row was
// appended at sizeBefore.
func (b *BytesBatcher) limitBytesNolock(sizeBefore int) {
	size := len(b.b) + b.footerSize()

	switch {
	case size == b.MaxBatchBytesSize, size > b.MaxBatchBytesSize && b.items == 1:
		// full with this row, or a single row over the limit: send as is
		b.execNolockNocheck()

	case size > b.MaxBatchBytesSize:
		// the row does not fit: send the batch without it, start the next
		// with it
		b.overflowB = append(b.overflowB[:0], b.b[sizeBefore:]...)
		b.b = b.b[:sizeBefore]
		b.items--

		if !b.execNolock(true) {
			// the previous batch is still in BatchFunc: keep the row here
			// and let this batch run over rather than drop it
			b.b = append(b.b, b.overflowB...)
			b.items++
			return
		}

		b.writeHeader()
		b.b = append(b.b, b.overflowB...)
		b.items++

		if len(b.b)+b.footerSize() >= b.MaxBatchBytesSize {
			// the moved row fills the new batch on its own
			b.execNolockNocheck()
		}

	case b.items >= b.MaxBatchSize:
		b.execNolockNocheck()
	}
}

// footerSize is how many bytes FooterFunc would add to the current batch. It
// is measured by appending the footer past the end of the batch and dropping
// it: the batch itself is untouched.
func (b *BytesBatcher) footerSize() int {
	if b.FooterFunc == nil {
		return 0
	}
	n := len(b.b)
	b.b = b.FooterFunc(b.b)
	size := len(b.b) - n
	b.b = b.b[:n]
	return size
}

// Flush triggers immediate processing of all accumulated items in the current batch.
func (b *BytesBatcher) Flush() (ok bool) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.items <= 0 {
		return true
	}

	for i := 0; i < 10; i++ {
		if b.execNolock(false) {
			return true
		}

		// the previous batch is still in BatchFunc, which needs the lock to
		// finish
		b.lock.Unlock()
		time.Sleep(time.Second)
		b.lock.Lock()

		if b.items <= 0 {
			return true
		}
	}

	return false
}

func (b *BytesBatcher) init() {
	go func() {
		maxDelay := b.MaxDelay
		delay := maxDelay
		for {
			time.Sleep(delay)
			b.lock.Lock()
			if b.stopped {
				b.lock.Unlock()
				return
			}
			d := time.Since(b.lastExecTime)
			if float64(d) > 0.9*float64(maxDelay) {
				if b.items > 0 {
					b.execNolockNocheck()
				}
				delay = maxDelay
			} else {
				delay = maxDelay - d
			}
			b.lock.Unlock()
		}
	}()
}

func (b *BytesBatcher) execNolockNocheck() {
	// Do not check the returned value, since the previous batch
	// may be still pending in BatchFunc.
	// The error will be discovered on the next Push.
	b.execNolock(true)
}

// execNolock hands the batch to BatchFunc - in a goroutine of its own when
// parallel is set - and starts the next one in the spare buffer. It returns
// false, sending nothing, while the previous batch is still in BatchFunc.
//
// The batch is swapped out, not copied: the lock is held for a constant time
// whatever the batch's size, and no batch-sized buffer is allocated per send.
func (b *BytesBatcher) execNolock(parallel bool) bool {
	if b.pending {
		return false
	}

	b.writeFooter()

	data := b.b
	b.b = b.pendingB[:0]
	b.pendingB = data

	items := b.items
	b.items = 0
	b.lastExecTime = time.Now()

	if !parallel {
		b.BatchFunc(data, items)
		b.pendingB = b.pendingB[:0]
		return true
	}

	b.pending = true
	b.inFlight.Add(1)
	go func() {
		defer b.inFlight.Done()

		b.BatchFunc(data, items)

		// BatchFunc has returned, so the buffer is free to collect the
		// batch after next
		b.lock.Lock()
		b.pendingB = data[:0]
		b.pending = false
		b.lock.Unlock()
	}()

	return true
}

func (b *BytesBatcher) writeHeader() {
	if b.HeaderFunc != nil {
		b.b = b.HeaderFunc(b.b)
	}
}

func (b *BytesBatcher) writeFooter() {
	if b.FooterFunc != nil {
		b.b = b.FooterFunc(b.b)
	}
}
