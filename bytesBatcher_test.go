package batcher

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBytesBatcherTriggerMaxBatchSize(t *testing.T) {
	header := "foo"
	footer := "bar"
	expectedB := header + "0123456789" + footer
	loops := 2
	maxBatchSize := 10
	resultCh := make(chan error, loops)
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			var err error
			if string(b) != expectedB {
				err = fmt.Errorf("unexpected b: %q. Expecting %q", b, expectedB)
			}
			if items != maxBatchSize {
				err = fmt.Errorf("unexpected number of items: %d. Expecting %d", items, maxBatchSize)
			}
			resultCh <- err
		},

		HeaderFunc:   func(b []byte) []byte { return append(b, header...) },
		FooterFunc:   func(b []byte) []byte { return append(b, footer...) },
		MaxBatchSize: maxBatchSize,
		MaxDelay:     time.Hour,
	}

	for j := 0; j < loops; j++ {
		for i := 0; i < bb.MaxBatchSize; i++ {
			s := fmt.Sprintf("%d", i%bb.MaxBatchSize)
			ok := bb.Push(func(b []byte, _ int) []byte {
				return append(b, s...)
			})
			if !ok {
				t.Fatalf("cannot push to batch on iteration %d", i)
			}
		}

		select {
		case <-time.After(time.Second):
			t.Fatalf("timeout on loop %d", j)
		case err := <-resultCh:
			if err != nil {
				t.Fatalf("unexpected error on loop %d: %s", j, err)
			}
		}
	}
}

func TestBytesBatcherTriggerMaxDelay(t *testing.T) {
	header := "foo"
	footer := "bar"
	expectedB := header + "012345" + footer

	resultCh := make(chan error, 1)
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			var err error
			if string(b) != expectedB {
				err = fmt.Errorf("unexpected b: %q. Expecting %q", b, expectedB)
			}
			if items != 6 {
				err = fmt.Errorf("unexpected items: %d. Expecting %d", items, 6)
			}
			resultCh <- err
		},

		HeaderFunc:   func(b []byte) []byte { return append(b, header...) },
		FooterFunc:   func(b []byte) []byte { return append(b, footer...) },
		MaxBatchSize: 20,
		MaxDelay:     30 * time.Millisecond,
	}

	for i := 0; i < 6; i++ {
		s := fmt.Sprintf("%d", i)
		ok := bb.Push(func(b []byte, _ int) []byte {
			return append(b, s...)
		})
		if !ok {
			t.Fatalf("cannot push to batch on iteration %d", i)
		}
	}

	select {
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
}

func TestBytesBatcherTriggerPushOverflow(t *testing.T) {
	waitCh := make(chan struct{})
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			<-waitCh
		},

		MaxBatchSize: 10,
		MaxDelay:     time.Hour,
	}

	// The first batch should be sent, then the second batch should fail.
	for i := 0; i < 2*bb.MaxBatchSize; i++ {
		ok := bb.Push(func(b []byte, _ int) []byte {
			return append(b, "foobar"...)
		})
		if !ok {
			t.Fatalf("cannot push to batch on iteration %d", i)
		}
	}

	// this push must fail, since bb.BatchFunc is hanging
	if bb.Push(func(b []byte, _ int) []byte { return b }) {
		t.Fatalf("expecting failed push")
	}
}

func TestBytesBatcherTriggerMaxBatchBytesSize(t *testing.T) {
	type result struct {
		b     string
		items int
	}
	tests := []struct {
		name     string
		header   string
		footer   string
		maxBytes int
		maxItems int
		pushes   []string
		expected []result
	}{
		{
			name:     "maxBytes == len on 1st flush",
			maxBytes: 4,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "abcd", items: 2}, {b: "ef", items: 1}},
		},
		{
			name:     "maxBytes > len on 1st flush",
			maxBytes: 5,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "abcd", items: 2}, {b: "ef", items: 1}},
		},
		{
			name:     "1 char header, maxBytes == len on 1st flush",
			header:   "_",
			maxBytes: 5,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "_abcd", items: 2}, {b: "_ef", items: 1}},
		},
		{
			name:     "1 char header, maxBytes > len on 1st flush",
			header:   "_",
			maxBytes: 6,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "_abcd", items: 2}, {b: "_ef", items: 1}},
		},
		{
			name:     "2 char header, maxBytes == len on 1st flush",
			header:   "__",
			maxBytes: 6,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "__abcd", items: 2}, {b: "__ef", items: 1}},
		},
		{
			name:     "2 char header, maxBytes > len on 1st flush",
			header:   "__",
			maxBytes: 7,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "__abcd", items: 2}, {b: "__ef", items: 1}},
		},
		{
			name:     "1 char footer, maxBytes == len on 1st flush",
			footer:   ";",
			maxBytes: 5,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "abcd;", items: 2}, {b: "ef;", items: 1}},
		},
		{
			name:     "1 char footer, maxBytes > len on 1st flush",
			footer:   ";",
			maxBytes: 6,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "abcd;", items: 2}, {b: "ef;", items: 1}},
		},
		{
			name:     "2 char footer, maxBytes == len on 1st flush",
			footer:   ";;",
			maxBytes: 6,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "abcd;;", items: 2}, {b: "ef;;", items: 1}},
		},
		{
			name:     "2 char footer, maxBytes > len on 1st flush",
			footer:   ";;",
			maxBytes: 7,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "abcd;;", items: 2}, {b: "ef;;", items: 1}},
		},
		{
			name:     "1 char header and footer, maxBytes == len on 1st flush",
			header:   "_",
			footer:   ";",
			maxBytes: 6,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "_abcd;", items: 2}, {b: "_ef;", items: 1}},
		},
		{
			name:     "1 char header and footer, maxBytes > len on 1st flush",
			header:   "_",
			footer:   ";",
			maxBytes: 7,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "_abcd;", items: 2}, {b: "_ef;", items: 1}},
		},
		{
			name:     "multi-char header and footer, maxBytes == len on 1st flush",
			header:   "___",
			footer:   ";;;",
			maxBytes: 10,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "___abcd;;;", items: 2}, {b: "___ef;;;", items: 1}},
		},
		{
			name:     "multi-char header and footer, maxBytes > len on 1st flush",
			header:   "___",
			footer:   ";;;",
			maxBytes: 11,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "___abcd;;;", items: 2}, {b: "___ef;;;", items: 1}},
		},
		{
			name:     "maxBytes < len should process at least 1 element",
			header:   "___",
			footer:   ";;;",
			maxBytes: 1,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "___ab;;;", items: 1}, {b: "___cd;;;", items: 1}, {b: "___ef;;;", items: 1}},
		},
		{
			name:     "MaxBatchSize should also work when maxBytes is provided",
			header:   "_",
			footer:   ";",
			maxBytes: 20,
			maxItems: 2,
			pushes:   []string{"ab", "cd", "ef"},
			expected: []result{{b: "_abcd;", items: 2}, {b: "_ef;", items: 1}},
		},
	}

	for _, tt := range tests {
		tt := tt
		var name string
		if tt.header != "" {
			name += "header=" + tt.header + ","
		}
		if tt.footer != "" {
			name += "footer=" + tt.footer + ","
		}
		name += fmt.Sprintf("maxBytes=%d,pushes=", tt.maxBytes)

		t.Run(tt.name, func(t *testing.T) {
			resultCh := make(chan result, len(tt.expected))
			maxItems := tt.maxItems
			if maxItems == 0 {
				maxItems = len(tt.pushes) + 1
			}
			bb := &BytesBatcher{
				BatchFunc: func(b []byte, items int) {
					resultCh <- result{b: string(b), items: items}
				},
				HeaderFunc: func(b []byte) []byte {
					return append(b, tt.header...)
				},
				FooterFunc: func(b []byte) []byte {
					return append(b, tt.footer...)
				},
				MaxBatchSize:      maxItems,
				MaxBatchBytesSize: tt.maxBytes,
				MaxDelay:          time.Hour,
			}

			for _, s := range tt.pushes {
				ok := bb.Push(func(b []byte, _ int) []byte {
					return append(b, s...)
				})
				if !ok {
					t.Fatalf("cannot push %q", s)
				}
				time.Sleep(1 * time.Millisecond)
			}

			for i, expected := range tt.expected {
				if i == len(tt.expected)-1 {
					if !bb.Flush() {
						t.Fatalf("flush failed")
					}
				}

				select {
				case <-time.After(time.Second):
					t.Fatalf("timeout waiting for the batch")
				case got := <-resultCh:
					if got.b != expected.b {
						t.Fatalf("unexpected batch payload: %q; want %q", got.b, expected.b)
					}
					if got.items != expected.items {
						t.Fatalf("unexpected batch items: %d; want %d", got.items, expected.items)
					}
				}
			}
		})
	}
}

func TestBytesBatcherConcurrent(t *testing.T) {
	header := "foo"
	footer := "bar"
	maxBatchSize := 100
	batchesCount := 10
	batchCh := make(chan error, batchesCount)
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			var err error
			if !bytes.HasPrefix(b, []byte(header)) {
				err = fmt.Errorf("unexpected batch prefix: %q. Expecting %q", b[:3], header)
			} else if !bytes.HasSuffix(b, []byte(footer)) {
				err = fmt.Errorf("unexpected batch suffix: %q. Expecting %q", b[len(b)-3:], footer)
			} else if bytes.Index(b, []byte("xxx")) < 0 {
				err = fmt.Errorf("cannot find %q inside batch %q", "xxx", b)
			} else if items > maxBatchSize {
				err = fmt.Errorf("items shouldn't exceed %d. Current value: %d", maxBatchSize, items)
			} else if items <= 0 {
				err = fmt.Errorf("items must be positive. Current value: %d", items)
			}
			batchCh <- err
		},
		HeaderFunc:   func(b []byte) []byte { return append(b, header...) },
		FooterFunc:   func(b []byte) []byte { return append(b, footer...) },
		MaxBatchSize: maxBatchSize,
		MaxDelay:     20 * time.Millisecond,
	}

	workersCount := 20
	iterationsCount := maxBatchSize * batchesCount / workersCount
	resultCh := make(chan error, workersCount)
	for i := 0; i < workersCount; i++ {
		go func(i int) {
			var err error
			for j := 0; j < iterationsCount; j++ {
				if !bb.Push(func(b []byte, _ int) []byte { return append(b, "xxx"...) }) {
					err = fmt.Errorf("cannot push to batch from worker %d on iteration %d", i, j)
					break
				}
				time.Sleep(time.Millisecond)
			}
			resultCh <- err
		}(i)
	}

	for i := 0; i < workersCount; i++ {
		select {
		case <-time.After(time.Second):
			t.Fatalf("timeout when waiting for worker %d", i)
		case err := <-resultCh:
			if err != nil {
				t.Fatalf("unexpected error from worker %d: %s", i, err)
			}
		}
	}

	for i := 0; i < batchesCount; i++ {
		select {
		case <-time.After(time.Second):
			t.Fatalf("timeout when waiting for batch func %d", i)
		case err := <-batchCh:
			if err != nil {
				t.Fatalf("unexpected error from batch func %d: %s", i, err)
			}
		}
	}
}

func TestBytesBatcherSwapsBuffersInsteadOfCopying(t *testing.T) {
	var (
		mu    sync.Mutex
		addrs = map[*byte]bool{}
	)
	done := make(chan struct{}, 8)
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			mu.Lock()
			addrs[&b[:1][0]] = true
			mu.Unlock()
			done <- struct{}{}
		},
		MaxBatchSize: 3,
		MaxDelay:     time.Hour,
	}

	for range 8 {
		for range 3 {
			bb.Push(func(b []byte, _ int) []byte { return append(b, "row"...) })
		}
		<-done
		// let the batch's goroutine give its buffer back before the next one
		waitFor(t, func() bool { bb.lock.Lock(); defer bb.lock.Unlock(); return !bb.pending })
	}

	mu.Lock()
	defer mu.Unlock()
	if len(addrs) != 2 {
		t.Fatalf("8 batches were sent from %d distinct buffers; want the same 2 swapped", len(addrs))
	}
}

// TestBytesBatcherBytesOverflowWhilePending checks that a row that does not
// fit while the previous batch is still in BatchFunc stays in the current
// batch - past the limit, with a single header - rather than being dropped or
// starting a second header mid-batch.
func TestBytesBatcherBytesOverflowWhilePending(t *testing.T) {
	release := make(chan struct{})
	batches := make(chan string, 4)
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			if string(b) == "Hab" {
				<-release
			}
			batches <- string(b)
		},
		HeaderFunc:        func(b []byte) []byte { return append(b, 'H') },
		MaxBatchSize:      100,
		MaxBatchBytesSize: 3,
		MaxDelay:          time.Hour,
	}

	push := func(s string) {
		if !bb.Push(func(b []byte, _ int) []byte { return append(b, s...) }) {
			t.Fatalf("push %q refused", s)
		}
	}

	push("ab") // "Hab" fills the limit: sent, and blocks in BatchFunc
	push("cd") // "Hcd" fills it too, but cannot be sent while "Hab" is pending
	push("ef") // does not fit, cannot be sent: stays in the batch

	close(release)
	if got := <-batches; got != "Hab" {
		t.Fatalf("first batch %q", got)
	}

	if !bb.Flush() {
		t.Fatal("flush failed")
	}
	if got := <-batches; got != "Hcdef" {
		t.Fatalf("second batch %q; want the pending rows after one header", got)
	}
}

func TestBytesBatcherStopWaitsAndSendsTheLastBatch(t *testing.T) {
	release := make(chan struct{})
	var (
		mu  sync.Mutex
		got []string
	)
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			if string(b) == "aa" {
				<-release
			}
			if items == 0 {
				return
			}
			mu.Lock()
			got = append(got, string(b))
			mu.Unlock()
		},
		MaxBatchSize: 2,
		MaxDelay:     time.Hour,
	}

	for _, s := range []string{"a", "a", "b"} {
		bb.Push(func(b []byte, _ int) []byte { return append(b, s...) })
	}

	stopped := make(chan struct{})
	go func() {
		bb.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned while a batch was still in BatchFunc")
	case <-time.After(30 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != "aa" || got[1] != "b" {
		t.Fatalf("batches %q; want the pending one and the last one", got)
	}

	if bb.Push(func(b []byte, _ int) []byte { return b }) {
		t.Fatal("push after Stop accepted")
	}
}

// TestBytesBatcherFlushDoesNotBlockPushes: while Flush waits for a pending
// batch, other goroutines can still push.
func TestBytesBatcherFlushDoesNotBlockPushes(t *testing.T) {
	release := make(chan struct{})
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			if string(b) == "xx" {
				<-release
			}
		},
		MaxBatchSize: 2,
		MaxDelay:     time.Hour,
	}
	defer close(release)

	bb.Push(func(b []byte, _ int) []byte { return append(b, 'x') })
	bb.Push(func(b []byte, _ int) []byte { return append(b, 'x') }) // sent, blocks
	bb.Push(func(b []byte, _ int) []byte { return append(b, 'y') })

	go bb.Flush() // waits for "xx"

	time.Sleep(20 * time.Millisecond)

	pushed := make(chan struct{})
	go func() {
		bb.Push(func(b []byte, _ int) []byte { return append(b, 'z') })
		close(pushed)
	}()

	select {
	case <-pushed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Push blocked behind Flush waiting for a pending batch")
	}
}

// TestBytesBatcherParallelSendDoesNotShareBuffer checks that a batch handed to
// BatchFunc in its own goroutine is not written to by later Push calls while
// BatchFunc is still reading it.
//
// An empty batch (rows that append nothing) has no bytes, but its BatchFunc is
// running all the same: the next batch must not be sent until it returns.
// Otherwise two goroutines are in flight with one pendingB between them, the
// empty one's goroutine finishes and marks the other's buffer free, and Push
// writes into it while its BatchFunc is still reading.
func TestBytesBatcherParallelSendDoesNotShareBuffer(t *testing.T) {
	var (
		calls    atomic.Int32
		started1 = make(chan struct{})
		release1 = make(chan struct{})
		got2     = make(chan string, 1)
		early    = make(chan struct{}, 1)
	)
	bb := &BytesBatcher{
		BatchFunc: func(b []byte, items int) {
			switch calls.Add(1) {
			case 1:
				close(started1)
				<-release1
			case 2:
				// a second call before the first returned
				select {
				case <-release1:
				default:
					early <- struct{}{}
				}
				got2 <- string(b)
			}
		},
		MaxBatchSize: 1,
		MaxDelay:     time.Hour,
	}
	defer bb.Stop()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(release1) }) }
	defer release() // runs before Stop, so a failed check does not hang it

	push := func(s string) bool {
		return bb.Push(func(b []byte, _ int) []byte { return append(b, s...) })
	}

	if !push("") { // empty batch: its BatchFunc blocks
		t.Fatal("push of the empty batch refused")
	}
	<-started1

	// the empty batch is still in BatchFunc, so "abc" must wait for it
	push("abc")
	select {
	case <-early:
		t.Fatal("BatchFunc called while the previous batch was still in it")
	case <-time.After(50 * time.Millisecond):
	}

	release()
	waitFor(t, func() bool { bb.lock.Lock(); defer bb.lock.Unlock(); return !bb.pending })

	if !bb.Flush() {
		t.Fatal("flush failed")
	}
	if got := <-got2; got != "abc" {
		t.Fatalf("BatchFunc saw %q; want %q", got, "abc")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkBytesBatcherPushParallel(b *testing.B) {
	bb := &BytesBatcher{
		BatchFunc:    func([]byte, int) {},
		MaxBatchSize: 100_000,
		MaxDelay:     time.Hour,
	}
	row := bytes.Repeat([]byte("x"), 512)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Push(func(dst []byte, _ int) []byte { return append(dst, row...) })
		}
	})
}
