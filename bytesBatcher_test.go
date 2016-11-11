package batcher

import (
	"bytes"
	"fmt"
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
			ok := bb.Push(func(b []byte) []byte {
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
		ok := bb.Push(func(b []byte) []byte {
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
		ok := bb.Push(func(b []byte) []byte {
			return append(b, "foobar"...)
		})
		if !ok {
			t.Fatalf("cannot push to batch on iteration %d", i)
		}
	}

	// this push must fail, since bb.BatchFunc is hanging
	if bb.Push(func(b []byte) []byte { return b }) {
		t.Fatalf("expecting failed push")
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
				if !bb.Push(func(b []byte) []byte { return append(b, "xxx"...) }) {
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
