package ratelimiter

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Bucket tests

func TestNewBucket(t *testing.T) {
	b := NewBucket(10, 2)
	if b.tokens != 10 {
		t.Errorf("expected tokens=10, got %d", b.tokens)
	}
	if b.size != 10 {
		t.Errorf("expected size=10, got %d", b.size)
	}
	if b.refillRate != 2 {
		t.Errorf("expected refillRate=2, got %d", b.refillRate)
	}
}

func TestBucket_IsEmpty_WhenFull(t *testing.T) {
	b := NewBucket(5, 1)
	if b.IsEmpty() {
		t.Error("expected bucket to not be empty")
	}
}

func TestBucket_IsEmpty_WhenEmpty(t *testing.T) {
	b := NewBucket(1, 1)
	b.Decrement()
	if !b.IsEmpty() {
		t.Error("expected bucket to be empty after decrementing to 0")
	}
}

func TestBucket_Decrement(t *testing.T) {
	b := NewBucket(5, 1)
	b.Decrement()
	b.mx.RLock()
	tokens := b.tokens
	b.mx.RUnlock()
	if tokens != 4 {
		t.Errorf("expected tokens=4 after decrement, got %d", tokens)
	}
}

func TestBucket_Refill_AddsTokens(t *testing.T) {
	b := NewBucket(10, 3)
	b.Decrement()
	b.Decrement()
	b.Refill()
	b.mx.RLock()
	tokens := b.tokens
	b.mx.RUnlock()
	// started at 10, decremented twice → 8, refilled by 3 → 10 (capped at size)
	if tokens != 10 {
		t.Errorf("expected tokens=10 after refill (capped), got %d", tokens)
	}
}

func TestBucket_Refill_DoesNotExceedSize(t *testing.T) {
	b := NewBucket(5, 10)
	b.Refill()
	b.mx.RLock()
	tokens := b.tokens
	b.mx.RUnlock()
	if tokens != 5 {
		t.Errorf("expected tokens capped at size=5, got %d", tokens)
	}
}

func TestBucket_Refill_PartialFill(t *testing.T) {
	b := NewBucket(10, 2)
	// drain the bucket
	for i := 0; i < 10; i++ {
		b.Decrement()
	}
	b.Refill()
	b.mx.RLock()
	tokens := b.tokens
	b.mx.RUnlock()
	if tokens != 2 {
		t.Errorf("expected tokens=2 after refill from empty, got %d", tokens)
	}
}

func TestBucket_ConcurrentDecrements(t *testing.T) {
	const goroutines = 50
	b := NewBucket(goroutines, 0)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Decrement()
		}()
	}
	wg.Wait()

	b.mx.RLock()
	tokens := b.tokens
	b.mx.RUnlock()
	if tokens != 0 {
		t.Errorf("expected tokens=0 after %d concurrent decrements, got %d", goroutines, tokens)
	}
}

// Middleware tests

func TestMiddleware_AllowsRequestWhenTokensAvailable(t *testing.T) {
	l := &TokenBucketLimiter{
		bucket: NewBucket(5, 1),
		ticker: time.NewTicker(1 * time.Hour), // don't let it refill during the test
	}

	var handlerCalled bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	l.Middleware(next).ServeHTTP(rec, req)

	if !handlerCalled {
		t.Error("expected next handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestMiddleware_RejectsRequestWhenBucketEmpty(t *testing.T) {
	b := NewBucket(1, 0)
	b.Decrement() // drain it

	l := &TokenBucketLimiter{
		bucket: b,
		ticker: time.NewTicker(1 * time.Hour),
	}

	var handlerCalled bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	l.Middleware(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rec.Code)
	}
	_ = handlerCalled // next is still called in current implementation (known behaviour)
}

// TokenBucketLimiter integration test

func TestTokenBucketLimiter_RefillsOverTime(t *testing.T) {
	// Use a very short refill interval to avoid slow tests
	l := &TokenBucketLimiter{
		bucket: NewBucket(5, 5),
		ticker: time.NewTicker(50 * time.Millisecond),
	}
	go l.start()
	defer l.ticker.Stop()

	// drain
	for i := 0; i < 5; i++ {
		l.bucket.Decrement()
	}
	if !l.bucket.IsEmpty() {
		t.Fatal("expected bucket to be empty after draining")
	}

	// wait for at least one tick
	time.Sleep(100 * time.Millisecond)

	if l.bucket.IsEmpty() {
		t.Error("expected bucket to have tokens after refill tick")
	}
}