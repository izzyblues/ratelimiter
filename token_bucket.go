package ratelimiter

import (
	"net/http"
	"sync"
	"time"
)

func TokenBucketLimiterMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {

			next.ServeHTTP(w, r)
		},
	)
}

type TokenBucketLimiter struct {
	next   http.Handler
	bucket *Bucket
	ticker *time.Ticker
}

func NewTokenBucketLimiter(next http.Handler, capacity uint) *TokenBucketLimiter {
	l := &TokenBucketLimiter{
		next:   next,
		bucket: NewBucket(capacity),
		ticker: time.NewTicker(1 * time.Second),
	}

	l.start()
	return l
}

func (l *TokenBucketLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if l.bucket.IsEmpty() {
		w.WriteHeader(http.StatusTooManyRequests)
	}
	l.bucket.Decrement()

	l.next.ServeHTTP(w, r)
}

func (l *TokenBucketLimiter) start() {
	for range l.ticker.C {
		l.bucket.Refill()
	}
}

type Bucket struct {
	tokens   uint
	capacity uint
	mx       sync.RWMutex
}

func NewBucket(capacity uint) *Bucket {
	return &Bucket{
		tokens:   capacity,
		capacity: capacity,
	}
}

func (b *Bucket) Decrement() {
	b.mx.Lock()
	defer b.mx.Unlock()
	b.tokens--
}

func (b *Bucket) IsEmpty() bool {
	b.mx.RLock()
	defer b.mx.RUnlock()
	return b.tokens == 0
}

func (b *Bucket) Refill() {
	b.mx.Lock()
	defer b.mx.Unlock()
	b.tokens = b.capacity
}
