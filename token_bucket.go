package ratelimiter

import (
	"net/http"
	"sync"
	"time"
)

type TokenBucketLimiter struct {
	bucket *Bucket
	ticker *time.Ticker
}

func NewTokenBucketLimiter(size, refillRate uint) *TokenBucketLimiter {
	l := &TokenBucketLimiter{
		bucket: NewBucket(size, refillRate),
		ticker: time.NewTicker(1 * time.Second),
	}

	go l.start()
	return l
}

func (l *TokenBucketLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if l.bucket.IsEmpty() {
				w.WriteHeader(http.StatusTooManyRequests)
			}
			l.bucket.Decrement()

			next.ServeHTTP(w, r)
		},
	)
}

func (l *TokenBucketLimiter) start() {
	for range l.ticker.C {
		l.bucket.Refill()
	}
}

type Bucket struct {
	tokens     uint
	size       uint
	refillRate uint
	mx         sync.RWMutex
}

func NewBucket(size, refillRate uint) *Bucket {
	return &Bucket{
		tokens:     size,
		size:       size,
		refillRate: refillRate,
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
	b.tokens = b.tokens + b.refillRate
	if b.tokens > b.size {
		b.tokens = b.size
	}
}
