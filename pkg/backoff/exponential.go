package backoff

import (
	mrand "math/rand/v2"
	"time"
)

type Exponential struct {
	current    time.Duration
	minBackoff time.Duration
	maxBackoff time.Duration
}

func NewExponential(minBackoff, maxBackoff time.Duration) *Exponential {
	return &Exponential{current: minBackoff, minBackoff: minBackoff, maxBackoff: maxBackoff}
}

func (b *Exponential) Wait() {
	jitterRange := max(1, int64(b.current/2))
	jitter := time.Duration(mrand.Int64N(jitterRange))
	time.Sleep(b.current + jitter)

	next := b.current * 2
	if next > b.maxBackoff {
		next = b.maxBackoff
	}

	b.current = next
}

func (b *Exponential) Reset() {
	b.current = b.minBackoff
}
