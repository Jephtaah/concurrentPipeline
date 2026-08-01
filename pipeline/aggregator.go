package pipeline

import (
	"sync"
)

type Aggregator struct {
	mu sync.Mutex
	count int
	sum int
}

func (a *Aggregator) Add(r Result) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.count++
	a.sum += r.Value
}

func (a *Aggregator) Snapshot() (count int, sum int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.count, a.sum
}