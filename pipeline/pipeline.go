package pipeline

import (
	"context"
	"time"
)

type Config struct {
	JobCount   int
	NumWorkers int
	Timeout    time.Duration
}

func Run(cfg Config) (count int, sum int, completed bool) {
	ctx := context.Background()
	var cancel context.CancelFunc

	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	agg := &Aggregator{}

	jobs := Generate(ctx, cfg.JobCount)
	results := StartWorkers(ctx, jobs, cfg.NumWorkers, agg)

	done := make(chan struct{})
	go func() {
		for range results {
		}
		close(done)
	}()

	select {
	case <-done:
		completed = true
	case <-ctx.Done():
		completed = false
	}

	c, s := agg.Snapshot()
	return c, s, completed
}
