package pipeline

import (
	"context"
	"time"
)

func RateLimit(ctx context.Context, in <-chan Job, interval time.Duration) <-chan Job {
	out := make(chan Job)
	ticker := time.NewTicker(interval)

	go func() {
		defer close(out)
		defer ticker.Stop()

		for {
			select {
			case job, ok := <-in:
				if !ok {
					return
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					return
				}
				select {
				case out <- job:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
