package pipeline

import (
	"context"
)

func Generate(ctx context.Context, count int) <-chan Job {
	out := make(chan Job)

	go func() {
		defer close(out)
		for i := 0; i < count; i++ {
			job := Job{ID: i, Value: i}
			select {
			case out <- job:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
