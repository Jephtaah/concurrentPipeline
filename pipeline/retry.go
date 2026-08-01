package pipeline

import (
	"context"
	"time"
)

	
func processWithRetry(ctx context.Context, job Job, fn func(Job) (Result, error), maxAttempts int, base time.Duration) (Result, error) {
	var lastErr error
	delay := base

	for attempt := 0; attempt < maxAttempts; attempt++ {
		result, err := fn(job)
		if err == nil {
			return result, nil
		}
		lastErr = err

		if attempt == maxAttempts-1 {
			break
		}

		select {
		case <-time.After(delay):
			delay *= 2
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}

	return Result{}, lastErr
}