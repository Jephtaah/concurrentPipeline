package pipeline

import (
	"context"
	"errors"
	"testing"
)

func TestProcessWithRetryEventuallySucceeds(t *testing.T) {
	attempts := 0
	fn := func(job Job) (Result, error) {
		attempts++
		if attempts < 3 {
			return Result{}, errors.New("fail")
		}
		return Result{JobID: job.ID, Value: 42}, nil
	}

	result, err := processWithRetry(context.Background(), Job{ID: 1}, fn, 5, 1)
	if err != nil {
		t.Fatalf("expected eventual success, got error: %v", err)
	}
	if result.Value != 42 {
		t.Fatalf("expected value 42, got %d", result.Value)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestProcessWithRetryExhaustsAttempts(t *testing.T) {
	attempts := 0
	fn := func(job Job) (Result, error) {
		attempts++
		return Result{}, errors.New("always fails")
	}

	_, err := processWithRetry(context.Background(), Job{ID: 1}, fn, 3, 1)
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if attempts != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", attempts)
	}
}
