package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestPipelineCorrectness(t *testing.T) {
	workerCounts := []int{1, 4, 16}
	jobCount := 5000

	for _, w := range workerCounts {
		count, _, completed := Run(Config{
			JobCount:   jobCount,
			NumWorkers: w,
		})

		if !completed {
			t.Fatalf("workers=%d: pipeline did not complete", w)
		}
		if count != jobCount {
			t.Fatalf("workers=%d: expected %d results, got %d", w, jobCount, count)
		}
	}
}

func benchmarkPipeline(b *testing.B, workers int) {
	for i := 0; i < b.N; i++ {
		Run(Config{
			JobCount:   10000,
			NumWorkers: workers,
		})
	}
}

func BenchmarkPipeline1Worker(b *testing.B)   { benchmarkPipeline(b, 1) }
func BenchmarkPipeline4Workers(b *testing.B)  { benchmarkPipeline(b, 4) }
func BenchmarkPipeline16Workers(b *testing.B) { benchmarkPipeline(b, 16) }

func benchmarkPipelineSlow(b *testing.B, workers int) {
	for i := 0; i < b.N; i++ {
		runSlow(Config{
			JobCount:   200,
			NumWorkers: workers,
		})
	}
}

func BenchmarkPipelineSlow1Worker(b *testing.B)   { benchmarkPipelineSlow(b, 1) }
func BenchmarkPipelineSlow4Workers(b *testing.B)  { benchmarkPipelineSlow(b, 4) }
func BenchmarkPipelineSlow8Workers(b *testing.B)  { benchmarkPipelineSlow(b, 8) }
func BenchmarkPipelineSlow16Workers(b *testing.B) { benchmarkPipelineSlow(b, 16) }

func TestWorkersWithRetryRecoverFromFailures(t *testing.T) {
	ctx := context.Background()
	jobCount := 500

	jobs := Generate(ctx, jobCount)
	agg := &Aggregator{}
	results := StartWorkersWithRetry(ctx, jobs, 4, agg)

	got := 0
	for range results {
		got++
	}

	if got < jobCount-5 {
		t.Fatalf("expected nearly all %d jobs to succeed via retry, got %d", jobCount, got)
	}
}

func TestPauserActuallyBlocksWorkers(t *testing.T) {
	ctx := context.Background()
	pauser := NewPauser()
	pauser.Pause() // start paused

	jobs := Generate(ctx, 5)
	agg := &Aggregator{}
	results := StartWorkersPausable(ctx, jobs, 2, agg, pauser)

	done := make(chan struct{})
	go func() {
		for range results {
		}
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("pipeline completed while paused — pause is not actually blocking workers")
	case <-time.After(200 * time.Millisecond):
	}

	pauser.Resume()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not complete after Resume — workers stuck")
	}

	count, _ := agg.Snapshot()
	if count != 5 {
		t.Fatalf("expected 5 results after resume, got %d", count)
	}
}
