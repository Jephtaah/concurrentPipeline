package pipeline

import "testing"

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