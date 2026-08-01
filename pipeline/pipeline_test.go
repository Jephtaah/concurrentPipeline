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