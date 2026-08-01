package pipeline

import (
	"context"
	"sync"
)

func StartWorkers(ctx context.Context, in <-chan Job, numWorkers int, agg *Aggregator) <-chan Result {
	out := make(chan Result)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for {
			select {
			case job, ok := <-in:
				if !ok {
					return
				}
				result := process(job)
				agg.Add(result)
				select {
				case out <- result:
				case <-ctx.Done():
					return
				}

			case <-ctx.Done():
				return
			}
		}
	}

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func process(job Job) Result {
	return Result{JobID: job.ID, Value: job.Value * job.Value}
}
