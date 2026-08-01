package pipeline

import (
	"context"
	"sync"
	"time"
	"errors"
	"math/rand"
)

var errTransient = errors.New("transient processing failure")

func processFlaky(job Job) (Result, error) {
	if rand.Intn(4) == 0 {
		return Result{}, errTransient
	}

	return Result{JobID: job.ID, Value: job.Value * job.Value}, nil
}

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

func startWorkersSlow(ctx context.Context, in <-chan Job, numWorkers int, agg *Aggregator) <-chan Result {
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
				result := processSlow(job)
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

func processSlow(job Job) Result {
	time.Sleep(200 * time.Microsecond)
	return Result{JobID: job.ID, Value: job.Value * job.Value}
}

func StartWorkersWithRetry(ctx context.Context, in <-chan Job, numWorkers int, agg *Aggregator) <-chan Result {
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
				result, err := processWithRetry(ctx, job, processFlaky, 5, 10*time.Millisecond)
				if err != nil {
					continue
				}
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