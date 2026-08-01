package main

import (
	"flag"
	"fmt"
	"time"

	"concurrentPipeline/pipeline"
)

func main() {
	jobCount := flag.Int("jobs", 1000, "number of jobs to process")
	workers := flag.Int("workers", 4, "number of worker goroutines")
	timeout := flag.Duration("timeout", 5*time.Second, "pipeline timeout")
	flag.Parse()

	start := time.Now()
	count, sum, completed := pipeline.Run(pipeline.Config{
		JobCount:   *jobCount,
		NumWorkers: *workers,
		Timeout:    *timeout,
	})
	elapsed := time.Since(start)

	fmt.Printf("completed=%v processed=%d sum=%d elapsed=%s\n", completed, count, sum, elapsed)
}
