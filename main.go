package main

import (
	"context"
	"fmt"

	"concurrentPipeline/pipeline"
)

func main() {
	ctx := context.Background()

	jobs := pipeline.Generate(ctx, 20)
	agg := &pipeline.Aggregator{}
	results := pipeline.StartWorkers(ctx, jobs, 4, agg)

	for r := range results {
		fmt.Println(r)
	}

	count, sum := agg.Snapshot()
	fmt.Printf("count=%d sum=%d\n", count, sum)
}
