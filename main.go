package main

import (
	"context"
	"fmt"

	"concurrentPipeline/pipeline"
)

func main() {
	ctx := context.Background()
	jobs := pipeline.Generate(ctx, 5)
	for j := range jobs {
		fmt.Println(j)
	}
}
