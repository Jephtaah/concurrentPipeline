# concurrentPipeline

A multi-stage concurrent data processing pipeline in Go, built to prove mastery of goroutines, channels, `sync.WaitGroup`, `sync.Mutex`, the `context` package, and the race detector — not just use them individually, but combine them the way a real distributed system's internal data path would.

## What it is

A pipeline with three core stages:

```
Generator → Worker Pool (N goroutines) → Aggregator
```

- **Generator** produces a stream of jobs onto a channel, respecting context cancellation.
- **Worker Pool** fans out across a configurable number of goroutines, each pulling jobs, processing them, and writing results directly into a shared `Aggregator`.
- **Aggregator** holds a mutex-protected running count and sum, written to concurrently by every worker.

The whole pipeline is wrapped in `context.WithCancel` / `context.WithTimeout`, so it can be cancelled cleanly or bounded by a deadline from outside.

Three additional stages were built as stretch goals, each independently wired and tested:

- **Rate limiter** — throttles jobs using `time.Ticker`, so the pipeline can be made to run no faster than N jobs/interval.
- **Retry with exponential backoff** — wraps job processing so transient failures get retried with a doubling delay before giving up.
- **Pause/resume** — a channel-based gate that lets the entire worker pool be paused and resumed from outside without cancelling anything.

## Why it's hard

The brief for this phase wasn't "use goroutines" — it was "prove you understand concurrency correctness." That's a much higher bar. Specifically:

- **The mutex has to actually be contended, or it proves nothing.** My first draft had only one goroutine writing to the `Aggregator`, which meant the mutex compiled, passed tests, and did absolutely nothing under the race detector. I redesigned it so every worker goroutine calls `Aggregator.Add()` directly, creating real lock contention — then proved it mattered (see below).
- **Coordination overhead is real, and it can dominate.** My first throughput benchmark showed 1 worker outperforming 16. That's not a bug — it's a demonstration that when per-job work is cheap, the cost of channel handshakes, goroutine scheduling, and mutex locking outweighs any parallelism gained. I added a second benchmark with realistic per-job latency to show the same code inverts to a ~15.5x speedup once there's actual work to parallelize.
- **Cancellation and pausing both have to be provably real, not decorative.** It's trivial to wire a `context.Context` or a pause flag into code that never actually blocks on it. Every behavior below is backed by a test that would fail if the mechanism were a no-op — not just a test that happens to pass either way.

## Proof, not just claims

### 1. The mutex is load-bearing

I temporarily removed the lock from `Aggregator.Add()` and re-ran the test suite under `go test -race`:

```
WARNING: DATA RACE
Read at 0x00c0000180f8 by goroutine 14:
  ...Aggregator.Add() aggregator.go:16
Previous write at 0x00c0000180f8 by goroutine 16:
  ...Aggregator.Add() aggregator.go:16
...
--- FAIL: TestPipelineCorrectness (0.05s)
    expected 5000 results, got 2617
```

Without the lock, nearly half the increments to the shared counter were silently lost to a classic read-modify-write race. With the lock restored, a fresh run (`-count=1`, no caching) came back clean:

```
ok      concurrentPipeline/pipeline     1.629s
```

### 2. Concurrency isn't automatically faster — it depends on the workload

Benchmark with cheap per-job work (`value * value`, 10,000 jobs):

| Workers | ns/op | B/op | allocs/op |
|---|---|---|---|
| 1 | 8,070,479 | 765 | 12 |
| 4 | 14,263,272 | 1,126 | 13 |
| 16 | 23,209,192 | 2,686 | 19 |

More workers made this *slower* — pure coordination overhead (unbuffered channel handshakes, mutex contention) with no compute-bound work to amortize it against.

Benchmark with realistic per-job latency (`time.Sleep(200µs)`, 200 jobs):

| Workers | ns/op | Speedup vs 1 worker |
|---|---|---|
| 1 | 47,151,797 | 1x |
| 4 | 11,709,973 | ~4x |
| 8 | 5,794,127 | ~8.1x |
| 16 | 3,036,353 | ~15.5x |

Same code, same mutex, same channels — the only thing that changed is the shape of the work. When jobs block or take real time, concurrency pays off close to linearly; when they're trivially cheap, it doesn't.

### 3. Timeout and cancellation actually cut work short

```
$ go run main.go -jobs 10000000 -workers 2 -timeout 1ns
completed=false processed=0 sum=0 elapsed=22.166µs
```

A 1-nanosecond timeout against 10 million jobs returns almost instantly with zero jobs processed — proving `context.WithTimeout` is actually propagated and checked, not just passed in for show.

### 4. Pause genuinely blocks; it isn't a flag nobody reads

`TestPauserActuallyBlocksWorkers` starts the pipeline paused, asserts it does **not** complete within a 200ms window (it doesn't — the test measured 0.20s, meaning it actually waited out the window), then calls `Resume()` and asserts it completes shortly after. If pause were a no-op, the first assertion would fail immediately.

### 5. Retry recovers from real, injected failures

`processFlaky` fails ~25% of the time by design. Wired into a live 4-worker pool processing 500 jobs, `TestWorkersWithRetryRecoverFromFailures` confirms retry with exponential backoff recovers nearly all of them despite the injected failure rate — proven end-to-end, not just against a mocked function in isolation.

## Architecture

```
                     ┌─────────────┐
  count=N       ───► │  Generator  │
                     └──────┬──────┘
                            │ chan Job
                            ▼
                   ┌──────────────────┐
                   │  (Rate Limiter)  │  optional throttle stage
                   └────────┬─────────┘
                            │ chan Job
                            ▼
                  ┌───────────────────┐
                  │   Worker Pool      │  N goroutines
                  │  (retry, pause     │  each: process → Aggregator.Add()
                  │   variants avail.) │
                  └─────────┬──────────┘
                            │ chan Result
                            ▼
                    ┌───────────────┐
                    │  Aggregator    │  mutex-protected count/sum
                    └───────────────┘

  All stages respect context.Context for cancellation/timeout.
```

## How to run it

Build and run the CLI:

```bash
go run main.go -jobs 10000 -workers 8 -timeout 5s
```

Flags:
- `-jobs` — number of jobs to process (default 1000)
- `-workers` — number of worker goroutines (default 4)
- `-timeout` — pipeline timeout (default 5s)

Run the full test suite:

```bash
go test ./...
```

Run with the race detector (fresh, no caching):

```bash
go test -race -count=1 ./...
```

Run benchmarks:

```bash
go test -bench=. -benchmem ./pipeline/
```

## Project structure

```
concurrentPipeline/
├── main.go                  # CLI entry point
├── pipeline/
│   ├── types.go              # Job, Result
│   ├── generator.go          # Generate()
│   ├── worker.go             # StartWorkers(), StartWorkersWithRetry(), StartWorkersPausable()
│   ├── aggregator.go         # Aggregator (mutex-protected)
│   ├── ratelimiter.go        # RateLimit()
│   ├── retry.go              # processWithRetry()
│   ├── pauser.go             # Pauser
│   ├── pipeline.go           # Run() — wires generator → workers → aggregator
│   └── pipeline_test.go      # correctness tests, benchmarks, retry/pause tests
└── go.mod
```

## Critical constraint honored

No third-party concurrency libraries. Everything above uses only Go's standard library: `sync`, `context`, `time`.