// raidkv-bench measures throughput and latency of a running raidkv server.
//
// Each client has its own connection and sends one request at a time
// (closed loop). Requests made during the warmup are not counted.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/bachtg/raidkv/internal/client"
)

type config struct {
	Addr       string        `json:"addr"`
	Clients    int           `json:"clients"`
	Duration   time.Duration `json:"duration_ns"`
	Warmup     time.Duration `json:"warmup_ns"`
	Keys       int           `json:"keys"`
	ValueSize  int           `json:"value_size"`
	WriteRatio float64       `json:"write_ratio"`
	Seed       int64         `json:"seed"`
	Label      string        `json:"label,omitempty"`
}

type result struct {
	Config    config    `json:"config"`
	StartedAt time.Time `json:"started_at"`
	GoOS      string    `json:"goos"`
	GoArch    string    `json:"goarch"`
	NumCPU    int       `json:"num_cpu"`

	Ops       int     `json:"ops"`
	Errors    int     `json:"errors"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50       float64 `json:"p50_ms"`
	P95       float64 `json:"p95_ms"`
	P99       float64 `json:"p99_ms"`
	Max       float64 `json:"max_ms"`
}

func main() {
	var cfg config
	flag.StringVar(&cfg.Addr, "addr", "127.0.0.1:7000", "server address")
	flag.IntVar(&cfg.Clients, "clients", 8, "concurrent clients, one connection each")
	flag.DurationVar(&cfg.Duration, "duration", 10*time.Second, "how long to measure")
	flag.DurationVar(&cfg.Warmup, "warmup", time.Second, "run this long first without measuring")
	flag.IntVar(&cfg.Keys, "keys", 10000, "number of distinct keys, picked uniformly")
	flag.IntVar(&cfg.ValueSize, "value-size", 100, "value size in bytes for Put")
	flag.Float64Var(&cfg.WriteRatio, "write-ratio", 1.0, "fraction of requests that are Put; the rest are Get")
	flag.Int64Var(&cfg.Seed, "seed", 0, "random seed (default: time based)")
	flag.StringVar(&cfg.Label, "label", "", "free text saved with the result, e.g. fsync=always")
	jsonPath := flag.String("json", "", "also write the result as JSON to this file")
	flag.Parse()

	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	if err := validate(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "raidkv-bench:", err)
		os.Exit(2)
	}

	res, err := run(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "raidkv-bench:", err)
		os.Exit(1)
	}
	printResult(res)

	if *jsonPath != "" {
		data, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*jsonPath, append(data, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "raidkv-bench:", err)
			os.Exit(1)
		}
	}
}

func validate(cfg config) error {
	switch {
	case cfg.Clients < 1:
		return errors.New("-clients must be at least 1")
	case cfg.Duration <= 0:
		return errors.New("-duration must be positive")
	case cfg.Warmup < 0:
		return errors.New("-warmup must not be negative")
	case cfg.Keys < 1:
		return errors.New("-keys must be at least 1")
	case cfg.ValueSize < 0:
		return errors.New("-value-size must not be negative")
	case cfg.WriteRatio < 0 || cfg.WriteRatio > 1:
		return errors.New("-write-ratio must be between 0 and 1")
	}
	return nil
}

func run(cfg config) (result, error) {
	clients := make([]*client.Client, cfg.Clients)
	for i := range clients {
		c, err := client.Dial(cfg.Addr)
		if err != nil {
			return result{}, err
		}
		defer c.Close()
		clients[i] = c
	}

	// Fail fast if the server is not there, instead of 10s of errors.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := clients[0].Get(ctx, []byte("bench-probe"))
	cancel()
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return result{}, fmt.Errorf("server at %s: %w", cfg.Addr, err)
	}

	start := time.Now()
	measureFrom := start.Add(cfg.Warmup)
	stopAt := measureFrom.Add(cfg.Duration)

	type workerResult struct {
		latencies []time.Duration
		errors    int
	}
	results := make([]workerResult, cfg.Clients)

	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(i)))
			value := make([]byte, cfg.ValueSize)
			rng.Read(value)
			res := &results[i]

			for {
				begin := time.Now()
				if begin.After(stopAt) {
					return
				}
				key := []byte(fmt.Sprintf("bench-%08d", rng.Intn(cfg.Keys)))

				var err error
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if rng.Float64() < cfg.WriteRatio {
					err = c.Put(ctx, key, value)
				} else {
					_, err = c.Get(ctx, key)
					if errors.Is(err, client.ErrNotFound) {
						err = nil
					}
				}
				cancel()
				took := time.Since(begin)

				if begin.Before(measureFrom) {
					continue
				}
				if err != nil {
					res.errors++
					continue
				}
				res.latencies = append(res.latencies, took)
			}
		}()
	}
	wg.Wait()

	var all []time.Duration
	res := result{
		Config:    cfg,
		StartedAt: start,
		GoOS:      runtime.GOOS,
		GoArch:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
	}
	for _, w := range results {
		all = append(all, w.latencies...)
		res.Errors += w.errors
	}
	slices.Sort(all)

	res.Ops = len(all)
	res.OpsPerSec = float64(res.Ops) / cfg.Duration.Seconds()
	res.P50 = ms(percentile(all, 50))
	res.P95 = ms(percentile(all, 95))
	res.P99 = ms(percentile(all, 99))
	if len(all) > 0 {
		res.Max = ms(all[len(all)-1])
	}
	return res, nil
}

// percentile uses the nearest-rank method on sorted latencies.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(float64(len(sorted))*p/100+0.5) - 1
	rank = max(0, min(rank, len(sorted)-1))
	return sorted[rank]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func printResult(r result) {
	c := r.Config
	fmt.Printf("clients %d, %v (after %v warmup), %d keys, value %d B, writes %.0f%%",
		c.Clients, c.Duration, c.Warmup, c.Keys, c.ValueSize, c.WriteRatio*100)
	if c.Label != "" {
		fmt.Printf(", %s", c.Label)
	}
	fmt.Println()
	fmt.Printf("throughput  %.0f ops/s  (%d ops, %d errors)\n", r.OpsPerSec, r.Ops, r.Errors)
	fmt.Printf("latency     p50 %.2f ms  p95 %.2f ms  p99 %.2f ms  max %.2f ms\n", r.P50, r.P95, r.P99, r.Max)
}
