// raidkv-bench measures throughput and latency of a running raidkv server.
//
// By default each client sends one request at a time (closed loop). With
// -rate requests go out on a fixed schedule (open loop) and latency counts
// from when each was due, so server stalls are not hidden.
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
	Addr        string        `json:"addr"`
	Clients     int           `json:"clients"`
	Duration    time.Duration `json:"duration_ns"`
	Warmup      time.Duration `json:"warmup_ns"`
	Keys        int           `json:"keys"`
	ValueSize   int           `json:"value_size"`
	WriteRatio  float64       `json:"write_ratio"`
	Seed        int64         `json:"seed"`
	Rate        int           `json:"rate,omitempty"`
	MaxInFlight int           `json:"max_in_flight,omitempty"`
	Label       string        `json:"label,omitempty"`
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

	// Open loop: how late the client sent; if high, the run is not valid.
	SendLagP99 float64 `json:"send_lag_p99_ms,omitempty"`
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
	flag.IntVar(&cfg.Rate, "rate", 0, "open loop: total requests per second, spread over the -clients connections (0: closed loop)")
	flag.IntVar(&cfg.MaxInFlight, "max-in-flight", 100000, "open loop: requests allowed in flight before sending waits")
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
	case cfg.Rate < 0:
		return errors.New("-rate must not be negative")
	case cfg.MaxInFlight < 1:
		return errors.New("-max-in-flight must be at least 1")
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

	if cfg.Rate > 0 {
		return runOpenLoop(cfg, clients), nil
	}
	return runClosedLoop(cfg, clients), nil
}

func runClosedLoop(cfg config, clients []*client.Client) result {
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

				err := do(c, key, value, rng.Float64() < cfg.WriteRatio)
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
	errs := 0
	for _, w := range results {
		all = append(all, w.latencies...)
		errs += w.errors
	}
	return summarize(cfg, start, all, errs)
}

func runOpenLoop(cfg config, clients []*client.Client) result {
	value := make([]byte, cfg.ValueSize)
	rand.New(rand.NewSource(cfg.Seed)).Read(value)

	total := int((cfg.Warmup + cfg.Duration).Seconds() * float64(cfg.Rate))
	interval := time.Second / time.Duration(cfg.Rate)
	warmupReqs := int(cfg.Warmup.Seconds() * float64(cfg.Rate))

	// One slot per request, so goroutines record without a lock.
	latencies := make([]time.Duration, total)
	lags := make([]time.Duration, total)
	failed := make([]bool, total)
	inFlight := make(chan struct{}, cfg.MaxInFlight)

	// One sender per 50k req/s: a single one falls behind above that.
	var wg sync.WaitGroup
	start := time.Now()
	n := min((cfg.Rate+49999)/50000, len(clients))
	for k := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(k) + 1))
			for i := k; i < total; i += n {
				c := clients[(i/n)%len(clients)]
				due := start.Add(time.Duration(i) * interval)
				key := fmt.Appendf(nil, "bench-%08d", rng.Intn(cfg.Keys))
				put := rng.Float64() < cfg.WriteRatio
				waitUntil(due)
				inFlight <- struct{}{}
				wg.Add(1)
				go func() {
					defer wg.Done()
					sent := time.Now()
					err := do(c, key, value, put)
					latencies[i] = time.Since(due)
					lags[i] = sent.Sub(due)
					failed[i] = err != nil
					<-inFlight
				}()
			}
		}()
	}
	wg.Wait()

	var all, lag []time.Duration
	errs := 0
	for i := warmupReqs; i < total; i++ {
		if failed[i] {
			errs++
			continue
		}
		all = append(all, latencies[i])
		lag = append(lag, lags[i])
	}
	res := summarize(cfg, start, all, errs)
	slices.Sort(lag)
	res.SendLagP99 = ms(percentile(lag, 99))
	return res
}

// waitUntil never returns early. time.Sleep can overshoot by ~1ms, so the
// last 1.2ms spins.
func waitUntil(t time.Time) {
	for wait := time.Until(t); wait > 0; wait = time.Until(t) {
		if wait > 1200*time.Microsecond {
			time.Sleep(wait - 1200*time.Microsecond)
		} else {
			runtime.Gosched()
		}
	}
}

func do(c *client.Client, key, value []byte, put bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if put {
		return c.Put(ctx, key, value)
	}
	_, err := c.Get(ctx, key)
	if errors.Is(err, client.ErrNotFound) {
		return nil
	}
	return err
}

func summarize(cfg config, start time.Time, all []time.Duration, errs int) result {
	slices.Sort(all)
	res := result{
		Config:    cfg,
		StartedAt: start,
		GoOS:      runtime.GOOS,
		GoArch:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		Errors:    errs,
		Ops:       len(all),
		OpsPerSec: float64(len(all)) / cfg.Duration.Seconds(),
		P50:       ms(percentile(all, 50)),
		P95:       ms(percentile(all, 95)),
		P99:       ms(percentile(all, 99)),
	}
	if len(all) > 0 {
		res.Max = ms(all[len(all)-1])
	}
	return res
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
	loop := "closed loop"
	if c.Rate > 0 {
		loop = fmt.Sprintf("open loop at %d req/s", c.Rate)
	}
	fmt.Printf("%s, clients %d, %v (after %v warmup), %d keys, value %d B, writes %.0f%%",
		loop, c.Clients, c.Duration, c.Warmup, c.Keys, c.ValueSize, c.WriteRatio*100)
	if c.Label != "" {
		fmt.Printf(", %s", c.Label)
	}
	fmt.Println()
	fmt.Printf("throughput  %.0f ops/s  (%d ops, %d errors)\n", r.OpsPerSec, r.Ops, r.Errors)
	fmt.Printf("latency     p50 %.2f ms  p95 %.2f ms  p99 %.2f ms  max %.2f ms\n", r.P50, r.P95, r.P99, r.Max)
	if c.Rate > 0 {
		fmt.Printf("send lag    p99 %.2f ms\n", r.SendLagP99)
	}
}
