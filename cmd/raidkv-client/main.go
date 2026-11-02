package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/bachtg/raidkv/internal/client"
)

const usage = `usage: raidkv [flags] <command> [args]

commands:
  get <key>            print the value of key
  put <key> <value>    set key to value
  delete <key>         remove key

flags:
`

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "server address")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request timeout")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Parse()

	os.Exit(run(*addr, *timeout, flag.Args()))
}

// run returns the process exit code: 0 ok, 1 error or not found, 2 bad usage.
func run(addr string, timeout time.Duration, args []string) int {
	if len(args) == 0 {
		flag.Usage()
		return 2
	}
	cmd, args := args[0], args[1:]
	want := map[string]int{"get": 1, "put": 2, "delete": 1}
	if n, ok := want[cmd]; !ok || len(args) != n {
		flag.Usage()
		return 2
	}

	c, err := client.Dial(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch cmd {
	case "get":
		v, err := c.Get(ctx, []byte(args[0]))
		if errors.Is(err, client.ErrNotFound) {
			fmt.Fprintln(os.Stderr, "(not found)")
			return 1
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		fmt.Println(string(v))
	case "put":
		err = c.Put(ctx, []byte(args[0]), []byte(args[1]))
	case "delete":
		err = c.Delete(ctx, []byte(args[0]))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	if cmd != "get" {
		fmt.Println("OK")
	}
	return 0
}
