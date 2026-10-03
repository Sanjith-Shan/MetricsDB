// tsbs-dump prints the queries in a TSBS query file (gob-encoded query.HTTP) as JSON lines,
// so the answer diff and the latency harness can replay exactly the benchmark's queries.
package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/timescale/tsbs/pkg/query"
)

type out struct {
	ID    uint64 `json:"id"`
	Label string `json:"label"`
	Desc  string `json:"desc"`
	Path  string `json:"path"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: tsbs-dump <queries.gob>")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	dec := gob.NewDecoder(bufio.NewReader(f))
	enc := json.NewEncoder(os.Stdout)
	for {
		q := query.NewHTTP()
		if err := dec.Decode(q); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		_ = enc.Encode(out{ID: q.GetID(), Label: string(q.HumanLabel), Desc: string(q.HumanDescription), Path: string(q.Path)})
	}
}
