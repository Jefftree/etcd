// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"
)

// rangeCmd represents the range command
var rangeCmd = &cobra.Command{
	Use:   "range key [end-range]",
	Short: "Benchmark range",

	Run: rangeFunc,
}

var (
	rangeRate        int
	rangeTotal       int
	rangeConsistency string
	rangeLimit       int64
	rangeCountOnly   bool
	rangeStream         bool
	rangeStreamDiscard  bool
	rangeStreamAccumCap int
	rangePaginate       int64
	rangeFromKey        bool
	rangeMemSampleMs    int
)

func init() {
	RootCmd.AddCommand(rangeCmd)
	rangeCmd.Flags().IntVar(&rangeRate, "rate", 0, "Maximum range requests per second (0 is no limit)")
	rangeCmd.Flags().IntVar(&rangeTotal, "total", 10000, "Total number of range requests")
	rangeCmd.Flags().StringVar(&rangeConsistency, "consistency", "l", "Linearizable(l) or Serializable(s)")
	rangeCmd.Flags().Int64Var(&rangeLimit, "limit", 0, "Maximum number of results to return from range request (0 is no limit)")
	rangeCmd.Flags().BoolVar(&rangeCountOnly, "count-only", false, "Only returns the count of keys")
	rangeCmd.Flags().BoolVar(&rangeStream, "stream", false, "Use RangeStream instead of unary Range")
	rangeCmd.Flags().BoolVar(&rangeStreamDiscard, "stream-discard", false, "With --stream, discard chunks instead of accumulating (tests if client drain is bottleneck)")
	rangeCmd.Flags().IntVar(&rangeStreamAccumCap, "stream-accum-cap", 0, "Pre-allocate stream accumulator to this capacity to remove slice-doubling overhead (0 = grow naturally)")
	rangeCmd.Flags().Int64Var(&rangePaginate, "paginate", 0, "Page size for paginated unary range (0 = single-shot; overrides --limit)")
	rangeCmd.Flags().BoolVar(&rangeFromKey, "from-key", false, "Range over all keys (sets key and range_end to 0x00, mirroring etcdctl --from-key)")
	rangeCmd.Flags().IntVar(&rangeMemSampleMs, "mem-sample-ms", 0, "Sample client runtime.MemStats every N ms and report peak HeapInuse (0 = off)")
}

func rangeFunc(cmd *cobra.Command, args []string) {
	if len(args) > 2 || (len(args) == 0 && !rangeFromKey) {
		fmt.Fprintln(os.Stderr, cmd.Usage())
		os.Exit(1)
	}

	k := ""
	end := ""
	if len(args) >= 1 {
		k = args[0]
	}
	if len(args) == 2 {
		end = args[1]
	}

	if rangeConsistency == "l" {
		fmt.Println("bench with linearizable range")
	} else if rangeConsistency == "s" {
		fmt.Println("bench with serializable range")
	} else {
		fmt.Fprintln(os.Stderr, cmd.Usage())
		os.Exit(1)
	}

	if rangeRate == 0 {
		rangeRate = math.MaxInt32
	}
	limit := rate.NewLimiter(rate.Limit(rangeRate), 1)

	requests := make(chan struct{}, totalClients)
	clients := mustCreateClients(totalClients, totalConns)

	bar = pb.New(rangeTotal)
	bar.Start()

	r := newReport(cmd.Name())
	request := &etcdserverpb.RangeRequest{
		Key:       []byte(k),
		RangeEnd:  []byte(end),
		Limit:     rangeLimit,
		CountOnly: rangeCountOnly,
	}
	if rangeFromKey {
		request.Key = []byte{0}
		request.RangeEnd = []byte{0}
	}
	if rangeConsistency == "s" {
		request.Serializable = true
	}
	callOpts := []grpc.CallOption{
		grpc.WaitForReady(true),
		grpc.MaxCallSendMsgSize(2 * 1024 * 1024),
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
	}

	var sampler *memSampler
	if rangeMemSampleMs > 0 {
		sampler = startMemSampler(time.Duration(rangeMemSampleMs) * time.Millisecond)
	}

	for i := range clients {
		wg.Add(1)
		go func(c *v3.Client) {
			defer wg.Done()
			kv := etcdserverpb.NewKVClient(c.ActiveConnection())
			for range requests {
				limit.Wait(context.Background())
				st := time.Now()
				var err error
				switch {
				case rangeStream:
					var stream etcdserverpb.KV_RangeStreamClient
					stream, err = kv.RangeStream(context.Background(), request, callOpts...)
					if err == nil {
						if rangeStreamDiscard {
							err = drainRangeStreamDiscard(stream)
						} else {
							err = drainRangeStream(stream)
						}
					}
				case rangePaginate > 0:
					err = paginatedRange(kv, request, rangePaginate, callOpts)
				default:
					_, err = kv.Range(context.Background(), request, callOpts...)
				}
				r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
				bar.Increment()
			}
		}(clients[i])
	}

	go func() {
		for i := 0; i < rangeTotal; i++ {
			requests <- struct{}{}
		}
		close(requests)
	}()

	rc := r.Run()
	wg.Wait()
	close(r.Results())
	bar.Finish()
	fmt.Printf("%s", <-rc)
	if sampler != nil {
		peak := sampler.Stop()
		fmt.Printf("CLIENT_PEAK_HEAP_MB: %.2f\n", float64(peak)/1024/1024)
	}
}

// drainRangeStreamDiscard reads chunks and immediately discards them. Used to
// test whether client-side accumulation is the bottleneck causing server-side
// memory pressure (gRPC outbound buffer pile-up).
func drainRangeStreamDiscard(stream etcdserverpb.KV_RangeStreamClient) error {
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// drainRangeStream reads chunks from a RangeStream and accumulates all KVs
// into a single slice so client-side memory matches what an application that
// materializes the full response would see (fair comparison with unary).
//
// The accumulator is pre-sized via --stream-accum-cap to remove slice-doubling
// reallocation pressure; pass a value >= expected total KV count for cleanest
// numbers.
func drainRangeStream(stream etcdserverpb.KV_RangeStreamClient) error {
	var all []*mvccpb.KeyValue
	if rangeStreamAccumCap > 0 {
		all = make([]*mvccpb.KeyValue, 0, rangeStreamAccumCap)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				_ = all
				return nil
			}
			return err
		}
		if resp.RangeResponse != nil {
			all = append(all, resp.RangeResponse.Kvs...)
		}
	}
}

// paginatedRange walks a range with pageSize-bounded unary RPCs, accumulating
// all KVs across pages so client-side memory matches what an application that
// materializes the full response would see.
func paginatedRange(kv etcdserverpb.KVClient, base *etcdserverpb.RangeRequest, pageSize int64, callOpts []grpc.CallOption) error {
	page := *base
	page.Limit = pageSize
	var all []*mvccpb.KeyValue
	for {
		resp, err := kv.Range(context.Background(), &page, callOpts...)
		if err != nil {
			return err
		}
		all = append(all, resp.Kvs...)
		if !resp.More || len(resp.Kvs) == 0 {
			_ = all
			return nil
		}
		last := resp.Kvs[len(resp.Kvs)-1].Key
		next := make([]byte, len(last)+1)
		copy(next, last)
		page.Key = next
	}
}

type memSampler struct {
	stop chan struct{}
	peak chan uint64
}

func startMemSampler(interval time.Duration) *memSampler {
	s := &memSampler{stop: make(chan struct{}), peak: make(chan uint64, 1)}
	go func() {
		var ms runtime.MemStats
		var max uint64
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				s.peak <- max
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > max {
					max = ms.HeapInuse
				}
			}
		}
	}()
	return s
}

func (s *memSampler) Stop() uint64 {
	close(s.stop)
	return <-s.peak
}
