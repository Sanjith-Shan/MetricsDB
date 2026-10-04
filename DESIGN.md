# Design

How MetricsDB is built and why, with the sources each part comes from. Measured consequences
of these choices are in [`NUMBERS.md`](NUMBERS.md); the bugs found along the way are in
[`BUG_LOG.md`](BUG_LOG.md).

## Credits

MetricsDB is written from scratch, but none of its ideas are new. It follows, and its README
compares against, these published designs and systems:

- **Gorilla** (Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory Time Series Database",
  VLDB 2015): delta-of-delta timestamps and XOR-compressed floats.
- **Prometheus TSDB** (Fabian Reinartz's "Writing a Time Series Database from Scratch" and the
  Prometheus storage documentation): the head block, two-hour blocks, block compaction, the WAL
  with checkpoints, inverted index postings, out-of-order ingestion, and PromQL semantics
  (`rate` extrapolation, left-open range windows).
- **VictoriaMetrics** and **MetricsQL**: the comparison baseline, the line-protocol naming
  (`measurement_field`), step alignment of range queries, and keeping the metric name after
  `*_over_time` functions that do not change what a series measures.
- **M3** (Uber) and **Thanos**: downsampled rollups for long ranges and replication across
  storage nodes behind a stateless query layer.
- **Dynamo** (DeCandia et al., SOSP 2007) and **Cassandra**: consistent hashing with virtual
  nodes, sloppy quorum with hinted handoff, read repair, and Merkle-tree anti-entropy.
- **Netflix Atlas** and the **Netflix TimeSeries Abstraction** posts: in-memory recent data
  with tiered rollups, and the operational focus on dimensional (high-cardinality) metrics.
- **TSBS** (Time Series Benchmark Suite, `timescale/tsbs`): the DevOps workload, data
  generator, query generator and loaders used for every comparison.
- **Jepsen**: the shape of the fault-injection runs (a workload, a nemesis, a checker).

## Data model

A series is a sorted set of label pairs, the metric name being the label `__name__`. A
sample is a millisecond timestamp and a float64. InfluxDB line protocol maps to this as
VictoriaMetrics does: each field becomes the series `measurement_field` with the tags as
labels, so the standard benchmark's loaders and its PromQL queries run unchanged.

## Write path

1. **Parse.** Line protocol, Prometheus remote write (snappy and protobuf decoded by hand, no
   protobuf runtime), or the Prometheus text format. Tag strings are interned per parser, so a
   repeated `hostname=host_7` costs a hash lookup, not an allocation.
2. **Resolve the series.** A hash map from labels to the in-memory series. A new series gets
   the next id and is added to the inverted index; past the node's **series limit** it is
   refused (the cardinality limit) and the client gets a 422 naming the limit.
3. **Append to the head.** Each series has an open Gorilla chunk. Samples newer than the
   series' newest go straight in; a sample older than that, but within the **out-of-order
   window** (10 minutes by default), goes to a small sorted per-series buffer; anything older
   is refused and counted.
4. **Log it.** The accepted samples become one WAL record. A single writer thread takes every
   record queued, writes them with one `write`, calls `fsync` once for the group, and only then
   completes the callers' futures (**group commit**). The HTTP request is answered after its
   future completes, so an acknowledged sample is on disk.

What the WAL protects: everything in the head (up to about three hours of data). Blocks are
immutable and written atomically (temp directory, fsync, rename), so they need no log. On
restart the newest checkpoint is replayed, then later segments; a torn final record (a crash
mid-write) fails its CRC32C and the segment is truncated there, which only drops records that
were never acknowledged. Replay accepts samples in any order, because concurrent writers can
log in a different order than they applied.

## Storage

**Gorilla chunks.** Up to 120 samples per chunk. The first timestamp and value are stored raw;
after that, timestamps store the delta of deltas in 1, 16, 20, 24 or 68 bits (the paper's
buckets are for second precision; these are sized for milliseconds), and values store the XOR
with the previous value, either one zero bit (unchanged) or the meaningful bits inside a
leading/trailing-zero window that is reused while it fits. Metrics scraped at a fixed interval
have delta-of-delta 0 almost always, so a timestamp costs one bit; slowly changing values share
most of their high bits, so the XOR is short.

**Integer chunks.** Many metrics are whole numbers stored as doubles: counters, byte counts,
percentages rounded by the exporter. Their XOR is poor (a counter that grows by 50 a step
changes many mantissa bits), so when a head chunk moves into a block and every value in it is an
exact integer (the round trip through `long` keeps the same bits, which rules out -0.0, NaN and
fractions), the chunk is re-encoded with the same timestamp scheme and integer values: either the
difference from the previous value or the difference of differences, whichever comes out smaller
for that chunk (deltas suit gauges, delta-of-deltas suit counters), in 1, 6, 11, 20, 37 or 69-bit
buckets. Two header bits say which. On the benchmark the raw chunks took 1.46 bytes per sample
with Gorilla alone, 1.21 with integer deltas, and 1.01 choosing per chunk
(`results/exp1_encoding_steps.jsonl`). Head chunks stay Gorilla because they must stay appendable.

**Head to block.** A chunk is sealed at 120 samples or when a sample crosses a block-range
boundary, so every chunk falls inside one two-hour range and moves to a block without being
re-encoded (unless late samples landed in that range, in which case it is merged and
re-encoded). When the head spans 1.5 ranges, the oldest full range is cut: the lower bound for
new samples moves up first (so nothing can slip into a range being written), the block is
written, the in-memory chunks are dropped, and the WAL is rewritten as a checkpoint of what is
still in the head.

**Blocks.** A directory with `chunks` (the Gorilla chunks back to back), `rollup` (downsampled
chunks), `index` (each series' labels and chunk references, CRC-checked) and
`meta.properties`. The chunk files are memory-mapped: reading a chunk is a page-cache hit or a
fault, never a copy. Blocks are compacted 2h to 6h to 24h; retention deletes whole blocks, or
drops a block's raw chunks while keeping its rollups when rollups are kept longer.

**Rollups.** Every block carries five-minute buckets per series: min, max, sum, count and last.
A bucket ending at `e` covers `(e - 5m, e]`, the same left-open convention as PromQL range
windows, so `max_over_time(x[1h])` at a bucket-aligned time is exactly the max of twelve
buckets. The block's last bucket needs the sample at the range's end, which is still in the
head when the block is cut (the very first block also keeps the bucket ending at its start).

Windows need not line up with buckets. A window `(t - w, t]` splits exactly into a raw slice
`(t - w, a]`, whole buckets ending in `(a, b]`, and a raw slice `(b, t]`, where `a` is `t - w`
rounded up and `b` is `t` rounded down to five minutes. Only the two edge slices are decoded from
raw chunks, so a one-hour window reads 12 buckets and roughly two partial chunks instead of 360
samples. The engine takes this path for `min/max/sum/count/avg/last_over_time` with windows of
30 minutes or more whose steps are at least half the window (sliding windows are cheaper as one raw pass; configurable, or forced per query with `rollup=on|off`), and the answer is
the raw evaluation's, up to float rounding in sums (tests compare the two on aligned and unaligned windows, and exp4 compares
with VictoriaMetrics). `rate` always reads raw data.

## Index

An inverted index from each label pair to the sorted list of series ids holding it (posting
lists). Ids are assigned in increasing order, so appending keeps every list sorted for free. A
selector is answered by intersecting the lists of its positive matchers, smallest first, with
galloping search (exponential probe then binary search) through the larger list, so a selective
matcher costs about `small * log(large / small)`. Regex matchers union the lists of every value
that matches; negative matchers, and any matcher that also matches the empty string, subtract.
The index is held fully in memory and rebuilt at startup from the blocks' series tables and the
WAL.

**Why high cardinality kills a TSDB.** Every series costs memory in the head (its labels, an
open chunk, index entries) whether or not it gets more than one sample, and every distinct
label value adds a posting list. A label like a request id or a pod name that changes on each
deploy multiplies the series count without bound; memory grows until the node falls over, and
queries that select by the bloated label get slower. The series limit is the guard: the node
refuses new series past the limit, with a clear error, and keeps serving the ones it has.

## Query

A PromQL subset: selectors with `=`, `!=`, `=~`, `!~`; range selectors and `offset`; `rate`,
`irate`, `increase`, `delta`, `*_over_time`; `sum/avg/min/max/count/stddev/stdvar/topk/bottomk/
quantile/group` with `by` and `without`; arithmetic and comparison operators (with `bool`);
`histogram_quantile` and a few math functions. It is enough for the benchmark's queries, for
Grafana's Prometheus data source, and for the self-monitoring dashboard. It is not enough for
everything: no subqueries, no `on`/`ignoring`/`group_left`, no `label_replace`.

Evaluation is over a grid of steps. Each selected series is decoded once over the whole query
range into arrays, and two pointers slide along the steps to find each window, so a range query
is one pass over the samples. Ranges of 50 or more steps are aligned to the step the way
VictoriaMetrics does it (start down, end up, then end back one step if the point count changed),
so the answers can be compared with it point for point.

**Query protection.** Each query has a budget: series selected, samples decoded, and wall-clock
time. They are checked inside the selection and decode loops, and a query over budget fails
with a message naming the limit and its setting, rather than taking the node's memory.

## Distribution

Storage nodes each own a `Tsdb`. A router (stateless except for its hint log) takes writes and
queries:

- **Placement.** Consistent hashing of the series labels onto a ring with 64 virtual nodes per
  storage node; a series lives on the first `rf` distinct nodes clockwise (RF=2 by default).
- **Writes.** The batch is split per node and the shares are sent concurrently on virtual
  threads. A sample is acknowledged once `write-quorum` replicas made it durable (1 by default).
  For every replica that failed or is known to be down, its share goes into the router's hint
  log, fsynced, before the client is answered (**hinted handoff**). When the node answers its
  health probe again the hints are replayed in order; samples are idempotent (a duplicate
  timestamp is ignored), so a replay that overlaps is harmless.
- **Reads.** The selector goes to every node at once; replies are the stored Gorilla chunks
  themselves, merged per series and de-duplicated by timestamp. **Partial results:** if a node
  fails, the answer is complete as long as each ring range still has a replica that answered;
  otherwise the response is marked `isPartial` with a warning naming the unreachable nodes. It
  is never silently short.
- **Read repair.** When the replicas of a series disagree in a query's range, the router writes
  the union back to the lagging replica in the background.
- **Anti-entropy.** A two-level hash tree: each node returns one hash per ring range; only
  ranges whose replicas disagree are expanded into per-series, per-hour leaves; only series
  whose leaves disagree are fetched, merged and written back. Each sample contributes
  `mix(timestamp, value bits)` to a sum, so the hash does not depend on chunk boundaries or
  arrival order. It repairs what hints cannot: a node restored with an empty disk, or a router
  that lost its hint log.

Trade-off of `write-quorum=1`: an acknowledged sample is durable on one replica's WAL plus the
router's hint log. Losing that replica's disk and the router's disk before repair loses it; set
`write-quorum=2` to require both replicas instead, at the price of failing writes whenever a
replica is down.

## Self-monitoring

Each node serves `/metrics` in Prometheus format: samples ingested and refused by reason,
ingest request latency by protocol, WAL fsync latency, head series and samples, block bytes,
head-cut and compaction time, query latency by type and by raw or rollup path, and on the
router node up/down, hint bytes pending, replica write latency, partial queries and read
repairs. A real Prometheus scrapes it and remote-writes into MetricsDB, and the Grafana
dashboard in `deploy/` reads MetricsDB's own metrics back out of MetricsDB.

## What it does not do

No backfill: a sample older than the head's lower bound (the end of the newest block) is refused,
so loading days-old data into a node that is already taking live data loses whatever arrives
after the old range was cut into a block (the Grafana demo loads its old data first for this
reason). Prometheus has the same rule and a separate backfill tool. No multi-tenancy,
authentication, or TLS. No rebalancing when nodes join or leave (the ring is
static configuration). The router is a single process (it can be run as several, but hints are
per router). Exemplars, native histograms and remote read are not implemented. The label index
is in memory, so the series count per node is bounded by heap.
