## Run it

Needs JDK 21 and Docker. The benchmark runs need TSBS (Go) and use about 7 GB of scratch disk
outside the repo (`$HOME/metricsdb-work`).

```bash
./gradlew build                        # unit, property, kill -9 crash, HTTP and VictoriaMetrics parity tests
java -jar server/build/libs/metricsdb.jar --metricsdb.data-dir=/tmp/mdb      # single node on :9201
curl --data-binary 'cpu,host=a usage=42 1790812800000000000' localhost:9201/write
curl 'localhost:9201/api/v1/query?query=cpu_usage&time=1790812800'

scripts/demo.sh up                     # MetricsDB + Prometheus (remote write) + Grafana on 127.0.0.1:3301
bench/scripts/final_run.sh             # every measurement in results/, then charts and NUMBERS.md
bench/build/install/bench/bin/bench faults --jar server/build/libs/metricsdb.jar --runs 5
```

A three-node cluster: start three storage nodes and a router.

```bash
for i in 0 1 2; do java -jar server/build/libs/metricsdb.jar --server.port=$((9211+i)) \
  --metricsdb.role=storage --metricsdb.data-dir=/tmp/n$i & done
java -jar server/build/libs/metricsdb.jar --server.port=9201 --metricsdb.role=router \
  --metricsdb.data-dir=/tmp/router --metricsdb.cluster.replication-factor=2 \
  --metricsdb.cluster.nodes=n0=http://127.0.0.1:9211,n1=http://127.0.0.1:9212,n2=http://127.0.0.1:9213
```

## What is where

| Path | What |
|---|---|
| `core/` | The engine, no framework: Gorilla and integer chunk codecs (`encoding/`), label index and posting lists (`index/`), WAL with group commit and checkpoints (`wal/`), head series with the out-of-order buffer (`head/`), blocks, rollups and compaction (`block/`, `storage/Tsdb`), the PromQL subset (`promql/`), line protocol, remote write and snappy (`ingest/`), consistent hashing, wire format and anti-entropy digests (`cluster/`) |
| `server/` | Spring Boot: Prometheus query API, write endpoints, the storage node's internal API, the router (replication, hinted handoff, read repair, anti-entropy, partial results), self-monitoring |
| `bench/` | The experiment harness (`bench load`, `querybench`, `diff`, `exp5`, `ooo`, `faults`), scripts that run each database in turn, `ledger.py` (writes `NUMBERS.md`), `charts.py` (writes `docs/*.png`), and a small Go tool that reads TSBS query files |
| `core/src/jmh` | JMH microbenchmarks |
| `deploy/` | Prometheus and Grafana (provisioned data source and two dashboards) for the demo |
| `results/` | Every measured run, one JSON row each, with machine and load |
| `DESIGN.md`, `NUMBERS.md`, `BUG_LOG.md` | Why it is built this way and whose ideas it uses, every figure with its source, every bug with what found it |
