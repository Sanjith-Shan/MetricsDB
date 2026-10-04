#!/bin/bash
# exp5 (one node killed mid-write, RF=1 vs RF=2, and RF=2 restarted on an empty disk),
# the out-of-order experiment, and the fault-injection runs. All on this machine.
source "$(dirname "$0")/env.sh"
set -x
wait_idle
# exp5: 3 storage nodes + router, 1000 series x 10 samples/s each = 10,000 samples/s offered
for rf in 1 2; do
  $BENCH exp5 --jar $JAR --rf $rf --series 1000 --ticks-per-s 10 --kill-at 20 --restart-at 40 --end-at 60 \
    --work $W/run/exp5 --out $RESULTS/exp5.jsonl --timeline $RESULTS/exp5_timeline.jsonl
done
$BENCH exp5 --jar $JAR --rf 2 --wipe --series 1000 --ticks-per-s 10 --kill-at 20 --restart-at 40 --end-at 60 \
  --work $W/run/exp5 --out $RESULTS/exp5.jsonl --label rf2-wipe
# throughput of the 3-node cluster: the benchmark file through the router (RF=2)
$REPO/bench/scripts/cluster_ingest.sh
# out-of-order: 10% of samples late by up to 5 min (inside the 10 min window), then up to 20 min
$REPO/bench/scripts/db.sh start metricsdb
$BENCH ooo --url http://localhost:9201 --late-fraction 0.10 --max-delay-ms 300000 --window-ms 600000 --label late-within-window --run w1 --out $RESULTS/ooo.jsonl
$BENCH ooo --url http://localhost:9201 --late-fraction 0.10 --max-delay-ms 1200000 --window-ms 600000 --label late-up-to-2x-window --run w2 --seed 2 --out $RESULTS/ooo.jsonl
$REPO/bench/scripts/db.sh stop metricsdb
MDB_ARGS="--metricsdb.storage.out-of-order-window=0s" $REPO/bench/scripts/db.sh start metricsdb
$BENCH ooo --url http://localhost:9201 --late-fraction 0.10 --max-delay-ms 300000 --window-ms 0 --label no-window --run w3 --out $RESULTS/ooo.jsonl
$REPO/bench/scripts/db.sh stop metricsdb
# fault injection: 5 runs of 90 s with kill -9, SIGSTOP and link drops
$BENCH faults --jar $JAR --runs ${FAULT_RUNS:-5} --duration 90 --series 300 --ticks-per-s 10 --seed 100 --work $W/run/faults --out $RESULTS/faults.jsonl
echo CLUSTER DONE
