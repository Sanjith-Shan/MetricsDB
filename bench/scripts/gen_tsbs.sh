#!/bin/bash
# The benchmark data and queries: TSBS DevOps, 100 hosts, 2026-10-01 for 24 h at 10 s, seed 123;
# 100 queries per type for the 11 types both PromQL and InfluxQL support. About 6.6 GB under $W.
set -e
source "$(dirname "$0")/env.sh"
mkdir -p $DATA
for fmt in victoriametrics influx; do
  f=$DATA/data-$fmt-s$SCALE.txt
  [ -s $f ] || tsbs_generate_data --use-case=devops --seed=123 --scale=$SCALE \
    --timestamp-start=2026-10-01T00:00:00Z --timestamp-end=2026-10-02T00:00:00Z --log-interval=10s --format=$fmt > $f
done
TYPES="single-groupby-1-1-1 single-groupby-1-1-12 single-groupby-1-8-1 single-groupby-5-1-1 single-groupby-5-1-12 single-groupby-5-8-1 cpu-max-all-1 cpu-max-all-8 double-groupby-1 double-groupby-5 double-groupby-all"
for fmt in victoriametrics influx; do
  mkdir -p $DATA/queries-$fmt
  for t in $TYPES; do
    q=$DATA/queries-$fmt/$t.gob
    [ -s $q ] || tsbs_generate_queries --use-case=devops --seed=123 --scale=$SCALE \
      --timestamp-start=2026-10-01T00:00:00Z --timestamp-end=2026-10-02T00:00:00Z --queries=100 --query-type=$t --format=$fmt > $q
  done
done
$REPO/bench/scripts/dump_queries.sh
