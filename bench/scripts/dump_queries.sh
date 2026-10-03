#!/bin/bash
# Converts the TSBS query files (gob) into JSON lines the Java harness replays.
source "$(dirname "$0")/env.sh"
for fmt in victoriametrics influx; do
  mkdir -p $DATA/q-$fmt
  for f in $DATA/queries-$fmt/*.gob; do
    tsbs-dump $f > $DATA/q-$fmt/$(basename $f .gob).jsonl
  done
done
head -c 400 $DATA/q-victoriametrics/double-groupby-1.jsonl; echo
head -c 400 $DATA/q-influx/double-groupby-1.jsonl; echo
