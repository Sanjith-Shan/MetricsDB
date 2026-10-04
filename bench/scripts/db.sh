#!/bin/bash
# Start or stop one database at a time on this machine: db.sh start|stop metricsdb|vm|influx
# Comparison databases run in Docker with their defaults; their data lives in named Docker volumes (inside WSL) that are
# deleted on stop so disk use stays bounded.
source "$(dirname "$0")/env.sh"
cmd=$1; db=$2
wait_http() { for i in $(seq 1 240); do curl -sf "$1" >/dev/null && return 0; sleep 0.5; done; echo "timeout waiting for $1"; return 1; }
case "$cmd:$db" in
  start:metricsdb)
    rm -rf $W/run/metricsdb && mkdir -p $W/run/metricsdb
    nohup java ${MDB_JAVA_OPTS:--Xmx2g} -jar $JAR --server.port=9201 --metricsdb.data-dir=$W/run/metricsdb/data $MDB_ARGS > $W/run/metricsdb/node.log 2>&1 &
    echo $! > $W/run/metricsdb/pid
    wait_http localhost:9201/internal/health ;;
  stop:metricsdb)
    [ -f $W/run/metricsdb/pid ] && kill $(cat $W/run/metricsdb/pid) 2>/dev/null; sleep 2
    [ -n "$KEEP" ] || rm -rf $W/run/metricsdb ;;
  start:vm)
    docker rm -f mdb-vm >/dev/null 2>&1; docker volume rm -f mdb-vm >/dev/null 2>&1; docker volume create mdb-vm >/dev/null
    docker run -d --name mdb-vm -p 127.0.0.1:8428:8428 -v mdb-vm:/victoria-metrics-data $VM_IMAGE >/dev/null
    wait_http localhost:8428/health ;;
  stop:vm)
    if [ -n "$KEEP" ]; then docker stop mdb-vm >/dev/null 2>&1; else docker rm -f mdb-vm >/dev/null 2>&1; docker volume rm -f mdb-vm >/dev/null 2>&1; fi ;;
  start:influx)
    docker rm -f mdb-influx >/dev/null 2>&1; docker volume rm -f mdb-influx >/dev/null 2>&1; docker volume create mdb-influx >/dev/null
    docker run -d --name mdb-influx -p 127.0.0.1:8086:8086 -e INFLUXDB_REPORTING_DISABLED=true -v mdb-influx:/var/lib/influxdb $INFLUX_IMAGE >/dev/null
    wait_http localhost:8086/ping ;;
  stop:influx)
    if [ -n "$KEEP" ]; then docker stop mdb-influx >/dev/null 2>&1; else docker rm -f mdb-influx >/dev/null 2>&1; docker volume rm -f mdb-influx >/dev/null 2>&1; fi ;;
  *) echo "usage: db.sh start|stop metricsdb|vm|influx"; exit 2 ;;
esac
