#!/bin/bash
# Start or stop one database at a time on this machine: db.sh start|stop metricsdb|vm|influx
# Comparison databases run in Docker with their defaults; their volumes live under $W and are
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
    rm -rf $W/run/vm && mkdir -p $W/run/vm
    docker run -d --name mdb-vm -p 127.0.0.1:8428:8428 -v $W/run/vm:/victoria-metrics-data $VM_IMAGE >/dev/null
    wait_http localhost:8428/health ;;
  stop:vm)
    docker rm -f mdb-vm >/dev/null 2>&1; [ -n "$KEEP" ] || sudo -n rm -rf $W/run/vm 2>/dev/null || docker run --rm -v $W/run:/w alpine rm -rf /w/vm ;;
  start:influx)
    rm -rf $W/run/influx && mkdir -p $W/run/influx
    docker run -d --name mdb-influx -p 127.0.0.1:8086:8086 -v $W/run/influx:/var/lib/influxdb $INFLUX_IMAGE >/dev/null
    wait_http localhost:8086/ping ;;
  stop:influx)
    docker rm -f mdb-influx >/dev/null 2>&1; [ -n "$KEEP" ] || sudo -n rm -rf $W/run/influx 2>/dev/null || docker run --rm -v $W/run:/w alpine rm -rf /w/influx ;;
  *) echo "usage: db.sh start|stop metricsdb|vm|influx"; exit 2 ;;
esac
