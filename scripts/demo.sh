#!/bin/bash
# The Grafana demo: MetricsDB on 127.0.0.1:9201, a real Prometheus (127.0.0.1:9391) scraping
# MetricsDB's /metrics and remote-writing into MetricsDB, and Grafana (127.0.0.1:3301) reading
# MetricsDB as a Prometheus data source. With RENDER=1 it renders both dashboards to docs/.
#   scripts/demo.sh up | render | down
set -e
cd "$(dirname "$0")/.."
REPO=$PWD
W=${W:-$HOME/metricsdb-work}
[ -f $W/env.sh ] && source $W/env.sh   # a project-local JDK, if there is one
JAR=$REPO/server/build/libs/metricsdb.jar
case ${1:-up} in
  up)
    mkdir -p $W/run/demo
    nohup java -Xmx1g -jar $JAR --server.port=9201 --metricsdb.data-dir=$W/run/demo/data > $W/run/demo/node.log 2>&1 &
    echo $! > $W/run/demo/pid
    for i in $(seq 1 240); do curl -sf localhost:9201/internal/health >/dev/null && break; sleep 0.5; done
    # the benchmark's first two hours, for the DevOps dashboard
    if [ -f $W/data/tsbs/data-victoriametrics-s100.txt ]; then
      bench/build/install/bench/bin/bench load --file $W/data/tsbs/data-victoriametrics-s100.txt \
        --url http://localhost:9201/write --limit-lines 648000 --workers 2 --label demo > /dev/null
    fi
    docker compose -f deploy/docker-compose.yml up -d
    echo "Grafana: http://127.0.0.1:3301  Prometheus: http://127.0.0.1:9391  MetricsDB: http://127.0.0.1:9201"
    ;;
  render)
    mkdir -p docs
    curl -s -o docs/grafana-self-monitoring.png "http://127.0.0.1:3301/render/d/metricsdb-self/x?orgId=1&from=now-20m&to=now&width=1400&height=1500&kiosk"
    curl -s -o docs/grafana-tsbs.png "http://127.0.0.1:3301/render/d/tsbs-devops/x?orgId=1&from=1790812800000&to=1790820000000&width=1400&height=700&kiosk"
    ls -la docs/*.png
    ;;
  down)
    docker compose -f deploy/docker-compose.yml down -v
    [ -f $W/run/demo/pid ] && kill $(cat $W/run/demo/pid) 2>/dev/null || true
    rm -rf $W/run/demo
    ;;
esac
