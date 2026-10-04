#!/bin/bash
# One-time setup outside the repo ($W, default ~/metricsdb-work): a project-local JDK 21 and Go,
# TSBS's generators, loaders and query runners, and the tsbs-dump helper. Linux (WSL) only.
set -e
W=${W:-$HOME/metricsdb-work}
mkdir -p $W/tools/bin $W/data $W/logs
cd $W/tools
[ -x jdk21/bin/java ] || { curl -fsSL -o jdk.tgz "https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse"; mkdir -p jdk21; tar xzf jdk.tgz -C jdk21 --strip-components=1; rm jdk.tgz; }
[ -x go/bin/go ] || { curl -fsSL -o go.tgz https://go.dev/dl/go1.23.4.linux-amd64.tar.gz; tar xzf go.tgz; rm go.tgz; }
cat > $W/env.sh <<ENV
export W=$W
export JAVA_HOME=$W/tools/jdk21
export GOROOT=$W/tools/go GOPATH=$W/tools/gopath GOMODCACHE=$W/tools/gopath/pkg/mod GOCACHE=$W/tools/gocache
export GRADLE_USER_HOME=$W/gradle
export PATH="$W/tools/jdk21/bin:$W/tools/go/bin:$W/tools/bin:\$PATH"
ENV
source $W/env.sh
[ -d tsbs ] || git clone -q https://github.com/timescale/tsbs.git
(cd tsbs && for c in tsbs_generate_data tsbs_generate_queries tsbs_load_victoriametrics tsbs_load_influx tsbs_run_queries_victoriametrics tsbs_run_queries_influx; do go build -o ../bin/$c ./cmd/$c; done)
REPO=$(cd "$(dirname "$0")/../.." && pwd)
(cd $REPO/bench/tsbs-dump && go mod edit -replace github.com/timescale/tsbs=$W/tools/tsbs && go mod tidy >/dev/null 2>&1; go build -o $W/tools/bin/tsbs-dump .)
echo "tools ready in $W/tools; next: bench/scripts/gen_tsbs.sh"
