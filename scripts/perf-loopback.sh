#!/bin/bash
# Loopback throughput for one tagged build. Does not touch the live service.
# Usage: scripts/perf-loopback.sh LABEL [pipe_size] [seconds] [runs]
set -euo pipefail

LABEL=${1:-untitled}
PIPE=${2:-262144}
SECONDS_EACH=${3:-10}
RUNS=${4:-3}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${PUNCH_BIN:-$ROOT/target/release/punch}
LOADGEN=${LOADGEN:-/tmp/punch-loadgen}
KEY=perf-key-0123456789
CTRL=127.0.0.1:17100
DATA=127.0.0.1:17101
WEB=127.0.0.1:17102
BENCH=127.0.0.1:19190
PUBLIC=17180
WORKDIR=$(mktemp -d /tmp/punch-perf-run.XXXXXX)
SERVER_PID=""
CLIENT_PID=""

cleanup() {
    if [[ -n "$CLIENT_PID" ]]; then kill "$CLIENT_PID" 2>/dev/null || true; fi
    if [[ -n "$SERVER_PID" ]]; then kill "$SERVER_PID" 2>/dev/null || true; fi
    wait "$CLIENT_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

if [[ ! -x "$BIN" ]]; then
    echo "missing binary: $BIN" >&2
    exit 1
fi
if [[ ! -x "$LOADGEN" ]]; then
    cc -O2 -pthread -o "$LOADGEN" "$ROOT/scripts/punch-loadgen.c"
fi

ulimit -n 1048576 || true
cat > "$WORKDIR/punch-server.yaml" << EOF
control_listen: "$CTRL"
data_listen: "$DATA"
api_key: "$KEY"
web:
  listen: "$WEB"
  cert: "$WORKDIR/cert.pem"
  key: "$WORKDIR/key.pem"
tcp:
  nodelay: true
  recv_buffer: 4194304
  send_buffer: 4194304
  pipe_size: $PIPE
  backlog: 4096
pool:
  idle: 32
  max_idle: 64
EOF
cat > "$WORKDIR/punch-clients.json" << EOF
{
  "clients": [
    {"name": "perf", "secret": "$KEY", "allow_maps": true}
  ],
  "maps": []
}
EOF

"$BIN" server -c "$WORKDIR/punch-server.yaml" > "$WORKDIR/server.log" 2>&1 &
SERVER_PID=$!
"$BIN" client --server "$CTRL" --secret "$KEY" \
    --bench "$BENCH" --expose-bench "127.0.0.1:$PUBLIC" --pool 32 \
    > "$WORKDIR/client.log" 2>&1 &
CLIENT_PID=$!

ready=0
for _ in $(seq 1 100); do
    if bash -c "echo >/dev/tcp/127.0.0.1/$PUBLIC" 2>/dev/null; then
        ready=1
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null || ! kill -0 "$CLIENT_PID" 2>/dev/null; then
        echo "process exited early" >&2
        cat "$WORKDIR/server.log" "$WORKDIR/client.log" >&2
        exit 1
    fi
    sleep 0.1
done
if [[ "$ready" != 1 ]]; then
    echo "public port did not open" >&2
    cat "$WORKDIR/server.log" "$WORKDIR/client.log" >&2
    exit 1
fi

cpu_ticks() {
    awk '{print $14 + $15}' "/proc/$1/stat"
}

# One short pass so the pool and pipes exist before the measured runs.
"$LOADGEN" 127.0.0.1 "$PUBLIC" GET 262144 2 24 > "$WORKDIR/warmup.txt"
grep -E 'errors=0$' "$WORKDIR/warmup.txt" >/dev/null

HZ=$(getconf CLK_TCK)
echo "label=$LABEL pipe=$PIPE runs=$RUNS seconds=$SECONDS_EACH"
for n in $(seq 1 "$RUNS"); do
    loadf="$WORKDIR/load-$n.txt"
    s1=$(cpu_ticks "$SERVER_PID")
    c1=$(cpu_ticks "$CLIENT_PID")
    "$LOADGEN" 127.0.0.1 "$PUBLIC" GET 262144 "$SECONDS_EACH" 24 > "$loadf"
    s2=$(cpu_ticks "$SERVER_PID")
    c2=$(cpu_ticks "$CLIENT_PID")
    python3 - "$n" "$loadf" "$s1" "$s2" "$c1" "$c2" "$HZ" << 'PY'
import sys
n, loadf, s1, s2, c1, c2, hz = sys.argv[1:]
vals = {}
for line in open(loadf):
    for part in line.split():
        if "=" in part:
            k, v = part.split("=", 1)
            vals[k] = v
if vals.get("errors") != "0":
    sys.stderr.write(open(loadf).read())
    raise SystemExit(1)
elapsed = float(vals["elapsed_s"])
hz = float(hz)
server = (int(s2) - int(s1)) / hz / elapsed
client = (int(c2) - int(c1)) / hz / elapsed
print(
    "run={n} gbps={gbps} rps={rps} errors=0 p50_us={p50} p99_us={p99} max_us={max} "
    "elapsed_s={elapsed} bytes={bytes} reqs={reqs} server_cores={server:.3f} client_cores={client:.3f}".format(
        n=n,
        gbps=vals["gbps"],
        rps=vals["rps"],
        p50=vals["p50_us"],
        p99=vals["p99_us"],
        max=vals["max_us"],
        elapsed=vals["elapsed_s"],
        bytes=vals["bytes"],
        reqs=vals["reqs"],
        server=server,
        client=client,
    )
)
PY
done
