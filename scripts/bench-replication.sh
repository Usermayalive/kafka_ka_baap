#!/usr/bin/env bash
# Cost of replication on a 3-broker cluster (separate JVMs, one machine, loopback):
# RF=1, RF=3 with acks=1, RF=3 with acks=all. One partition led by broker 1, 1 KB records,
# one producer connection with 5 requests in flight; plus p50/p99 at a fixed 5,000 records/s.
#   ./scripts/bench-replication.sh [out.csv]
set -euo pipefail
cd "$(dirname "$0")/.."
JAR=target-cli/simple-kafka-1.0-SNAPSHOT.jar
OUT=${1:-target-cli/bench/replication.csv}
WORK=${WORK:-$(mktemp -d)}
HOST=127.0.0.1
QUORUM="1@$HOST:29291,2@$HOST:29292,3@$HOST:29293"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; for p in "${PIDS[@]:-}"; do wait "$p" 2>/dev/null || true; done; }
trap cleanup EXIT
for id in 1 2 3; do
  STORMLOG_QUORUM="$QUORUM" STORMLOG_QUOTA_BYTES_PER_SEC=0 STORMLOG_QUOTA_REQUESTS_PER_SEC=0 \
    java -Xms1g -Xmx1g --enable-native-access=ALL-UNNAMED -Dstormlog.data.dir="$WORK" \
    -jar "$JAR" "$id" "$HOST" "2919$((id + 1))" "2929$id" > "$WORK/broker-$id.log" 2>&1 &
  PIDS+=($!)
done
cli() { java -cp "$JAR" com.simplekafka.cli.StormLogCli --bootstrap "$HOST:29192" "$@" 2>/dev/null; }
for _ in $(seq 1 240); do cli describe | grep -o 'id=[123],' | sort -u | wc -l | grep -q 3 && break; sleep 0.25; done
cli create-topic rf1 1 1 >/dev/null
cli create-topic rf3 1 3 >/dev/null
sleep 3 # followers start fetching
perf() { java -cp "$JAR" com.simplekafka.cli.StormLogPerf "$HOST:29192" "$@" 2>/dev/null; }
mkdir -p "$(dirname "$OUT")"
echo "config,records,record_size,seconds,records_per_sec,mb_per_sec,p50_ms,p99_ms,errors,rate" > "$OUT"
run() { # label topic acks [mode]
  perf "$2" 50000 1024 5 "${4:-plain}" 0 "$3" >/dev/null # warmup
  for spec in "200000 0 max" "150000 5000 5000"; do
    set -- "$1" "$2" "$3" "${4:-plain}" $spec
    line="$(perf "$2" "$5" 1024 5 "$4" "$6" "$3")"
    errors=$(echo "$line" | cut -d, -f8)
    if [ "$errors" != "0" ]; then echo "FAILED: $1 had $errors errored requests: $line" >&2; exit 1; fi
    echo "$1,$line,$7" | tee -a "$OUT"
  done
}
run "RF=1 acks=1" rf1 1
run "RF=3 acks=1" rf3 1
run "RF=3 acks=all" rf3 all
run "RF=3 acks=all batched" rf3 all batched
