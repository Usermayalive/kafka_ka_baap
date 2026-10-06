#!/usr/bin/env bash
# StormLog vs Apache Kafka on the same machine and workload: one broker process each, one
# partition, acks=1, page-cache durability (both systems' default), 1 GB heap each.
#
# Kafka runs twice per record size:
#   kafka-unbatched  batch.size=0, linger.ms=0: one record per request, like StormLog's client
#   kafka-batched    producer defaults (batching): what Kafka users actually get
# Both producers keep up to 5 requests in flight (Kafka's default max.in.flight).
#
#   KAFKA_HOME=/path/to/kafka_2.13-4.3.1 ./scripts/bench-vs-kafka.sh [out.csv]
set -euo pipefail
cd "$(dirname "$0")/.."

: "${KAFKA_HOME:?set KAFKA_HOME to an extracted Apache Kafka 4.x distribution}"
OUT=${1:-target-cli/bench/vs-kafka.csv}
WORK=${WORK:-$(mktemp -d)}
JAR=target-cli/simple-kafka-1.0-SNAPSHOT.jar
SIZES=${SIZES:-"64 256 1024 4096 16384"}
LATENCY_RATE=${LATENCY_RATE:-5000}   # records/s for the fixed-rate latency pass (below both systems' saturation)
LATENCY_RECORDS=${LATENCY_RECORDS:-50000}
DISK=${DISK:-disk0}
mkdir -p "$(dirname "$OUT")"
echo "system,mode,record_size,records,seconds,records_per_sec,mb_per_sec,p50_ms,p99_ms,broker_cpu_cores,disk_mb_written" > "$OUT"

records_for() { # keep each run to a few seconds
  case $1 in 64|256) echo 300000 ;; 1024) echo 200000 ;; 4096) echo 100000 ;; *) echo 40000 ;; esac
}
cpu_seconds() { # cumulative CPU time of a process, from ps TIME ([[dd-]hh:]mm:ss.ss)
  ps -o time= -p "$1" | awk -F'[:-]' '{ s=0; for (i=1;i<=NF;i++) s=s*60+$i; print s }'
}
disk_mb() { iostat -Id "$DISK" | tail -1 | awk '{print $3}'; }

measure() { # measure PID SYSTEM MODE SIZE RECORDS COMMAND... ; command prints records/s,MB/s,p50,p99,seconds
  local pid=$1 system=$2 mode=$3 size=$4 records=$5; shift 5
  local cpu0 disk0 t0 out cpu1 disk1 t1
  cpu0=$(cpu_seconds "$pid"); disk0=$(disk_mb); t0=$(python3 -c 'import time;print(time.time())')
  out=$("$@")
  t1=$(python3 -c 'import time;print(time.time())'); cpu1=$(cpu_seconds "$pid"); disk1=$(disk_mb)
  python3 - "$out" "$system" "$mode" "$size" "$records" "$cpu0" "$cpu1" "$t0" "$t1" "$disk0" "$disk1" >> "$OUT" <<'PY'
import sys
out, system, mode, size, records, cpu0, cpu1, t0, t1, d0, d1 = sys.argv[1:]
rps, mbps, p50, p99, secs = out.strip().split(",")
cores = (float(cpu1) - float(cpu0)) / (float(t1) - float(t0))
print(f"{system},{mode},{size},{records},{secs},{rps},{mbps},{p50},{p99},{cores:.2f},{float(d1) - float(d0):.1f}")
PY
  tail -1 "$OUT"
}

# ---------------- StormLog ----------------
stormlog_perf() { # size records [rate] -> rps,mbps,p50,p99,seconds
  java -cp "$JAR" com.simplekafka.cli.StormLogPerf 127.0.0.1:29092 perf "$2" "$1" 5 plain "${3:-0}" 2>/dev/null \
    | awk -F, '{print $4","$5","$6","$7","$3}'
}
echo "== StormLog (work dir $WORK)"
STORMLOG_QUOTA_BYTES_PER_SEC=0 STORMLOG_QUOTA_REQUESTS_PER_SEC=0 \
  java -Xms1g -Xmx1g --enable-native-access=ALL-UNNAMED -Dstormlog.data.dir="$WORK/stormlog" \
  -jar "$JAR" 1 127.0.0.1 29092 29191 > "$WORK/stormlog.log" 2>&1 &
SL_PID=$!
trap 'kill $SL_PID 2>/dev/null || true; [ -n "${KF_PID:-}" ] && kill $KF_PID 2>/dev/null || true' EXIT
for _ in $(seq 1 120); do curl -s "http://127.0.0.1:30092/admin/quorum" | grep -q '"leader":1' && break; sleep 0.25; done
java -cp "$JAR" com.simplekafka.cli.StormLogCli --bootstrap 127.0.0.1:29092 create-topic perf 1 >/dev/null 2>&1
stormlog_perf 1024 50000 >/dev/null # warmup (JIT)
for size in $SIZES; do
  n=$(records_for "$size")
  measure "$SL_PID" stormlog unbatched "$size" "$n" stormlog_perf "$size" "$n"
done
for size in $SIZES; do
  measure "$SL_PID" stormlog "fixed-rate-$LATENCY_RATE" "$size" "$LATENCY_RECORDS" stormlog_perf "$size" "$LATENCY_RECORDS" "$LATENCY_RATE"
done
kill $SL_PID; wait $SL_PID 2>/dev/null || true

# ---------------- Apache Kafka ----------------
kafka_perf() { # size records throughput extra-producer-props... -> rps,mbps,p50,p99,seconds
  local size=$1 records=$2 throughput=$3; shift 3
  local start end
  start=$(python3 -c 'import time;print(time.time())')
  "$KAFKA_HOME/bin/kafka-producer-perf-test.sh" --topic perf --num-records "$records" --record-size "$size" \
    --throughput "$throughput" --producer-props bootstrap.servers=127.0.0.1:29192 acks=1 "$@" 2>/dev/null | tail -1 \
    | python3 -c '
import re, sys
line = sys.stdin.read()
rps = re.search(r"([\d.]+) records/sec", line).group(1)
mbps = re.search(r"\(([\d.]+) MB/sec\)", line).group(1)
p50 = re.search(r"(\d+) ms 50th", line).group(1)
p99 = re.search(r"(\d+) ms 99th", line).group(1)
secs = int(re.search(r"^(\d+) records sent", line).group(1)) / float(rps)
print(f"{rps},{mbps},{p50},{p99},{secs:.3f}")'
}
echo "== Apache Kafka $("$KAFKA_HOME/bin/kafka-topics.sh" --version 2>/dev/null | head -1)"
mkdir -p "$WORK/kafka"
cat > "$WORK/kafka/server.properties" <<PROPS
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:29193
listeners=PLAINTEXT://127.0.0.1:29192,CONTROLLER://127.0.0.1:29193
advertised.listeners=PLAINTEXT://127.0.0.1:29192
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=$WORK/kafka/logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
PROPS
"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" \
  -c "$WORK/kafka/server.properties" --standalone >/dev/null 2>&1 \
  || "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" \
       -c "$WORK/kafka/server.properties" >/dev/null
KAFKA_HEAP_OPTS="-Xms1g -Xmx1g" "$KAFKA_HOME/bin/kafka-server-start.sh" "$WORK/kafka/server.properties" > "$WORK/kafka.log" 2>&1 &
sleep 1
KF_PID=$(pgrep -f "kafka.Kafka $WORK/kafka/server.properties" | head -1)
for _ in $(seq 1 120); do "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:29192 --list >/dev/null 2>&1 && break; sleep 0.5; done
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:29192 --create --topic perf --partitions 1 --replication-factor 1 >/dev/null
kafka_perf 1024 50000 -1 batch.size=0 linger.ms=0 >/dev/null # warmup
for size in $SIZES; do
  n=$(records_for "$size")
  measure "$KF_PID" kafka unbatched "$size" "$n" kafka_perf "$size" "$n" -1 batch.size=0 linger.ms=0
  measure "$KF_PID" kafka batched "$size" "$n" kafka_perf "$size" "$n" -1
done
for size in $SIZES; do
  measure "$KF_PID" kafka "fixed-rate-$LATENCY_RATE" "$size" "$LATENCY_RECORDS" \
    kafka_perf "$size" "$LATENCY_RECORDS" "$LATENCY_RATE" batch.size=0 linger.ms=0
done
echo "Results: $OUT"
