#!/usr/bin/env bash
# Run LoadIndices in parallel over disjoint URN-prefix ranges of the bench dataset.
# LoadIndices processes on a single thread (~500 aspects/s), so one run over 3.6M aspects takes
# ~2 h; N ranges in parallel divide that. Usage: ./load_indices_parallel.sh [parallelism] [logdir]
set -euo pipefail
cd "$(dirname "$0")"

PAR=${1:-6}
LOGDIR=${2:-results/load_indices_logs}
mkdir -p "$LOGDIR"
P='urn:li:dataset:(urn:li:dataPlatform:snowflake,bench.'

# Disjoint and complete for dataset/v1, largest first. LIKE's "_" matches any single character,
# which is harmless here because every pattern still starts with a distinct literal family name.
RANGES=(prod_like.l3% prod_like.l2% deep100% wide.% prod_like.l4% mid.% small% prod_like.l5%
  prod_like.l1% hub.% deep.% scc.% prod_like.l6% prod_like.l7% prod_like.l8% prod_like.l9%
  prod_like.root%)

run_one() {
  local range=$1 name
  name=$(echo "$range" | tr -c 'a-z0-9_\n' '_')
  if DATAHUB_VERSION=head docker compose -p datahub -f docker-compose.bench.yml --profile quickstart \
    run --rm --no-deps \
    -e ES_BULK_REQUESTS_LIMIT=5000 -e ES_BULK_FLUSH_PERIOD=300 -e ELASTICSEARCH_THREAD_COUNT=4 \
    -e JAVA_OPTS=-Xmx2g \
    system-update-quickstart -u LoadIndices -a batchSize=5000 -a "urnLike=${P}${range}" \
    >"$LOGDIR/$name.log" 2>&1; then
    echo "$(date +%H:%M:%S) done $range"
  else
    echo "$(date +%H:%M:%S) FAILED $range (see $LOGDIR/$name.log)"
  fi
}

start=$(date +%s)
# bash 3.2 (macOS) has no `wait -n`, so deal ranges round-robin into PAR worker queues.
for w in $(seq 0 $((PAR - 1))); do
  (
    i=0
    for r in "${RANGES[@]}"; do
      if [ $((i % PAR)) -eq "$w" ]; then run_one "$r"; fi
      i=$((i + 1))
    done
  ) &
done
wait
echo "ALL DONE in $(($(date +%s) - start))s"
