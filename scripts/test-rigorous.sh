#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_MVN_ARGS=( -s .mvn/settings.xml )

contains_done_marker() {
  local file_path="$1"
  local pattern='^\[done\] recordsIngested='

  if command -v rg >/dev/null 2>&1; then
    rg -q "${pattern}" "${file_path}"
  else
    grep -qE "${pattern}" "${file_path}"
  fi
}

run_app() {
  local config_path="$1"
  local extra_java_opts="${2:-}"
  local output_file
  output_file="$(mktemp)"

  echo "[run] java ${extra_java_opts} -jar target/high-throughput-fanout-engine-1.0.0.jar ${config_path}"
  # shellcheck disable=SC2086
  java ${extra_java_opts} -jar target/high-throughput-fanout-engine-1.0.0.jar "${config_path}" | tee "${output_file}"

  if ! contains_done_marker "${output_file}"; then
    echo "Run did not complete successfully for config: ${config_path}" >&2
    rm -f "${output_file}"
    exit 1
  fi
  rm -f "${output_file}"
}

echo "[1/5] Running full unit + integration test suite"
mvn "${CONFIG_MVN_ARGS[@]}" clean test

echo "[2/5] Packaging shaded application jar"
mvn "${CONFIG_MVN_ARGS[@]}" -DskipTests package

echo "[3/5] Executing application against all sample input formats"
run_app "config/application.yaml"
run_app "config/application-jsonl.yaml"
run_app "config/application-fixed-width.yaml"

echo "[4/5] Building large streaming test input"
mkdir -p build/generated
LARGE_INPUT="build/generated/large_customers.csv"
{
  echo "id,name,country,active,balance"
  for i in $(seq 1 50000); do
    echo "${i},user${i},US,true,${i}.5"
  done
} > "${LARGE_INPUT}"

LARGE_CFG="build/generated/application-large.yaml"
cat > "${LARGE_CFG}" <<CFG
input:
  type: CSV
  path: ${LARGE_INPUT}
  delimiter: ","
  hasHeader: true

engine:
  useVirtualThreads: true
  defaultWorkersPerSink: 4
  queueCapacity: 1024
  maxRetries: 3
  retryBackoffMillis: 1
  shutdownTimeoutSeconds: 30
  deadLetterPath: build/dlq/dead-letter-large.jsonl

observability:
  statusIntervalSeconds: 5

sinks:
  - name: rest-large
    type: REST_API
    endpoint: https://mock.rest.internal/v1/records
    rateLimitPerSecond: 100000
    workers: 4
    queueCapacity: 1024
    minLatencyMs: 0
    maxLatencyMs: 0
    failureRate: 0.0
CFG

echo "[5/5] Running constrained-heap streaming smoke test"
run_app "${LARGE_CFG}" "-Xmx512m"

echo "All rigorous checks passed."
