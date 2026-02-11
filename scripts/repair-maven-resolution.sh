#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
M2_REPO="${HOME}/.m2/repository"

if ! command -v mvn >/dev/null 2>&1; then
  echo "mvn not found in PATH. Install Maven first."
  exit 1
fi

echo "[1/4] Removing stale Maven resolver cache markers (*.lastUpdated)..."
if [ -d "$M2_REPO" ]; then
  find "$M2_REPO" -name "*.lastUpdated" -type f -delete || true
fi

echo "[2/4] Removing cached failed compiler-plugin metadata (if present)..."
rm -rf "$M2_REPO/org/apache/maven/plugins/maven-compiler-plugin/3.13.0" || true

echo "[3/4] Resolving all plugins/dependencies with project settings..."
cd "$PROJECT_ROOT"
mvn -s .mvn/settings.xml -U -e -DskipTests dependency:go-offline

echo "[4/4] Running clean validation build..."
mvn -s .mvn/settings.xml -U -e clean test -DskipTests

echo "Maven resolution repair completed successfully."
