#!/usr/bin/env bash
set -euo pipefail

if [ -z "${MI_HOME:-}" ]; then
  echo "Error: set MI_HOME to your WSO2 MI 4.4.0 installation before running this script." >&2
  echo "  export MI_HOME=/path/to/wso2mi-4.4.0" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$SCRIPT_DIR/target/classes"
JAR_NAME="async-to-sync-integration-mediator-1.0.0.jar"

rm -rf "$SCRIPT_DIR/target"
mkdir -p "$OUT_DIR"

# Wildcarded on purpose: the exact WSO2 update-level jar filenames (e.g.
# synapse-core_4.0.0.wso2v215_20.jar vs ..._7.jar) vary across installations depending on how many
# WSO2 updates have been applied. Putting every jar in these two directories on the classpath finds
# whatever version is actually present instead of hardcoding one patch suffix that only matches a
# single specific install.
CLASSPATH="$(find "$MI_HOME/wso2/components/plugins" "$MI_HOME/wso2/lib" -name '*.jar' | tr '\n' ':')"

javac -source 8 -target 8 -cp "$CLASSPATH" -d "$OUT_DIR" \
  "$SCRIPT_DIR"/src/main/java/com/wso2cre/asyncdemo/*.java

jar cf "$SCRIPT_DIR/target/$JAR_NAME" -C "$OUT_DIR" .

echo "Built: $SCRIPT_DIR/target/$JAR_NAME"
