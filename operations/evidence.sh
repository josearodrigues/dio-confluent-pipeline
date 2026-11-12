#!/usr/bin/env bash
# Coleta o estado dos recursos criados no ambiente selecionado.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent
: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
OUT="$ROOT_DIR/evidence/cloud/operational-state.txt"
mkdir -p "$ROOT_DIR/evidence/cloud"
{
  echo "Coleta: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Environment: $ENV_ID"
  confluent kafka cluster describe "$CLUSTER_ID"
  echo
  confluent connect cluster list
  echo
  [[ -n "${FLINK_POOL_ID:-}" ]] && confluent flink compute-pool describe "$FLINK_POOL_ID"
  echo
  [[ -n "${FLINK_POOL_ID:-}" ]] && confluent flink statement list --compute-pool "$FLINK_POOL_ID" --environment "$ENV_ID"
} 2>&1 | tee "$OUT"
