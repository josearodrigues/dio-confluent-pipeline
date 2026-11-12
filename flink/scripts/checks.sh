#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../../scripts/_common.sh"
require confluent jq
: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
: "${FLINK_POOL_ID:?FLINK_POOL_ID ausente}"
confluent environment use "$ENV_ID" >/dev/null
confluent flink region use --cloud "$CC_CLOUD" --region "$CC_REGION" >/dev/null
OUT="$ROOT_DIR/evidence/homologation/04-flink-checks.txt"
mkdir -p "$(dirname "$OUT")"
{
  echo "Verificação Flink — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  confluent flink query --sql 'SELECT * FROM enriched_payments LIMIT 10' \
    --compute-pool "$FLINK_POOL_ID" --database "$CLUSTER_ID" --environment "$ENV_ID" --output json
  confluent flink query --sql 'SELECT * FROM fraud_alerts LIMIT 10' \
    --compute-pool "$FLINK_POOL_ID" --database "$CLUSTER_ID" --environment "$ENV_ID" --output json
} 2>&1 | tee "$OUT"
