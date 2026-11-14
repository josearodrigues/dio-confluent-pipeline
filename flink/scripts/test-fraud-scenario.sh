#!/usr/bin/env bash
# Consulta somente os alertas no tópico/tabela de saída do Flink.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../../scripts/_common.sh"
require confluent

: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
: "${FLINK_POOL_ID:?FLINK_POOL_ID ausente}"
: "${CC_CLOUD:?CC_CLOUD ausente}"
: "${CC_REGION:?CC_REGION ausente}"

confluent flink query \
  --sql 'SELECT *
FROM `desafio.fraud.detected`;' \
  --compute-pool "$FLINK_POOL_ID" \
  --database "$CLUSTER_ID" \
  --environment "$ENV_ID" \
  --cloud "$CC_CLOUD" \
  --region "$CC_REGION" \
  --timeout 2m \
  --output json
