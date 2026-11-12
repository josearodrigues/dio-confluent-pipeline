#!/usr/bin/env bash
# Limpeza destrutiva. Exige IDs e nomes conferidos antes de remover recursos.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq
: "${ENV_ID:?ENV_ID ausente; recuse executar sem estado local válido}"
: "${CLUSTER_ID:?CLUSTER_ID ausente; recuse executar sem estado local válido}"
: "${CC_ENV_NAME:?CC_ENV_NAME ausente}"
: "${CC_CLUSTER_NAME:?CC_CLUSTER_NAME ausente}"
ENV_NAME="$(confluent environment describe "$ENV_ID" -o json | jq -r '.name // empty')"
CLUSTER_NAME="$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.name // empty')"
[[ "$ENV_NAME" == "$CC_ENV_NAME" ]] || { echo "ENV_ID não corresponde ao Environment esperado." >&2; exit 1; }
[[ "$CLUSTER_NAME" == "$CC_CLUSTER_NAME" ]] || { echo "CLUSTER_ID não corresponde ao cluster esperado." >&2; exit 1; }
echo "ATENÇÃO: operação destrutiva. Environment: $ENV_NAME ($ENV_ID)"
echo "Cluster: $CLUSTER_NAME ($CLUSTER_ID)"
read -r -p "Digite exatamente '$ENV_ID/$CLUSTER_ID' para confirmar: " answer
[[ "$answer" == "$ENV_ID/$CLUSTER_ID" ]] || { echo "Cancelado."; exit 1; }
confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$CLUSTER_ID" >/dev/null

for statement in payments-fraud-detection payments-append-only payments-enrichment \
  payments-pk-transactions payments-pk-accounts payments-transactions-startup; do
  confluent flink statement delete "$statement" --force 2>/dev/null || true
done
if [[ -n "${CDC_CONNECTOR_ID:-}" ]]; then
  confluent connect cluster delete "$CDC_CONNECTOR_ID" --force
fi
if [[ -n "${FLINK_POOL_ID:-}" ]]; then
  confluent flink compute-pool delete "$FLINK_POOL_ID" --force
fi
for key in "${WRITER_API_KEY:-}" "${READER_API_KEY:-}" "${SR_API_KEY:-}"; do
  [[ -n "$key" ]] && confluent api-key delete "$key" --force
done
for sa in "${SA_WRITER_ID:-}" "${SA_READER_ID:-}"; do
  [[ -n "$sa" ]] && confluent iam service-account delete "$sa" --force
done
confluent kafka cluster delete "$CLUSTER_ID" --force
confluent environment delete "$ENV_ID" --force
echo "Recursos Confluent solicitados para remoção. Verifique o console e Billing."
echo "A limpeza PostgreSQL é separada e também destrutiva:"
echo "  CONFIRM_CLEANUP=yes ./scripts/run-sql.sh database/cleanup/postgres-cleanup.sql"
