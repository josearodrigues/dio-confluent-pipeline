#!/usr/bin/env bash
# Submete a sequência inspirada no mentor sem excluir statements existentes.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../../scripts/_common.sh"
require confluent jq
: "${ENV_ID:?Execute infrastructure/foundation.sh primeiro}"
: "${CLUSTER_ID:?Execute infrastructure/foundation.sh primeiro}"
FLINK_DIR="$ROOT_DIR/flink/sql"
OUT="$ROOT_DIR/evidence/cloud/04-flink.txt"
mkdir -p "$(dirname "$OUT")"

confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
confluent flink region use --cloud "$CC_CLOUD" --region "$CC_REGION" >/dev/null

# O compute pool pode ter sido excluído; sempre redescobrir pelo nome.
unset FLINK_POOL_ID
if [[ -z "${FLINK_POOL_ID:-}" ]]; then
  pools="$(confluent flink compute-pool list --environment "$ENV_ID" -o json)"
  pool="$(jq -r --arg n "$CC_FLINK_POOL_NAME" '[.[] | select(.name==$n and (.status=="PROVISIONED" or .status=="RUNNING"))][0].id // empty' <<<"$pools")"
  if [[ -z "$pool" ]]; then
    if jq -e --arg n "$CC_FLINK_POOL_NAME" 'any(.[]; .name==$n)' <<<"$pools" >/dev/null; then
      echo "Compute pool '$CC_FLINK_POOL_NAME' existe, mas não está disponível. Inspecione-o antes de criar outro." >&2
      exit 1
    fi
    pool="$(confluent flink compute-pool create "$CC_FLINK_POOL_NAME" \
      --cloud "$CC_CLOUD" --region "$CC_REGION" --max-cfu 5 -o json | jq -r '.id')"
  fi
  save_state FLINK_POOL_ID "$pool"
fi

POOL_NAME="$(confluent flink compute-pool describe "$FLINK_POOL_ID" -o json | jq -r '.name // empty')"
[[ "$POOL_NAME" == "$CC_FLINK_POOL_NAME" ]] || {
  echo "FLINK_POOL_ID aponta para '$POOL_NAME', esperado '$CC_FLINK_POOL_NAME'. Revise .state.env." >&2
  exit 1
}
POOL_STATUS=""
for _ in $(seq 1 60); do
  POOL_STATUS="$(confluent flink compute-pool describe "$FLINK_POOL_ID" -o json | jq -r '.status')"
  [[ "$POOL_STATUS" == PROVISIONED || "$POOL_STATUS" == RUNNING ]] && break
  [[ "$POOL_STATUS" == FAILED ]] && { echo "Compute pool em FAILED." >&2; exit 1; }
  sleep 10
done
[[ "$POOL_STATUS" == PROVISIONED || "$POOL_STATUS" == RUNNING ]] || {
  echo "Compute pool não ficou disponível (status=$POOL_STATUS)." >&2; exit 1;
}

submit() {
  local name="$1" file="$2" expected="$3" status="" detail="" data
  data="$(confluent flink statement list --compute-pool "$FLINK_POOL_ID" --environment "$ENV_ID" -o json)"
  if jq -e --arg n "$name" 'any(.[]; .name==$n)' <<<"$data" >/dev/null; then
    echo "Statement '$name' já existe. Interrompendo sem sobrescrever." >&2
    return 1
  fi
  echo "Submetendo $name"
  confluent flink statement create "$name" --sql "$(cat "$file")" \
    --compute-pool "$FLINK_POOL_ID" --database "$CLUSTER_ID"
  for _ in $(seq 1 60); do
    data="$(confluent flink statement describe "$name" -o json)"
    status="$(jq -r '.status // .phase // empty' <<<"$data")"
    detail="$(jq -r '.status_detail // .detail // empty' <<<"$data")"
    [[ "$status" == "$expected" ]] && return 0
    [[ "$status" == FAILED || "$status" == STOPPED ]] && {
      echo "Statement $name: $status $detail" >&2; return 1;
    }
    sleep 5
  done
  echo "Timeout aguardando $name (status: $status)" >&2
  return 1
}

{
  # A carga inicial CDC deve estar concluída antes de iniciar esta sequência.
  submit payments-transactions-startup "$FLINK_DIR/00-startup-transactions.sql" COMPLETED
  submit payments-pk-accounts "$FLINK_DIR/00-pk-accounts.sql" COMPLETED
  submit payments-enrichment "$FLINK_DIR/01-enrichment.sql" RUNNING
  submit payments-append-only "$FLINK_DIR/02-append-only.sql" COMPLETED
  submit payments-fraud-detection "$FLINK_DIR/04-fraud-detection.sql" RUNNING
  echo
  confluent flink statement list --compute-pool "$FLINK_POOL_ID" --environment "$ENV_ID"
} 2>&1 | tee "$OUT"
