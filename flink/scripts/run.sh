#!/usr/bin/env bash
# Submete a sequência inspirada no mentor sem excluir statements existentes.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../../scripts/_common.sh"
require confluent jq
: "${ENV_ID:?Execute infrastructure/foundation.sh primeiro}"
: "${CLUSTER_ID:?Execute infrastructure/foundation.sh primeiro}"
: "${CC_CLOUD:?Defina CC_CLOUD em .env}"
: "${CC_REGION:?Defina CC_REGION em .env}"
: "${CC_FLINK_POOL_NAME:?Defina CC_FLINK_POOL_NAME em .env}"
FLINK_DIR="$ROOT_DIR/flink/sql"
FLINK_IDLE_TIMEOUT="${FLINK_IDLE_TIMEOUT:-}"
OUT="$ROOT_DIR/evidence/cloud/04-flink.txt"
mkdir -p "$(dirname "$OUT")"

for file in \
  "$FLINK_DIR/00-startup-transactions.sql" \
  "$FLINK_DIR/00-pk-accounts.sql" \
  "$FLINK_DIR/01-enrichment.sql" \
  "$FLINK_DIR/02-append-only.sql" \
  "$FLINK_DIR/04-fraud-detection.sql"; do
  [[ -r "$file" ]] || { echo "SQL ausente ou sem leitura: $file" >&2; exit 1; }
done

if [[ -n "$FLINK_IDLE_TIMEOUT" && ! "$FLINK_IDLE_TIMEOUT" =~ ^[1-9][0-9]*[[:space:]]*(ms|s|m)$ ]]; then
  echo "FLINK_IDLE_TIMEOUT inválido; use, por exemplo, \"5 s\", ou deixe vazio." >&2
  exit 1
fi
if [[ -n "$FLINK_IDLE_TIMEOUT" ]]; then
  echo "Idleness timeout fixo por statement: $FLINK_IDLE_TIMEOUT"
else
  echo "Idleness: detecção progressiva padrão da Confluent"
fi

confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
confluent flink region use --cloud "$CC_CLOUD" --region "$CC_REGION" >/dev/null

# Reutiliza o pool nomeado; na primeira execução, renomeia somente o pool
# legado previamente identificado. Nunca cria compute pool automaticamente.
LEGACY_POOL_ID="lfcp-2268w8o"
LEGACY_POOL_NAME="default.env-mn3vgw.us-east-2"
pools="$(confluent flink compute-pool list --environment "$ENV_ID" -o json)"

target_count="$(jq --arg n "$CC_FLINK_POOL_NAME" '[.[] | select(.name==$n)] | length' <<<"$pools")"
legacy_count="$(jq --arg id "$LEGACY_POOL_ID" --arg n "$LEGACY_POOL_NAME" \
  '[.[] | select(.id==$id or .name==$n)] | length' <<<"$pools")"

if [[ "$target_count" -gt 1 || "$legacy_count" -gt 1 ]]; then
  echo "Há compute pools duplicados/ambíguos no environment '$ENV_ID'. Nenhuma alteração foi feita." >&2
  exit 1
fi

if [[ "$target_count" -eq 1 ]]; then
  pool="$(jq -r --arg n "$CC_FLINK_POOL_NAME" '.[] | select(.name==$n) | .id' <<<"$pools")"
  pool_status="$(jq -r --arg id "$pool" '.[] | select(.id==$id) | .status' <<<"$pools")"
  [[ "$pool_status" == PROVISIONED || "$pool_status" == RUNNING ]] || {
    echo "Compute pool '$CC_FLINK_POOL_NAME' existe, mas está em status '$pool_status'. Interrompendo." >&2
    exit 1
  }
  if [[ "$legacy_count" -eq 1 ]]; then
    echo "Pool legado e pool de destino coexistem. Inspecione manualmente; nada será alterado." >&2
    exit 1
  fi
else
  [[ "$legacy_count" -eq 1 ]] || {
    echo "Pool '$CC_FLINK_POOL_NAME' não encontrado e o pool legado conhecido '$LEGACY_POOL_ID' também não está presente. Nenhum pool será criado." >&2
    exit 1
  }

  legacy="$(jq -c --arg id "$LEGACY_POOL_ID" '.[] | select(.id==$id)' <<<"$pools")"
  [[ "$(jq -r '.name' <<<"$legacy")" == "$LEGACY_POOL_NAME" &&
     "$(jq -r '.environment' <<<"$legacy")" == "$ENV_ID" &&
     "$(jq -r '.cloud | ascii_upcase' <<<"$legacy")" == "$(tr '[:lower:]' '[:upper:]' <<<"$CC_CLOUD" | tr -d '\n')" &&
     "$(jq -r '.region' <<<"$legacy")" == "$CC_REGION" ]] || {
    echo "O pool legado não corresponde ao ID, nome, environment, cloud e região esperados. Interrompendo sem renomear." >&2
    exit 1
  }
  pool_status="$(jq -r '.status' <<<"$legacy")"
  [[ "$pool_status" == PROVISIONED || "$pool_status" == RUNNING ]] || {
    echo "Pool legado está em status '$pool_status'; não será renomeado." >&2
    exit 1
  }

  echo "Renomeando pool existente $LEGACY_POOL_ID para '$CC_FLINK_POOL_NAME' (sem alterar CFUs)."
  confluent flink compute-pool update "$LEGACY_POOL_ID" \
    --environment "$ENV_ID" --name "$CC_FLINK_POOL_NAME" >/dev/null
  pools="$(confluent flink compute-pool list --environment "$ENV_ID" -o json)"
  target_count="$(jq --arg n "$CC_FLINK_POOL_NAME" '[.[] | select(.name==$n)] | length' <<<"$pools")"
  [[ "$target_count" -eq 1 ]] || {
    echo "Não foi possível confirmar unicamente o pool renomeado. Interrompendo." >&2
    exit 1
  }
  pool="$(jq -r --arg n "$CC_FLINK_POOL_NAME" '.[] | select(.name==$n) | .id' <<<"$pools")"
fi

# Confirma novamente identidade e localização antes de persistir o ID local.
selected="$(jq -c --arg id "$pool" '.[] | select(.id==$id)' <<<"$pools")"
[[ "$(jq -r '.name' <<<"$selected")" == "$CC_FLINK_POOL_NAME" &&
   "$(jq -r '.environment' <<<"$selected")" == "$ENV_ID" &&
   "$(jq -r '.cloud | ascii_upcase' <<<"$selected")" == "$(tr '[:lower:]' '[:upper:]' <<<"$CC_CLOUD" | tr -d '\n')" &&
   "$(jq -r '.region' <<<"$selected")" == "$CC_REGION" ]] || {
  echo "Pool selecionado não corresponde ao nome, environment, cloud e região configurados." >&2
  exit 1
}
save_state FLINK_POOL_ID "$pool"

POOL_NAME="$(confluent flink compute-pool describe "$pool" --environment "$ENV_ID" -o json | jq -r '.name // empty')"
[[ "$POOL_NAME" == "$CC_FLINK_POOL_NAME" ]] || {
  echo "A descrição do pool retornou '$POOL_NAME', esperado '$CC_FLINK_POOL_NAME'. Interrompendo." >&2
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
  local -a property_args=()
  if [[ -n "$FLINK_IDLE_TIMEOUT" ]]; then
    property_args=(--property "sql.tables.scan.idle-timeout=$FLINK_IDLE_TIMEOUT")
  fi
  confluent flink statement create "$name" --sql "$(cat "$file")" \
    --compute-pool "$FLINK_POOL_ID" --database "$CLUSTER_ID" \
    "${property_args[@]}"
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
