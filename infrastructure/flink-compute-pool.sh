#!/usr/bin/env bash
# Cria ou valida o Flink Compute Pool configurado, sem alterar pools existentes.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq

: "${ENV_ID:?ENV_ID ausente; execute infrastructure/foundation.sh primeiro}"
: "${CC_CLOUD:?CC_CLOUD ausente no .env}"
: "${CC_REGION:?CC_REGION ausente no .env}"
: "${CC_FLINK_POOL_NAME:?CC_FLINK_POOL_NAME ausente no .env}"

MAX_CFU="${FLINK_MAX_CFU:-5}"
[[ "$MAX_CFU" =~ ^[1-9][0-9]*$ ]] || {
  echo "FLINK_MAX_CFU deve ser um inteiro positivo." >&2
  exit 1
}

EVIDENCE="$ROOT_DIR/evidence/cloud/04-flink-compute-pool.txt"
mkdir -p "$(dirname "$EVIDENCE")"

main() {
  local pools count pool pool_json status actual_name actual_env actual_cloud actual_region
  local legacy_id="lfcp-2268w8o" legacy_name="default.env-mn3vgw.us-east-2"
  local legacy_count legacy legacy_status legacy_max_cfu
  echo "Environment: $ENV_ID"
  echo "Compute pool desejado: $CC_FLINK_POOL_NAME"
  echo "Cloud/região: $CC_CLOUD/$CC_REGION"

  pools="$(confluent flink compute-pool list --environment "$ENV_ID" -o json)"
  count="$(jq --arg n "$CC_FLINK_POOL_NAME" '[.[] | select(.name==$n)] | length' <<<"$pools")"
  [[ "$count" -le 1 ]] || {
    echo "Há mais de um compute pool com o nome '$CC_FLINK_POOL_NAME'. Nenhuma alteração foi feita." >&2
    return 1
  }

  legacy_count="$(jq --arg id "$legacy_id" --arg n "$legacy_name" \
    '[.[] | select(.id==$id or .name==$n)] | length' <<<"$pools")"
  [[ "$legacy_count" -le 1 ]] || {
    echo "Pool legado conhecido aparece mais de uma vez; nenhuma alteração foi feita." >&2
    return 1
  }

  if [[ "$count" -eq 1 ]]; then
    [[ "$legacy_count" -eq 0 ]] || {
      echo "Pool de destino e pool legado coexistem. Inspecione manualmente; nada será alterado." >&2
      return 1
    }
    pool="$(jq -er --arg n "$CC_FLINK_POOL_NAME" '.[] | select(.name==$n) | .id' <<<"$pools")"
    echo "Compute pool já existe; será validado, não recriado."
  elif [[ "$legacy_count" -eq 1 ]]; then
    legacy="$(jq -c --arg id "$legacy_id" '.[] | select(.id==$id)' <<<"$pools")"
    [[ "$(jq -r '.name' <<<"$legacy")" == "$legacy_name" && \
       "$(jq -r '.environment' <<<"$legacy")" == "$ENV_ID" && \
       "$(jq -r '.cloud | ascii_upcase' <<<"$legacy")" == "$(printf '%s' "$CC_CLOUD" | tr '[:lower:]' '[:upper:]')" && \
       "$(jq -r '.region' <<<"$legacy")" == "$CC_REGION" ]] || {
      echo "Pool legado não corresponde ao ID, nome, Environment, cloud e região esperados. Interrompendo." >&2
      return 1
    }
    legacy_status="$(jq -r '.status' <<<"$legacy")"
    [[ "$legacy_status" == PROVISIONED || "$legacy_status" == RUNNING ]] || {
      echo "Pool legado está em status '$legacy_status'; não será renomeado." >&2
      return 1
    }
    legacy_max_cfu="$(jq -er '.max_cfu | select(type == "number" and . > 0)' <<<"$legacy")" || {
      echo "Não foi possível confirmar max_cfu do pool legado; não será renomeado." >&2
      return 1
    }
    echo "Renomeando pool legado validado para '$CC_FLINK_POOL_NAME', preservando max_cfu=$legacy_max_cfu."
    confluent flink compute-pool update "$legacy_id" \
      --environment "$ENV_ID" --name "$CC_FLINK_POOL_NAME" \
      --max-cfu "$legacy_max_cfu" >/dev/null
    pools="$(confluent flink compute-pool list --environment "$ENV_ID" -o json)"
    count="$(jq --arg n "$CC_FLINK_POOL_NAME" '[.[] | select(.name==$n)] | length' <<<"$pools")"
    [[ "$count" -eq 1 ]] || {
      echo "Não foi possível confirmar unicamente o pool legado renomeado." >&2
      return 1
    }
    pool="$(jq -er --arg n "$CC_FLINK_POOL_NAME" '.[] | select(.name==$n) | .id' <<<"$pools")"
  else
    echo "Criando compute pool (max CFU: $MAX_CFU)..."
    confluent flink compute-pool create "$CC_FLINK_POOL_NAME" \
      --cloud "$CC_CLOUD" \
      --region "$CC_REGION" \
      --max-cfu "$MAX_CFU" \
      --environment "$ENV_ID"
    pools="$(confluent flink compute-pool list --environment "$ENV_ID" -o json)"
    count="$(jq --arg n "$CC_FLINK_POOL_NAME" '[.[] | select(.name==$n)] | length' <<<"$pools")"
    [[ "$count" -eq 1 ]] || {
      echo "Não foi possível identificar unicamente o compute pool após a criação." >&2
      return 1
    }
    pool="$(jq -er --arg n "$CC_FLINK_POOL_NAME" '.[] | select(.name==$n) | .id' <<<"$pools")"
  fi

  pool_json="$(confluent flink compute-pool describe "$pool" --environment "$ENV_ID" -o json)"
  actual_name="$(jq -r '.name // empty' <<<"$pool_json")"
  actual_env="$(jq -r '.environment // empty' <<<"$pool_json")"
  actual_cloud="$(jq -r '.cloud // empty' <<<"$pool_json")"
  actual_region="$(jq -r '.region // empty' <<<"$pool_json")"
  status="$(jq -r '.status // empty' <<<"$pool_json")"

  [[ "$actual_name" == "$CC_FLINK_POOL_NAME" ]] || {
    echo "Nome retornado divergente: '$actual_name'." >&2
    return 1
  }
  [[ "$actual_env" == "$ENV_ID" ]] || {
    echo "Environment retornado divergente: '$actual_env'." >&2
    return 1
  }
  [[ "${actual_cloud,,}" == "${CC_CLOUD,,}" && "$actual_region" == "$CC_REGION" ]] || {
    echo "Cloud/região retornadas (${actual_cloud:-?}/${actual_region:-?}) divergem da configuração." >&2
    return 1
  }

  for _ in $(seq 1 60); do
    [[ "$status" == PROVISIONED || "$status" == RUNNING ]] && break
    [[ "$status" == FAILED ]] && {
      echo "Compute pool entrou em FAILED." >&2
      return 1
    }
    sleep 10
    pool_json="$(confluent flink compute-pool describe "$pool" --environment "$ENV_ID" -o json)"
    status="$(jq -r '.status // empty' <<<"$pool_json")"
  done

  [[ "$status" == PROVISIONED || "$status" == RUNNING ]] || {
    echo "Timeout aguardando o compute pool; status atual: ${status:-desconhecido}." >&2
    return 1
  }

  save_state FLINK_POOL_ID "$pool"
  echo "Compute pool ID: $pool"
  echo "Status: $status"
  echo "Detalhes confirmados:"
  jq '{id,name,environment,cloud,region,status,max_cfu}' <<<"$pool_json"
  echo "RESULTADO: compute pool disponível e FLINK_POOL_ID salvo em .state.env."
}

main 2>&1 | tee "$EVIDENCE"
exit "${PIPESTATUS[0]}"
