#!/usr/bin/env bash
# Para somente enrichment e detecção após a validação funcional bem-sucedida.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../../scripts/_common.sh"
require confluent jq realpath grep stat date

: "${ENV_ID:?ENV_ID ausente}"
: "${FLINK_POOL_ID:?FLINK_POOL_ID ausente}"
: "${CC_CLOUD:?CC_CLOUD ausente}"
: "${CC_REGION:?CC_REGION ausente}"

[[ $# -eq 1 ]] || {
  echo "Uso: $0 evidence/homologation/<timestamp>-fraud-alerts-validation.txt" >&2
  exit 2
}
VALIDATION_EVIDENCE="$(realpath -e "$1")" || {
  echo "Evidência de validação não encontrada." >&2
  exit 2
}
EXPECTED_DIR="$(realpath -e "$ROOT_DIR/evidence/homologation")"
[[ "$VALIDATION_EVIDENCE" == "$EXPECTED_DIR"/* && "$VALIDATION_EVIDENCE" == *-fraud-alerts-validation.txt ]] || {
  echo "Informe uma evidência de validação de fraude dentro de evidence/homologation/." >&2
  exit 2
}
grep -q '^VALIDAÇÃO APROVADA$' "$VALIDATION_EVIDENCE" || {
  echo "A evidência informada não contém VALIDAÇÃO APROVADA. Statements preservados." >&2
  exit 1
}
EVIDENCE_AGE="$(( $(date +%s) - $(stat -c %Y "$VALIDATION_EVIDENCE") ))"
(( EVIDENCE_AGE >= 0 && EVIDENCE_AGE <= 7200 )) || {
  echo "A evidência tem mais de duas horas ou data inválida. Execute nova validação antes de parar." >&2
  exit 1
}

confluent environment use "$ENV_ID" >/dev/null
confluent flink region use --cloud "$CC_CLOUD" --region "$CC_REGION" >/dev/null

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$ROOT_DIR/evidence/homologation/${STAMP}-flink-statements-stop.txt"
mkdir -p "$(dirname "$OUT")"

{
  echo "Encerramento dos statements contínuos — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Environment: $ENV_ID"
  echo "Compute pool: $FLINK_POOL_ID"
  echo "Evidência de validação aprovada: ${VALIDATION_EVIDENCE#"$ROOT_DIR"/}"
  echo

  statements="$(confluent flink statement list --compute-pool "$FLINK_POOL_ID" --environment "$ENV_ID" -o json)"

  resolve_name() {
    local role="$1" canonical="$2" legacy="$3" count name
    count="$(jq --arg a "$canonical" --arg b "$legacy" '[.[] | select(.name==$a or .name==$b)] | length' <<<"$statements")"
    [[ "$count" -eq 1 ]] || {
      echo "Esperado exatamente um statement para '$role' ($canonical ou $legacy); encontrados: $count. Nenhuma parada foi solicitada." >&2
      return 1
    }
    name="$(jq -r --arg a "$canonical" --arg b "$legacy" '.[] | select(.name==$a or .name==$b) | .name' <<<"$statements")"
    printf '%s' "$name"
  }

  ENRICHMENT_NAME="$(resolve_name enrichment payments-enrichment payments-enrichment-test)"
  FRAUD_NAME="$(resolve_name fraud-detection payments-fraud-detection payments-fraud-detection-test)"

  describe_status() {
    local name="$1" data
    data="$(confluent flink statement describe "$name" -o json)"
    jq -er '.status // .phase // empty' <<<"$data"
  }

  ENRICHMENT_STATUS="$(describe_status "$ENRICHMENT_NAME")"
  FRAUD_STATUS="$(describe_status "$FRAUD_NAME")"
  echo "Enrichment: $ENRICHMENT_NAME — $ENRICHMENT_STATUS"
  echo "Fraud detection: $FRAUD_NAME — $FRAUD_STATUS"

  for status in "$ENRICHMENT_STATUS" "$FRAUD_STATUS"; do
    [[ "$status" == RUNNING || "$status" == STOPPED ]] || {
      echo "Estado inesperado '$status'. Nenhuma parada foi solicitada." >&2
      exit 1
    }
  done

  stop_and_confirm() {
    local name="$1" status="$2" current="" attempt
    if [[ "$status" == STOPPED ]]; then
      echo "Já estava STOPPED: $name"
      return 0
    fi
    echo "Solicitando parada: $name"
    confluent flink statement stop "$name"
    for attempt in $(seq 1 60); do
      current="$(describe_status "$name")"
      [[ "$current" == STOPPED ]] && {
        echo "Parada confirmada: $name — STOPPED"
        return 0
      }
      [[ "$current" == FAILED || "$current" == COMPLETED ]] && {
        echo "Statement terminou em estado inesperado '$current': $name" >&2
        return 1
      }
      sleep 5
    done
    echo "Timeout aguardando STOPPED: $name (último estado: $current)" >&2
    return 1
  }

  stop_and_confirm "$ENRICHMENT_NAME" "$ENRICHMENT_STATUS"
  stop_and_confirm "$FRAUD_NAME" "$FRAUD_STATUS"

  echo
  echo "Estado final confirmado:"
  echo "$ENRICHMENT_NAME: $(describe_status "$ENRICHMENT_NAME")"
  echo "$FRAUD_NAME: $(describe_status "$FRAUD_NAME")"
  echo "ENCERRAMENTO APROVADO"
} 2>&1 | tee "$OUT"

echo "Evidência salva em: ${OUT#"$ROOT_DIR"/}"
