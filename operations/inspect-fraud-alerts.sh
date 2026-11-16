#!/usr/bin/env bash
# Inspeciona mensagens Avro do tópico fraud_alerts e registra a sessão.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent date tee timeout

: "${ENV_ID:?ENV_ID ausente; execute infrastructure/foundation.sh}"
: "${CLUSTER_ID:?CLUSTER_ID ausente; execute infrastructure/foundation.sh}"
: "${BOOTSTRAP:?BOOTSTRAP ausente}"
: "${READER_API_KEY:?READER_API_KEY ausente}"
: "${READER_API_SECRET:?READER_API_SECRET ausente}"
: "${SR_URL:?SR_URL ausente}"
: "${SR_API_KEY:?SR_API_KEY ausente}"
: "${SR_API_SECRET:?SR_API_SECRET ausente}"

TOPIC="fraud_alerts"
INSPECT_TIMEOUT_SECONDS="${INSPECT_TIMEOUT_SECONDS:-30}"
[[ "$INSPECT_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
  echo "ERRO: INSPECT_TIMEOUT_SECONDS deve ser um inteiro positivo." >&2
  exit 1
}

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG="$ROOT_DIR/evidence/homologation/${STAMP}-fraud-alerts-consume.log"
mkdir -p "$(dirname "$LOG")"

confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
confluent kafka topic describe "$TOPIC" --environment "$ENV_ID" --cluster "$CLUSTER_ID" >/dev/null

{
  echo "Inspeção automática do tópico Kafka"
  echo "UTC início: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Environment: $ENV_ID"
  echo "Cluster: $CLUSTER_ID"
  echo "Tópico: $TOPIC"
  echo "Modo: desde o início; encerramento automático após ${INSPECT_TIMEOUT_SECONDS}s."
  echo "Credenciais não são registradas."
  echo
} | tee "$LOG"

echo "==> Consumindo $TOPIC por até ${INSPECT_TIMEOUT_SECONDS}s."
set +e
timeout --signal=TERM "${INSPECT_TIMEOUT_SECONDS}s" \
  confluent kafka topic consume "$TOPIC" \
    --group "app-reader" \
    --from-beginning \
    --value-format avro \
    --print-offset \
    --schema-registry-endpoint "$SR_URL" \
    --schema-registry-api-key "$SR_API_KEY" \
    --schema-registry-api-secret "$SR_API_SECRET" \
    --api-key "$READER_API_KEY" \
    --api-secret "$READER_API_SECRET" \
    --bootstrap "$BOOTSTRAP" \
    --cluster "$CLUSTER_ID" \
    --environment "$ENV_ID" 2>&1 | tee -a "$LOG"
rc=${PIPESTATUS[0]}
set -e

{
  echo
  echo "UTC fim: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$rc" -eq 124 ]]; then
    echo "Encerramento: limite de tempo atingido (normal)."
  elif [[ "$rc" -eq 0 ]]; then
    echo "Encerramento: consumidor finalizado normalmente."
  else
    echo "Código de saída do consumidor: $rc"
  fi
  echo "Evidência: ${LOG#"$ROOT_DIR"/}"
} | tee -a "$LOG"

# O timeout configurado é um encerramento esperado, não uma falha.
[[ "$rc" -eq 124 ]] && exit 0
exit "$rc"
