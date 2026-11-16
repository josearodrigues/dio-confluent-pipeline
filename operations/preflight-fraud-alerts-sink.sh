#!/usr/bin/env bash
# Preflight do tópico fraud_alerts: consome mensagens Avro e registra a sessão.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent date tee timeout awk grep

: "${ENV_ID:?ENV_ID ausente; execute infrastructure/foundation.sh}"
: "${CLUSTER_ID:?CLUSTER_ID ausente; execute infrastructure/foundation.sh}"
: "${BOOTSTRAP:?BOOTSTRAP ausente}"
: "${READER_API_KEY:?READER_API_KEY ausente}"
: "${READER_API_SECRET:?READER_API_SECRET ausente}"
: "${SR_URL:?SR_URL ausente}"
: "${SR_API_KEY:?SR_API_KEY ausente}"
: "${SR_API_SECRET:?SR_API_SECRET ausente}"

TOPIC="fraud_alerts"
INSPECT_TIMEOUT_SECONDS="${INSPECT_TIMEOUT_SECONDS:-5}"
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
  echo "Preflight do tópico Kafka"
  echo "UTC início: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Environment: $ENV_ID"
  echo "Cluster: $CLUSTER_ID"
  echo "Tópico: $TOPIC"
  echo "Modo: desde o início; captura da primeira mensagem Avro; timeout de ${INSPECT_TIMEOUT_SECONDS}s se nenhuma chegar."
  echo "Credenciais não são registradas."
  echo
} | tee "$LOG"

echo "==> Verificando a primeira mensagem Avro de $TOPIC (limite: ${INSPECT_TIMEOUT_SECONDS}s)."
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
    --environment "$ENV_ID" 2>&1 |
  awk '
    /^Starting Kafka Consumer\./ { next }
    /^Partition:/ { print; fflush(); found=1; exit }
    { print; fflush() }
    END { if (!found) exit 1 }
  ' | tee -a "$LOG"
statuses=("${PIPESTATUS[@]}")
set -e
consumer_rc="${statuses[0]}"
filter_rc="${statuses[1]}"
tee_rc="${statuses[2]}"

{
  echo
  echo "UTC fim: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if grep -q '^Partition:' "$LOG"; then
    echo "Resultado: primeira mensagem Avro recebida e decodificada."
    echo "Encerramento: consumidor interrompido após a primeira mensagem."
  else
    echo "Resultado: nenhuma mensagem Avro foi recebida dentro do limite."
    echo "Códigos: consumidor/timeout=$consumer_rc; filtro=$filter_rc; tee=$tee_rc"
  fi
  echo "Evidência: ${LOG#"$ROOT_DIR"/}"
} | tee -a "$LOG"

if grep -q '^Partition:' "$LOG"; then
  # 141 é SIGPIPE esperado: o consumidor é encerrado após a primeira mensagem.
  [[ "$consumer_rc" -eq 0 || "$consumer_rc" -eq 124 || "$consumer_rc" -eq 141 ]] || exit "$consumer_rc"
  [[ "$tee_rc" -eq 0 ]] || exit "$tee_rc"
  exit 0
fi

[[ "$consumer_rc" -eq 124 ]] && {
  echo "ERRO: nenhuma mensagem foi recebida em ${INSPECT_TIMEOUT_SECONDS}s." >&2
  exit 1
}
exit "${consumer_rc:-1}"
