#!/usr/bin/env bash
# Preflight do tópico fraud_alerts: valida a primeira mensagem Avro e registra a sessão.
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
INSPECT_TIMEOUT_SECONDS="${INSPECT_TIMEOUT_SECONDS:-7}"
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
  echo "Modo: desde o início; validação da primeira mensagem Avro; timeout de ${INSPECT_TIMEOUT_SECONDS}s se nenhuma chegar."
  echo "Campos obrigatórios: card_id, first_txn, last_txn, txn_count."
  echo "Credenciais não são registradas."
  echo
} | tee "$LOG"

echo "==> Validando a primeira mensagem Avro de $TOPIC (limite: ${INSPECT_TIMEOUT_SECONDS}s)."
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
    /^Partition:/ {
      if (found) exit
      found=1
    }
    found { print; fflush() }
    END { if (!found) exit 1 }
  ' | tee -a "$LOG"
statuses=("${PIPESTATUS[@]}")
set -e
consumer_rc="${statuses[0]}"
filter_rc="${statuses[1]}"
tee_rc="${statuses[2]}"

has_message=false
missing_fields=()
if grep -q '^Partition:' "$LOG"; then
  has_message=true
  for field in card_id first_txn last_txn txn_count; do
    if ! grep -Eq "\"?${field}\"?[[:space:]]*:" "$LOG"; then
      missing_fields+=("$field")
    fi
  done
fi

{
  echo
  echo "UTC fim: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$has_message" == true && "${#missing_fields[@]}" -eq 0 ]]; then
    echo "Resultado: APROVADO — primeira mensagem recebida e os quatro campos obrigatórios foram identificados."
    echo "Encerramento: captura concluída após a primeira mensagem; timeout/SIGPIPE do consumidor é esperado."
  elif [[ "$has_message" == true ]]; then
    echo "Resultado: REPROVADO — mensagem recebida, mas faltam campos obrigatórios: ${missing_fields[*]}."
    echo "Encerramento: captura da primeira mensagem concluída; conteúdo incompatível com o contrato esperado."
  elif [[ "$consumer_rc" -eq 124 ]]; then
    echo "Resultado: INCONCLUSIVO — nenhuma mensagem chegou dentro do limite de ${INSPECT_TIMEOUT_SECONDS}s."
    echo "Encerramento: limite de tempo atingido."
  else
    echo "Resultado: FALHA — o consumidor não conseguiu concluir a leitura."
    echo "Códigos: consumidor/timeout=$consumer_rc; filtro=$filter_rc; tee=$tee_rc"
  fi
  echo "Evidência: ${LOG#"$ROOT_DIR"/}"
} | tee -a "$LOG"

if [[ "$has_message" == true && "${#missing_fields[@]}" -eq 0 ]]; then
  # 124 é o timeout normal quando só há uma mensagem; 141 é SIGPIPE após fechar o pipe.
  [[ "$consumer_rc" -eq 0 || "$consumer_rc" -eq 124 || "$consumer_rc" -eq 141 ]] || exit "$consumer_rc"
  [[ "$tee_rc" -eq 0 ]] || exit "$tee_rc"
  exit 0
fi

if [[ "$has_message" == true ]]; then
  exit 1
fi
[[ "$consumer_rc" -eq 124 ]] && exit 2
exit "${consumer_rc:-1}"
