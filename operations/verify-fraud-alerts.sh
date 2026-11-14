#!/usr/bin/env bash
# Valida alertas do cenário positivo por cartão, quantidade e janela de event time.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq date sed

: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
: "${FLINK_POOL_ID:?FLINK_POOL_ID ausente}"
: "${CC_CLOUD:?CC_CLOUD ausente}"
: "${CC_REGION:?CC_REGION ausente}"

usage() {
  echo "Uso: $0 'YYYY-MM-DD HH:MM:SS.mmm' 'YYYY-MM-DD HH:MM:SS.mmm'" >&2
  echo "Informe os limites UTC de event time esperados para o cenário." >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
START_UTC="$1"
END_UTC="$2"
TS_RE='^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}$'
[[ "$START_UTC" =~ $TS_RE && "$END_UTC" =~ $TS_RE ]] || usage

# Valida calendário/horário e garante que a janela não esteja invertida.
START_EPOCH="$(date -u -d "$START_UTC" +%s%3N)" || usage
END_EPOCH="$(date -u -d "$END_UTC" +%s%3N)" || usage
(( START_EPOCH <= END_EPOCH )) || { echo "Janela UTC invertida." >&2; exit 2; }

confluent environment use "$ENV_ID" >/dev/null
confluent flink region use --cloud "$CC_CLOUD" --region "$CC_REGION" >/dev/null

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"*OUT="$ROOT_DIR/evidence/homologation/${STAMP}-fraud-alerts-validation.txt"
TMP_JSON="$(mktemp)"
trap 'rm -f "$TMP_JSON"' EXIT
mkdir -p "$(dirname "$OUT")"

{
  echo "Validação de alertas de fraude — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Environment: $ENV_ID"
  echo "Kafka cluster/database: $CLUSTER_ID"
  echo "Compute pool: $FLINK_POOL_ID"
  echo "Cartão esperado: 1"
  echo "txn_count mínimo: 3"
  echo "Janela UTC de event time: [$START_UTC, $END_UTC]"
  echo
  echo "Consulta limitada à partição do cartão esperado":
  echo 'SELECT card_id, first_txn, last_txn, txn_count FROM fraud_alerts WHERE card_id = 1 ORDER BY first_txn DESC LIMIT 100;'
  echo
  confluent flink query \
    --sql 'SELECT card_id, first_txn, last_txn, txn_count FROM fraud_alerts WHERE card_id = 1 ORDER BY first_txn DESC LIMIT 100' \
    --compute-pool "$FLINK_POOL_ID" \
    --database "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    --cloud "$CC_CLOUD" \
    --region "$CC_REGION" \
    --timeout 2m \
    --max-rows 100 \
    --output json
} 2>&1 | tee "$OUT"

# A CLI pode emitir uma linha informativa antes do array JSON; extrai apenas o array.
sed -n '/^\[/,$p' "$OUT" > "$TMP_JSON"
jq -e 'type == "array"' "$TMP_JSON" >/dev/null || {
  echo "Não foi possível interpretar a resposta JSON da consulta. Evidência preservada: $OUT" >&2
  exit 1
}

MATCHES="$(jq -c --arg start "$START_UTC" --arg end "$END_UTC" '
  [.[] | select(
    ((.card_id | tonumber) == 1) and
    ((.txn_count | tonumber) >= 3) and
    (.first_txn >= $start) and (.first_txn <= $end) and
    (.last_txn >= .first_txn) and (.last_txn <= $end)
  )]
' "$TMP_JSON")"
MATCH_COUNT="$(jq 'length' <<<"$MATCHES")"

{
  echo
  echo "Resultado da correlação:"
  echo "Alertas consultados: $(jq 'length' "$TMP_JSON")"
  echo "Alertas correlacionados: $MATCH_COUNT"
  if (( MATCH_COUNT > 0 )); then
    jq -r '.[] | "card_id=\(.card_id), first_txn=\(.first_txn), last_txn=\(.last_txn), txn_count=\(.txn_count)"' <<<"$MATCHES"
    echo "VALIDAÇÃO APROVADA"
  else
    echo "VALIDAÇÃO REPROVADA: nenhum alerta corresponde ao cartão, quantidade e janela informados."
  fi
} | tee -a "$OUT"

if (( MATCH_COUNT == 0 )); then
  echo "Não interrompa os statements: preserve-os para diagnóstico." >&2
  exit 1
fi

echo "Evidência salva em: ${OUT#"$ROOT_DIR"/}"
