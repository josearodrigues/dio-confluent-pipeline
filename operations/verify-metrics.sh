#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/_common.sh
source "$ROOT_DIR/scripts/_common.sh"

require curl jq date

: "${METRICS_API_KEY:?METRICS_API_KEY ausente no .env/.state.env}"
: "${METRICS_API_SECRET:?METRICS_API_SECRET ausente no .env/.state.env}"
: "${CLUSTER_ID:?CLUSTER_ID ausente no .state.env}"

METRICS_INTERVAL="${METRICS_INTERVAL:-PT6H/now}"
METRICS_GRANULARITY="${METRICS_GRANULARITY:-PT1H}"
METRICS_NAME="io.confluent.kafka.server/received_bytes"

# Evita enviar parâmetros arbitrários ou malformados à API.
[[ "$METRICS_INTERVAL" =~ ^PT[0-9]+H/now$ ]] || {
  echo "METRICS_INTERVAL inválido. Use o formato PT<número>H/now." >&2
  exit 1
}
[[ "$METRICS_GRANULARITY" =~ ^PT[0-9]+[MH]$ ]] || {
  echo "METRICS_GRANULARITY inválida. Use PT<número>M ou PT<número>H." >&2
  exit 1
}

EVIDENCE_DIR="$ROOT_DIR/evidence/observability"
mkdir -p "$EVIDENCE_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
JSON_FILE="$EVIDENCE_DIR/${STAMP}-metrics.json"
SUMMARY_FILE="$EVIDENCE_DIR/${STAMP}-metrics.md"
TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT

PAYLOAD="$(jq -n \
  --arg cluster "$CLUSTER_ID" \
  --arg metric "$METRICS_NAME" \
  --arg granularity "$METRICS_GRANULARITY" \
  --arg interval "$METRICS_INTERVAL" '{
    aggregations: [{metric: $metric}],
    filter: {
      field: "resource.kafka.id",
      op: "EQ",
      value: $cluster
    },
    granularity: $granularity,
    intervals: [$interval]
  }')"

echo "Consultando Confluent Metrics API (somente leitura)..."
curl -fsS \
  --user "$METRICS_API_KEY:$METRICS_API_SECRET" \
  -H "Content-Type: application/json" \
  --data "$PAYLOAD" \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/query \
  --output "$TMP_FILE"

jq -e '.data | type == "array"' "$TMP_FILE" >/dev/null || {
  echo "Resposta da Metrics API não contém data como array; evidência não foi aceita." >&2
  exit 1
}

mv "$TMP_FILE" "$JSON_FILE"
chmod 600 "$JSON_FILE"

# Consulta consumer lag e estado dos consumer groups via Confluent CLI / Metrics
echo "Consultando estado dos Consumer Groups..."
GROUPS_TMP="$(mktemp)"
trap 'rm -f "$TMP_FILE" "$GROUPS_TMP"' EXIT
confluent kafka consumer group list -o json > "$GROUPS_TMP" 2>/dev/null || echo "[]" > "$GROUPS_TMP"

{
  echo "# Coleta da Confluent Metrics API"
  echo
  echo "- Coletado em (UTC): $STAMP"
  echo "- Kafka cluster ID: $CLUSTER_ID"
  echo "- Métrica: \`$METRICS_NAME\`"
  echo "- Intervalo solicitado: \`$METRICS_INTERVAL\`"
  echo "- Granularidade: \`$METRICS_GRANULARITY\`"
  echo "- Unidade: bytes (By); contador agregado na granularidade solicitada."
  echo "- Escopo: tráfego recebido pelo cluster Kafka; não identifica isoladamente o produtor."
  echo
  echo "## Pontos retornados (Throughput / Received Bytes)"
  echo
  echo "| Timestamp UTC | Bytes | KiB |"
  echo "|---|---:|---:|"
  jq -r '.data[] | [(.timestamp // "timestamp ausente"), ((.value // 0) | tostring), (((.value // 0) / 1024) | . * 100 | round / 100 | tostring)] | "| " + join(" | ") + " |"' "$JSON_FILE"
  if [[ "$(jq '.data | length' "$JSON_FILE")" -eq 0 ]]; then
    echo
    echo "A API respondeu corretamente, mas não retornou pontos para o filtro e intervalo solicitados."
  fi
  echo
  echo "## Estado dos Consumer Groups e Lag"
  echo
  echo "| Consumer Group | Estado | Coordinator |"
  echo "|---|---|---:|"
  jq -r '.[] | "| " + (.consumer_group // "-") + " | " + (.state // "-") + " | " + ((.coordinator // "-") | tostring) + " |"' "$GROUPS_TMP" 2>/dev/null || echo "| - | - | - |"
  echo
  echo "## Detalhes dos Consumer Groups (Describe)"
  echo
  if jq -e '. | length > 0' "$GROUPS_TMP" >/dev/null 2>&1; then
    for cg in $(jq -r '.[].consumer_group' "$GROUPS_TMP" 2>/dev/null); do
      echo "### Consumer Group: \`$cg\`"
      echo
      echo '```text'
      confluent kafka consumer group describe "$cg" 2>&1 || true
      echo '```'
      echo
    done
  else
    echo "Nenhum Consumer Group encontrado no cluster."
    echo
  fi
  echo "## Arquivos"
  echo
  echo "- JSON original: \`evidence/observability/$(basename "$JSON_FILE")\`"
  echo "- Este resumo: \`evidence/observability/$(basename "$SUMMARY_FILE")\`"
  echo
  echo "A ausência de pontos não deve ser interpretada como tráfego zero. Esta consulta não comprova cobertura contínua de todos os intervalos."
} > "$SUMMARY_FILE"
chmod 600 "$SUMMARY_FILE"

echo "Coleta concluída."
echo "JSON: evidence/observability/$(basename "$JSON_FILE")"
echo "Resumo: evidence/observability/$(basename "$SUMMARY_FILE")"
