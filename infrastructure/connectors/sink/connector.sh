#!/usr/bin/env bash
# Persiste fraud_alerts em public.fraud_alerts por UPSERT.
# Execute somente depois de validar o tópico, o schema Avro e os privilégios PostgreSQL.
source "$(dirname "${BASH_SOURCE[0]}")/../../../scripts/_common.sh"
require confluent jq

: "${ENV_ID:?Rode infrastructure/foundation.sh primeiro}"
: "${CLUSTER_ID:?Rode infrastructure/foundation.sh primeiro}"
: "${SA_READER_ID:?Service Account app-reader não encontrada}"
: "${PG_HOST:?PG_HOST não definido em .env}"
: "${PG_PORT:?PG_PORT não definido em .env}"
: "${PG_DATABASE:?PG_DATABASE não definido em .env}"
: "${PG_SINK_USER:?PG_SINK_USER não definido em .env}"
: "${PG_SINK_PASSWORD:?PG_SINK_PASSWORD não definido em .env}"

NAME="payments-fraud-alerts-sink"
OUT="$ROOT_DIR/infrastructure/.rendered/04-sink-connector.json"
mkdir -p "$(dirname "$OUT")" "$ROOT_DIR/evidence/cloud"
trap 'rm -f "$OUT"' EXIT

fail() { echo "ERRO: $*" >&2; exit 1; }

main() {
  confluent environment use "$ENV_ID" >/dev/null
  confluent kafka cluster use "$CLUSTER_ID" >/dev/null

  echo "==> Verificando se o conector já existe"
  CONNECTORS="$(confluent connect cluster list --environment "$ENV_ID" --cluster "$CLUSTER_ID" -o json)"
  jq -e 'type == "array" and all(.[]; (.name | type) == "string")' <<<"$CONNECTORS" >/dev/null \
    || fail "resposta inesperada ao listar conectores; nenhuma criação será tentada"
  if jq -e --arg n "$NAME" 'any(.[]; .name == $n)' <<<"$CONNECTORS" >/dev/null; then
    fail "o conector $NAME já existe; inspecione seu estado antes de continuar"
  fi

  echo "==> Verificando se o tópico fraud_alerts existe"
  confluent kafka topic describe fraud_alerts >/dev/null \
    || fail "tópico fraud_alerts não encontrado; valide a saída do Flink antes de criar o Sink"

  render_template "$ROOT_DIR/infrastructure/connectors/sink/connector.json" "$OUT" \
    SA_READER_ID PG_HOST PG_PORT PG_SINK_USER PG_SINK_PASSWORD PG_DATABASE

  echo "==> Criando conector Postgres Sink"
  confluent connect cluster create --config-file "$OUT"

  echo "==> Aguardando conector ficar RUNNING (máx. 10 min)"
  STATUS=""; SINK_ID=""
  for _ in $(seq 1 60); do
    CONNECTORS="$(confluent connect cluster list --environment "$ENV_ID" --cluster "$CLUSTER_ID" -o json)"
    jq -e 'type == "array" and all(.[]; (.name | type) == "string")' <<<"$CONNECTORS" >/dev/null \
      || fail "resposta inesperada ao consultar o estado dos conectores"
    STATUS="$(jq -r --arg n "$NAME" '.[] | select(.name==$n) | .status // empty' <<<"$CONNECTORS")"
    SINK_ID="$(jq -r --arg n "$NAME" '.[] | select(.name==$n) | .id // empty' <<<"$CONNECTORS")"
    [[ -n "$SINK_ID" ]] && save_state SINK_CONNECTOR_ID "$SINK_ID"
    [[ "$STATUS" == "RUNNING" ]] && break
    [[ "$STATUS" == "FAILED" ]] && fail "sink FAILED — veja: confluent connect cluster describe ${SINK_ID:-<id>}"
    sleep 10
  done
  [[ "$STATUS" == "RUNNING" ]] \
    || fail "sink não ficou RUNNING a tempo (último estado: ${STATUS:-desconhecido}). id=${SINK_ID:-?}"

  echo
  echo "================ EVIDÊNCIA — SINK ================"
  confluent connect cluster describe "$SINK_ID"
  echo
  echo "RUNNING não comprova persistência. Confira o destino:"
  echo "./scripts/run-sql.sh database/verify/alerts.sql"
}

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG="$ROOT_DIR/evidence/cloud/$STAMP-05-sink-connector.txt"
: > "$LOG"
exec > >(tee "$LOG") 2>&1
main
