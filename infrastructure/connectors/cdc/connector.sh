#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../../../scripts/_common.sh"
require confluent jq psql
: "${ENV_ID:?Execute foundation.sh primeiro}"
: "${CLUSTER_ID:?Execute foundation.sh primeiro}"
: "${SA_WRITER_ID:?Service account writer ausente}"
: "${PG_HOST:?PG_HOST ausente}"
: "${PG_PORT:?PG_PORT ausente}"
: "${PG_DATABASE:?PG_DATABASE ausente}"
: "${PG_ADMIN_USER:?PG_ADMIN_USER ausente}"
: "${PG_ADMIN_PASSWORD:?PG_ADMIN_PASSWORD ausente}"
: "${PG_CDC_USER:?PG_CDC_USER ausente}"
: "${PG_CDC_PASSWORD:?PG_CDC_PASSWORD ausente}"

NAME="payments-postgres-cdc"
OUT="$ROOT_DIR/infrastructure/.rendered/cdc-connector.json"
mkdir -p "$(dirname "$OUT")"
trap 'rm -f "$OUT"' EXIT
confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
CONNECTORS="$(confluent connect cluster list -o json)"
if jq -e --arg n "$NAME" 'any(.[]; .name==$n)' <<<"$CONNECTORS" >/dev/null; then
  echo "Conector $NAME já existe; inspecione antes de continuar." >&2
  exit 1
fi
SLOT_EXISTS="$(PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql \
  -X -h "$PG_HOST" -p "$PG_PORT" \
  -U "$PG_ADMIN_USER" -d "$PG_DATABASE" \
  -v ON_ERROR_STOP=1 -Atqc \
  "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'payments_slot');")"
[[ "$SLOT_EXISTS" == "f" ]] || { echo "Slot payments_slot já existe; não será reutilizado automaticamente." >&2; exit 1; }
render_template "$ROOT_DIR/infrastructure/connectors/cdc/connector.json" "$OUT" \
  SA_WRITER_ID PG_HOST PG_PORT PG_CDC_USER PG_CDC_PASSWORD PG_DATABASE
confluent connect cluster create --config-file "$OUT"
for _ in $(seq 1 60); do
  CONNECTORS="$(confluent connect cluster list -o json)"
  STATUS="$(jq -r --arg n "$NAME" '[.[]|select(.name==$n)][0].status // empty' <<<"$CONNECTORS")"
  ID="$(jq -r --arg n "$NAME" '[.[]|select(.name==$n)][0].id // empty' <<<"$CONNECTORS")"
  [[ -n "$ID" ]] && save_state CDC_CONNECTOR_ID "$ID"
  [[ "$STATUS" == RUNNING ]] && break
  [[ "$STATUS" == FAILED ]] && { confluent connect cluster describe "$ID"; exit 1; }
  sleep 10
done
[[ "$STATUS" == RUNNING ]] || { echo "Conector não chegou a RUNNING (status=$STATUS)." >&2; exit 1; }
mkdir -p "$ROOT_DIR/evidence/cloud"
confluent connect cluster describe "$ID" | tee "$ROOT_DIR/evidence/cloud/03-cdc-connector.txt"
