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

ROLE_OK="$(PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -At \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" \
  -v ON_ERROR_STOP=1 -v "cdc_role=$PG_CDC_USER" \
  -c "SELECT COALESCE(bool_and(rolcanlogin AND rolreplication), false) FROM pg_roles WHERE rolname = :'cdc_role'")"
[[ "$ROLE_OK" == "t" ]] || {
  echo "A role PostgreSQL '$PG_CDC_USER' não existe ou não possui LOGIN e REPLICATION." >&2
  echo "Crie/ajuste a role no provedor antes de provisionar o conector." >&2
  exit 1
}

# Confirma que a credencial usada pelo conector autentica no banco.
PGPASSWORD="$PG_CDC_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 -At \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_CDC_USER" -d "$PG_DATABASE" \
  -c 'SELECT 1' >/dev/null || {
    echo "A role CDC não conseguiu autenticar no banco; conector não será criado." >&2
    exit 1
  }

# A publication deve conter exatamente as quatro tabelas configuradas no JSON.
PUBLICATION_OK="$(PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -At \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" \
  -v ON_ERROR_STOP=1 -c "SELECT (count(*) = 4 AND count(*) FILTER (WHERE schemaname = 'payments' AND tablename IN ('customers','accounts','cards','transactions')) = 4)::text FROM pg_publication_tables WHERE pubname = 'payments_pub'")"
[[ "$PUBLICATION_OK" == "true" ]] || {
  echo "A publication payments_pub não contém exatamente customers, accounts, cards e transactions." >&2
  echo "Conector não será criado; confira a inicialização do banco e o escopo da publication." >&2
  exit 1
}

# A role do conector precisa ler todas as tabelas capturadas.
SELECT_OK="$(PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -At \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" \
  -v ON_ERROR_STOP=1 -c "SELECT bool_and(has_table_privilege('$PG_CDC_USER', 'payments.' || t, 'SELECT'))::text FROM unnest(ARRAY['customers','accounts','cards','transactions']) AS t")"
[[ "$SELECT_OK" == "true" ]] || {
  echo "A role '$PG_CDC_USER' não possui SELECT em todas as tabelas CDC." >&2
  echo "Conector não será criado; corrija as permissões antes de continuar." >&2
  exit 1
}

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
SLOT="$(PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -At \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" \
  -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name='payments_slot'")"
[[ "$SLOT" == "0" ]] || { echo "Slot payments_slot já existe; não será reutilizado automaticamente." >&2; exit 1; }
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
