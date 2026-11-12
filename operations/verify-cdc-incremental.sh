#!/usr/bin/env bash
# Valida INSERT, UPDATE e DELETE de uma linha temporária na tabela customers.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent psql grep sleep
: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
: "${BOOTSTRAP:?BOOTSTRAP ausente}"
: "${READER_API_KEY:?READER_API_KEY ausente}"
: "${READER_API_SECRET:?READER_API_SECRET ausente}"
: "${SR_URL:?SR_URL ausente}"
: "${SR_API_KEY:?SR_API_KEY ausente}"
: "${SR_API_SECRET:?SR_API_SECRET ausente}"
TEST_ID=999999
TOPIC=payments.customers
mkdir -p "$ROOT_DIR/evidence/homologation"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RAW="$(mktemp)"
OUT="$ROOT_DIR/evidence/homologation/$STAMP-cdc-incremental.txt"
mkdir -p "$(dirname "$OUT")"
cleanup(){
  [[ -n "${PID:-}" ]] && { kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; }
  PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -h "$PG_HOST" -p "$PG_PORT" \
    -U "$PG_ADMIN_USER" -d "$PG_DATABASE" -c "DELETE FROM payments.customers WHERE customer_id=$TEST_ID" >/dev/null 2>&1 || true
  rm -f "$RAW"
}
trap cleanup EXIT
confluent kafka topic consume "$TOPIC" --group app-reader --value-format avro --print-offset \
  --schema-registry-endpoint "$SR_URL" --schema-registry-api-key "$SR_API_KEY" \
  --schema-registry-api-secret "$SR_API_SECRET" --api-key "$READER_API_KEY" \
  --api-secret "$READER_API_SECRET" --bootstrap "$BOOTSTRAP" \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" >"$RAW" 2>&1 &
PID=$!
sleep 5
psql_admin(){
  PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" "$@"
}
psql_admin -c "INSERT INTO payments.customers(customer_id,full_name,email,city) VALUES($TEST_ID,'CDC test','cdc-test@example.test','Rio')"
sleep 2
psql_admin -c "UPDATE payments.customers SET city='Niterói' WHERE customer_id=$TEST_ID"
sleep 2
psql_admin -c "DELETE FROM payments.customers WHERE customer_id=$TEST_ID"
for _ in $(seq 1 30); do
  if grep -q '"op":"d"' "$RAW" && grep -q '"op":"u"' "$RAW" && grep -q '"op":"c"' "$RAW"; then break; fi
  sleep 2
done
kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
unset PID
{
  echo "CDC incremental — $STAMP UTC"
  for op in c u d; do
    if grep -q "\"op\":\"$op\"" "$RAW"; then echo "op=$op CONFIRMADO"; else echo "op=$op AUSENTE"; fi
  done
  grep "cdc-test@example.test" "$RAW" || true
} | tee "$OUT"
