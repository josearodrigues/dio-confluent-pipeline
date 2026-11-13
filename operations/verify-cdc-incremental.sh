#!/usr/bin/env bash
# Valida INSERT, UPDATE e DELETE incrementais em payments.customers.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent psql grep sleep date

: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
: "${BOOTSTRAP:?BOOTSTRAP ausente}"
: "${READER_API_KEY:?READER_API_KEY ausente}"
: "${READER_API_SECRET:?READER_API_SECRET ausente}"
: "${SR_URL:?SR_URL ausente}"
: "${SR_API_KEY:?SR_API_KEY ausente}"
: "${SR_API_SECRET:?SR_API_SECRET ausente}"
: "${PG_HOST:?PG_HOST ausente}"
: "${PG_PORT:?PG_PORT ausente}"
: "${PG_DATABASE:?PG_DATABASE ausente}"
: "${PG_ADMIN_USER:?PG_ADMIN_USER ausente}"
: "${PG_ADMIN_PASSWORD:?PG_ADMIN_PASSWORD ausente}"

TOPIC="payments.customers"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
TEST_ID="$(( $(date +%s) % 1000000000 + 1000000000 ))"
TEST_EMAIL="cdc-test-${STAMP}-${RANDOM}@example.test"
# app-reader é o grupo autorizado pela ACL literal documentada na fundação.
GROUP="app-reader"
TIMEOUT_SECONDS="${CDC_INCREMENTAL_TIMEOUT:-120}"
[[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || { echo "CDC_INCREMENTAL_TIMEOUT deve ser inteiro positivo." >&2; exit 1; }

mkdir -p "$ROOT_DIR/evidence/homologation"
RAW="$(mktemp)"
OUT="$ROOT_DIR/evidence/homologation/$STAMP-cdc-incremental.txt"
CREATED=0
PID=""

psql_admin() {
  PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" "$@"
}
cleanup() {
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  if [[ "$CREATED" == "1" ]]; then
    psql_admin -v test_id="$TEST_ID" >/dev/null 2>&1 <<'SQL' || true
DELETE FROM payments.customers WHERE customer_id = :'test_id';
SQL
  fi
  rm -f "$RAW"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

EXISTS="$(psql_admin -At -v test_id="$TEST_ID" -v test_email="$TEST_EMAIL" <<'SQL'
SELECT (
  EXISTS (SELECT 1 FROM payments.customers WHERE customer_id = :'test_id')
  OR EXISTS (SELECT 1 FROM payments.customers WHERE email = :'test_email')
)::int;
SQL
)"
[[ "$EXISTS" == "0" ]] || { echo "Colisão detectada; nenhum dado foi alterado." >&2; exit 1; }

echo "Iniciando consumidor de $TOPIC (grupo $GROUP)."
confluent kafka topic consume "$TOPIC" --group "$GROUP" --value-format avro --print-offset \
  --schema-registry-endpoint "$SR_URL" --schema-registry-api-key "$SR_API_KEY" \
  --schema-registry-api-secret "$SR_API_SECRET" --api-key "$READER_API_KEY" \
  --api-secret "$READER_API_SECRET" --bootstrap "$BOOTSTRAP" \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" >"$RAW" 2>&1 &
PID=$!
sleep 5

psql_admin -v test_id="$TEST_ID" -v test_email="$TEST_EMAIL" <<'SQL'
INSERT INTO payments.customers(customer_id,full_name,email,city,created_at)
VALUES (:'test_id','CDC test',:'test_email','Rio',CURRENT_TIMESTAMP);
SQL
CREATED=1
sleep 2
psql_admin -v test_id="$TEST_ID" <<'SQL'
UPDATE payments.customers SET city='Niterói' WHERE customer_id = :'test_id';
SQL
sleep 2
psql_admin -v test_id="$TEST_ID" <<'SQL'
DELETE FROM payments.customers WHERE customer_id = :'test_id';
SQL

START_TIME=$SECONDS
while (( SECONDS - START_TIME < TIMEOUT_SECONDS )); do
  MATCHED="$(grep -F "$TEST_EMAIL" "$RAW" || true)"
  FOUND_C=0; FOUND_U=0; FOUND_D=0
  while IFS= read -r line; do
    [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"c\" ]] &&
      [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_C=1
    [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"u\" ]] &&
      [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_U=1
    [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"d\" ]] &&
      [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_D=1
  done <<< "$MATCHED"
  (( FOUND_C && FOUND_U && FOUND_D )) && break
  sleep 2
done

kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
PID=""

MATCHED="$(grep -F "$TEST_EMAIL" "$RAW" || true)"
FOUND_C=0; FOUND_U=0; FOUND_D=0
while IFS= read -r line; do
  [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"c\" ]] &&
    [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_C=1
  [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"u\" ]] &&
    [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_U=1
  [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"d\" ]] &&
    [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_D=1
done <<< "$MATCHED"

STATUS=0
(( FOUND_C )) || STATUS=1
(( FOUND_U )) || STATUS=1
(( FOUND_D )) || STATUS=1
{
  echo "CDC incremental — $STAMP UTC"
  echo "Tópico: $TOPIC"
  echo "Consumer group: $GROUP"
  echo "customer_id: $TEST_ID"
  echo "Marcador: $TEST_EMAIL"
  echo "op=c + snapshot=false: $([[ $FOUND_C == 1 ]] && echo CONFIRMADO || echo AUSENTE)"
  echo "op=u + snapshot=false: $([[ $FOUND_U == 1 ]] && echo CONFIRMADO || echo AUSENTE)"
  echo "op=d + snapshot=false: $([[ $FOUND_D == 1 ]] && echo CONFIRMADO || echo AUSENTE)"
  echo "Eventos correlacionados:"
  printf '%s\n' "$MATCHED"
} | tee "$OUT"

if (( STATUS != 0 )); then
  echo "Falha: não foram confirmados os três eventos incrementais com snapshot=false. Evidência: $OUT" >&2
  exit 1
fi
echo "CDC incremental confirmado. A limpeza do registro é tentada ao encerrar."
