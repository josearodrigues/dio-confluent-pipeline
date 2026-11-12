#!/usr/bin/env bash
# Valida INSERT, UPDATE e DELETE de um registro temporário na tabela customers.
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

TOPIC=payments.customers
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
# IDs ficam dentro do intervalo INT e são conferidos antes do INSERT.
TEST_ID="$(( $(date +%s) % 1000000000 + 1000000000 ))"
TEST_EMAIL="cdc-test-${STAMP}-${RANDOM}@example.test"
GROUP="snapshot-check-incremental-$STAMP"
mkdir -p "$ROOT_DIR/evidence/homologation"
RAW="$(mktemp)"
OUT="$ROOT_DIR/evidence/homologation/$STAMP-cdc-incremental.txt"
CREATED=0
PID=""

psql_admin(){
  PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" "$@"
}

cleanup(){
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  # Só remove o registro quando este processo confirmou que o criou.
  if [[ "$CREATED" == "1" ]]; then
    psql_admin -v test_id="$TEST_ID" \
      -c "DELETE FROM payments.customers WHERE customer_id = :'test_id'" \
      >/dev/null 2>&1 || true
  fi
  rm -f "$RAW"
}
trap cleanup EXIT

# Pré-valida ID e e-mail para não colidir com dados preexistentes.
EXISTS="$(psql_admin -At -v test_id="$TEST_ID" -v test_email="$TEST_EMAIL" \
  -c "SELECT (EXISTS (SELECT 1 FROM payments.customers WHERE customer_id = :'test_id') OR EXISTS (SELECT 1 FROM payments.customers WHERE email = :'test_email'))::int")"
[[ "$EXISTS" == "0" ]] || {
  echo "Colisão detectada para o registro de teste; nenhum dado foi alterado." >&2
  exit 1
}

confluent kafka topic consume "$TOPIC" --group "$GROUP" --value-format avro --print-offset \
  --schema-registry-endpoint "$SR_URL" --schema-registry-api-key "$SR_API_KEY" \
  --schema-registry-api-secret "$SR_API_SECRET" --api-key "$READER_API_KEY" \
  --api-secret "$READER_API_SECRET" --bootstrap "$BOOTSTRAP" \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" >"$RAW" 2>&1 &
PID=$!
sleep 5

psql_admin -v test_id="$TEST_ID" -v test_email="$TEST_EMAIL" \
  -c "INSERT INTO payments.customers(customer_id,full_name,email,city) VALUES (:'test_id','CDC test',:'test_email','Rio')"
CREATED=1
sleep 2
psql_admin -v test_id="$TEST_ID" \
  -c "UPDATE payments.customers SET city='Niterói' WHERE customer_id = :'test_id'"
sleep 2
psql_admin -v test_id="$TEST_ID" \
  -c "DELETE FROM payments.customers WHERE customer_id = :'test_id'"

for _ in $(seq 1 30); do
  if grep -Eq '"op"[[:space:]]*:[[:space:]]*"d"' "$RAW" &&
     grep -Eq '"op"[[:space:]]*:[[:space:]]*"u"' "$RAW" &&
     grep -Eq '"op"[[:space:]]*:[[:space:]]*"c"' "$RAW"; then
    break
  fi
  sleep 2
done

kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
PID=""

FOUND_C=0
FOUND_U=0
FOUND_D=0
# Exige que cada operação esteja no evento que contém o marcador exclusivo.
grep -F "$TEST_EMAIL" "$RAW" | grep -Eq '"op"[[:space:]]*:[[:space:]]*"c"' && FOUND_C=1 || true
grep -F "$TEST_EMAIL" "$RAW" | grep -Eq '"op"[[:space:]]*:[[:space:]]*"u"' && FOUND_U=1 || true
grep -F "$TEST_EMAIL" "$RAW" | grep -Eq '"op"[[:space:]]*:[[:space:]]*"d"' && FOUND_D=1 || true

status=0
[[ "$FOUND_C" == "1" ]] || status=1
[[ "$FOUND_U" == "1" ]] || status=1
[[ "$FOUND_D" == "1" ]] || status=1
{
  echo "CDC incremental — $STAMP UTC"
  echo "Consumer group: $GROUP"
  echo "Test customer_id: $TEST_ID"
  echo "Test email: $TEST_EMAIL"
  [[ "$FOUND_C" == "1" ]] && echo "op=c CONFIRMADO" || echo "op=c AUSENTE"
  [[ "$FOUND_U" == "1" ]] && echo "op=u CONFIRMADO" || echo "op=u AUSENTE"
  [[ "$FOUND_D" == "1" ]] && echo "op=d CONFIRMADO" || echo "op=d AUSENTE"
  grep -F "$TEST_EMAIL" "$RAW" || true
} | tee "$OUT"

if [[ "$status" -ne 0 ]]; then
  echo "Falha: uma ou mais operações CDC esperadas não foram observadas. Consulte $OUT." >&2
  exit 1
fi
echo "As operações c, u e d foram observadas. Evidência: $OUT"
