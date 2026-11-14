#!/usr/bin/env bash
# Valida INSERT, UPDATE e DELETE incrementais em payments.customers.
set -Eeuo pipefail
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
RUN_ID="${STAMP}-${BASHPID}"
TEST_ID="$(( 1000000000 + ($(date +%s) % 800000000) + RANDOM ))"
TEST_EMAIL="cdc-test-${RUN_ID}-${RANDOM}@example.test"
# Grupo exclusivo coberto pela ACL prefixada snapshot-check-* da fundação.
GROUP="snapshot-check-${RUN_ID}"
TIMEOUT_SECONDS="${CDC_INCREMENTAL_TIMEOUT:-120}"
STARTUP_TIMEOUT="${CDC_INCREMENTAL_STARTUP_TIMEOUT:-20}"
for value in "$TIMEOUT_SECONDS" "$STARTUP_TIMEOUT"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "Timeout deve ser inteiro positivo." >&2; exit 1; }
done

cd "$ROOT_DIR"
mkdir -p "$ROOT_DIR/evidence/homologation"
RAW="$(mktemp)"
OUT="$ROOT_DIR/evidence/homologation/${RUN_ID}-cdc-incremental.txt"
PID=""
SAFE_TO_CLEAN=0
CLEANUP_STATUS="não executada"
CLEANUP_RESIDUAL="não verificado"
CLEANUP_DONE=0
FOUND_C=0; FOUND_U=0; FOUND_D=0

psql_admin() {
  PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" "$@"
}
stop_consumer() {
  if [[ -n "$PID" ]]; then
    kill -TERM "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
    PID=""
  fi
}
cleanup_test_record() {
  local residual
  [[ "$SAFE_TO_CLEAN" == "1" && "$CLEANUP_DONE" == "0" ]] || return 0
  if ! psql_admin -v test_id="$TEST_ID" -v test_email="$TEST_EMAIL" <<'SQL' >/dev/null; then
DELETE FROM payments.customers
WHERE customer_id = :'test_id' AND email = :'test_email';
SQL
    CLEANUP_STATUS="FALHOU"
    CLEANUP_RESIDUAL="erro ao excluir"
  else
    residual="$(psql_admin -At -v test_id="$TEST_ID" -v test_email="$TEST_EMAIL" <<'SQL'
SELECT count(*) FROM payments.customers
WHERE customer_id = :'test_id' AND email = :'test_email';
SQL
    )" || residual="erro SQL"
    if [[ "$residual" == "0" ]]; then
      CLEANUP_STATUS="confirmada"; CLEANUP_RESIDUAL="0"
    else
      CLEANUP_STATUS="FALHOU"; CLEANUP_RESIDUAL="${residual:-erro SQL}"
    fi
  fi
  CLEANUP_DONE=1
}
cleanup() {
  local rc=$?
  stop_consumer
  cleanup_test_record
  if [[ -n "${OUT:-}" && -f "$OUT" ]]; then
    {
      echo
      echo "=== ENCERRAMENTO ==="
      echo "Limpeza: $CLEANUP_STATUS"
      echo "Resíduos com ID+marcador: $CLEANUP_RESIDUAL"
      [[ "$rc" == "0" ]] || echo "Código de saída: $rc"
    } >> "$OUT"
  fi
  [[ -z "${RAW:-}" ]] || rm -f "$RAW"
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
SAFE_TO_CLEAN=1

{
  echo "CDC incremental — $STAMP UTC"
  echo "Tópico: $TOPIC"
  echo "Consumer group exclusivo: $GROUP"
  echo "customer_id: $TEST_ID"
  echo "Marcador: $TEST_EMAIL"
  echo "auto.offset.reset=latest; sem --from-beginning"
  echo "Timeout de inicialização: ${STARTUP_TIMEOUT}s; timeout CDC: ${TIMEOUT_SECONDS}s"
  echo
} > "$OUT"

# O grupo novo e latest evitam replay do snapshot; a ACL prefixada já é criada pela fundação.
confluent kafka topic consume "$TOPIC" --group "$GROUP" --value-format avro --print-offset \
  --config "auto.offset.reset=latest" \
  --schema-registry-endpoint "$SR_URL" --schema-registry-api-key "$SR_API_KEY" \
  --schema-registry-api-secret "$SR_API_SECRET" --api-key "$READER_API_KEY" \
  --api-secret "$READER_API_SECRET" --bootstrap "$BOOTSTRAP" \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" >"$RAW" 2>&1 &
PID=$!

START_TIME=$SECONDS
while (( SECONDS - START_TIME < STARTUP_TIMEOUT )); do
  grep -Fq "Starting Kafka Consumer" "$RAW" && break
  if ! kill -0 "$PID" 2>/dev/null; then
    wait "$PID" || true; PID=""
    cat "$RAW" >> "$OUT"
    echo "ERRO: consumidor encerrou antes de iniciar." >&2
    exit 1
  fi
  sleep 1
done
if ! grep -Fq "Starting Kafka Consumer" "$RAW"; then
  cat "$RAW" >> "$OUT"
  echo "ERRO: consumidor não confirmou inicialização em ${STARTUP_TIMEOUT}s." >&2
  exit 1
fi
sleep 3
kill -0 "$PID" 2>/dev/null || { cat "$RAW" >> "$OUT"; echo "ERRO: consumidor encerrou antes do SQL." >&2; exit 1; }

psql_admin -v test_id="$TEST_ID" -v test_email="$TEST_EMAIL" <<'SQL' | tee -a "$OUT"
INSERT INTO payments.customers(customer_id,full_name,email,city,created_at)
VALUES (:'test_id','CDC test',:'test_email','Rio',CURRENT_TIMESTAMP);
SQL
sleep 2
psql_admin -v test_id="$TEST_ID" <<'SQL' | tee -a "$OUT"
UPDATE payments.customers SET city='Niterói' WHERE customer_id = :'test_id';
SQL
sleep 2
psql_admin -v test_id="$TEST_ID" <<'SQL' | tee -a "$OUT"
DELETE FROM payments.customers WHERE customer_id = :'test_id';
SQL

scan_events() {
  local line
  FOUND_C=0; FOUND_U=0; FOUND_D=0
  while IFS= read -r line; do
    [[ "$line" == *"$TEST_EMAIL"* ]] || continue
    [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"c\" ]] &&
      [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_C=1
    [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"u\" ]] &&
      [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_U=1
    [[ "$line" =~ \"op\"[[:space:]]*:[[:space:]]*\"d\" ]] &&
      [[ "$line" =~ \"snapshot\"[[:space:]]*:[[:space:]]*(\"false\"|false) ]] && FOUND_D=1
  done < "$RAW"
}
START_TIME=$SECONDS
while (( SECONDS - START_TIME < TIMEOUT_SECONDS )); do
  scan_events
  (( FOUND_C == 1 && FOUND_U == 1 && FOUND_D == 1 )) && break
  kill -0 "$PID" 2>/dev/null || break
  sleep 2
done
stop_consumer
scan_events
STATUS=0
(( FOUND_C == 1 )) || STATUS=1
(( FOUND_U == 1 )) || STATUS=1
(( FOUND_D == 1 )) || STATUS=1

{
  echo
  echo "=== SAÍDA BRUTA DO CONSUMIDOR (inclui offsets) ==="
  cat "$RAW"
  echo
  echo "=== RESULTADO CDC ==="
  echo "op=c, snapshot=false: $([[ $FOUND_C == 1 ]] && echo CONFIRMADO || echo AUSENTE)"
  echo "op=u, snapshot=false: $([[ $FOUND_U == 1 ]] && echo CONFIRMADO || echo AUSENTE)"
  echo "op=d, snapshot=false: $([[ $FOUND_D == 1 ]] && echo CONFIRMADO || echo AUSENTE)"
  echo "Consumer group: $GROUP"
  echo "Código de resultado CDC: $STATUS"
} | tee -a "$OUT"

cleanup_test_record
{
  echo
  echo "=== LIMPEZA ==="
  echo "Resultado: $CLEANUP_STATUS"
  echo "Registros residuais com ID+marcador: $CLEANUP_RESIDUAL"
} | tee -a "$OUT"
[[ "$CLEANUP_STATUS" == "confirmada" ]] || STATUS=1

if (( STATUS != 0 )); then
  echo "CDC incremental incompleto ou limpeza não confirmada. Evidência: $OUT" >&2
  exit 1
fi
echo "CDC incremental confirmado. Evidência: $OUT"
