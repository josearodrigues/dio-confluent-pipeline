#!/usr/bin/env bash
# Valida a evolução do subject real payments.transactions-value ponta a ponta.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq curl psql grep sleep date

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

[[ "${CONFIRM_SCHEMA_EVOLUTION:-}" == "yes" ]] || {
  echo "A operação altera a tabela payments.transactions, a compatibilidade do subject e insere um registro. Use CONFIRM_SCHEMA_EVOLUTION=yes após conferir o ambiente." >&2
  exit 1
}

SUBJECT="${CDC_SUBJECT:-payments.transactions-value}"
CONNECTOR_NAME="payments-postgres-cdc"
TOPIC="payments.transactions"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
TEST_CHANNEL="schema-evolution-${STAMP}-${RANDOM}"
GROUP="app-reader"
TIMEOUT_SECONDS="${SCHEMA_EVOLUTION_TIMEOUT:-120}"
[[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || { echo "SCHEMA_EVOLUTION_TIMEOUT deve ser inteiro positivo." >&2; exit 1; }

mkdir -p "$ROOT_DIR/evidence/homologation"
OUT="$ROOT_DIR/evidence/homologation/$STAMP-real-schema-evolution.txt"
RAW="$(mktemp)"
PID=""
INSERTED=0

fail() { echo "ERRO: $*" >&2; exit 1; }
sr_call() {
  local method="$1" path="$2" data="${3:-}" body code
  body="$(mktemp)"
  local -a args=(-sS -o "$body" -w '%{http_code}' -X "$method"
    -u "$SR_API_KEY:$SR_API_SECRET" -H "Content-Type: application/vnd.schemaregistry.v1+json")
  [[ -n "$data" ]] && args+=(-d "$data")
  code="$(curl "${args[@]}" "$SR_URL$path")" || { rm -f "$body"; fail "falha de transporte no Schema Registry ($method $path)"; }
  if (( code < 200 || code >= 300 )); then
    cat "$body" >&2; rm -f "$body"; fail "HTTP $code no Schema Registry ($method $path)"
  fi
  cat "$body"
  rm -f "$body"
}
psql_admin() {
  PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" "$@"
}
cleanup() {
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  if (( INSERTED )); then
    psql_admin -v marker="$TEST_CHANNEL" >/dev/null 2>&1 <<'SQL' || true
DELETE FROM payments.transactions WHERE channel = :'marker';
SQL
  fi
  rm -f "$RAW"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Confirma existência do subject e evita uma execução que não possa gerar nova versão.
VERSIONS_BEFORE="$(sr_call GET "/subjects/$SUBJECT/versions")" ||
  fail "subject $SUBJECT inexistente; confirme que o CDC já publicou eventos."
BEFORE_COUNT="$(jq 'length' <<< "$VERSIONS_BEFORE")"
(( BEFORE_COUNT > 0 )) || fail "subject $SUBJECT não possui versões."
LATEST_BEFORE="$(jq -r 'max' <<< "$VERSIONS_BEFORE")"
SCHEMA_BEFORE="$(sr_call GET "/subjects/$SUBJECT/versions/$LATEST_BEFORE" | jq -r '.schema')"
if jq -e '[.. | objects | select(.name? == "channel")] | length > 0' <<< "$SCHEMA_BEFORE" >/dev/null; then
  fail "o schema mais recente já contém channel; não há evolução nova a demonstrar."
fi

# Confirma o conector antes de alterar a tabela.
confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
CONNECTORS="$(confluent connect cluster list -o json)"
CONNECTOR_STATUS="$(jq -r --arg n "$CONNECTOR_NAME" '[.[] | select(.name==$n)][0].status // empty' <<< "$CONNECTORS")"
[[ "$CONNECTOR_STATUS" == "RUNNING" ]] || fail "conector $CONNECTOR_NAME não está RUNNING (status=${CONNECTOR_STATUS:-ausente})."

# A política é alterada somente após a confirmação explícita acima.
sr_call PUT "/config/$SUBJECT" '{"compatibility":"BACKWARD"}' >/dev/null
LEVEL="$(sr_call GET "/config/$SUBJECT?defaultToGlobal=true" | jq -r '.compatibilityLevel // .compatibility // empty')"
case "$LEVEL" in
  BACKWARD|BACKWARD_TRANSITIVE|FULL|FULL_TRANSITIVE) ;;
  *) fail "nível efetivo '$LEVEL' não garante compatibilidade BACKWARD." ;;
esac

# Abre o consumidor antes do evento; app-reader é o grupo coberto pela ACL documentada.
confluent kafka topic consume "$TOPIC" --group "$GROUP" --value-format avro --print-offset \
  --schema-registry-endpoint "$SR_URL" --schema-registry-api-key "$SR_API_KEY" \
  --schema-registry-api-secret "$SR_API_SECRET" --api-key "$READER_API_KEY" \
  --api-secret "$READER_API_SECRET" --bootstrap "$BOOTSTRAP" \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" >"$RAW" 2>&1 &
PID=$!
sleep 5

# A coluna é opcional no Avro (default null); o INSERT produz o evento que registra o novo schema.
psql_admin -c "ALTER TABLE payments.transactions ADD COLUMN IF NOT EXISTS channel TEXT"
psql_admin -v marker="$TEST_CHANNEL" <<'SQL'
INSERT INTO payments.transactions (card_id,account_id,merchant_id,amount,channel,created_at)
SELECT card_id,account_id,1,12.34,:'marker',CURRENT_TIMESTAMP
FROM payments.cards WHERE card_id=1;
SQL
INSERTED=1

START_TIME=$SECONDS
AFTER_COUNT="$BEFORE_COUNT"
MATCHED_LINE=""
while (( SECONDS - START_TIME < TIMEOUT_SECONDS )); do
  VERSIONS_AFTER="$(sr_call GET "/subjects/$SUBJECT/versions")"
  AFTER_COUNT="$(jq 'length' <<< "$VERSIONS_AFTER")"
  MATCHED_LINE="$(grep -F "$TEST_CHANNEL" "$RAW" | head -n 1 || true)"
  if (( AFTER_COUNT > BEFORE_COUNT )) && [[ -n "$MATCHED_LINE" ]]; then break; fi
  sleep 3
done

kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
PID=""

(( AFTER_COUNT > BEFORE_COUNT )) || fail "nenhuma nova versão foi registrada para $SUBJECT."
[[ -n "$MATCHED_LINE" ]] || fail "evento com channel=$TEST_CHANNEL não foi observado em $TOPIC."

LATEST="$(jq -r 'max' <<< "$VERSIONS_AFTER")"
PREVIOUS="$(jq -r --argjson latest "$LATEST" '[.[] | select(. < $latest)] | max' <<< "$VERSIONS_AFTER")"
LATEST_RESPONSE="$(sr_call GET "/subjects/$SUBJECT/versions/$LATEST")"
NEW_SCHEMA="$(jq -r '.schema' <<< "$LATEST_RESPONSE")"
jq -e '[.. | objects | select(.name? == "channel")] | length > 0' <<< "$NEW_SCHEMA" >/dev/null ||
  fail "campo channel ausente no schema v$LATEST."

COMPAT="$(sr_call POST "/compatibility/subjects/$SUBJECT/versions/$PREVIOUS" \
  "$(jq -Rn --arg schema "$NEW_SCHEMA" '{schemaType:"AVRO",schema:$schema}')")"
[[ "$(jq -r '.is_compatible' <<< "$COMPAT")" == "true" ]] ||
  fail "v$LATEST não é compatível com v$PREVIOUS sob $LEVEL."

# Confere novamente o estado do conector após a evolução.
CONNECTORS="$(confluent connect cluster list -o json)"
CONNECTOR_STATUS="$(jq -r --arg n "$CONNECTOR_NAME" '[.[] | select(.name==$n)][0].status // empty' <<< "$CONNECTORS")"
[[ "$CONNECTOR_STATUS" == "RUNNING" ]] || fail "conector deixou de estar RUNNING após a evolução."

{
  echo "Evolução do subject CDC real — $STAMP UTC"
  echo "Subject: $SUBJECT"
  echo "Tópico: $TOPIC"
  echo "Versões antes/depois: $BEFORE_COUNT/$AFTER_COUNT"
  echo "Versão anterior/nova: $PREVIOUS/$LATEST"
  echo "Compatibilidade efetiva: $LEVEL"
  echo "is_compatible: true"
  echo "Campo channel no schema novo: confirmado"
  echo "Conector $CONNECTOR_NAME: RUNNING"
  echo "Evento correlacionado: $TEST_CHANNEL"
  echo "$MATCHED_LINE"
} | tee "$OUT"
echo "Evidência: $OUT"
