#!/usr/bin/env bash
# Evolução demonstrativa no schema real publicado pelo conector Debezium.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require psql curl jq
: "${SR_URL:?Execute schema-registry.sh primeiro}"
: "${SR_API_KEY:?API key do Schema Registry ausente}"
: "${SR_API_SECRET:?API secret do Schema Registry ausente}"
SUBJECT="${CDC_SUBJECT:-payments.transactions-value}"
API="$SR_URL/subjects/$SUBJECT/versions"
BEFORE="$(curl -fsS -u "$SR_API_KEY:$SR_API_SECRET" "$API" | jq 'length')"
PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" \
  -c "ALTER TABLE payments.transactions ADD COLUMN IF NOT EXISTS channel TEXT" \
  -c "INSERT INTO payments.transactions (card_id,account_id,merchant_id,amount,channel) SELECT card_id,account_id,1,12.34,'schema-evolution' FROM payments.cards WHERE card_id=1"
AFTER="$BEFORE"
for _ in $(seq 1 40); do
  AFTER="$(curl -fsS -u "$SR_API_KEY:$SR_API_SECRET" "$API" | jq 'length')"
  (( AFTER > BEFORE )) && break
  sleep 3
done
(( AFTER > BEFORE )) || { echo "Nenhuma nova versão apareceu para $SUBJECT." >&2; exit 1; }
LATEST="$(curl -fsS -u "$SR_API_KEY:$SR_API_SECRET" "$SR_URL/subjects/$SUBJECT/versions/latest")"
SCHEMA="$(jq -r '.schema' <<<"$LATEST")"
jq -e '.fields | any(.[]; .name=="channel")' <<<"$SCHEMA" >/dev/null || {
  echo "A versão nova não contém channel." >&2; exit 1;
}
{
  echo "Subject: $SUBJECT"
  echo "Versões antes: $BEFORE"
  echo "Versões depois: $AFTER"
  echo "Campo channel: confirmado no schema mais recente"
  jq '{id,subject,version,schemaType}' <<<"$LATEST"
} | tee "$ROOT_DIR/evidence/homologation/07-real-schema-evolution.txt"
