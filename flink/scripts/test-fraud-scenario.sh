#!/usr/bin/env bash
# Homologa o cenário positivo: Neon -> CDC -> Flink enrichment -> fraud_alerts.
# Insere quatro registros no Neon; exige confirmação explícita.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../../scripts/_common.sh"
require confluent jq psql

: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
: "${FLINK_POOL_ID:?FLINK_POOL_ID ausente}"
: "${CC_CLOUD:?CC_CLOUD ausente}"
: "${CC_REGION:?CC_REGION ausente}"
: "${PG_HOST:?PG_HOST ausente}"
: "${PG_PORT:?PG_PORT ausente}"
: "${PG_DATABASE:?PG_DATABASE ausente}"
: "${PG_ADMIN_USER:?PG_ADMIN_USER ausente}"
: "${PG_ADMIN_PASSWORD:?PG_ADMIN_PASSWORD ausente}"

[[ "${CONFIRM_FRAUD_TEST:-}" == yes ]] || {
  echo "O teste insere quatro transações no Neon. Confira o alvo e execute com CONFIRM_FRAUD_TEST=yes." >&2
  exit 1
}

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG="$ROOT_DIR/evidence/homologation/${STAMP}-fraud-scenario.log"
mkdir -p "$(dirname "$LOG")"
exec > >(tee "$LOG") 2>&1

echo "Homologação do cenário positivo de fraude"
echo "Início UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "PostgreSQL: host=$PG_HOST database=$PG_DATABASE user=$PG_ADMIN_USER"
echo "Environment: $ENV_ID | Kafka cluster: $CLUSTER_ID | Flink pool: $FLINK_POOL_ID"

pg_query() {
  PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require \
    psql -X -v ON_ERROR_STOP=1 -Atq \
      -h "$PG_HOST" -p "$PG_PORT" \
      -U "$PG_ADMIN_USER" -d "$PG_DATABASE" "$@"
}

flink_query() {
  confluent flink query --sql "$1" \
    --compute-pool "$FLINK_POOL_ID" \
    --database "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    --cloud "$CC_CLOUD" \
    --region "$CC_REGION" \
    --timeout 2m \
    --output json
}

# Confirma os statements contínuos antes de modificar o banco.
confluent environment use "$ENV_ID" >/dev/null
confluent flink region use --cloud "$CC_CLOUD" --region "$CC_REGION" >/dev/null

STATEMENTS="$(confluent flink statement list \
  --compute-pool "$FLINK_POOL_ID" \
  --environment "$ENV_ID" \
  --cloud "$CC_CLOUD" --region "$CC_REGION" -o json)"

for pair in \
  "payments-enrichment|payments-enrichment-test" \
  "payments-fraud-detection|payments-fraud-detection-test"; do
  IFS='|' read -r canonical test_name <<< "$pair"
  count="$(jq --arg a "$canonical" --arg b "$test_name" \
    '[.[] | select((.name == $a or .name == $b) and .status == "RUNNING")] | length' \
    <<< "$STATEMENTS")"
  [[ "$count" -eq 1 ]] || {
    echo "ERRO: esperado exatamente um statement RUNNING entre '$canonical' e '$test_name'; encontrados: $count."
    exit 1
  }
done

# Registra a fronteira de IDs antes do seed para isolar esta execução.
BASELINE_ID="$(pg_query -c \
  'SELECT COALESCE(MAX(transaction_id), 0) FROM payments.transactions')"
[[ "$BASELINE_ID" =~ ^[0-9]+$ ]] || {
  echo "ERRO: não foi possível obter o último transaction_id."
  exit 1
}
echo "Último transaction_id antes do teste: $BASELINE_ID"

# O seed contém os quatro INSERTs e pg_sleep(90). O wrapper gera log próprio.
bash "$ROOT_DIR/scripts/run-sql.sh" database/seed/simulate-events.sql

# Busca os registros posteriores à fronteira, conferindo todos os atributos.
SEED_ROWS="$(pg_query -c "
SELECT COALESCE(
  json_agg(json_build_object(
    'transaction_id', transaction_id,
    'card_id', card_id,
    'account_id', account_id,
    'merchant_id', merchant_id,
    'amount', amount
  ) ORDER BY transaction_id),
  '[]'::json
)
FROM payments.transactions
WHERE transaction_id > $BASELINE_ID
  AND (
    (card_id = 1 AND account_id = 1 AND merchant_id = 1 AND amount = 100.00)
    OR
    (card_id = 320 AND account_id = 80 AND merchant_id = 1 AND amount = 10.00)
  );
")"

jq -e '
  length == 4
  and ([.[].transaction_id | tostring] | unique | length == 4)
  and ([.[] | select(
    .card_id == 1 and .account_id == 1
    and .merchant_id == 1 and .amount == 100
  )] | length == 3)
  and ([.[] | select(
    .card_id == 320 and .account_id == 80
    and .merchant_id == 1 and .amount == 10
  )] | length == 1)
' <<< "$SEED_ROWS" >/dev/null || {
  echo "FALHA: não foram identificados exatamente os quatro registros esperados no Neon."
  exit 2
}

CARD_IDS_CSV="$(jq -r '[.[] | select(.card_id == 1) | .transaction_id | tostring] | join(",")' <<< "$SEED_ROWS")"
CARD_IDS_JSON="$(jq -c '[.[] | select(.card_id == 1) | .transaction_id]' <<< "$SEED_ROWS")"
echo "Registros inseridos no Neon:"
jq . <<< "$SEED_ROWS"

# Confirma a chegada ao enrichment dos três IDs do cartão 1.
ENRICHED_SQL='SELECT transaction_id, card_id, amount, `$rowtime` AS event_time FROM enriched_payments WHERE transaction_id IN ('"$CARD_IDS_CSV"') ORDER BY `$rowtime`'
ENRICHED_JSON='[]'
ENRICHED_OK=false

for attempt in 1 2 3 4; do
  echo "Consultando enriched_payments (tentativa $attempt/4)..."
  ENRICHED_JSON="$(flink_query "$ENRICHED_SQL")"
  if jq -e --argjson ids "$CARD_IDS_JSON" '
    type == "array"
    and length == 3
    and ([.[].transaction_id | tostring] | sort)
        == ($ids | map(tostring) | sort)
    and all(.[]; .card_id == 1
      and ((.amount | tonumber) > 0)
      and (.event_time | type) == "string")
  ' <<< "$ENRICHED_JSON" >/dev/null; then
    ENRICHED_OK=true
    break
  fi
  [[ "$attempt" -eq 4 ]] || sleep 15
done

[[ "$ENRICHED_OK" == true ]] || {
  echo "NÃO CONFIRMADO: os três IDs do cartão 1 não apareceram no enrichment."
  exit 2
}

# Compara os limites temporais do alerta com os três eventos efetivamente vistos.
EVENT_RANGE="$(jq -c '[.[].event_time] | sort | {first: .[0], last: .[-1]}' <<< "$ENRICHED_JSON")"
FIRST_TXN="$(jq -r '.first' <<< "$EVENT_RANGE")"
LAST_TXN="$(jq -r '.last' <<< "$EVENT_RANGE")"
echo "Rowtime observado no enrichment: $FIRST_TXN até $LAST_TXN"

FRAUD_SQL='SELECT card_id, first_txn, last_txn, txn_count FROM fraud_alerts WHERE card_id = 1 AND txn_count = 3 ORDER BY first_txn DESC LIMIT 100'
FRAUD_JSON='[]'
FRAUD_OK=false

for attempt in 1 2 3 4; do
  echo "Consultando fraud_alerts (tentativa $attempt/4)..."
  FRAUD_JSON="$(flink_query "$FRAUD_SQL")"
  if jq -e --arg first "$FIRST_TXN" --arg last "$LAST_TXN" '
    any(.[]; .card_id == 1
      and (.txn_count | tostring) == "3"
      and .first_txn == $first
      and .last_txn == $last)
  ' <<< "$FRAUD_JSON" >/dev/null; then
    FRAUD_OK=true
    break
  fi
  [[ "$attempt" -eq 4 ]] || sleep 15
done

[[ "$FRAUD_OK" == true ]] || {
  echo "NÃO CONFIRMADO: nenhum alerta correspondeu aos rowtimes dos três eventos."
  echo "Alertas retornados para diagnóstico:"
  jq . <<< "$FRAUD_JSON"
  exit 2
}

echo "Alerta correlacionado:"
jq --arg first "$FIRST_TXN" --arg last "$LAST_TXN" '
  .[] | select(.first_txn == $first and .last_txn == $last)
' <<< "$FRAUD_JSON"
echo "APROVADO: os três eventos chegaram ao enrichment e o alerta correspondente foi observado."
echo "Log: ${LOG#"$ROOT_DIR"/}"
