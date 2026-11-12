#!/usr/bin/env bash
# Consome os quatro tópicos desde o início e contabiliza eventos de snapshot (op=r).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq timeout grep
: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
: "${BOOTSTRAP:?BOOTSTRAP ausente}"
: "${READER_API_KEY:?READER_API_KEY ausente}"
: "${READER_API_SECRET:?READER_API_SECRET ausente}"
: "${SR_URL:?SR_URL ausente}"
: "${SR_API_KEY:?SR_API_KEY ausente}"
: "${SR_API_SECRET:?SR_API_SECRET ausente}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
GROUP="snapshot-check-$STAMP"
DIR="$ROOT_DIR/evidence/homologation/$STAMP-cdc-snapshot"
mkdir -p "$DIR"
declare -A EXPECTED=([payments.customers]=200 [payments.accounts]=240 [payments.cards]=320 [payments.transactions]=2000)
PIDS=()
cleanup(){ for pid in "${PIDS[@]:-}"; do kill -TERM "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

for topic in "${!EXPECTED[@]}"; do
  log="$DIR/${topic//./-}.log"
  timeout "${CDC_SNAPSHOT_TIMEOUT:-300}s" confluent kafka topic consume "$topic" \
    --from-beginning --group "$GROUP" --value-format avro --print-offset \
    --schema-registry-endpoint "$SR_URL" --schema-registry-api-key "$SR_API_KEY" \
    --schema-registry-api-secret "$SR_API_SECRET" --api-key "$READER_API_KEY" \
    --api-secret "$READER_API_SECRET" --bootstrap "$BOOTSTRAP" \
    --cluster "$CLUSTER_ID" --environment "$ENV_ID" >"$log" 2>&1 &
  PIDS+=("$!")
done
for pid in "${PIDS[@]}"; do wait "$pid" || true; done
trap - EXIT INT TERM

status=0
SUMMARY="$DIR/summary.txt"
{
  echo "Snapshot CDC — $STAMP UTC"
  echo "Consumer group: $GROUP"
} | tee "$SUMMARY"
for topic in payments.customers payments.accounts payments.cards payments.transactions; do
  log="$DIR/${topic//./-}.log"
  reads="$(grep -c '"op"[[:space:]]*:[[:space:]]*"r"' "$log" || true)"
  expected="${EXPECTED[$topic]}"
  if [[ "$reads" -eq "$expected" ]]; then result=OK; else result=REVISAR; status=1; fi
  printf '%-28s esperado=%-5s recebido=%-5s %s\n' "$topic" "$expected" "$reads" "$result" | tee -a "$SUMMARY"
done
exit "$status"
