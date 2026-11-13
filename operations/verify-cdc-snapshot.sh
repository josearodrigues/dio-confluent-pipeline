#!/usr/bin/env bash
# Valida o snapshot CDC com um consumidor independente por tópico.
# Cada consumidor é encerrado assim que recebe a quantidade esperada de eventos op=r.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent awk date sleep

: "${ENV_ID:?ENV_ID ausente}"
: "${CLUSTER_ID:?CLUSTER_ID ausente}"
: "${BOOTSTRAP:?BOOTSTRAP ausente}"
: "${READER_API_KEY:?READER_API_KEY ausente}"
: "${READER_API_SECRET:?READER_API_SECRET ausente}"
: "${SR_URL:?SR_URL ausente}"
: "${SR_API_KEY:?SR_API_KEY ausente}"
: "${SR_API_SECRET:?SR_API_SECRET ausente}"

TIMEOUT_SECONDS="${CDC_SNAPSHOT_TIMEOUT:-1800}"
[[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
  echo "ERRO: CDC_SNAPSHOT_TIMEOUT deve ser um inteiro positivo (segundos)." >&2
  exit 1
}

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
GROUP="snapshot-check-$STAMP"
DIR="$ROOT_DIR/evidence/homologation/$STAMP-cdc-snapshot"
SUMMARY="$DIR/summary.txt"
mkdir -p "$DIR"
cd "$ROOT_DIR"

TOPICS=(payments.customers payments.accounts payments.cards payments.transactions)
declare -A EXPECTED=(
  [payments.customers]=200
  [payments.accounts]=240
  [payments.cards]=320
  [payments.transactions]=2000
)
declare -A PIDS STARTS STOP_REASONS RETURN_CODES

count_log() {
  awk '
    /"op"[[:space:]]*:[[:space:]]*"r"/ { op_r++ }
    /"snapshot"[[:space:]]*:[[:space:]]*"(true|first|first_in_data_collection|last|last_in_data_collection)"/ { snapshot_markers++ }
    /^Partition:[[:space:]]*[0-9]+[[:space:]]+Offset:[[:space:]]*[0-9]+/ { records++ }
    END { printf "%d %d %d\n", op_r, snapshot_markers, records }
  ' "$1"
}

stop_consumer() {
  local topic="$1" reason="$2" pid="${PIDS[$1]}" rc=0
  STOP_REASONS["$topic"]="$reason"
  kill -TERM "$pid" 2>/dev/null || true
  wait "$pid" || rc=$?
  RETURN_CODES["$topic"]="$rc"
  unset 'PIDS[$topic]'
}

cleanup() {
  local topic pid
  for topic in "${!PIDS[@]}"; do
    pid="${PIDS[$topic]}"
    kill -TERM "$pid" 2>/dev/null || true
  done
  for topic in "${!PIDS[@]}"; do
    wait "${PIDS[$topic]}" 2>/dev/null || true
  done
}
trap cleanup EXIT
trap 'exit 130' INT TERM

{
  echo "Validação paralela do snapshot CDC"
  echo "UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Consumer group: $GROUP"
  echo "Timeout máximo individual: ${TIMEOUT_SECONDS}s"
  echo "Critério de conclusão: quantidade esperada de eventos op=r por tópico."
  echo "Marcadores snapshot são registrados para diagnóstico, não condicionam a conclusão."
  echo
} | tee "$SUMMARY"

echo "==> Iniciando os quatro consumidores em paralelo"
for topic in "${TOPICS[@]}"; do
  log="$DIR/${topic//./-}.log"
  echo "Iniciando $topic (esperado: ${EXPECTED[$topic]}) -> $log"

  confluent kafka topic consume "$topic" \
    --from-beginning \
    --group "$GROUP" \
    --value-format avro \
    --print-offset \
    --schema-registry-endpoint "$SR_URL" \
    --schema-registry-api-key "$SR_API_KEY" \
    --schema-registry-api-secret "$SR_API_SECRET" \
    --api-key "$READER_API_KEY" \
    --api-secret "$READER_API_SECRET" \
    --bootstrap "$BOOTSTRAP" \
    --cluster "$CLUSTER_ID" \
    --environment "$ENV_ID" >"$log" 2>&1 &

  PIDS["$topic"]=$!
  STARTS["$topic"]=$SECONDS
  STOP_REASONS["$topic"]="running"
done

active=${#TOPICS[@]}
while (( active > 0 )); do
  for topic in "${TOPICS[@]}"; do
    [[ -n "${PIDS[$topic]:-}" ]] || continue

    log="$DIR/${topic//./-}.log"
    expected="${EXPECTED[$topic]}"
    read -r op_r snapshot_markers records < <(count_log "$log")

    if (( op_r >= expected )); then
      stop_consumer "$topic" "target-reached"
      active=$((active - 1))
      echo "Concluído: $topic ($op_r/$expected eventos op=r)."
    elif ! kill -0 "${PIDS[$topic]}" 2>/dev/null; then
      rc=0
      wait "${PIDS[$topic]}" || rc=$?
      RETURN_CODES["$topic"]="$rc"
      STOP_REASONS["$topic"]="exited-before-target"
      unset 'PIDS[$topic]'
      active=$((active - 1))
      echo "Consumidor encerrou antes da meta: $topic ($op_r/$expected; código $rc)."
    elif (( SECONDS - STARTS[$topic] >= TIMEOUT_SECONDS )); then
      stop_consumer "$topic" "timeout"
      active=$((active - 1))
      echo "Timeout individual: $topic ($op_r/$expected após ${TIMEOUT_SECONDS}s)."
    fi
  done
  (( active == 0 )) || sleep 2
done

trap - EXIT INT TERM
status=0
{
  echo
  printf '%-28s %8s %10s %12s %10s %s\n' "TÓPICO" "ESPERADO" "op=r" "MARCADORES" "REGISTROS" "RESULTADO"
} | tee -a "$SUMMARY"

for topic in "${TOPICS[@]}"; do
  log="$DIR/${topic//./-}.log"
  expected="${EXPECTED[$topic]}"
  read -r op_r snapshot_markers records < <(count_log "$log")
  reason="${STOP_REASONS[$topic]}"
  rc="${RETURN_CODES[$topic]:-unknown}"

  if (( op_r == expected )); then
    result="OK"
  elif (( op_r > expected )); then
    result="DIVERGENTE"
    status=1
  else
    result="INCOMPLETO"
    status=1
  fi

  printf '%-28s %8s %10s %12s %10s %s\n' \
    "$topic" "$expected" "$op_r" "$snapshot_markers" "$records" "$result" | tee -a "$SUMMARY"
  echo "Encerramento: $reason (código do processo: $rc)" | tee -a "$SUMMARY"
  echo "Log: $log" | tee -a "$SUMMARY"
done

echo | tee -a "$SUMMARY"
if (( status == 0 )); then
  echo "RESULTADO: snapshot validado nos quatro tópicos (2.760 eventos op=r)." | tee -a "$SUMMARY"
else
  echo "RESULTADO: snapshot incompleto ou divergente; consulte os logs individuais." | tee -a "$SUMMARY"
fi
echo "Resumo: $SUMMARY"
exit "$status"
