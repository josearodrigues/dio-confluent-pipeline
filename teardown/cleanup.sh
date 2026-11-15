#!/usr/bin/env bash
# Remove o Environment Confluent e, mediante confirmação independente,
# somente os objetos PostgreSQL pertencentes ao laboratório no Neon.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq
: "${ENV_ID:?ENV_ID ausente; recuse executar sem estado local válido}"
: "${CC_ENV_NAME:?CC_ENV_NAME ausente}"
: "${NEON_PROJECT_ID:?NEON_PROJECT_ID ausente no .env}"
: "${NEON_BRANCH:=main}"
: "${PG_HOST:?PG_HOST ausente no .env}"
: "${PG_DATABASE:?PG_DATABASE ausente no .env}"

normalize_list() {
  jq -ce 'if type == "array" then . elif type == "object" and (.data | type) == "array" then .data else error("formato JSON não reconhecido") end | if all(.[]; type == "object") then . else error("item inválido na listagem") end'
}
ENVIRONMENTS="$(confluent environment list -o json | normalize_list)"
ENV_MATCH="$(jq -c --arg id "$ENV_ID" '[.[] | select(.id == $id)]' <<<"$ENVIRONMENTS")"
[[ "$(jq -r 'length' <<<"$ENV_MATCH")" == 1 ]] || { echo "Environment não encontrado de forma única; abortando." >&2; exit 1; }
ENV_NAME="$(jq -r '.[0].name // empty' <<<"$ENV_MATCH")"
[[ "$ENV_NAME" == "$CC_ENV_NAME" ]] || { echo "ENV_ID aponta para '$ENV_NAME', esperado '$CC_ENV_NAME'; abortando." >&2; exit 1; }

SA_WRITER_ID="${SA_WRITER_ID:-}"; SA_READER_ID="${SA_READER_ID:-}"; SA_METRICS_ID="${SA_METRICS_ID:-}"
SA_WRITER_CREATED="${SA_WRITER_CREATED:-no}"; SA_READER_CREATED="${SA_READER_CREATED:-no}"; SA_METRICS_CREATED="${SA_METRICS_CREATED:-no}"
METRICS_RBAC_CREATED="${METRICS_RBAC_CREATED:-no}"
CONFLUENT_STATUS="preservado (não confirmado)"
CONFLUENT_DETAIL="Environment não removido."
NEON_STATUS="preservado (não confirmado)"
NEON_DETAIL="Limpeza SQL não executada."
ENV_AFTER="não consultado"
SA_REMOVED="nenhuma"
RBAC_STATUS="não aplicável"

echo "TEARDOWN CONFLUENT"
echo "Environment: $ENV_NAME ($ENV_ID)"
echo "A exclusão remove os recursos associados ao Environment."
read -r -p "Digite exatamente '$ENV_ID/$ENV_NAME' para confirmar a exclusão Confluent: " answer
if [[ "$answer" == "$ENV_ID/$ENV_NAME" ]]; then
  if confluent environment delete "$ENV_ID" --force; then
    CONFLUENT_STATUS="solicitado"
    CONFLUENT_DETAIL="Comando de exclusão aceito; verificando Environment."
  else
    CONFLUENT_STATUS="falha"
    CONFLUENT_DETAIL="A CLI retornou erro ao solicitar a exclusão do Environment."
  fi
  ENVIRONMENTS_AFTER="$(confluent environment list -o json | normalize_list)"
  ENV_AFTER="$(jq -r --arg id "$ENV_ID" '[.[] | select(.id == $id)] | length' <<<"$ENVIRONMENTS_AFTER")"
  if [[ "$ENV_AFTER" == "0" ]]; then
    CONFLUENT_STATUS="Environment ausente na verificação posterior"
    CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe mais."
    delete_owned_sa() {
      local id="$1" created="$2" label="$3" list normalized exists
      [[ "$created" == yes && -n "$id" ]] || return 0
      list="$(confluent iam service-account list -o json)"
      normalized="$(printf '%s' "$list" | normalize_list)"
      exists="$(jq -r --arg id "$id" '[.[] | select(.id == $id)] | length' <<<"$normalized")"
      if [[ "$exists" == "1" ]]; then
        confluent iam service-account delete "$id" --force
        list="$(confluent iam service-account list -o json)"
        normalized="$(printf '%s' "$list" | normalize_list)"
        exists="$(jq -r --arg id "$id" '[.[] | select(.id == $id)] | length' <<<"$normalized")"
        [[ "$exists" == "0" ]] || { echo "Service Account $label ($id) ainda aparece na listagem." >&2; return 1; }
        SA_REMOVED="$SA_REMOVED $label"
      fi
    }
    if [[ "$METRICS_RBAC_CREATED" == yes && -n "$SA_METRICS_ID" ]]; then
      bindings="$(confluent iam rbac role-binding list --principal "User:$SA_METRICS_ID" --role MetricsViewer -o json)"
      binding_list="$(printf '%s' "$bindings" | normalize_list)"
      if [[ "$(jq -r 'length' <<<"$binding_list")" -gt 0 ]]; then
        confluent iam rbac role-binding delete --principal "User:$SA_METRICS_ID" --role MetricsViewer --force
      fi
      RBAC_STATUS="MetricsViewer removido quando presente (marcador de criação confirmado)"
    fi
    delete_owned_sa "$SA_WRITER_ID" "$SA_WRITER_CREATED" "cdc-writer"
    delete_owned_sa "$SA_READER_ID" "$SA_READER_CREATED" "app-reader"
    delete_owned_sa "$SA_METRICS_ID" "$SA_METRICS_CREATED" "metrics-reader"
    [[ "$SA_REMOVED" == "nenhuma" ]] || SA_REMOVED="${SA_REMOVED#nenhuma }"
  else
    CONFLUENT_STATUS="não confirmado"
    CONFLUENT_DETAIL="A listagem ainda mostra o Environment; Service Accounts não foram removidas."
  fi
else
  CONFLUENT_DETAIL="Confirmação não fornecida; Environment e identidades foram preservados."
fi

echo
echo "TEARDOWN NEON (confirmação independente)"
echo "Projeto: $NEON_PROJECT_ID | Branch: $NEON_BRANCH"
echo "PostgreSQL: host=$PG_HOST database=$PG_DATABASE"
echo "Serão removidos payments_slot, payments_pub, schema payments, public.fraud_alerts"
echo "e as roles configuradas em PG_CDC_USER e PG_SINK_USER."
echo "Projeto, branch, compute, endpoint e banco serão preservados."
read -r -p "Digite exatamente '$NEON_PROJECT_ID/$NEON_BRANCH/$PG_DATABASE' para confirmar a limpeza Neon: " neon_answer
if [[ "$neon_answer" == "$NEON_PROJECT_ID/$NEON_BRANCH/$PG_DATABASE" ]]; then
  if [[ -z "${PG_CDC_USER:-}" || -z "${PG_SINK_USER:-}" ]]; then
    NEON_STATUS="falha de configuração"
    NEON_DETAIL="PG_CDC_USER ou PG_SINK_USER ausente; SQL não executado."
  elif NEON_OUTPUT="$(CONFIRM_CLEANUP=yes "$ROOT_DIR/scripts/run-sql.sh" database/cleanup/postgres-cleanup.sql 2>&1)"; then
    NEON_STATUS="SQL executado"
    NEON_DETAIL="$NEON_OUTPUT"
  else
    NEON_STATUS="falha durante a limpeza SQL"
    NEON_DETAIL="$NEON_OUTPUT"
  fi
else
  NEON_DETAIL="Confirmação independente não fornecida; banco e objetos Neon foram preservados."
fi

REPORT_DIR="$ROOT_DIR/evidence/teardown"; mkdir -p "$REPORT_DIR"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_PATH="$REPORT_DIR/${TIMESTAMP}-teardown-report.md"
cat > "$REPORT_PATH" <<EOF
# Relatório de Teardown

- Data UTC: $(date -u)
- Environment esperado: $ENV_NAME
- Environment ID: $ENV_ID
- Environment encontrado após exclusão: $ENV_AFTER
- Service Accounts removidas (marcador de criação): $SA_REMOVED
- Vínculo MetricsViewer: $RBAC_STATUS

## Confluent

**Estado:** $CONFLUENT_STATUS

$CONFLUENT_DETAIL

A exclusão do Environment remove recursos associados, incluindo clusters Kafka, Schema Registry, Connect e Flink Compute Pools. API keys relacionadas ao Flink podem ter escopo regional e não são removidas pela exclusão do Environment. A exclusão das Service Accounts criadas pelo laboratório é tratada separadamente e remove suas API keys associadas. Revise manualmente possíveis chaves regionais remanescentes e identidades preexistentes.

## Neon

**Estado:** $NEON_STATUS

Projeto: $NEON_PROJECT_ID  
Branch: $NEON_BRANCH  
Host: $PG_HOST  
Database: $PG_DATABASE

$NEON_DETAIL

Projeto, branch, compute, endpoint e banco não são excluídos. A limpeza SQL remove somente os objetos indicados. Dependências adicionais podem impedir DROP ROLE; não é usado DROP OWNED CASCADE.

## Limites

O Environment é considerado ausente somente após verificação por listagem. Service Accounts são registradas como removidas somente após nova listagem. A limpeza Neon é considerada executada somente quando o wrapper SQL termina com código zero. Estados não consultados não equivalem a zero.
EOF
echo "Relatório salvo: $REPORT_PATH"
