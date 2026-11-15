#!/usr/bin/env bash
# Teardown com confirmações independentes para Confluent e Neon.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require jq

normalize_list() {
  jq -ce 'if type == "array" then . elif type == "object" and (.data | type) == "array" then .data else error("formato JSON não reconhecido") end | if all(.[]; type == "object") then . else error("item inválido na listagem") end'
}

ENV_ID="${ENV_ID:-}"
CC_ENV_NAME="${CC_ENV_NAME:-}"
NEON_PROJECT_ID="${NEON_PROJECT_ID:-}"
NEON_BRANCH="${NEON_BRANCH:-main}"
PG_HOST="${PG_HOST:-}"
PG_DATABASE="${PG_DATABASE:-}"

SA_WRITER_ID="${SA_WRITER_ID:-}"; SA_READER_ID="${SA_READER_ID:-}"; SA_METRICS_ID="${SA_METRICS_ID:-}"
SA_WRITER_CREATED="${SA_WRITER_CREATED:-no}"; SA_READER_CREATED="${SA_READER_CREATED:-no}"; SA_METRICS_CREATED="${SA_METRICS_CREATED:-no}"
METRICS_RBAC_CREATED="${METRICS_RBAC_CREATED:-no}"

CONFLUENT_STATUS="não solicitado"
CONFLUENT_DETAIL="Dados/CLI Confluent ausentes; nenhuma ação executada."
ENV_NAME="não configurado"
ENV_AFTER="não consultado"
SA_REMOVED="nenhuma"
RBAC_STATUS="não aplicável"

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
delete_owned_identities() {
  if [[ "$METRICS_RBAC_CREATED" == yes && -n "$SA_METRICS_ID" ]]; then
    local bindings binding_list
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
}

echo "TEARDOWN CONFLUENT"
if [[ -n "$ENV_ID" && -n "$CC_ENV_NAME" ]] && command -v confluent >/dev/null 2>&1; then
  if ENVIRONMENTS_RAW="$(confluent environment list -o json 2>&1)" &&
     ENVIRONMENTS="$(printf '%s' "$ENVIRONMENTS_RAW" | normalize_list 2>/dev/null)"; then
    ENV_MATCH="$(jq -c --arg id "$ENV_ID" '[.[] | select(.id == $id)]' <<<"$ENVIRONMENTS")"
    MATCH_COUNT="$(jq -r 'length' <<<"$ENV_MATCH")"
    if [[ "$MATCH_COUNT" == "0" ]]; then
      CONFLUENT_STATUS="Environment já ausente"
      CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe antes desta execução."
      ENV_NAME="$CC_ENV_NAME"
      ENV_AFTER="0"
      # Environment pode ter sido removido em execução anterior; limpe apenas
      # identidades cujo estado local registra criação pelo laboratório.
      delete_owned_identities
    elif [[ "$MATCH_COUNT" != "1" ]]; then
      CONFLUENT_STATUS="não verificado"
      CONFLUENT_DETAIL="A listagem não retornou correspondência única; nenhuma exclusão executada."
    else
      ENV_NAME="$(jq -r '.[0].name // empty' <<<"$ENV_MATCH")"
      if [[ "$ENV_NAME" != "$CC_ENV_NAME" ]]; then
        CONFLUENT_STATUS="identidade divergente"
        CONFLUENT_DETAIL="ENV_ID aponta para '$ENV_NAME', esperado '$CC_ENV_NAME'; nenhuma exclusão executada."
      else
        echo "Environment: $ENV_NAME ($ENV_ID)"
        read -r -p "Digite exatamente '$ENV_ID/$ENV_NAME' para confirmar a exclusão Confluent: " answer
        if [[ "$answer" == "$ENV_ID/$ENV_NAME" ]]; then
          if confluent environment delete "$ENV_ID" --force; then
            CONFLUENT_STATUS="solicitado"
            CONFLUENT_DETAIL="Comando de exclusão aceito; verificando Environment."
          else
            CONFLUENT_STATUS="falha"
            CONFLUENT_DETAIL="A CLI retornou erro ao solicitar a exclusão."
          fi
          ENVIRONMENTS_AFTER_RAW="$(confluent environment list -o json)"
          ENVIRONMENTS_AFTER="$(printf '%s' "$ENVIRONMENTS_AFTER_RAW" | normalize_list)"
          ENV_AFTER="$(jq -r --arg id "$ENV_ID" '[.[] | select(.id == $id)] | length' <<<"$ENVIRONMENTS_AFTER")"
          if [[ "$ENV_AFTER" == "0" ]]; then
            CONFLUENT_STATUS="Environment ausente na verificação posterior"
            CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe mais."
            delete_owned_identities
            [[ "$SA_REMOVED" == "nenhuma" ]] || SA_REMOVED="${SA_REMOVED#nenhuma }"
          else
            CONFLUENT_STATUS="não confirmado"
            CONFLUENT_DETAIL="A listagem ainda mostra o Environment; Service Accounts não foram removidas."
          fi
        else
          CONFLUENT_STATUS="preservado"
          CONFLUENT_DETAIL="Confirmação não fornecida; Environment e identidades preservados."
        fi
      fi
    fi
  else
    CONFLUENT_STATUS="não verificado"
    CONFLUENT_DETAIL="Falha ao consultar Environments; nenhuma exclusão Confluent executada."
  fi
else
  echo "Confluent ignorado: informe ENV_ID/CC_ENV_NAME e tenha a Confluent CLI disponível."
fi

echo
echo "TEARDOWN NEON (confirmação independente)"
NEON_STATUS="não solicitado"
NEON_DETAIL="Dados Neon ausentes; nenhuma ação executada."
if [[ -n "$NEON_PROJECT_ID" && -n "$PG_HOST" && -n "$PG_DATABASE" ]]; then
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
    NEON_STATUS="preservado"
    NEON_DETAIL="Confirmação independente não fornecida; banco e objetos Neon preservados."
  fi
else
  echo "Neon ignorado: informe NEON_PROJECT_ID, PG_HOST e PG_DATABASE."
fi

REPORT_DIR="$ROOT_DIR/evidence/teardown"; mkdir -p "$REPORT_DIR"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_PATH="$REPORT_DIR/${TIMESTAMP}-teardown-report.md"
cat > "$REPORT_PATH" <<EOF
# Relatório de Teardown

- Data UTC: $(date -u)
- Environment esperado: $ENV_NAME
- Environment ID: ${ENV_ID:-não informado}
- Environment encontrado após operação: $ENV_AFTER
- Service Accounts removidas (marcador de criação): $SA_REMOVED
- Vínculo MetricsViewer: $RBAC_STATUS

## Confluent

**Estado:** $CONFLUENT_STATUS

$CONFLUENT_DETAIL

A exclusão do Environment remove recursos associados, incluindo clusters Kafka, Schema Registry, Connect e Flink Compute Pools. Confira a página de API Keys após a exclusão: chaves Flink regionais podem exigir verificação específica. A exclusão das Service Accounts criadas pelo laboratório também remove suas API keys associadas. Preserve identidades preexistentes.

## Neon

**Estado:** $NEON_STATUS

Projeto: ${NEON_PROJECT_ID:-não informado}  
Branch: $NEON_BRANCH  
Host: ${PG_HOST:-não informado}  
Database: ${PG_DATABASE:-não informado}

$NEON_DETAIL

Projeto, branch, compute, endpoint e banco não são excluídos. A limpeza SQL remove somente os objetos indicados. Dependências adicionais podem impedir DROP ROLE; não é usado DROP OWNED CASCADE.

## Limites

O Environment é considerado ausente somente após verificação por listagem. Service Accounts são registradas como removidas somente após nova listagem. A limpeza Neon é considerada executada somente quando o wrapper SQL termina com código zero. Estados não consultados não equivalem a zero.
EOF
echo "Relatório salvo: $REPORT_PATH"
