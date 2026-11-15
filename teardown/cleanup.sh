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
CONFLUENT_ENV_CONFIRMED="no"
ENV_NAME="não configurado"
ENV_AFTER="não consultado"
SA_REMOVED="nenhuma"
RBAC_STATUS="não aplicável"
IDENTITIES_STATUS="não consultadas"

delete_owned_sa() {
  local id="$1" created="$2" label="$3" list normalized exists
  [[ "$created" == yes && -n "$id" ]] || return 0
  list="$(confluent iam service-account list -o json)" || { echo "Falha ao listar Service Accounts antes de remover $label ($id)." >&2; return 1; }
  normalized="$(printf '%s' "$list" | normalize_list)" || return 1
  exists="$(jq -r --arg id "$id" '[.[] | select(.id == $id)] | length' <<<"$normalized")" || return 1
  if [[ "$exists" == "1" ]]; then
    confluent iam service-account delete "$id" --force || { echo "Falha ao solicitar a remoção de $label ($id)." >&2; return 1; }
    list="$(confluent iam service-account list -o json)" || { echo "Falha ao verificar a remoção de $label ($id)." >&2; return 1; }
    normalized="$(printf '%s' "$list" | normalize_list)" || return 1
    exists="$(jq -r --arg id "$id" '[.[] | select(.id == $id)] | length' <<<"$normalized")" || return 1
    [[ "$exists" == "0" ]] || { echo "Service Account $label ($id) ainda aparece na listagem." >&2; return 1; }
    if [[ "$SA_REMOVED" == "nenhuma" ]]; then SA_REMOVED="$label"; else SA_REMOVED="$SA_REMOVED $label"; fi
  elif [[ "$exists" != "0" ]]; then
    echo "Resultado inesperado ao verificar $label ($id): $exists." >&2
    return 1
  fi
}

delete_owned_identities() {
  if [[ "$METRICS_RBAC_CREATED" == yes && -n "$SA_METRICS_ID" ]]; then
    local bindings binding_list count
    bindings="$(confluent iam rbac role-binding list --principal "User:$SA_METRICS_ID" --role MetricsViewer -o json)" || { echo "Falha ao consultar o vínculo MetricsViewer de $SA_METRICS_ID." >&2; return 1; }
    binding_list="$(printf '%s' "$bindings" | normalize_list)" || return 1
    count="$(jq -er 'length' <<<"$binding_list")" || return 1
    if [[ "$count" -gt 0 ]]; then
      confluent iam rbac role-binding delete --principal "User:$SA_METRICS_ID" --role MetricsViewer --force || { echo "Falha ao solicitar a remoção do vínculo MetricsViewer de $SA_METRICS_ID." >&2; return 1; }
      bindings="$(confluent iam rbac role-binding list --principal "User:$SA_METRICS_ID" --role MetricsViewer -o json)" || { echo "Falha ao verificar a remoção do vínculo MetricsViewer de $SA_METRICS_ID." >&2; return 1; }
      binding_list="$(printf '%s' "$bindings" | normalize_list)" || return 1
      count="$(jq -er 'length' <<<"$binding_list")" || return 1
      [[ "$count" == "0" ]] || { echo "O vínculo MetricsViewer de $SA_METRICS_ID ainda aparece na listagem." >&2; return 1; }
      RBAC_STATUS="MetricsViewer removido e ausência confirmada por listagem"
    else
      RBAC_STATUS="MetricsViewer já ausente, confirmado por listagem"
    fi
  fi
  delete_owned_sa "$SA_WRITER_ID" "$SA_WRITER_CREATED" "cdc-writer" || return 1
  delete_owned_sa "$SA_READER_ID" "$SA_READER_CREATED" "app-reader" || return 1
  delete_owned_sa "$SA_METRICS_ID" "$SA_METRICS_CREATED" "metrics-reader" || return 1
}

has_owned_identity_state() {
  [[ ( "$SA_WRITER_CREATED" == yes && -n "$SA_WRITER_ID" ) ||
     ( "$SA_READER_CREATED" == yes && -n "$SA_READER_ID" ) ||
     ( "$SA_METRICS_CREATED" == yes && -n "$SA_METRICS_ID" ) ||
     ( "$METRICS_RBAC_CREATED" == yes && -n "$SA_METRICS_ID" ) ]]
}

confirm_and_delete_owned_identities() {
  if ! has_owned_identity_state; then
    IDENTITIES_STATUS="nenhuma identidade marcada como criada pelo laboratório"
    return 0
  fi
  echo "Identidades/vínculos marcados como criados pelo laboratório:"
  [[ "$SA_WRITER_CREATED" == yes ]] && echo "  cdc-writer: $SA_WRITER_ID"
  [[ "$SA_READER_CREATED" == yes ]] && echo "  app-reader: $SA_READER_ID"
  [[ "$SA_METRICS_CREATED" == yes ]] && echo "  metrics-reader: $SA_METRICS_ID"
  [[ "$METRICS_RBAC_CREATED" == yes ]] && echo "  MetricsViewer: User:$SA_METRICS_ID"
  read -r -p "Digite exatamente 'IDENTITIES/$ENV_ID' para confirmar a remoção dessas identidades/vínculos: " identity_answer
  if [[ "$identity_answer" == "IDENTITIES/$ENV_ID" ]]; then
    if delete_owned_identities; then
      IDENTITIES_STATUS="remoção solicitada e verificações posteriores concluídas"
    else
      IDENTITIES_STATUS="falha; a remoção pode ter sido parcial"
      return 1
    fi
  else
    IDENTITIES_STATUS="preservadas: confirmação independente não fornecida"
    echo "Identidades Confluent preservadas por falta de confirmação."
  fi
}

print_resource_list() {
  local label="$1" json="$2" rows
  rows="$(jq -r '
    .[] |
    "  - \(.name // .connector_name // .display_name // "(nome não retornado)") | ID: \(.id // .connector_id // .cluster_id // "(ID não retornado)")"
  ' <<<"$json")" || return 1
  if [[ -n "$rows" ]]; then
    printf '%s\n' "$rows"
  else
    echo "  - Nenhum recurso encontrado."
  fi
}

inventory_environment() {
  local env_id="$1" clusters cluster_list cluster_id cluster_name
  local sr_json sr_id sr_name pools pool_list connectors connector_list failed=no

  echo
  echo "INVENTÁRIO SOMENTE LEITURA — Environment $CC_ENV_NAME ($env_id)"
  echo "Nenhum recurso será alterado durante estas consultas."

  if clusters="$(confluent kafka cluster list --environment "$env_id" -o json 2>&1)" &&
     cluster_list="$(printf "%s" "$clusters" | normalize_list 2>/dev/null)"; then
    echo "Kafka clusters:"
    print_resource_list "Kafka" "$cluster_list" || failed=yes
  else
    echo "Kafka clusters: FALHA NA CONSULTA"
    printf "  %s\n" "$clusters"
    failed=yes; cluster_list="[]"
  fi

  if sr_json="$(confluent schema-registry cluster describe --environment "$env_id" -o json 2>&1)"; then
    sr_id="$(jq -r ".cluster_id // .id // .cluster // empty" <<<"$sr_json" 2>/dev/null || true)"
    sr_name="$(jq -r ".name // .display_name // empty" <<<"$sr_json" 2>/dev/null || true)"
    echo "Schema Registry:"
    if [[ -n "$sr_id" ]]; then
      printf "  - %s | ID: %s\n" "${sr_name:-nome não retornado}" "$sr_id"
    else
      echo "  - Consulta concluída, mas a resposta não apresentou um ID reconhecível."
      failed=yes
    fi
  else
    echo "Schema Registry: FALHA NA CONSULTA"
    printf "  %s\n" "$sr_json"
    failed=yes
  fi

  if pools="$(confluent flink compute-pool list --environment "$env_id" -o json 2>&1)" &&
     pool_list="$(printf "%s" "$pools" | normalize_list 2>/dev/null)"; then
    echo "Flink Compute Pools:"
    print_resource_list "Flink" "$pool_list" || failed=yes
  else
    echo "Flink Compute Pools: FALHA NA CONSULTA"
    printf "  %s\n" "$pools"
    failed=yes
  fi

  echo "Kafka Connect — conectores:"
  if [[ "$(jq -r "length" <<<"$cluster_list")" == "0" ]]; then
    echo "  - Não consultados: nenhum Kafka cluster retornado."
  else
    while read -r cluster_id cluster_name; do
      [[ -n "$cluster_id" ]] || continue
      printf "  Cluster %s (%s):\n" "${cluster_name:-nome não retornado}" "$cluster_id"
      if connectors="$(confluent connect cluster list --cluster "$cluster_id" --environment "$env_id" -o json 2>&1)" &&
         connector_list="$(printf "%s" "$connectors" | normalize_list 2>/dev/null)"; then
        print_resource_list "Connect" "$connector_list" || failed=yes
      else
        echo "  - FALHA NA CONSULTA"
        printf "    %s\n" "$connectors"
        failed=yes
      fi
    done < <(jq -r ".[] | [(.id // .cluster_id // \"\"), (.name // \"\")] | @tsv" <<<"$cluster_list")
  fi

  if [[ "$failed" == yes ]]; then
    echo
    echo "INVENTÁRIO INCOMPLETO: exclusão bloqueada para evitar confirmação sem conhecer os recursos."
    return 1
  fi
  return 0
}

print_deletion_warning() {
  local yellow='' red='' reset='' bold='' line
  if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
    red=if [[ -n "$ENV_ID" && -n "$CC_ENV_NAME" ]] && command -v confluent >/dev/null 2>&1; then
  if ENVIRONMENTS_RAW="$(confluent environment list -o json 2>&1)" &&
     ENVIRONMENTS="$(printf '%s' "$ENVIRONMENTS_RAW" | normalize_list 2>/dev/null)"; then
    ENV_MATCH="$(jq -c --arg id "$ENV_ID" '[.[] | select(.id == $id)]' <<<"$ENVIRONMENTS")"
    MATCH_COUNT="$(jq -r 'length' <<<"$ENV_MATCH")"
    if [[ "$MATCH_COUNT" == "0" ]]; then
      CONFLUENT_ENV_CONFIRMED="yes"
      CONFLUENT_STATUS="Environment já ausente"
      CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe antes desta execução."
      ENV_NAME="$CC_ENV_NAME"
      ENV_AFTER="0"
      # Environment pode ter sido removido em execução anterior; limpe apenas
      # identidades cujo estado local registra criação pelo laboratório.
      confirm_and_delete_owned_identities || {
        CONFLUENT_STATUS="falha ao remover identidades"
        CONFLUENT_DETAIL="Environment já ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
      }
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
          if ENVIRONMENTS_AFTER_RAW="$(confluent environment list -o json 2>&1)" &&
             ENVIRONMENTS_AFTER="$(printf '%s' "$ENVIRONMENTS_AFTER_RAW" | normalize_list 2>/dev/null)"; then
            ENV_AFTER="$(jq -r --arg id "$ENV_ID" '[.[] | select(.id == $id)] | length' <<<"$ENVIRONMENTS_AFTER")"
          else
            ENV_AFTER="não consultado"
          fi
          if [[ "$ENV_AFTER" == "0" ]]; then
            CONFLUENT_ENV_CONFIRMED="yes"
            CONFLUENT_STATUS="Environment ausente na verificação posterior"
            CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe mais."
            confirm_and_delete_owned_identities || {
              CONFLUENT_STATUS="Environment removido; falha ao remover identidades"
              CONFLUENT_DETAIL="Environment ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
            }
            [[ "$SA_REMOVED" == "nenhuma" ]] || SA_REMOVED="${SA_REMOVED#nenhuma }"
          elif [[ "$ENV_AFTER" == "não consultado" ]]; then
            CONFLUENT_STATUS="não verificado"
            CONFLUENT_DETAIL="Não foi possível consultar novamente a listagem do Environment; a limpeza Neon será bloqueada."
          else
            CONFLUENT_STATUS="não confirmado"
            CONFLUENT_DETAIL="A listagem ainda mostra o Environment; identidades e vínculos próprios não foram removidos."
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
if [[ "$CONFLUENT_ENV_CONFIRMED" != "yes" ]]; then
  NEON_STATUS="bloqueado: Environment Confluent não confirmado como ausente"
  NEON_DETAIL="A limpeza Neon não foi iniciada. Confirme a ausência do Environment Confluent por listagem e execute novamente o teardown."
  echo "Neon bloqueado: a ausência do Environment Confluent não foi confirmada."
elif [[ -n "$NEON_PROJECT_ID" && -n "$PG_HOST" && -n "$PG_DATABASE" ]]; then
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
    elif NEON_OUTPUT="$(CONFLUENT_ENV_CONFIRMED=1 CONFIRM_CLEANUP=yes "$ROOT_DIR/scripts/run-sql.sh" database/cleanup/postgres-cleanup.sql 2>&1)"; then
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
- Estado das identidades próprias: $IDENTITIES_STATUS

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
\033[31m'; yellow=if [[ -n "$ENV_ID" && -n "$CC_ENV_NAME" ]] && command -v confluent >/dev/null 2>&1; then
  if ENVIRONMENTS_RAW="$(confluent environment list -o json 2>&1)" &&
     ENVIRONMENTS="$(printf '%s' "$ENVIRONMENTS_RAW" | normalize_list 2>/dev/null)"; then
    ENV_MATCH="$(jq -c --arg id "$ENV_ID" '[.[] | select(.id == $id)]' <<<"$ENVIRONMENTS")"
    MATCH_COUNT="$(jq -r 'length' <<<"$ENV_MATCH")"
    if [[ "$MATCH_COUNT" == "0" ]]; then
      CONFLUENT_ENV_CONFIRMED="yes"
      CONFLUENT_STATUS="Environment já ausente"
      CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe antes desta execução."
      ENV_NAME="$CC_ENV_NAME"
      ENV_AFTER="0"
      # Environment pode ter sido removido em execução anterior; limpe apenas
      # identidades cujo estado local registra criação pelo laboratório.
      confirm_and_delete_owned_identities || {
        CONFLUENT_STATUS="falha ao remover identidades"
        CONFLUENT_DETAIL="Environment já ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
      }
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
          if ENVIRONMENTS_AFTER_RAW="$(confluent environment list -o json 2>&1)" &&
             ENVIRONMENTS_AFTER="$(printf '%s' "$ENVIRONMENTS_AFTER_RAW" | normalize_list 2>/dev/null)"; then
            ENV_AFTER="$(jq -r --arg id "$ENV_ID" '[.[] | select(.id == $id)] | length' <<<"$ENVIRONMENTS_AFTER")"
          else
            ENV_AFTER="não consultado"
          fi
          if [[ "$ENV_AFTER" == "0" ]]; then
            CONFLUENT_ENV_CONFIRMED="yes"
            CONFLUENT_STATUS="Environment ausente na verificação posterior"
            CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe mais."
            confirm_and_delete_owned_identities || {
              CONFLUENT_STATUS="Environment removido; falha ao remover identidades"
              CONFLUENT_DETAIL="Environment ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
            }
            [[ "$SA_REMOVED" == "nenhuma" ]] || SA_REMOVED="${SA_REMOVED#nenhuma }"
          elif [[ "$ENV_AFTER" == "não consultado" ]]; then
            CONFLUENT_STATUS="não verificado"
            CONFLUENT_DETAIL="Não foi possível consultar novamente a listagem do Environment; a limpeza Neon será bloqueada."
          else
            CONFLUENT_STATUS="não confirmado"
            CONFLUENT_DETAIL="A listagem ainda mostra o Environment; identidades e vínculos próprios não foram removidos."
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
if [[ "$CONFLUENT_ENV_CONFIRMED" != "yes" ]]; then
  NEON_STATUS="bloqueado: Environment Confluent não confirmado como ausente"
  NEON_DETAIL="A limpeza Neon não foi iniciada. Confirme a ausência do Environment Confluent por listagem e execute novamente o teardown."
  echo "Neon bloqueado: a ausência do Environment Confluent não foi confirmada."
elif [[ -n "$NEON_PROJECT_ID" && -n "$PG_HOST" && -n "$PG_DATABASE" ]]; then
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
    elif NEON_OUTPUT="$(CONFLUENT_ENV_CONFIRMED=1 CONFIRM_CLEANUP=yes "$ROOT_DIR/scripts/run-sql.sh" database/cleanup/postgres-cleanup.sql 2>&1)"; then
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
- Estado das identidades próprias: $IDENTITIES_STATUS

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
\033[33m'; bold=if [[ -n "$ENV_ID" && -n "$CC_ENV_NAME" ]] && command -v confluent >/dev/null 2>&1; then
  if ENVIRONMENTS_RAW="$(confluent environment list -o json 2>&1)" &&
     ENVIRONMENTS="$(printf '%s' "$ENVIRONMENTS_RAW" | normalize_list 2>/dev/null)"; then
    ENV_MATCH="$(jq -c --arg id "$ENV_ID" '[.[] | select(.id == $id)]' <<<"$ENVIRONMENTS")"
    MATCH_COUNT="$(jq -r 'length' <<<"$ENV_MATCH")"
    if [[ "$MATCH_COUNT" == "0" ]]; then
      CONFLUENT_ENV_CONFIRMED="yes"
      CONFLUENT_STATUS="Environment já ausente"
      CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe antes desta execução."
      ENV_NAME="$CC_ENV_NAME"
      ENV_AFTER="0"
      # Environment pode ter sido removido em execução anterior; limpe apenas
      # identidades cujo estado local registra criação pelo laboratório.
      confirm_and_delete_owned_identities || {
        CONFLUENT_STATUS="falha ao remover identidades"
        CONFLUENT_DETAIL="Environment já ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
      }
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
          if ENVIRONMENTS_AFTER_RAW="$(confluent environment list -o json 2>&1)" &&
             ENVIRONMENTS_AFTER="$(printf '%s' "$ENVIRONMENTS_AFTER_RAW" | normalize_list 2>/dev/null)"; then
            ENV_AFTER="$(jq -r --arg id "$ENV_ID" '[.[] | select(.id == $id)] | length' <<<"$ENVIRONMENTS_AFTER")"
          else
            ENV_AFTER="não consultado"
          fi
          if [[ "$ENV_AFTER" == "0" ]]; then
            CONFLUENT_ENV_CONFIRMED="yes"
            CONFLUENT_STATUS="Environment ausente na verificação posterior"
            CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe mais."
            confirm_and_delete_owned_identities || {
              CONFLUENT_STATUS="Environment removido; falha ao remover identidades"
              CONFLUENT_DETAIL="Environment ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
            }
            [[ "$SA_REMOVED" == "nenhuma" ]] || SA_REMOVED="${SA_REMOVED#nenhuma }"
          elif [[ "$ENV_AFTER" == "não consultado" ]]; then
            CONFLUENT_STATUS="não verificado"
            CONFLUENT_DETAIL="Não foi possível consultar novamente a listagem do Environment; a limpeza Neon será bloqueada."
          else
            CONFLUENT_STATUS="não confirmado"
            CONFLUENT_DETAIL="A listagem ainda mostra o Environment; identidades e vínculos próprios não foram removidos."
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
if [[ "$CONFLUENT_ENV_CONFIRMED" != "yes" ]]; then
  NEON_STATUS="bloqueado: Environment Confluent não confirmado como ausente"
  NEON_DETAIL="A limpeza Neon não foi iniciada. Confirme a ausência do Environment Confluent por listagem e execute novamente o teardown."
  echo "Neon bloqueado: a ausência do Environment Confluent não foi confirmada."
elif [[ -n "$NEON_PROJECT_ID" && -n "$PG_HOST" && -n "$PG_DATABASE" ]]; then
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
    elif NEON_OUTPUT="$(CONFLUENT_ENV_CONFIRMED=1 CONFIRM_CLEANUP=yes "$ROOT_DIR/scripts/run-sql.sh" database/cleanup/postgres-cleanup.sql 2>&1)"; then
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
- Estado das identidades próprias: $IDENTITIES_STATUS

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
\033[1m'; reset=if [[ -n "$ENV_ID" && -n "$CC_ENV_NAME" ]] && command -v confluent >/dev/null 2>&1; then
  if ENVIRONMENTS_RAW="$(confluent environment list -o json 2>&1)" &&
     ENVIRONMENTS="$(printf '%s' "$ENVIRONMENTS_RAW" | normalize_list 2>/dev/null)"; then
    ENV_MATCH="$(jq -c --arg id "$ENV_ID" '[.[] | select(.id == $id)]' <<<"$ENVIRONMENTS")"
    MATCH_COUNT="$(jq -r 'length' <<<"$ENV_MATCH")"
    if [[ "$MATCH_COUNT" == "0" ]]; then
      CONFLUENT_ENV_CONFIRMED="yes"
      CONFLUENT_STATUS="Environment já ausente"
      CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe antes desta execução."
      ENV_NAME="$CC_ENV_NAME"
      ENV_AFTER="0"
      # Environment pode ter sido removido em execução anterior; limpe apenas
      # identidades cujo estado local registra criação pelo laboratório.
      confirm_and_delete_owned_identities || {
        CONFLUENT_STATUS="falha ao remover identidades"
        CONFLUENT_DETAIL="Environment já ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
      }
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
          if ENVIRONMENTS_AFTER_RAW="$(confluent environment list -o json 2>&1)" &&
             ENVIRONMENTS_AFTER="$(printf '%s' "$ENVIRONMENTS_AFTER_RAW" | normalize_list 2>/dev/null)"; then
            ENV_AFTER="$(jq -r --arg id "$ENV_ID" '[.[] | select(.id == $id)] | length' <<<"$ENVIRONMENTS_AFTER")"
          else
            ENV_AFTER="não consultado"
          fi
          if [[ "$ENV_AFTER" == "0" ]]; then
            CONFLUENT_ENV_CONFIRMED="yes"
            CONFLUENT_STATUS="Environment ausente na verificação posterior"
            CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe mais."
            confirm_and_delete_owned_identities || {
              CONFLUENT_STATUS="Environment removido; falha ao remover identidades"
              CONFLUENT_DETAIL="Environment ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
            }
            [[ "$SA_REMOVED" == "nenhuma" ]] || SA_REMOVED="${SA_REMOVED#nenhuma }"
          elif [[ "$ENV_AFTER" == "não consultado" ]]; then
            CONFLUENT_STATUS="não verificado"
            CONFLUENT_DETAIL="Não foi possível consultar novamente a listagem do Environment; a limpeza Neon será bloqueada."
          else
            CONFLUENT_STATUS="não confirmado"
            CONFLUENT_DETAIL="A listagem ainda mostra o Environment; identidades e vínculos próprios não foram removidos."
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
if [[ "$CONFLUENT_ENV_CONFIRMED" != "yes" ]]; then
  NEON_STATUS="bloqueado: Environment Confluent não confirmado como ausente"
  NEON_DETAIL="A limpeza Neon não foi iniciada. Confirme a ausência do Environment Confluent por listagem e execute novamente o teardown."
  echo "Neon bloqueado: a ausência do Environment Confluent não foi confirmada."
elif [[ -n "$NEON_PROJECT_ID" && -n "$PG_HOST" && -n "$PG_DATABASE" ]]; then
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
    elif NEON_OUTPUT="$(CONFLUENT_ENV_CONFIRMED=1 CONFIRM_CLEANUP=yes "$ROOT_DIR/scripts/run-sql.sh" database/cleanup/postgres-cleanup.sql 2>&1)"; then
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
- Estado das identidades próprias: $IDENTITIES_STATUS

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
\033[0m'
  fi
  line='======================================================================'
  printf '\n%s%s%s\n' "$red$bold" "$line" "$reset"
  printf '%s%s  OPERAÇÃO DESTRUTIVA: EXCLUSÃO DO ENVIRONMENT%s\n' "$red$bold" "$reset"
  printf '%s\n' "$line"
  printf 'Environment: %s (%s)\n' "$ENV_NAME" "$ENV_ID"
  echo "A exclusão do Environment remove os recursos associados listados acima."
  echo "A operação é irreversível e interrompe a disponibilidade desses recursos."
  echo "Objetivo: interromper o consumo de créditos pelos recursos Confluent deste Environment."
  echo
  echo "NÃO serão removidos nesta confirmação:"
  echo "  - Projeto, branch, compute, endpoint e banco Neon."
  echo "  - Service Accounts: exigem confirmação independente, se marcadas como próprias."
  echo "  - Limpeza SQL Neon: exige confirmação independente após validar a ausência do Environment."
  printf '%s%s%s\n\n' "$yellow$bold" "Somente digitar a sequência literal abaixo autoriza a exclusão." "$reset"
  printf '%s%s%s\n\n' "$red$bold" "$line" "$reset"
}

echo "TEARDOWN CONFLUENT"
if [[ -n "$ENV_ID" && -n "$CC_ENV_NAME" ]] && command -v confluent >/dev/null 2>&1; then
  if ENVIRONMENTS_RAW="$(confluent environment list -o json 2>&1)" &&
     ENVIRONMENTS="$(printf '%s' "$ENVIRONMENTS_RAW" | normalize_list 2>/dev/null)"; then
    ENV_MATCH="$(jq -c --arg id "$ENV_ID" '[.[] | select(.id == $id)]' <<<"$ENVIRONMENTS")"
    MATCH_COUNT="$(jq -r 'length' <<<"$ENV_MATCH")"
    if [[ "$MATCH_COUNT" == "0" ]]; then
      CONFLUENT_ENV_CONFIRMED="yes"
      CONFLUENT_STATUS="Environment já ausente"
      CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe antes desta execução."
      ENV_NAME="$CC_ENV_NAME"
      ENV_AFTER="0"
      # Environment pode ter sido removido em execução anterior; limpe apenas
      # identidades cujo estado local registra criação pelo laboratório.
      confirm_and_delete_owned_identities || {
        CONFLUENT_STATUS="falha ao remover identidades"
        CONFLUENT_DETAIL="Environment já ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
      }
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
          if ENVIRONMENTS_AFTER_RAW="$(confluent environment list -o json 2>&1)" &&
             ENVIRONMENTS_AFTER="$(printf '%s' "$ENVIRONMENTS_AFTER_RAW" | normalize_list 2>/dev/null)"; then
            ENV_AFTER="$(jq -r --arg id "$ENV_ID" '[.[] | select(.id == $id)] | length' <<<"$ENVIRONMENTS_AFTER")"
          else
            ENV_AFTER="não consultado"
          fi
          if [[ "$ENV_AFTER" == "0" ]]; then
            CONFLUENT_ENV_CONFIRMED="yes"
            CONFLUENT_STATUS="Environment ausente na verificação posterior"
            CONFLUENT_DETAIL="A listagem confirmou que o Environment não existe mais."
            confirm_and_delete_owned_identities || {
              CONFLUENT_STATUS="Environment removido; falha ao remover identidades"
              CONFLUENT_DETAIL="Environment ausente, mas a remoção de identidades/vínculos próprios falhou; consulte o estado detalhado no relatório."
            }
            [[ "$SA_REMOVED" == "nenhuma" ]] || SA_REMOVED="${SA_REMOVED#nenhuma }"
          elif [[ "$ENV_AFTER" == "não consultado" ]]; then
            CONFLUENT_STATUS="não verificado"
            CONFLUENT_DETAIL="Não foi possível consultar novamente a listagem do Environment; a limpeza Neon será bloqueada."
          else
            CONFLUENT_STATUS="não confirmado"
            CONFLUENT_DETAIL="A listagem ainda mostra o Environment; identidades e vínculos próprios não foram removidos."
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
if [[ "$CONFLUENT_ENV_CONFIRMED" != "yes" ]]; then
  NEON_STATUS="bloqueado: Environment Confluent não confirmado como ausente"
  NEON_DETAIL="A limpeza Neon não foi iniciada. Confirme a ausência do Environment Confluent por listagem e execute novamente o teardown."
  echo "Neon bloqueado: a ausência do Environment Confluent não foi confirmada."
elif [[ -n "$NEON_PROJECT_ID" && -n "$PG_HOST" && -n "$PG_DATABASE" ]]; then
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
    elif NEON_OUTPUT="$(CONFLUENT_ENV_CONFIRMED=1 CONFIRM_CLEANUP=yes "$ROOT_DIR/scripts/run-sql.sh" database/cleanup/postgres-cleanup.sql 2>&1)"; then
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
- Estado das identidades próprias: $IDENTITIES_STATUS

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
