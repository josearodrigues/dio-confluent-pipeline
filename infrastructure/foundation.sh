#!/usr/bin/env bash
# Provisiona Environment, Kafka Basic, identidades e ACLs do laboratório.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq
fail(){ echo "ERRO: $*" >&2; exit 1; }
find_id(){ jq -r --arg n "$2" '[.[]|select(.name==$n)][0].id // empty' <<<"$1"; }
SA_CREATED=no
ensure_sa(){
  local name="$1" desc="$2" list id
  list="$(confluent iam service-account list -o json)"
  id="$(find_id "$list" "$name")"
  if [[ -z "$id" ]]; then
    id="$(confluent iam service-account create "$name" --description "$desc" -o json | jq -r '.id')"
    SA_CREATED=yes
  else
    SA_CREATED=no
  fi
  SA_RESULT_ID="$id"
}
ensure_key(){
  local resource="$1" sa="$2" keyvar="$3" secretvar="$4" json existing
  if [[ -n "${!keyvar:-}" && -n "${!secretvar:-}" &&
        "${!keyvar}" != null && "${!secretvar}" != null ]]; then
    if confluent api-key list --resource "$resource" --service-account "$sa" -o json |
      jq -e --arg k "${!keyvar}" 'any(.[]; (.key // .api_key // .id)==$k)' >/dev/null; then
      return
    fi
  fi
  # Estado ausente/inválido (inclusive o literal "null") não é reutilizável.
  unset "$keyvar" "$secretvar"
  existing="$(confluent api-key list --resource "$resource" --service-account "$sa" -o json | jq 'length')"
  if [[ "$existing" -gt 0 ]]; then
    fail "já existem $existing API key(s) para $keyvar, mas o segredo não está salvo. Recupere o estado ou revise as chaves antes de criar outra."
  fi
  json="$(confluent api-key create --resource "$resource" --service-account "$sa" --description "payments-lab-$keyvar" -o json)"
  save_api_key_credentials "$json" "$keyvar" "$secretvar"
}
ensure_topic_acl(){
  local sa="$1" ops="$2" topic="$3" acls op
  acls="$(confluent kafka acl list -o json)"
  for op in ${ops//,/ }; do
    if jq -e --arg p "User:$sa" --arg r "$topic" --arg op "$op" '
      any(.[];
        (.principal==$p) and ((.resource_name // .resourceName // "")==$r) and
        ((.operation // "" | ascii_upcase)==($op|ascii_upcase)) and
        ((.permission // "" | ascii_upcase)=="ALLOW") and
        ((.pattern_type // .patternType // "" | ascii_upcase)=="PREFIXED")
      )' <<<"$acls" >/dev/null; then continue; fi
    confluent kafka acl create --allow --service-account "$sa" --operations "$op" --topic "$topic" --prefix >/dev/null
    acls="$(confluent kafka acl list -o json)"
  done
}
ensure_literal_topic_acl(){
  local sa="$1" ops="$2" topic="$3" acls op
  acls="$(confluent kafka acl list -o json)"
  for op in ${ops//,/ }; do
    if jq -e --arg p "User:$sa" --arg r "$topic" --arg op "$op" '
      any(.[];
        (.principal==$p) and ((.resource_name // .resourceName // "")==$r) and
        ((.operation // "" | ascii_upcase)==($op|ascii_upcase)) and
        ((.permission // "" | ascii_upcase)=="ALLOW") and
        ((.pattern_type // .patternType // "" | ascii_upcase)=="LITERAL")
      )' <<<"$acls" >/dev/null; then continue; fi
    confluent kafka acl create --allow --service-account "$sa" --operations "$op" --topic "$topic" >/dev/null
    acls="$(confluent kafka acl list -o json)"
  done
}
ensure_group_acl(){
  local sa="$1" group="$2" prefix="$3" acls pt
  pt="$([[ "$prefix" == yes ]] && echo PREFIXED || echo LITERAL)"
  acls="$(confluent kafka acl list -o json)"
  if jq -e --arg p "User:$sa" --arg r "$group" --arg pt "$pt" '
    any(.[];
      (.principal==$p) and ((.resource_name // .resourceName // "")==$r) and
      ((.resource_type // .resourceType // "" | ascii_upcase)=="GROUP") and
      ((.pattern_type // .patternType // "" | ascii_upcase)==$pt) and
      ((.operation // "" | ascii_upcase)=="READ") and ((.permission // "" | ascii_upcase)=="ALLOW")
    )' <<<"$acls" >/dev/null; then return; fi
  if [[ "$prefix" == yes ]]; then
    confluent kafka acl create --allow --service-account "$sa" --operations read --consumer-group "$group" --prefix >/dev/null
  else
    confluent kafka acl create --allow --service-account "$sa" --operations read --consumer-group "$group" >/dev/null
  fi
}

PREV_CLUSTER_ID="${CLUSTER_ID:-}"
PREV_SA_WRITER_ID="${SA_WRITER_ID:-}"
PREV_SA_READER_ID="${SA_READER_ID:-}"
PREV_SA_WRITER_CREATED="${SA_WRITER_CREATED:-no}"
PREV_SA_READER_CREATED="${SA_READER_CREATED:-no}"
confluent environment list >/dev/null 2>&1 || confluent login --save
ENVS="$(confluent environment list -o json)"
# Sempre redescobrir por nome; IDs locais podem apontar para recursos excluídos.
ENV_ID="$(find_id "$ENVS" "$CC_ENV_NAME")"
if [[ -z "$ENV_ID" ]]; then ENV_ID="$(confluent environment create "$CC_ENV_NAME" --governance-package essentials -o json | jq -r '.id')"; fi
ENV_ACTUAL="$(confluent environment describe "$ENV_ID" -o json | jq -r '.name // empty')"
[[ "$ENV_ACTUAL" == "$CC_ENV_NAME" ]] || fail "ENV_ID aponta para '$ENV_ACTUAL', esperado '$CC_ENV_NAME'. Revise .state.env."
save_state ENV_ID "$ENV_ID"
confluent environment use "$ENV_ID" >/dev/null
CLUSTERS="$(confluent kafka cluster list -o json)"
# A listagem ocorre no Environment atual; não reutilizar ID persistido.
CLUSTER_ID="$(find_id "$CLUSTERS" "$CC_CLUSTER_NAME")"
if [[ -z "$CLUSTER_ID" ]]; then
  CLUSTER_ID="$(confluent kafka cluster create "$CC_CLUSTER_NAME" --cloud "$CC_CLOUD" --region "$CC_REGION" --type basic -o json | jq -r '.id')"
fi
CLUSTER_ACTUAL="$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.name // empty')"
[[ "$CLUSTER_ACTUAL" == "$CC_CLUSTER_NAME" ]] || fail "CLUSTER_ID aponta para '$CLUSTER_ACTUAL', esperado '$CC_CLUSTER_NAME'. Revise .state.env."
save_state CLUSTER_ID "$CLUSTER_ID"
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
STATUS=""
for _ in $(seq 1 60); do
  STATUS="$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.status')"
  [[ "$STATUS" == UP ]] && break
  [[ "$STATUS" == FAILED ]] && fail "cluster Kafka em FAILED"
  sleep 10
done
[[ "$STATUS" == UP ]] || fail "cluster Kafka não ficou UP"
BOOTSTRAP="$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.endpoint')"
save_state BOOTSTRAP "$BOOTSTRAP"

ensure_sa cdc-writer 'Escrita dos eventos CDC'
SA_WRITER_ID="$SA_RESULT_ID"
if [[ "$PREV_SA_WRITER_ID" == "$SA_WRITER_ID" && "$PREV_SA_WRITER_CREATED" == yes ]]; then SA_CREATED=yes; fi
save_state SA_WRITER_ID "$SA_WRITER_ID"
save_state SA_WRITER_CREATED "$SA_CREATED"
ensure_sa app-reader 'Leitura dos tópicos do laboratório'
SA_READER_ID="$SA_RESULT_ID"
if [[ "$PREV_SA_READER_ID" == "$SA_READER_ID" && "$PREV_SA_READER_CREATED" == yes ]]; then SA_CREATED=yes; fi
save_state SA_READER_ID "$SA_READER_ID"
save_state SA_READER_CREATED "$SA_CREATED"
# Não confiar em API Keys antigas do arquivo de estado.
# Reutiliza segredos somente se cluster e service accounts continuam os mesmos.
if [[ "$PREV_CLUSTER_ID" != "$CLUSTER_ID" || "$PREV_SA_WRITER_ID" != "$SA_WRITER_ID" ]]; then
  unset WRITER_API_KEY WRITER_API_SECRET
fi
if [[ "$PREV_CLUSTER_ID" != "$CLUSTER_ID" || "$PREV_SA_READER_ID" != "$SA_READER_ID" ]]; then
  unset READER_API_KEY READER_API_SECRET
fi
ensure_key "$CLUSTER_ID" "$SA_WRITER_ID" WRITER_API_KEY WRITER_API_SECRET
ensure_key "$CLUSTER_ID" "$SA_READER_ID" READER_API_KEY READER_API_SECRET
ensure_topic_acl "$SA_WRITER_ID" create,write,describe "payments."
ensure_topic_acl "$SA_WRITER_ID" create,write,describe "__debezium-heartbeat"
ensure_topic_acl "$SA_READER_ID" read,describe "payments."
ensure_literal_topic_acl "$SA_READER_ID" read,describe "fraud_alerts"
ensure_group_acl "$SA_READER_ID" app-reader no
ensure_group_acl "$SA_READER_ID" snapshot-check- yes
mkdir -p "$ROOT_DIR/evidence/cloud"
{
  echo "Environment: $ENV_ID"
  echo "Kafka cluster: $CLUSTER_ID"
  echo "Bootstrap: $BOOTSTRAP"
  echo "Writer service account: $SA_WRITER_ID"
  echo "Reader service account: $SA_READER_ID"
  confluent kafka acl list
} | tee "$ROOT_DIR/evidence/cloud/01-foundation.txt"
