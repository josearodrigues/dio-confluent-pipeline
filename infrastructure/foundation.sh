#!/usr/bin/env bash
# Provisiona Environment, Kafka Basic, identidades e ACLs do laboratório.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq
fail(){ echo "ERRO: $*" >&2; exit 1; }
find_id(){
  jq -r --arg n "$2" '
    if type == "array" then .
    elif type == "object" and (.data | type) == "array" then .data
    else error("formato JSON de listagem não reconhecido")
    end
    | if all(.[]; type == "object") then .
      else error("listagem contém item que não é objeto")
      end
    | [ .[] | select(.name == $n) ] as $matches
    | if ($matches | length) == 0 then ""
      elif ($matches | length) > 1 then error("mais de um recurso com o mesmo nome")
      elif (($matches[0].id | type) == "string" and
            ($matches[0].id | length) > 0) then $matches[0].id
      else error("recurso encontrado sem ID válido")
      end
  ' <<<"$1"
}
extract_id(){
  jq -er '
    if type == "object" and (.id | type) == "string" and
       (.id | length) > 0 then .id
    else error("resposta de criação sem ID válido")
    end
  ' <<<"$1"
}
normalize_json_list(){
  jq -ce '
    if type == "array" then .
    elif type == "object" and (.data | type) == "array" then .data
    else error("formato JSON de listagem não reconhecido")
    end
    | if all(.[]; type == "object") then .
      else error("listagem contém item que não é objeto")
      end
  ' <<<"$1"
}
SA_CREATED=no
ensure_sa(){
  local name="$1" desc="$2" list id
  list="$(confluent iam service-account list -o json)"
  id="$(find_id "$list" "$name")"
  if [[ -z "$id" ]]; then
    local created
    created="$(confluent iam service-account create "$name" --description "$desc" -o json)"
    id="$(extract_id "$created")" || fail "não foi possível obter ID válido da Service Account $name."
    SA_CREATED=yes
  else
    SA_CREATED=no
  fi
  SA_RESULT_ID="$id"
}
ensure_key(){
  local resource="$1" sa="$2" keyvar="$3" secretvar="$4"
  local json normalized existing

  json="$(confluent api-key list \
    --resource "$resource" \
    --service-account "$sa" -o json)"

  normalized="$(jq -ce '
    if type == "array" then .
    elif type == "object" and (.data | type) == "array" then .data
    else error("formato JSON de API key list não reconhecido")
    end
    | if all(.[]; type == "object") then .
      else error("lista de API keys contém item que não é objeto")
      end
  ' <<<"$json")" || fail "não foi possível interpretar a lista de API Keys."

  if [[ -n "${!keyvar:-}" && -n "${!secretvar:-}" &&
        "${!keyvar}" != null && "${!secretvar}" != null ]]; then
    if jq -e --arg k "${!keyvar}" '
      any(.[];
        ((.key // .api_key // .id) | type == "string") and
        ((.key // .api_key // .id) == $k)
      )
    ' <<<"$normalized" >/dev/null; then
      return
    fi
  fi

  # Estado ausente/inválido: não reutilizar segredo antigo.
  unset "$keyvar" "$secretvar"

  existing="$(jq -er 'length' <<<"$normalized")" ||
    fail "não foi possível contar as API Keys existentes."

  if [[ "$existing" -gt 0 ]]; then
    fail "já existem $existing API key(s) para $keyvar, mas o segredo não está salvo. Recupere o estado ou revise as chaves antes de criar outra."
  fi

  json="$(confluent api-key create \
    --resource "$resource" \
    --service-account "$sa" \
    --description "payments-lab-$keyvar" -o json)"

  save_api_key_credentials "$json" "$keyvar" "$secretvar"
}
ensure_metrics_role(){
  local principal="$1" bindings normalized count
  bindings="$(confluent iam rbac role-binding list \
    --principal "$principal" --role MetricsViewer -o json)"
  normalized="$(jq -ce '
    if type == "array" then .
    elif type == "object" and (.data | type) == "array" then .data
    else error("formato JSON de role-binding não reconhecido")
    end
    | if all(.[]; type == "object") then .
      else error("lista de role-bindings contém item que não é objeto")
      end
  ' <<<"$bindings")" || fail "não foi possível interpretar a lista de role-bindings MetricsViewer."

  count="$(jq -er 'length' <<<"$normalized")" ||
    fail "não foi possível contar os role-bindings MetricsViewer."
  if [[ "$count" -gt 0 ]]; then
    echo "MetricsViewer já atribuído a $principal."
    return
  fi

  confluent iam rbac role-binding create \
    --principal "$principal" --role MetricsViewer >/dev/null

  # Confirmar que o vínculo criado aparece na listagem; não confiar apenas
  # no código de saída do comando de criação.
  bindings="$(confluent iam rbac role-binding list \
    --principal "$principal" --role MetricsViewer -o json)"
  normalized="$(jq -ce '
    if type == "array" then .
    elif type == "object" and (.data | type) == "array" then .data
    else error("formato JSON de role-binding não reconhecido")
    end
    | if all(.[]; type == "object") then .
      else error("lista de role-bindings contém item que não é objeto")
      end
  ' <<<"$bindings")" || fail "não foi possível validar o role-binding MetricsViewer criado."
  count="$(jq -er 'length' <<<"$normalized")" ||
    fail "não foi possível contar os role-bindings MetricsViewer após a criação."
  [[ "$count" -gt 0 ]] ||
    fail "o role-binding MetricsViewer não apareceu na listagem após a criação."

  echo "MetricsViewer atribuído e confirmado para $principal."
}
ensure_topic_acl(){
  local sa="$1" ops="$2" topic="$3" acls op
  acls="$(confluent kafka acl list -o json)"
  acls="$(normalize_json_list "$acls")" ||
    fail "não foi possível interpretar a listagem de ACLs."
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
    acls="$(normalize_json_list "$acls")" ||
      fail "não foi possível interpretar a listagem de ACLs após a criação."
  done
}
ensure_literal_topic_acl(){
  local sa="$1" ops="$2" topic="$3" acls op
  acls="$(confluent kafka acl list -o json)"
  acls="$(normalize_json_list "$acls")" ||
    fail "não foi possível interpretar a listagem de ACLs."
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
    acls="$(normalize_json_list "$acls")" ||
      fail "não foi possível interpretar a listagem de ACLs após a criação."
  done
}
ensure_group_acl(){
  local sa="$1" group="$2" prefix="$3" acls pt
  pt="$([[ "$prefix" == yes ]] && echo PREFIXED || echo LITERAL)"
  acls="$(confluent kafka acl list -o json)"
  acls="$(normalize_json_list "$acls")" ||
    fail "não foi possível interpretar a listagem de ACLs."
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
PREV_SA_METRICS_ID="${SA_METRICS_ID:-}"
PREV_SA_METRICS_CREATED="${SA_METRICS_CREATED:-no}"
confluent environment list >/dev/null 2>&1 || confluent login --save
ENVS="$(confluent environment list -o json)"
# Sempre redescobrir por nome; IDs locais podem apontar para recursos excluídos.
ENV_ID="$(find_id "$ENVS" "$CC_ENV_NAME")"
if [[ -z "$ENV_ID" ]]; then
  CREATED_ENV="$(confluent environment create "$CC_ENV_NAME" --governance-package essentials -o json)"
  ENV_ID="$(extract_id "$CREATED_ENV")" || fail "não foi possível obter ID válido do Environment criado."
fi
ENV_ACTUAL="$(confluent environment describe "$ENV_ID" -o json | jq -r '.name // empty')"
[[ "$ENV_ACTUAL" == "$CC_ENV_NAME" ]] || fail "ENV_ID aponta para '$ENV_ACTUAL', esperado '$CC_ENV_NAME'. Revise .state.env."
save_state ENV_ID "$ENV_ID"
confluent environment use "$ENV_ID" >/dev/null
CLUSTERS="$(confluent kafka cluster list -o json)"
# A listagem ocorre no Environment atual; não reutilizar ID persistido.
CLUSTER_ID="$(find_id "$CLUSTERS" "$CC_CLUSTER_NAME")"
if [[ -z "$CLUSTER_ID" ]]; then
  CREATED_CLUSTER="$(confluent kafka cluster create "$CC_CLUSTER_NAME" --cloud "$CC_CLOUD" --region "$CC_REGION" --type basic -o json)"
  CLUSTER_ID="$(extract_id "$CREATED_CLUSTER")" || fail "não foi possível obter ID válido do cluster Kafka criado."
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
ensure_sa metrics-reader 'Leitura da Confluent Metrics API'
SA_METRICS_ID="$SA_RESULT_ID"
if [[ "$PREV_SA_METRICS_ID" == "$SA_METRICS_ID" && "$PREV_SA_METRICS_CREATED" == yes ]]; then SA_CREATED=yes; fi
save_state SA_METRICS_ID "$SA_METRICS_ID"
save_state SA_METRICS_CREATED "$SA_CREATED"
ensure_metrics_role "User:$SA_METRICS_ID"
# Não confiar em API Keys antigas do arquivo de estado.
# Reutiliza segredos somente se cluster e service accounts continuam os mesmos.
if [[ "$PREV_CLUSTER_ID" != "$CLUSTER_ID" || "$PREV_SA_WRITER_ID" != "$SA_WRITER_ID" ]]; then
  unset WRITER_API_KEY WRITER_API_SECRET
fi
if [[ "$PREV_CLUSTER_ID" != "$CLUSTER_ID" || "$PREV_SA_READER_ID" != "$SA_READER_ID" ]]; then
  unset READER_API_KEY READER_API_SECRET
fi
if [[ "$PREV_SA_METRICS_ID" != "$SA_METRICS_ID" ]]; then
  unset METRICS_API_KEY METRICS_API_SECRET
fi
ensure_key "$CLUSTER_ID" "$SA_WRITER_ID" WRITER_API_KEY WRITER_API_SECRET
ensure_key "$CLUSTER_ID" "$SA_READER_ID" READER_API_KEY READER_API_SECRET
ensure_key cloud "$SA_METRICS_ID" METRICS_API_KEY METRICS_API_SECRET
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
  echo "Metrics service account: $SA_METRICS_ID"
  confluent kafka acl list
} | tee "$ROOT_DIR/evidence/cloud/01-foundation.txt"
