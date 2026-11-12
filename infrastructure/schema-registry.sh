#!/usr/bin/env bash
# Prepara Schema Registry e valida evolução Avro compatível e incompatível.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq curl
: "${ENV_ID:?Execute foundation.sh primeiro}"
confluent environment use "$ENV_ID" >/dev/null
SR_JSON="$(confluent schema-registry cluster describe -o json)"
SR_ID="$(jq -r '.cluster_id // .id // .cluster // empty' <<<"$SR_JSON")"
SR_URL="$(jq -r '.endpoint_url // .endpoint // empty' <<<"$SR_JSON")"
[[ -n "$SR_ID" && -n "$SR_URL" ]] || { echo "Não foi possível identificar Schema Registry." >&2; exit 1; }
save_state SR_ID "$SR_ID"
save_state SR_URL "$SR_URL"
if [[ -z "${SR_API_KEY:-}" || -z "${SR_API_SECRET:-}" ]]; then
  EXISTING="$(confluent api-key list --resource "$SR_ID" -o json | jq 'length')"
  [[ "$EXISTING" -eq 0 ]] || {
    echo "Já existem $EXISTING API key(s) para o Schema Registry, mas o segredo não está salvo. Revise as chaves antes de criar outra." >&2
    exit 1
  }
  KEY="$(confluent api-key create --resource "$SR_ID" --description payments-schema-registry -o json)"
  save_state SR_API_KEY "$(jq -r '.api_key' <<<"$KEY")"
  save_state SR_API_SECRET "$(jq -r '.api_secret' <<<"$KEY")"
fi
AUTH="$SR_API_KEY:$SR_API_SECRET"
SUBJECT="contract.transactions-value"
call(){ curl -fsS -u "$AUTH" -H 'Content-Type: application/vnd.schemaregistry.v1+json' "$@"; }
payload(){ jq -Rs '{schemaType:"AVRO",schema:.}'; }

call -X PUT "$SR_URL/config/$SUBJECT" -d '{"compatibility":"BACKWARD"}' >/dev/null
V1="$(payload < "$ROOT_DIR/schemas/transaction-v1.avsc")"
V2="$(payload < "$ROOT_DIR/schemas/transaction-v2.avsc")"
call -X POST "$SR_URL/subjects/$SUBJECT/versions" -d "$V1" >/dev/null

# Evolução compatível: campo opcional com default null.
COMPAT_OK="$(call -X POST "$SR_URL/compatibility/subjects/$SUBJECT/versions/latest" -d "$V2")"
[[ "$(jq -r '.is_compatible' <<<"$COMPAT_OK")" == true ]] || {
  echo "Schema v2 deveria ser compatível com v1." >&2; exit 1;
}

# Evolução incompatível: campo obrigatório sem default.
V1_BAD="$(jq -c '.fields += [{"name":"mandatory_field","type":"string"}]' "$ROOT_DIR/schemas/transaction-v1.avsc" | jq -Rs '{schemaType:"AVRO",schema:.}')"
COMPAT_BAD="$(call -X POST "$SR_URL/compatibility/subjects/$SUBJECT/versions/latest" -d "$V1_BAD")"
[[ "$(jq -r '.is_compatible' <<<"$COMPAT_BAD")" == false ]] || {
  echo "Schema com campo obrigatório sem default deveria ser incompatível." >&2; exit 1;
}

call -X POST "$SR_URL/subjects/$SUBJECT/versions" -d "$V2" >/dev/null
mkdir -p "$ROOT_DIR/evidence/cloud"
{
  echo "Subject: $SUBJECT"
  echo "Compatibilidade: BACKWARD"
  echo "Positivo (campo opcional):"
  jq . <<<"$COMPAT_OK"
  echo "Negativo (campo obrigatório sem default):"
  jq . <<<"$COMPAT_BAD"
  echo "Versões registradas:"
  call "$SR_URL/subjects/$SUBJECT/versions" | jq .
} | tee "$ROOT_DIR/evidence/cloud/02-schema-registry.txt"
