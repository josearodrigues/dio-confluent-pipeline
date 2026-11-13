#!/usr/bin/env bash
# Prepara Schema Registry e valida evolução Avro compatível e incompatível.
# Ordem: registra v1, configura BACKWARD, valida v2/v3, registra v2 e
# comprova que o Registry rejeita o registro incompatível (HTTP 40901).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq curl
: "${ENV_ID:?Execute foundation.sh primeiro}"

EVIDENCE="$ROOT_DIR/evidence/cloud/02-schema-registry.txt"
mkdir -p "$(dirname "$EVIDENCE")"

PREV_SR_ID="${SR_ID:-}"
confluent environment use "$ENV_ID" >/dev/null
SR_JSON="$(confluent schema-registry cluster describe -o json)"
SR_ID="$(jq -r '.cluster_id // .id // .cluster // empty' <<<"$SR_JSON")"
SR_URL="$(jq -r '.endpoint_url // .endpoint // empty' <<<"$SR_JSON")"
[[ -n "$SR_ID" && "$SR_ID" != null && -n "$SR_URL" && "$SR_URL" != null ]] || {
  echo "Não foi possível identificar ID e endpoint do Schema Registry." >&2
  exit 1
}
save_state SR_ID "$SR_ID"
save_state SR_URL "$SR_URL"

# Credenciais persistidas só são reutilizadas se a API key pertencer ao Registry atual.
if [[ "$PREV_SR_ID" != "$SR_ID" ]]; then
  unset SR_API_KEY SR_API_SECRET
fi
if [[ -n "${SR_API_KEY:-}" && -n "${SR_API_SECRET:-}" &&
      "$SR_API_KEY" != null && "$SR_API_SECRET" != null ]]; then
  if ! confluent api-key list --resource "$SR_ID" -o json |
    jq -e --arg k "$SR_API_KEY" 'any(.[]; (.key // .api_key // .id)==$k)' >/dev/null; then
    unset SR_API_KEY SR_API_SECRET
  fi
else
  unset SR_API_KEY SR_API_SECRET
fi

if [[ -z "${SR_API_KEY:-}" || -z "${SR_API_SECRET:-}" ]]; then
  EXISTING="$(confluent api-key list --resource "$SR_ID" -o json | jq 'length')"
  [[ "$EXISTING" -eq 0 ]] || {
    echo "Já existem $EXISTING API key(s) para o Schema Registry, mas o segredo não está salvo. Revise as chaves antes de criar outra." >&2
    exit 1
  }
  KEY="$(confluent api-key create --resource "$SR_ID" --description payments-schema-registry -o json)"
  save_api_key_credentials "$KEY" SR_API_KEY SR_API_SECRET
  sleep 10
fi

AUTH="$SR_API_KEY:$SR_API_SECRET"
SUBJECT="contract.transactions-value"
V1_FILE="$ROOT_DIR/schemas/transaction-v1.avsc"
V2_FILE="$ROOT_DIR/schemas/transaction-v2.avsc"

# Preserva o corpo e o status HTTP para que falhas sejam diagnosticáveis.
sr_call() {
  local method="$1" url="$2" data="${3:-}" out code
  out="$(mktemp)"
  if [[ -n "$data" ]]; then
    code="$(curl -sS -o "$out" -w '%{http_code}' -u "$AUTH" \
      -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      -X "$method" -d "$data" "$url")" || {
        rm -f "$out"
        echo "Falha de transporte em $method $url" >&2
        return 1
      }
  else
    code="$(curl -sS -o "$out" -w '%{http_code}' -u "$AUTH" \
      -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      -X "$method" "$url")" || {
        rm -f "$out"
        echo "Falha de transporte em $method $url" >&2
        return 1
      }
  fi
  if (( code < 200 || code >= 300 )); then
    echo "ERRO HTTP $code em $method $url" >&2
    cat "$out" >&2
    echo >&2
    rm -f "$out"
    return 1
  fi
  cat "$out"
  rm -f "$out"
}

payload() { jq -Rs '{schemaType:"AVRO",schema:.}'; }

# A chamada de registro incompatível deve falhar com HTTP 409 / código 40901.
assert_registration_rejected() {
  local data="$1" out code error_code
  out="$(mktemp)"
  code="$(curl -sS -o "$out" -w '%{http_code}' -u "$AUTH" \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -X POST -d "$data" "$SR_URL/subjects/$SUBJECT/versions")" || {
      rm -f "$out"
      echo "Falha de transporte ao tentar registrar schema incompatível." >&2
      return 1
    }
  error_code="$(jq -r '.error_code // empty' "$out")"
  if [[ "$code" != 409 || "$error_code" != 40901 ]]; then
    echo "Esperada rejeição HTTP 409 / código 40901; recebido HTTP $code / código ${error_code:-ausente}." >&2
    cat "$out" >&2
    rm -f "$out"
    return 1
  fi
  echo "Rejeição esperada confirmada: HTTP $code / código $error_code."
  jq . "$out"
  rm -f "$out"
}

main() {
  echo "Schema Registry: $SR_ID"
  echo "Endpoint: $SR_URL"
  echo "Subject: $SUBJECT"

  SUBJECTS="$(sr_call GET "$SR_URL/subjects")"
  if jq -e --arg s "$SUBJECT" 'index($s) != null' <<<"$SUBJECTS" >/dev/null; then
    echo "Subject $SUBJECT já existe. Inspecione as versões; não reexecute este provisionamento." >&2
    return 1
  fi

  V1="$(payload < "$V1_FILE")"
  V2="$(payload < "$V2_FILE")"
  V3_BAD="$(jq -c '.fields += [{"name":"mandatory_field","type":"string"}]' "$V1_FILE" | jq -Rs '{schemaType:"AVRO",schema:.}')"

  echo "==> [1/7] Registrando schema v1"
  sr_call POST "$SR_URL/subjects/$SUBJECT/versions" "$V1" | jq .

  echo "==> [2/7] Configurando compatibilidade BACKWARD"
  sr_call PUT "$SR_URL/config/$SUBJECT" '{"compatibility":"BACKWARD"}' | jq .

  echo "==> [3/7] Verificando evolução compatível (campo opcional com default null)"
  COMPAT_OK="$(sr_call POST "$SR_URL/compatibility/subjects/$SUBJECT/versions/latest" "$V2")"
  jq . <<<"$COMPAT_OK"
  [[ "$(jq -r '.is_compatible' <<<"$COMPAT_OK")" == true ]] || {
    echo "Schema v2 deveria ser compatível com v1." >&2
    return 1
  }

  echo "==> [4/7] Verificando evolução incompatível (campo obrigatório sem default)"
  COMPAT_BAD="$(sr_call POST "$SR_URL/compatibility/subjects/$SUBJECT/versions/latest" "$V3_BAD")"
  jq . <<<"$COMPAT_BAD"
  [[ "$(jq -r '.is_compatible' <<<"$COMPAT_BAD")" == false ]] || {
    echo "Schema com campo obrigatório sem default deveria ser incompatível." >&2
    return 1
  }

  echo "==> [5/7] Registrando v2 compatível"
  sr_call POST "$SR_URL/subjects/$SUBJECT/versions" "$V2" | jq .

  echo "==> [6/7] Tentando registrar v3 incompatível; rejeição HTTP 40901 é esperada"
  assert_registration_rejected "$V3_BAD"

  echo "==> [7/7] Conferindo versões e schemas registrados"
  sr_call GET "$SR_URL/subjects/$SUBJECT/versions" | jq .
  sr_call GET "$SR_URL/subjects/$SUBJECT/versions/1" | jq '{id,subject,version,schemaType,schema}'
  sr_call GET "$SR_URL/subjects/$SUBJECT/versions/2" | jq '{id,subject,version,schemaType,schema}'

  echo
  echo "RESULTADO: v1 e v2 registrados; BACKWARD aceitou a evolução compatível e rejeitou a incompatível."
}

main 2>&1 | tee "$EVIDENCE"
exit "${PIPESTATUS[0]}"
