#!/usr/bin/env bash
# Verifica ACLs Kafka e o vínculo RBAC MetricsViewer; não altera recursos.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/_common.sh
source "$ROOT_DIR/scripts/_common.sh"

require confluent jq date

: "${CLUSTER_ID:?CLUSTER_ID ausente no .state.env}"
: "${ENV_ID:?ENV_ID ausente no .state.env}"
: "${SA_WRITER_ID:?SA_WRITER_ID ausente no .state.env}"
: "${SA_READER_ID:?SA_READER_ID ausente no .state.env}"
: "${SA_METRICS_ID:?SA_METRICS_ID ausente no .state.env}"

WRITER="User:$SA_WRITER_ID"
READER="User:$SA_READER_ID"
METRICS="User:$SA_METRICS_ID"

normalize_list() {
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

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$ROOT_DIR/evidence/observability"
mkdir -p "$EVIDENCE_DIR"
SUMMARY="$EVIDENCE_DIR/${STAMP}-acls.md"
TMP_ACLS="$(mktemp)"
TMP_RBAC="$(mktemp)"
trap 'rm -f "$TMP_ACLS" "$TMP_RBAC"' EXIT

echo "Consultando ACLs Kafka (somente leitura)..."
confluent kafka acl list -o json >"$TMP_ACLS"
ACLS="$(normalize_list "$(cat "$TMP_ACLS")")"

echo "Consultando role binding MetricsViewer (somente leitura)..."
confluent iam rbac role-binding list \
  --principal "$METRICS" --role MetricsViewer -o json >"$TMP_RBAC"
RBAC="$(normalize_list "$(cat "$TMP_RBAC")")"

# Esperado conforme ensure_*_acl() em infrastructure/foundation.sh.
EXPECTED='[
  {"principal":"WRITER","permission":"ALLOW","operation":"CREATE","resource_type":"TOPIC","resource_name":"payments.","pattern_type":"PREFIXED","host":"*"},
  {"principal":"WRITER","permission":"ALLOW","operation":"WRITE","resource_type":"TOPIC","resource_name":"payments.","pattern_type":"PREFIXED","host":"*"},
  {"principal":"WRITER","permission":"ALLOW","operation":"DESCRIBE","resource_type":"TOPIC","resource_name":"payments.","pattern_type":"PREFIXED","host":"*"},
  {"principal":"WRITER","permission":"ALLOW","operation":"CREATE","resource_type":"TOPIC","resource_name":"__debezium-heartbeat","pattern_type":"PREFIXED","host":"*"},
  {"principal":"WRITER","permission":"ALLOW","operation":"WRITE","resource_type":"TOPIC","resource_name":"__debezium-heartbeat","pattern_type":"PREFIXED","host":"*"},
  {"principal":"WRITER","permission":"ALLOW","operation":"DESCRIBE","resource_type":"TOPIC","resource_name":"__debezium-heartbeat","pattern_type":"PREFIXED","host":"*"},
  {"principal":"READER","permission":"ALLOW","operation":"READ","resource_type":"TOPIC","resource_name":"payments.","pattern_type":"PREFIXED","host":"*"},
  {"principal":"READER","permission":"ALLOW","operation":"DESCRIBE","resource_type":"TOPIC","resource_name":"payments.","pattern_type":"PREFIXED","host":"*"},
  {"principal":"READER","permission":"ALLOW","operation":"READ","resource_type":"TOPIC","resource_name":"fraud_alerts","pattern_type":"LITERAL","host":"*"},
  {"principal":"READER","permission":"ALLOW","operation":"DESCRIBE","resource_type":"TOPIC","resource_name":"fraud_alerts","pattern_type":"LITERAL","host":"*"},
  {"principal":"READER","permission":"ALLOW","operation":"READ","resource_type":"GROUP","resource_name":"app-reader","pattern_type":"LITERAL","host":"*"},
  {"principal":"READER","permission":"ALLOW","operation":"READ","resource_type":"GROUP","resource_name":"snapshot-check-","pattern_type":"PREFIXED","host":"*"}
]'

EXPECTED="$(jq -c --arg w "$WRITER" --arg r "$READER" '
  map(.principal |= if . == "WRITER" then $w else $r end)
' <<<"$EXPECTED")"

# Canonicalize only fields relevant to Kafka ACL identity. The CLI omits host
# when it is the wildcard; this is normalized to "*", matching Foundation.
ACTUAL="$(jq -c --arg w "$WRITER" --arg r "$READER" '
  map(select(.principal == $w or .principal == $r)
    | {
        principal,
        permission: ((.permission // "") | ascii_upcase),
        operation: ((.operation // "") | ascii_upcase),
        resource_type: ((.resource_type // .resourceType // "") | ascii_upcase),
        resource_name: (.resource_name // .resourceName // ""),
        pattern_type: ((.pattern_type // .patternType // "") | ascii_upcase),
        host: (.host // "*")
      })
' <<<"$ACLS")"

ACL_RESULT="$(jq -n --argjson expected "$EXPECTED" --argjson actual "$ACTUAL" '
  def key: [.principal,.permission,.operation,.resource_type,.resource_name,.pattern_type,.host] | @json;
  ($expected | map(key)) as $ek
  | ($actual | map(key)) as $ak
  | {
      missing: [$expected[] as $e | select(($ak | index($e|key)) == null) | $e],
      unexpected: [$actual[] as $a | select(($ek | index($a|key)) == null) | $a],
      expected_count: ($expected|length),
      actual_count: ($actual|length)
    }
')"

MISSING="$(jq '.missing | length' <<<"$ACL_RESULT")"
UNEXPECTED="$(jq '.unexpected | length' <<<"$ACL_RESULT")"
ACL_OK=no
[[ "$MISSING" -eq 0 && "$UNEXPECTED" -eq 0 ]] && ACL_OK=yes

RBAC_RESULT="$(jq -c --arg principal "$METRICS" '
  [.[] | select(.principal == $principal and .role == "MetricsViewer")]
' <<<"$RBAC")"
RBAC_COUNT="$(jq 'length' <<<"$RBAC_RESULT")"
RBAC_OK=no
[[ "$RBAC_COUNT" -eq 1 ]] && RBAC_OK=yes

{
  echo "# Verificação de ACLs e RBAC"
  echo
  echo "- Coletado em (UTC): $STAMP"
  echo "- Environment ID: $ENV_ID"
  echo "- Kafka cluster ID: $CLUSTER_ID"
  echo "- Writer principal: $WRITER"
  echo "- Reader principal: $READER"
  echo "- Metrics principal: $METRICS"
  echo
  echo "## ACLs Kafka"
  echo
  echo "- Esperadas: $(jq -r '.expected_count' <<<"$ACL_RESULT")"
  echo "- Encontradas para writer/reader: $(jq -r '.actual_count' <<<"$ACL_RESULT")"
  echo "- Ausentes/divergentes: $MISSING"
  echo "- Extras para writer/reader: $UNEXPECTED"
  echo "- Resultado: $([[ "$ACL_OK" == yes ]] && echo APROVADO || echo REPROVADO)"
  echo
  if [[ "$MISSING" -gt 0 ]]; then
    echo "### ACLs ausentes"
    jq -r '.missing[] | "- " + ([.principal,.permission,.operation,.resource_type,.resource_name,.pattern_type,.host] | join(" | "))' <<<"$ACL_RESULT"
    echo
  fi
  if [[ "$UNEXPECTED" -gt 0 ]]; then
    echo "### ACLs não previstas"
    jq -r '.unexpected[] | "- " + ([.principal,.permission,.operation,.resource_type,.resource_name,.pattern_type,.host] | join(" | "))' <<<"$ACL_RESULT"
    echo
  fi
  echo "## RBAC MetricsViewer"
  echo
  echo "- Vínculos correspondentes encontrados: $RBAC_COUNT"
  echo "- Resultado: $([[ "$RBAC_OK" == yes ]] && echo APROVADO || echo REPROVADO)"
  if [[ "$RBAC_COUNT" -gt 0 ]]; then
    jq -r '.[] | "- ID: " + (.id // "não informado") + " — principal: " + .principal + " — role: " + .role' <<<"$RBAC_RESULT"
  fi
  echo
  echo "## Resultado consolidado"
  echo
  if [[ "$ACL_OK" == yes && "$RBAC_OK" == yes ]]; then
    echo "**APROVADO:** ACLs e vínculo RBAC correspondem ao estado esperado."
  else
    echo "**REPROVADO:** existem ausências, divergências ou vínculo RBAC não confirmado. Nenhuma permissão foi alterada."
  fi
} >"$SUMMARY"
chmod 600 "$SUMMARY"

cat "$SUMMARY"
echo
echo "Evidência: ${SUMMARY#"$ROOT_DIR"/}"

[[ "$ACL_OK" == yes && "$RBAC_OK" == yes ]]
