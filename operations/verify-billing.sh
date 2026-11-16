#!/usr/bin/env bash
# Consulta custos agregados da Confluent Cloud; somente leitura.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/_common.sh
source "$ROOT_DIR/scripts/_common.sh"

require confluent jq date

: "${ENV_ID:?ENV_ID ausente no .state.env}"

usage() {
  cat <<'USAGE'
Uso:
  ./operations/verify-billing.sh
  ./operations/verify-billing.sh <start-date> <end-date>

Datas no formato UTC YYYY-MM-DD. start-date é inclusiva e end-date é exclusiva.
Sem argumentos, consulta do primeiro dia do mês UTC corrente até amanhã (fim exclusivo).
USAGE
}

if [[ "$#" -eq 0 ]]; then
  START_DATE="$(date -u +%Y-%m-01)"
  END_DATE="$(date -u -d "tomorrow" +%F 2>/dev/null || date -u +%F)"
elif [[ "$#" -eq 2 ]]; then
  START_DATE="$1"
  END_DATE="$2"
else
  usage >&2
  exit 2
fi

validate_date() {
  local value="$1" parsed
  [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  parsed="$(date -u -d "$value" +%F 2>/dev/null)" || return 1
  [[ "$parsed" == "$value" ]]
}

validate_date "$START_DATE" || {
  echo "start-date inválida: use uma data real no formato YYYY-MM-DD." >&2
  exit 2
}
validate_date "$END_DATE" || {
  echo "end-date inválida: use uma data real no formato YYYY-MM-DD." >&2
  exit 2
}
[[ "$END_DATE" > "$START_DATE" ]] || {
  echo "end-date deve ser posterior a start-date (limite exclusivo)." >&2
  exit 2
}

MAX_END_DATE="$(date -u -d "$START_DATE +1 month" +%F)"
[[ "$END_DATE" < "$MAX_END_DATE" || "$END_DATE" == "$MAX_END_DATE" ]] || {
  echo "A Costs API aceita no máximo um mês por consulta." >&2
  exit 2
}

EVIDENCE_DIR="$ROOT_DIR/evidence/observability"
mkdir -p "$EVIDENCE_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
JSON_FILE="$EVIDENCE_DIR/${STAMP}-billing.json"
SUMMARY_FILE="$EVIDENCE_DIR/${STAMP}-billing.md"
TMP_JSON="$(mktemp)"
TMP_ROWS="$(mktemp)"
FILTERED_ROWS="$(mktemp)"
trap 'rm -f "$TMP_JSON" "$TMP_ROWS" "$FILTERED_ROWS"' EXIT

echo "Consultando custos Confluent Cloud via CLI (somente leitura)..."
if ! confluent billing cost list \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  -o json >"$TMP_JSON"; then
  echo "Falha na consulta de Billing. Confira autenticação CLI e acesso OrganizationAdmin/BillingAdmin; nenhuma configuração foi alterada." >&2
  exit 1
fi

# A CLI normalmente retorna a coleção JSON; também aceitamos o envelope data
# documentado pela Costs API, incluindo organizações no formato legado.
jq -e '
  def money:
    if type == "number" then .
    elif type == "string" then
      if test("^\\$-?[0-9]+([.][0-9]+)?$") then ltrimstr("$") | tonumber
      elif test("^-?[0-9]+([.][0-9]+)?$") then tonumber
      else error("valor monetário em formato não reconhecido") end
    else error("valor monetário ausente ou com tipo inválido") end;
  (if type == "array" then .
   elif type == "object" and (.data | type) == "array" then .data
   else error("formato JSON não reconhecido: esperada lista ou objeto com data[]") end)
  | if all(.[]; type == "object") then .
    else error("a coleção contém item que não é objeto") end
  | map(if has("amount") then .amount = (.amount | money)
        else error("item sem amount") end)
' "$TMP_JSON" >"$TMP_ROWS" || {
  echo "Resposta de Billing não passou na validação; JSON bruto preservado apenas temporariamente." >&2
  exit 1
}

# Filtra estritamente pelo Environment do projeto antes de persistir a evidência.
# A resposta organizacional integral permanece apenas em arquivo temporário.
jq --arg env "$ENV_ID" '
  def get_env:
    if (.environment | type) == "object" then .environment.id
    elif (.environment | type) == "string" then .environment
    elif (.resource | type) == "object" and (.resource.environment | type) == "object" then .resource.environment.id
    elif (.resource | type) == "object" and (.resource.environment | type) == "string" then .resource.environment
    else null end;
  map(select(get_env == $env))
' "$TMP_ROWS" >"$FILTERED_ROWS"
mv "$FILTERED_ROWS" "$TMP_ROWS"
cp "$TMP_ROWS" "$JSON_FILE"
chmod 600 "$JSON_FILE"

ROW_COUNT="$(jq 'length' "$TMP_ROWS")"
TOTAL_RAW="$(jq '[.[] | .amount] | add // 0' "$TMP_ROWS")"
# Exibe valores monetários com exatamente duas casas decimais.
TOTAL="$(LC_NUMERIC=C printf '%.2f' "$TOTAL_RAW")"

{
  echo "# Verificação automatizada de Billing"
  echo
  echo "- Coletado em (UTC): $STAMP"
  echo "- Environment: $ENV_ID"
  echo "- Período consultado: $START_DATE (inclusivo) até $END_DATE (exclusivo)"
  echo "- Itens retornados: $ROW_COUNT"
  echo "- Origem: \`confluent billing cost list -o json\`"
  echo
  echo "## Custo do Environment do projeto"
  echo
  echo "| Campo | USD |"
  echo "|---|---:|"
  echo "| Custo total (amount) | $TOTAL |"
  echo
  echo "## Itens de custo"
  echo
  echo "| Data inicial | Produto | Tipo | Ambiente | Recurso | Quantidade | Unidade | Custo (USD) |"
  echo "|---|---|---|---|---|---:|---|---:|"
  jq -r '
    def s: if . == null then "" else tostring | gsub("[|\\\\r\\\\n]"; " ") end;
    def get($value; $key):
      if ($value | type) == "object" then $value[$key] else null end;
    .[] |
    . as $item |
    ($item.resource as $resource |
     $item.environment as $environment |
     "| " + (($item.start_date // "") | s) +
     " | " + (($item.product // "") | s) +
     " | " + (($item.line_type // $item.lineType // "") | s) +
     " | " + ((get(get($resource; "environment"); "id")
                // get($resource; "environment")
                // get($environment; "id")
                // $environment // "") | s) +
     " | " + ((get($resource; "id")
                // $item.resource_id
                // $item.resource_name
                // (if ($resource | type) == "string" then $resource else "" end)) | s) +
     " | " + (($item.quantity // "") | s) +
     " | " + (($item.unit // "") | s) +
     " | " + (($item.amount // "") | s) + " |")
  ' "$TMP_ROWS"
  if [[ "$ROW_COUNT" -eq 0 ]]; then
    echo
    echo "Nenhum item foi retornado para o Environment e período informados. Isso não comprova custo zero fora dessa janela ou dos dados disponíveis."
  fi
  echo
  echo "## Limitações"
  echo
  echo "- A Costs API agrega os itens diariamente e pode levar até 72 horas para disponibilizar dados recentes."
  echo "- O resultado contém exclusivamente itens associados ao Environment configurado; não representa o total da organização."
  echo "- A consulta exige papel OrganizationAdmin ou BillingAdmin e reutiliza a sessão já autenticada na Confluent CLI."
  echo "- Nenhum recurso ou configuração Cloud foi alterado."
  echo
  echo "## Arquivos"
  echo
  echo "- JSON filtrado do Environment: \`evidence/observability/$(basename "$JSON_FILE")\`"
  echo "- Resumo: \`evidence/observability/$(basename "$SUMMARY_FILE")\`"
} >"$SUMMARY_FILE"
chmod 600 "$SUMMARY_FILE"


cat "$SUMMARY_FILE"
echo
echo "Evidência: ${SUMMARY_FILE#"$ROOT_DIR"/}"
