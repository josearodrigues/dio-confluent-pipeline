#!/usr/bin/env bash
# Consulta custos agregados da Confluent Cloud; somente leitura.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/_common.sh
source "$ROOT_DIR/scripts/_common.sh"

require confluent jq date

usage() {
  cat <<'USAGE'
Uso:
  ./operations/verify-billing.sh
  ./operations/verify-billing.sh <start-date> <end-date>

Datas no formato UTC YYYY-MM-DD. start-date é inclusiva e end-date é exclusiva.
Sem argumentos, consulta o mês atual até três dias atrás (janela conservadora
para o atraso de até 72 horas da Costs API).
USAGE
}

if [[ "$#" -eq 0 ]]; then
  TODAY_UTC="$(date -u +%F)"
  START_DATE="$(date -u -d "$TODAY_UTC" +%Y-%m-01)"
  END_DATE="$(date -u -d "$TODAY_UTC -3 days" +%F)"
  [[ "$END_DATE" > "$START_DATE" ]] || {
    echo "Ainda não há uma janela mensal com três dias completos de atraso. Informe explicitamente um intervalo de datas." >&2
    exit 2
  }
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
trap 'rm -f "$TMP_JSON" "$TMP_ROWS"' EXIT

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
  # A CLI Confluent serializa valores monetários como strings com prefixo USD.
  # Aceitamos também números JSON e strings decimais sem prefixo para compatibilidade.
  def money:
    if type == "number" then .
    elif type == "string" then
      if test("^\\$-?[0-9]+([.][0-9]+)?$") then ltrimstr("$") | tonumber
      elif test("^-?[0-9]+([.][0-9]+)?$") then tonumber
      else error("valor monetário em formato não reconhecido") end
    else error("valor monetário ausente ou com tipo inválido") end;
  (if type == "array" then .
   elif type == "object" and (.data | type) == "array" then .data
   else error("formato JSON não reconhecido: esperada lista ou objeto com data[]")
   end)
  | if all(.[]; type == "object") then .
    else error("a coleção contém item que não é objeto")
    end
  | map(
      if has("amount") then .amount = (.amount | money)
      else error("item sem amount") end
      | if .original_amount == null then .
        else .original_amount = (.original_amount | money) end
      | if .discount_amount == null then .
        else .discount_amount = (.discount_amount | money) end
    )
' "$TMP_JSON" >"$TMP_ROWS" || {
  echo "Resposta de Billing não passou na validação; JSON bruto preservado apenas temporariamente." >&2
  exit 1
}

mv "$TMP_JSON" "$JSON_FILE"
chmod 600 "$JSON_FILE"

ROW_COUNT="$(jq 'length' "$TMP_ROWS")"
TOTALS="$(jq -r '
  {
    item_count: length,
    original: ([.[] | (.original_amount // 0)] | add // 0),
    discount: (if all(.[]; .discount_amount? != null)
               then ([.[] | .discount_amount] | add // 0)
               else null end),
    discount_missing_count: ([.[] | select(.discount_amount? == null)] | length),
    amount: ([.[] | .amount] | add // 0)
  }
' "$TMP_ROWS")"

{
  echo "# Verificação automatizada de Billing"
  echo
  echo "- Coletado em (UTC): $STAMP"
  echo "- Período consultado: $START_DATE (inclusivo) até $END_DATE (exclusivo)"
  echo "- Itens retornados: $ROW_COUNT"
  echo "- Origem: \`confluent billing cost list -o json\`"
  echo "- Escopo: custos agregados diariamente pela Costs API."
  echo
  echo "## Totais retornados pela API"
  echo
  echo "| Campo | USD |"
  echo "|---|---:|"
  jq -r '
    "| Valor original | " + (.original | tostring) + " |",
    (if .discount == null
     then "| Descontos/créditos discriminados | Não consolidado: campo nulo/ausente em " + (.discount_missing_count | tostring) + " de " + (.item_count | tostring) + " itens |"
     else "| Descontos/créditos discriminados | " + (.discount | tostring) + " |"
     end),
    "| Valor líquido (amount) | " + (.amount | tostring) + " |"
  ' <<<"$TOTALS"
  echo
  echo "## Itens de custo"
  echo
  echo "| Data inicial | Produto | Tipo | Ambiente | Recurso | Quantidade | Unidade | Valor original (USD) | Desconto (USD) | Líquido (USD) |"
  echo "|---|---|---|---|---|---:|---|---:|---:|---:|"
  jq -r '
    def s: if . == null then "" else tostring | gsub("[|\r\n]"; " ") end;
    .[] |
    "| " + ((.start_date // "") | s) +
    " | " + ((.product // "") | s) +
    " | " + ((.line_type // .lineType // "") | s) +
    " | " + ((.resource.environment.id // .environment.id // .environment // "") | s) +
    " | " + ((.resource.id // .resource_id // .resource_name // "") | s) +
    " | " + ((.quantity // "") | s) +
    " | " + ((.unit // "") | s) +
    " | " + ((.original_amount // "") | s) +
    " | " + ((.discount_amount // "") | s) +
    " | " + ((.amount // "") | s) + " |"
  ' "$TMP_ROWS"
  if [[ "$ROW_COUNT" -eq 0 ]]; then
    echo
    echo "A consulta foi aceita, mas não retornou itens para o período. Isso não prova custo zero fora da janela/dos dados disponíveis."
  fi
  echo
  echo "## Limitações e interpretação"
  echo
  echo "- A Costs API pode levar até 72 horas para disponibilizar os dados; os valores não representam necessariamente os encargos correntes do Console."
  echo "- A consulta padrão termina três dias antes da data UTC atual para reduzir a exposição ao atraso. Datas explícitas podem ser informadas como argumentos."
  echo "- A API agrega os itens diariamente. O campo \`amount\` é o valor após os descontos discriminados pela API."
  echo "- Quando `discount_amount` vier nulo/ausente em qualquer item, o resumo não apresenta uma soma global de descontos como se estivesse completa."
  echo "- O resultado não informa o saldo restante de créditos promocionais nem substitui a tela de Invoice Breakdown."
  echo "- A consulta exige papel OrganizationAdmin ou BillingAdmin. A autenticação é a sessão já configurada na Confluent CLI; o script não cria nem altera credenciais."
  echo "- Nenhum recurso, permissão, método de pagamento ou configuração Cloud foi alterado."
  echo
  echo "## Arquivos"
  echo
  echo "- JSON original: \`evidence/observability/$(basename "$JSON_FILE")\`"
  echo "- Resumo: \`evidence/observability/$(basename "$SUMMARY_FILE")\`"
} >"$SUMMARY_FILE"
chmod 600 "$SUMMARY_FILE"

cat "$SUMMARY_FILE"
echo
echo "Evidência: ${SUMMARY_FILE#"$ROOT_DIR"/}"
