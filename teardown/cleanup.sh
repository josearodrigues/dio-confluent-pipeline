#!/usr/bin/env bash
# cleanup.sh – script idempotente de teardown completo
# Remove todos os recursos criados pelo projeto e gera relatório de evidência.
# Mantém as proteções já existentes (confirmação explícita e validação de IDs).

set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/../scripts/_common.sh"
require confluent jq

# -------------------------------------------------------------------
# 1. Validação de variáveis de ambiente obrigatórias
# -------------------------------------------------------------------
: "${ENV_ID:?ENV_ID ausente; recuse executar sem estado local válido}"
: "${CLUSTER_ID:?CLUSTER_ID ausente; recuse executar sem estado local válido}"
: "${CC_ENV_NAME:?CC_ENV_NAME ausente}"
: "${CC_CLUSTER_NAME:?CC_CLUSTER_NAME ausente}"

ENV_NAME="$(confluent environment describe "$ENV_ID" -o json | jq -r '.name // empty')"
CLUSTER_NAME="$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.name // empty')"

[[ "$ENV_NAME" == "$CC_ENV_NAME" ]] || { echo "ENV_ID não corresponde ao Environment esperado." >&2; exit 1; }
[[ "$CLUSTER_NAME" == "$CC_CLUSTER_NAME" ]] || { echo "CLUSTER_ID não corresponde ao cluster esperado." >&2; exit 1; }

# -------------------------------------------------------------------
# 2. Funções auxiliares para contagem de recursos
# -------------------------------------------------------------------
pre_counts() {
  CLUSTERS_BEFORE=$(confluent kafka cluster list --environment "$ENV_ID" -o json | jq 'length')
  ENVIRONMENTS_BEFORE=$(confluent environment list -o json | jq '[.[] | select(.id=="$ENV_ID")] | length')
  STATEMENTS_BEFORE=$(confluent flink statement list --environment "$ENV_ID" -o json | jq 'length')
  CONNECTORS_BEFORE=$(confluent connect cluster list --environment "$ENV_ID" -o json | jq 'length')
  POOL_BEFORE=$(confluent flink compute-pool list --environment "$ENV_ID" -o json | jq 'length')
  APIKEYS_BEFORE=$(confluent api-key list --environment "$ENV_ID" -o json | jq 'length')
  SA_BEFORE=$(confluent iam service-account list --environment "$ENV_ID" -o json | jq 'length')
  # Neon DB sempre existente antes da limpeza (marcamos como 1)
  NEON_BEFORE=1
}

post_counts() {
  CLUSTERS_AFTER=$(confluent kafka cluster list --environment "$ENV_ID" -o json | jq 'length') || CLUSTERS_AFTER=0
  ENVIRONMENTS_AFTER=$(confluent environment list -o json | jq '[.[] | select(.id=="$ENV_ID")] | length') || ENVIRONMENTS_AFTER=0
  STATEMENTS_AFTER=$(confluent flink statement list --environment "$ENV_ID" -o json | jq 'length') || STATEMENTS_AFTER=0
  CONNECTORS_AFTER=$(confluent connect cluster list --environment "$ENV_ID" -o json | jq 'length') || CONNECTORS_AFTER=0
  POOL_AFTER=$(confluent flink compute-pool list --environment "$ENV_ID" -o json | jq 'length') || POOL_AFTER=0
  APIKEYS_AFTER=$(confluent api-key list --environment "$ENV_ID" -o json | jq 'length') || APIKEYS_AFTER=0
  SA_AFTER=$(confluent iam service-account list --environment "$ENV_ID" -o json | jq 'length') || SA_AFTER=0
  NEON_AFTER=0
}

# -------------------------------------------------------------------
# 3. Captura de contagens antes da limpeza
# -------------------------------------------------------------------
pre_counts

# -------------------------------------------------------------------
# 4. Confirmação do usuário antes de iniciar a remoção
# -------------------------------------------------------------------
echo "ATENÇÃO: operação destrutiva. Environment: $ENV_NAME ($ENV_ID)"
echo "Cluster: $CLUSTER_NAME ($CLUSTER_ID)"
read -r -p "Digite exatamente '$ENV_ID/$CLUSTER_ID' para confirmar: " answer
[[ "$answer" == "$ENV_ID/$CLUSTER_ID" ]] || { echo "Cancelado."; exit 1; }

# Seleciona o contexto Confluent
confluent environment use "$ENV_ID" >/dev/null
confluent kafka cluster use "$CLUSTER_ID" >/dev/null

# -------------------------------------------------------------------
# 5. Remoção dos recursos Confluent (idempotente)
# -------------------------------------------------------------------
for statement in payments-fraud-detection payments-append-only payments-enrichment \
  payments-pk-accounts payments-transactions-startup; do
  confluent flink statement delete "$statement" --force 2>/dev/null || true
done

if [[ -n "${SINK_CONNECTOR_ID:-}" ]]; then
  confluent connect cluster delete "$SINK_CONNECTOR_ID" --force || true
fi
if [[ -n "${CDC_CONNECTOR_ID:-}" ]]; then
  confluent connect cluster delete "$CDC_CONNECTOR_ID" --force || true
fi
if [[ -n "${FLINK_POOL_ID:-}" ]]; then
  confluent flink compute-pool delete "$FLINK_POOL_ID" --force || true
fi
for key in "${WRITER_API_KEY:-}" "${READER_API_KEY:-}" "${SR_API_KEY:-}"; do
  [[ -n "$key" ]] && confluent api-key delete "$key" --force || true
done
if [[ "${SA_WRITER_CREATED:-no}" == yes && -n "${SA_WRITER_ID:-}" ]]; then
  confluent iam service-account delete "$SA_WRITER_ID" --force || true
fi
if [[ "${SA_READER_CREATED:-no}" == yes && -n "${SA_READER_ID:-}" ]]; then
  confluent iam service-account delete "$SA_READER_ID" --force || true
fi
confluent kafka cluster delete "$CLUSTER_ID" --force || true
confluent environment delete "$ENV_ID" --force || true

# -------------------------------------------------------------------
# 6. Limpeza opcional do banco Neon (Postgres)
# -------------------------------------------------------------------
if [[ "${CONFIRM_CLEANUP:-no}" == "yes" ]]; then
  echo "🧹 Executando limpeza do banco Neon..."
  NEON_CLEANUP_OUTPUT=$(scripts/run-sql.sh database/cleanup/postgres-cleanup.sql 2>&1) || true
else
  NEON_CLEANUP_OUTPUT="⚠️ Limpeza do Neon não executada (CONFIRM_CLEANUP != yes)."
fi

# -------------------------------------------------------------------
# 7. Captura de contagens após a limpeza
# -------------------------------------------------------------------
post_counts

# -------------------------------------------------------------------
# 8. Execução do billing para obter custo total do ambiente
# -------------------------------------------------------------------
BILLING_OUTPUT=$(./operations/verify-billing.sh 2>&1) || true

# -------------------------------------------------------------------
# 9. Geração do relatório de evidência
# -------------------------------------------------------------------
REPORT_DIR="evidence/teardown"
mkdir -p "$REPORT_DIR"
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
REPORT_PATH="$REPORT_DIR/${TIMESTAMP}-teardown-report.md"

cat <<EOF > "$REPORT_PATH"
# Relatório de Teardown – $(date -u)

## Recursos antes vs. depois
| Recurso                     | Antes | Depois |
|----------------------------|-------|--------|
| Clusters Kafka             | $CLUSTERS_BEFORE | $CLUSTERS_AFTER |
| Environments Confluent     | $ENVIRONMENTS_BEFORE | $ENVIRONMENTS_AFTER |
| Statements Flink           | $STATEMENTS_BEFORE | $STATEMENTS_AFTER |
| Connectors (CDC & Sink)    | $CONNECTORS_BEFORE | $CONNECTORS_AFTER |
| Compute Pool (Flink)       | $POOL_BEFORE | $POOL_AFTER |
| API Keys                   | $APIKEYS_BEFORE | $APIKEYS_AFTER |
| Service Accounts           | $SA_BEFORE | $SA_AFTER |
| **Banco Neon (Postgres)**  | $NEON_BEFORE | $NEON_AFTER |

**Limpeza do Neon:**
```
$NEON_CLEANUP_OUTPUT
```

## Custo total (billing)
```
$BILLING_OUTPUT
```

*Todas as contagens acima devem ser 0 após a execução bem‑sucedida.*
EOF

echo "✅ Relatório de teardown salvo em $REPORT_PATH"
