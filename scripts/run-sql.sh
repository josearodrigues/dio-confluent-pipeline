#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
: "${PG_HOST:?PG_HOST ausente}"
: "${PG_PORT:?PG_PORT ausente}"
: "${PG_DATABASE:?PG_DATABASE ausente}"
: "${PG_ADMIN_USER:?PG_ADMIN_USER ausente}"
: "${PG_ADMIN_PASSWORD:?PG_ADMIN_PASSWORD ausente}"
require psql

FILE="${1:?Uso: ./scripts/run-sql.sh caminho/arquivo.sql}"
if [[ "$FILE" = /* ]]; then
  case "$FILE" in
    "$ROOT_DIR"/*) SQL_RELATIVE="${FILE#"$ROOT_DIR"/}" ;;
    *) echo "Arquivo SQL deve estar dentro do projeto." >&2; exit 1 ;;
  esac
else
  SQL_RELATIVE="$FILE"
fi
case "/$SQL_RELATIVE/" in
  */../*) echo "Caminho SQL inválido." >&2; exit 1 ;;
esac
SQL_FILE="$ROOT_DIR/$SQL_RELATIVE"
[[ -f "$SQL_FILE" ]] || { echo "Arquivo não encontrado: $SQL_RELATIVE" >&2; exit 1; }

EXTRA=()
if [[ "$SQL_RELATIVE" == database/init/01-database.sql ]]; then
  : "${PG_CDC_USER:?PG_CDC_USER ausente}"
  : "${PG_CDC_PASSWORD:?PG_CDC_PASSWORD ausente}"
  [[ "$PG_CDC_USER" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || {
    echo "PG_CDC_USER contém caracteres inválidos para um nome de role." >&2
    exit 1
  }
  [[ "${CONFIRM_RESET:-}" == "yes" ]] || {
    echo "A inicialização recria o schema payments. Use CONFIRM_RESET=yes após conferir o alvo." >&2
    exit 1
  }

  echo "Alvo PostgreSQL: host=$PG_HOST database=$PG_DATABASE user=$PG_ADMIN_USER"
  echo "Pré-validação somente leitura: conexão, wal_level e role $PG_CDC_USER."

  WAL_LEVEL="$(PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 -Atq \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" \
    -c 'SHOW wal_level')"
  [[ "$WAL_LEVEL" == "logical" ]] || {
    echo "ERRO: wal_level='$WAL_LEVEL'; esperado 'logical'. Nenhum dado foi alterado." >&2
    exit 1
  }

  ROLE_FLAGS="$(PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 -Atq \
    -v cdc_role="$PG_CDC_USER" -h "$PG_HOST" -p "$PG_PORT" \
    -U "$PG_ADMIN_USER" -d "$PG_DATABASE" <<'SQL'
SELECT rolcanlogin::text || '|' ||
       (rolreplication OR pg_has_role(rolname, 'neon_superuser', 'MEMBER'))::text
FROM pg_roles
WHERE rolname = :'cdc_role';
SQL
)"
  [[ "$ROLE_FLAGS" == "true|true" ]] || {
    echo "ERRO: role $PG_CDC_USER ausente ou sem LOGIN + REPLICATION. Nenhum dado foi alterado." >&2
    exit 1
  }

  # Confirma também que a credencial da role CDC autentica no banco.
  PGPASSWORD="$PG_CDC_PASSWORD" PGSSLMODE=require psql -X -v ON_ERROR_STOP=1 -Atq \
    -h "$PG_HOST" -p "$PG_PORT" -U "$PG_CDC_USER" -d "$PG_DATABASE" \
    -c 'SELECT 1' >/dev/null || {
      echo "ERRO: a role $PG_CDC_USER não conseguiu autenticar no banco. Nenhum dado foi alterado." >&2
      exit 1
    }

  echo "Pré-validação aprovada: wal_level=logical; role $PG_CDC_USER com LOGIN + REPLICATION e autenticação válida."
  EXTRA+=(-v reset_ok=1)
  EXTRA+=(-v "cdc_role=$PG_CDC_USER")
fi

if [[ "$SQL_RELATIVE" == database/cleanup/postgres-cleanup.sql ]]; then
  [[ "${CONFIRM_CLEANUP:-}" == "yes" ]] || {
    echo "A limpeza remove o schema payments e a publication. Use CONFIRM_CLEANUP=yes após conferir o alvo." >&2
    exit 1
  }
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
case "$SQL_RELATIVE" in
  database/init/*|database/cleanup/*) GROUP=local ;;
  *) GROUP=homologation ;;
esac
mkdir -p "$ROOT_DIR/evidence/$GROUP"
LOG_RELATIVE="evidence/$GROUP/$STAMP-$(basename "$SQL_RELATIVE" .sql).log"
LOG="$ROOT_DIR/$LOG_RELATIVE"

cd "$ROOT_DIR"
set +e
PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require psql -X \
  -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DATABASE" \
  -v ON_ERROR_STOP=1 -v "DBNAME=$PG_DATABASE" "${EXTRA[@]}" \
  -f "$SQL_RELATIVE" 2>&1 | tee "$LOG"
status="${PIPESTATUS[0]}"
set -e
echo "Log: $LOG_RELATIVE"
exit "$status"
