#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

require neon jq
: "${NEON_PROJECT_ID:?NEON_PROJECT_ID ausente no .env}"
: "${NEON_BRANCH:=main}"
: "${PG_DATABASE:?PG_DATABASE ausente no .env}"
: "${PG_CDC_USER:=cdc}"

[[ "$PG_CDC_USER" == "cdc" ]] || { echo "Este projeto usa a role CDC com o nome fixo 'cdc'." >&2; exit 1; }
[[ "$NEON_PROJECT_ID" =~ ^[a-z0-9-]+$ ]] || { echo "NEON_PROJECT_ID contém caracteres inválidos." >&2; exit 1; }
[[ "$NEON_BRANCH" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "NEON_BRANCH contém caracteres inválidos." >&2; exit 1; }

echo "Validando autenticação da Neon CLI..."
neon me >/dev/null

cat <<'WARNING'
ATENÇÃO: habilitar Logical Replication é uma alteração no projeto Neon.
A operação pode suspender os endpoints ativos e não pode ser desfeita.
WARNING
read -r -p "Confirma habilitar Logical Replication no projeto $NEON_PROJECT_ID? [s/N] " answer
[[ "$answer" =~ ^[sS]$ ]] || { echo "Operação cancelada. Nenhuma alteração foi solicitada ao Neon."; exit 0; }

echo "Habilitando Logical Replication..."
neon projects update "$NEON_PROJECT_ID" --enable-logical-replication

echo "Verificando a role $PG_CDC_USER na branch $NEON_BRANCH..."
ROLES_JSON="$(neon roles list --project-id "$NEON_PROJECT_ID" --branch "$NEON_BRANCH" --output json)"
if jq -e --arg role "$PG_CDC_USER" 'any(.[]; .name == $role)' <<<"$ROLES_JSON" >/dev/null; then
  echo "A role $PG_CDC_USER já existe; não será criada novamente."
else
  echo "Criando a role $PG_CDC_USER..."
  # A saída contém a senha gerada. Ela é suprimida; a connection string
  # será recuperada em seguida e gravada diretamente no .env.
  neon roles create --name "$PG_CDC_USER" --project-id "$NEON_PROJECT_ID" --branch "$NEON_BRANCH" >/dev/null
fi

echo "Obtendo credenciais da role sem exibi-las..."
CONNECTION_STRING="$(neon connection-string "$NEON_BRANCH" \
  --project-id "$NEON_PROJECT_ID" \
  --role-name "$PG_CDC_USER" \
  --database-name "$PG_DATABASE" \
  --ssl require)"

if [[ "$CONNECTION_STRING" =~ ^postgres(ql)?://([^:/?]+):([^@]+)@ ]]; then
  CONNECTION_USER="${BASH_REMATCH[2]}"
  CONNECTION_PASSWORD="${BASH_REMATCH[3]}"
else
  echo "Não foi possível interpretar a connection string retornada pela Neon CLI." >&2
  echo "Nenhuma credencial foi gravada no .env." >&2
  exit 1
fi

[[ "$CONNECTION_USER" == "$PG_CDC_USER" && -n "$CONNECTION_PASSWORD" ]] || {
  echo "A connection string não retornou as credenciais esperadas da role $PG_CDC_USER." >&2
  exit 1
}

write_env_value() {
  local key="$1" value="$2" escaped tmp
  printf -v escaped '%q' "$value"
  tmp="$(mktemp "$ROOT_DIR/.env.XXXXXX")"
  awk -v key="$key" -v value="$escaped" '
    BEGIN { updated = 0 }
    index($0, key "=") == 1 {
      if (!updated) print key "=" value
      updated = 1
      next
    }
    { print }
    END { if (!updated) print key "=" value }
  ' "$ROOT_DIR/.env" > "$tmp"
  chmod 600 "$tmp"
  mv "$tmp" "$ROOT_DIR/.env"
}

write_env_value "PG_CDC_USER" "$CONNECTION_USER"
write_env_value "PG_CDC_PASSWORD" "$CONNECTION_PASSWORD"
unset CONNECTION_STRING CONNECTION_PASSWORD

echo "Preparação do Neon concluída."
echo "Role configurada: $PG_CDC_USER"
echo "Credenciais gravadas em .env (permissão 600); senha não exibida."
echo "Próxima etapa: CONFIRM_RESET=yes ./scripts/run-sql.sh database/init/01-database.sql"
