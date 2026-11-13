#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_FILE="$ROOT_DIR/.state.env"

[[ -f "$ROOT_DIR/.env" ]] || { echo "Crie .env a partir de .env.example." >&2; exit 1; }
set -a
# shellcheck disable=SC1091
source "$ROOT_DIR/.env"
if [[ -f "$STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$STATE_FILE"
fi
set +a

require() {
  local cmd
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || { echo "Comando ausente: $cmd" >&2; exit 1; }
  done
}

save_state() {
  local key="$1" value="$2" tmp
  touch "$STATE_FILE"
  chmod 600 "$STATE_FILE"
  tmp="$(mktemp)"
  grep -v "^$key=" "$STATE_FILE" > "$tmp" || true
  printf '%s=%q\n' "$key" "$value" >> "$tmp"
  mv "$tmp" "$STATE_FILE"
  chmod 600 "$STATE_FILE"
  export "$key=$value"
}

save_api_key_credentials() {
  local json="$1" keyvar="$2" secretvar="$3" key secret
  key="$(jq -er '.api_key | select(type == "string" and length > 0)' <<<"$json")" || {
    echo "ERRO: resposta da Confluent CLI sem api_key válida; credenciais não foram persistidas." >&2
    return 1
  }
  secret="$(jq -er '.api_secret | select(type == "string" and length > 0)' <<<"$json")" || {
    echo "ERRO: resposta da Confluent CLI sem api_secret válida; credenciais não foram persistidas." >&2
    return 1
  }
  save_state "$keyvar" "$key"
  save_state "$secretvar" "$secret"
}

render_template() {
  local input="$1" output="$2"; shift 2
  local vars='{}' key
  for key in "$@"; do
    vars="$(jq -c --arg k "$key" --arg v "${!key}" '. + {($k):$v}' <<<"$vars")"
  done
  jq --argjson vars "$vars" '
    walk(if type == "string"
      then gsub("\\$\\{(?<key>[A-Za-z_]+)\\}"; $vars[.key] // "")
      else . end)' "$input" > "$output"
  chmod 600 "$output"
}
