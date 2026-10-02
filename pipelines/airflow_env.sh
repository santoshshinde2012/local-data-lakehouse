#!/usr/bin/env bash
# Generate the Airflow overlay's local secrets once (make airflow-up runs this):
#   .env: AIRFLOW_FERNET_KEY, AIRFLOW_JWT_SECRET, AIRFLOW_API_SECRET_KEY, AIRFLOW_DB_PASSWORD,
#         _AIRFLOW_WWW_USER_PASSWORD, AIRFLOW_UID (Linux only) (only the empty / missing ones are filled; existing values are kept)
#   airflow/auth/passwords.json: the simple auth manager's user -> password file (gitignored)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
[[ -f .env ]] || cp .env.example .env

rand() { python3 -c 'import secrets,sys; print(secrets.token_urlsafe(int(sys.argv[1])))' "$1"; }
fernet() { python3 -c 'import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())'; }
current() { sed -n "s/^$1=//p" .env | tail -1; }
ensure() {
  local key="$1" value="$2"
  if [[ -n "$(current "$key")" ]]; then return; fi
  if grep -q "^$key=" .env; then
    python3 - "$key" "$value" <<'PY'
import pathlib, sys
key, value = sys.argv[1], sys.argv[2]
p = pathlib.Path(".env")
p.write_text("".join(f"{key}={value}\n" if l.split("=", 1)[0] == key else l
                     for l in p.read_text().splitlines(keepends=True)))
PY
  else
    printf '%s=%s\n' "$key" "$value" >> .env
  fi
  echo "==> generated $key in .env"
}

ensure AIRFLOW_FERNET_KEY "$(fernet)"
ensure AIRFLOW_JWT_SECRET "$(rand 32)"
ensure AIRFLOW_API_SECRET_KEY "$(rand 32)"
ensure AIRFLOW_DB_PASSWORD "$(rand 18)"
ensure _AIRFLOW_WWW_USER_PASSWORD "$(rand 12)"
# Linux bind mounts keep host ownership: run Airflow as you (Docker Desktop maps ownership itself).
if [[ "$(uname -s)" == Linux ]]; then ensure AIRFLOW_UID "$(id -u)"; fi
chmod 600 .env

user="$(current _AIRFLOW_WWW_USER_USERNAME)"; user="${user:-admin}"
mkdir -p airflow/auth
python3 - "$user" "$(current _AIRFLOW_WWW_USER_PASSWORD)" <<'PY'
import json, pathlib, sys
p = pathlib.Path("airflow/auth/passwords.json")
p.write_text(json.dumps({sys.argv[1]: sys.argv[2]}) + "\n")
p.chmod(0o600)   # gitignored; the API server (AIRFLOW_UID) opens it read-write
PY
echo "==> Airflow UI user: $user (password: _AIRFLOW_WWW_USER_PASSWORD in .env)"
