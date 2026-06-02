#!/usr/bin/env bash
set -e

echo "==> Initialising Airflow DB..."
airflow db migrate

echo "==> Setting admin password to 'airflow'..."
# SimpleAuthManager reads passwords from this file; writing it before standalone
# starts means the auto-generated random password is never used.
python3 -c "
import json, pathlib
p = pathlib.Path('/opt/airflow/simple_auth_manager_passwords.json.generated')
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps({'admin': 'airflow'}))
"

echo "==> Starting Airflow standalone..."
exec airflow standalone
