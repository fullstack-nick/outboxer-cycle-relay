#!/usr/bin/env bash
set -euo pipefail

psql --set=ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
  --set=app_password="${POSTGRES_APP_PASSWORD}" <<'SQL'
CREATE USER outboxer_app WITH PASSWORD :'app_password';
GRANT CONNECT ON DATABASE outboxer TO outboxer_app;
GRANT ALL ON SCHEMA public TO outboxer_app;
SQL

