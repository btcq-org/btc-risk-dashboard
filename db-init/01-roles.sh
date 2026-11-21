#!/bin/bash
# Creates the non-superuser roles the indexer and API connect as.
#
# The postgres image runs this automatically on first start (empty data volume).
# It is idempotent, so it can also be run on an existing database; see README
# "Database roles" for migrating a volume created before this script existed.
#
#   indexer : owns the schema, writes blocks/UTXOs/stats. NOSUPERUSER.
#   api_ro  : SELECT only, read-only transactions. Used by indexer/api.py.
set -euo pipefail

: "${POSTGRES_USER:?}" "${POSTGRES_DB:?}"
: "${INDEXER_DB_PASSWORD:?INDEXER_DB_PASSWORD must be set}"
: "${API_DB_PASSWORD:?API_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v db="$POSTGRES_DB" \
  -v indexer_pw="$INDEXER_DB_PASSWORD" \
  -v api_pw="$API_DB_PASSWORD" <<'SQL'
-- timescaledb is not a trusted extension, so the superuser creates it here.
-- schema.sql's CREATE EXTENSION IF NOT EXISTS is then a no-op for the indexer role.
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

SELECT 'CREATE ROLE indexer' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'indexer')
\gexec
SELECT 'CREATE ROLE api_ro' WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_ro')
\gexec

ALTER ROLE indexer WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'indexer_pw';
ALTER ROLE api_ro  WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD :'api_pw';
ALTER ROLE api_ro SET default_transaction_read_only = on;

REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE :"db" TO indexer;
GRANT CONNECT ON DATABASE :"db" TO api_ro;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO indexer;
GRANT USAGE ON SCHEMA public TO api_ro;

-- Existing tables and views, then everything the indexer creates later
-- (schema.sql drops and recreates its materialized views on every start).
GRANT SELECT ON ALL TABLES IN SCHEMA public TO api_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE indexer IN SCHEMA public GRANT SELECT ON TABLES TO api_ro;
SQL
