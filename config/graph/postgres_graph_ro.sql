-- Optional hardening (TEACHING stack): a SELECT-only Postgres role for the graph reader (ldl-graph).
-- PyIceberg in ldl-graph then cannot INSERT/UPDATE/DELETE/ALTER/DROP anything in the Iceberg JDBC
-- catalog that Spark owns: it only reads iceberg_tables / iceberg_namespace_properties and follows
-- the metadata pointers to the tags + snapshots the graph build pins.
--
-- Idempotent: run it as often as you like, before or after the first Spark job (the catalog tables
-- are created lazily by the first Iceberg write; ALTER DEFAULT PRIVILEGES covers tables created later).
-- Re-running re-applies the attributes and the password.
--
-- Scope of that default privilege (step 3b): ALTER DEFAULT PRIVILEGES cannot name tables. It gives
-- graph_ro SELECT on EVERY table the role running this script (the catalog owner, the role Spark's
-- JdbcCatalog connects as) creates later in schema public of THIS database, not only the two catalog
-- tables. That is safe in this stack: the database (POSTGRES_DB, default iceberg) holds the Iceberg
-- JDBC catalog alone (config/spark-defaults.conf: spark.sql.catalog.lakehouse.uri; Iceberg's JdbcCatalog
-- creates exactly iceberg_tables and iceberg_namespace_properties; Airflow's metadata database is
-- another Postgres service, airflow-postgres), the data itself lives in the warehouse, not in
-- Postgres, and reading catalog rows (table names, metadata pointers) is all graph_ro is for. On a
-- database that also holds other tables, run this script after the first Spark publish (step 3a
-- then grants SELECT on the two catalog tables by name) and drop the default privilege as the same
-- owner:  ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM graph_ro;
--
-- Run (repo root, base stack up), as the catalog owner, with POSTGRES_USER / POSTGRES_DB read INSIDE
-- the postgres container (where Compose set them):
--   docker compose exec -T postgres sh -c 'exec psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" \
--     -d "$POSTGRES_DB" -v ro_password="$0" -f -' "${GRAPH_PG_PASSWORD:-graph_ro}" \
--     < config/graph/postgres_graph_ro.sql
-- then (re)start ldl-graph with the role:
--   GRAPH_PG_USER=graph_ro GRAPH_PG_PASSWORD=<same> \
--     docker compose -f docker-compose.yml -f docker-compose.graph.yml up -d graph
-- (`make graph-pg-readonly` wraps both steps when the Makefile has it.)
--
-- The password must be URL-safe (letters, digits and . _ ~ -): docker-compose.graph.yml puts it
-- verbatim into the SQLAlchemy URI PYICEBERG_CATALOG__LAKEHOUSE__URI, where @ : / % ? # or a space
-- would break or silently change the URI. This script refuses any other password.
--
-- What it does NOT narrow: the S3 side. ldl-graph still reads the warehouse with the Silo ROOT key
-- (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD); a read-only S3 identity needs a second Silo user and
-- policy. A PyIceberg write attempted under this role fails at the catalog commit, but it can leave
-- orphan data/metadata objects in the bucket first.
--
-- Needs psql (uses \if, \gexec, \gset and :'var'); it is not a Spark / Iceberg SQL file, so it lives
-- under config/, not sql/.
\set ON_ERROR_STOP on
\if :{?ro_password}
\else
  \set ro_password graph_ro
\endif

-- 0. The password goes into a URI: URL-safe characters only.
SELECT :'ro_password' ~ '^[A-Za-z0-9._~-]+$' AS ro_password_url_safe \gset
\if :ro_password_url_safe
\else
  DO $$ BEGIN RAISE EXCEPTION 'graph_ro password must be URL-safe (letters, digits, . _ ~ -): it goes into PYICEBERG_CATALOG__LAKEHOUSE__URI'; END $$;
\endif

-- 1. The role (created once; attributes and password re-applied on every run).
SELECT 'CREATE ROLE graph_ro'
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'graph_ro') \gexec
ALTER ROLE graph_ro WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  NOINHERIT CONNECTION LIMIT 8 PASSWORD :'ro_password';

-- 2. Connect + look, nothing else.
SELECT format('GRANT CONNECT ON DATABASE %I TO graph_ro', current_database()) \gexec
GRANT USAGE ON SCHEMA public TO graph_ro;

-- 3. The two Iceberg JDBC catalog tables. PyIceberg's SqlCatalog probes BOTH at start-up; with
--    SELECT on only one of them it mis-detects the catalog schema as v1 and every lookup then fails
--    ('column iceberg_tables.iceberg_type does not exist').
--    (a) the tables that exist now
SELECT format('GRANT SELECT ON TABLE public.%I TO graph_ro', c.relname)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
  AND c.relname IN ('iceberg_tables', 'iceberg_namespace_properties') \gexec
--    (b) the tables the catalog owner (the role running this script, the one Spark connects as)
--        creates later, so the order of this script and the first Spark job does not matter. Every
--        later table of that owner in schema public of this database, in fact (see "Scope" above:
--        in this stack those are the two catalog tables and nothing else).
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO graph_ro;

-- 4. Belt and braces. NOT the security boundary (a session can switch it off); the boundary is the
--    missing INSERT / UPDATE / DELETE / TRUNCATE / REFERENCES / TRIGGER / CREATE privileges above.
ALTER ROLE graph_ro SET default_transaction_read_only = on;
ALTER ROLE graph_ro SET statement_timeout = '15s';
ALTER ROLE graph_ro SET idle_in_transaction_session_timeout = '60s';

-- 5. Report.
SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolconnlimit
FROM pg_catalog.pg_roles WHERE rolname = 'graph_ro';
SELECT table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'graph_ro' ORDER BY table_name, privilege_type;
