-- Read-only role for dashboards (Metabase).
-- Can ONLY read the masked marts and the reconciliation results.
-- Has no access to raw or warehouse, where unmasked PII lives.
--
-- The password is passed in as a psql variable, never stored in this file:
--   psql -v reader_password="$METABASE_READER_PASSWORD" -f sql/security/01_reader_role.sql
-- (See "Run it yourself" in the README for the Docker version.)

\if :{?reader_password}
\else
    \echo 'ERROR: run with -v reader_password=... (value of METABASE_READER_PASSWORD in .env)'
    \quit
\endif

-- Create the role on the first run; rotate the password on every later run.
SELECT format('CREATE ROLE metabase_reader LOGIN PASSWORD %L', :'reader_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_reader') \gexec

SELECT format('ALTER ROLE metabase_reader WITH LOGIN PASSWORD %L', :'reader_password') \gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO metabase_reader', current_database()) \gexec

GRANT USAGE  ON SCHEMA marts, reconciliation TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts, reconciliation TO metabase_reader;

-- dbt drops and recreates tables on every run. Default privileges make sure
-- tables that the warehouse owner (the user running this script) creates in
-- these schemas in the future are readable too.
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA marts GRANT SELECT ON TABLES TO metabase_reader', current_user) \gexec
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA reconciliation GRANT SELECT ON TABLES TO metabase_reader', current_user) \gexec
