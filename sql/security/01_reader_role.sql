-- Read-only role for dashboards (Metabase).
-- Can ONLY read the masked marts and the reconciliation results.
-- Has no access to raw or warehouse, where unmasked PII lives.

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_reader') THEN
        CREATE ROLE metabase_reader LOGIN PASSWORD 'reader_password';
    END IF;
END $$;

GRANT CONNECT ON DATABASE bank_dw TO metabase_reader;

GRANT USAGE  ON SCHEMA marts, reconciliation TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts, reconciliation TO metabase_reader;

-- dbt drops and recreates tables on every run. Default privileges make sure
-- tables that bank_admin creates in these schemas in the future are readable too.
ALTER DEFAULT PRIVILEGES FOR ROLE bank_admin IN SCHEMA marts
    GRANT SELECT ON TABLES TO metabase_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE bank_admin IN SCHEMA reconciliation
    GRANT SELECT ON TABLES TO metabase_reader;
