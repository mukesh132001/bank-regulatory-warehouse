-- Raw layer: data exactly as received from source systems (our "data lake")
CREATE SCHEMA IF NOT EXISTS raw;

-- Audit layer: load history, reconciliation results
CREATE SCHEMA IF NOT EXISTS audit;
