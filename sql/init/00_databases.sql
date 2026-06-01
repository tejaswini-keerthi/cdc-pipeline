-- =============================================================================
-- Create the database that backs the Iceberg JDBC catalog (Step 5).
-- Runs before the schema/seed scripts (alphabetical order).
-- \gexec makes this idempotent: CREATE DATABASE cannot run conditionally inline,
-- so we generate the statement only when the DB is missing.
-- =============================================================================
SELECT 'CREATE DATABASE iceberg_catalog'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iceberg_catalog')\gexec
