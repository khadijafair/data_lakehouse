-- Base du catalogue Iceberg JDBC (partagée par Spark et Trino).
-- Idempotent : peut être rejoué sans erreur sur une base déjà initialisée.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'iceberg') THEN
        CREATE ROLE iceberg LOGIN PASSWORD 'iceberg';
    END IF;
END $$;

SELECT 'CREATE DATABASE iceberg OWNER iceberg'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iceberg')\gexec
