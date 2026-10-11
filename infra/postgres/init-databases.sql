-- Initialize multiple databases for Lakehouse
SELECT 'CREATE DATABASE nessie'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'nessie')\gexec
