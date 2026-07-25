-- ============================================================================
-- Create Airflow Database
-- ============================================================================
-- This script runs BEFORE init_postgres.sql (due to 00_ prefix)
-- Docker postgres image executes scripts in /docker-entrypoint-initdb.d/ 
-- in alphabetical order

-- This script is executed against the default 'postgres' database
-- Connect to postgres database explicitly
\c postgres;

-- Create airflow database (ignore if exists)
CREATE DATABASE airflow OWNER streammart_user;

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO streammart_user;
