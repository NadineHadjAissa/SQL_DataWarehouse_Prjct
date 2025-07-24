-- 1. Create the database (only if it does not exist already)
DO $$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'dwh_prjct'
   ) THEN
      CREATE DATABASE dwh_prjct;
   END IF;
END
$$;

-- 2. Connect to the database (working i n pgAdmin query tool after selecting dwh_prjct)
-- \c dwh_prjct

-- 3. Create the schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
