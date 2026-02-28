-- Admin schema: roles, users, stores, auth_view, and optional password-check procedure.
-- Run in Databricks SQL (Workspace or SQL Warehouse).

-- 1. Roles table
CREATE TABLE IF NOT EXISTS workspace.admin.roles (
  role_id STRING,
  role_name STRING
);
-- Seed roles (run once)
INSERT INTO workspace.admin.roles (role_id, role_name) VALUES
  ('role_business', 'Business User'),
  ('role_analyst', 'Data Analyst'),
  ('role_admin', 'IT Admin')
;

-- 2. Users table (linked to roles and store)
CREATE TABLE IF NOT EXISTS workspace.admin.users (
  user_id STRING,
  firstname STRING,
  lastname STRING,
  username STRING,
  password_hash STRING,
  role_id STRING,
  created_at TIMESTAMP,
  store_id STRING
);

-- 3. Stores table (optional: seed one or more stores for signup dropdown)
CREATE TABLE IF NOT EXISTS workspace.admin.stores (
  store_id STRING,
  store_name STRING,
  location STRING,
  user_id STRING,
  created_at TIMESTAMP
);

-- 4. Secure view for app: user identity and role name (no password_hash).
--    App uses this after login to get role_name and store_id.
CREATE OR REPLACE VIEW workspace.admin.auth_view AS
SELECT
  u.user_id,
  u.username,
  u.firstname,
  u.lastname,
  r.role_name AS role_name,
  u.store_id
FROM workspace.admin.users u
LEFT JOIN workspace.admin.roles r ON u.role_id = r.role_id;

-- 5. Optional: secure password-check procedure (checks password without exposing hash).
--    Takes username and plaintext password; result set has one row with column "result": 1 = success, 0 = failure.
--    Requires password_hash in users to be stored as SHA256 hex (same as app hash_password()).
--    Requires Databricks Runtime 17.0+ and Unity Catalog. If not supported, the app uses query-based check.
CREATE OR REPLACE PROCEDURE workspace.admin.check_password(p_username STRING, p_password STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS BEGIN
  SELECT CASE
    WHEN EXISTS (
      SELECT 1 FROM workspace.admin.users u
      WHERE u.username = p_username AND u.password_hash = sha2(p_password, 256)
    ) THEN 1 ELSE 0
  END AS result;
END;
