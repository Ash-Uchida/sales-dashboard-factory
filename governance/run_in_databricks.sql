-- ============================================================
-- Run these in Databricks SQL Editor ONE QUERY AT A TIME.
-- (Select only the lines for ONE query, then click Run.)
-- ============================================================
--
-- If you use a different catalog than "workspace", replace
--   workspace.admin  -->  your_catalog.admin
-- in the queries below.
--
-- ============================================================

-- ------------------------------------------------------------
-- 1. AUTH_VIEW (works with 5-column users: user_id, firstname, lastname, username, password_hash)
--    Run this so the app can load user info after login. No role_id/store_id needed.
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW workspace.admin.auth_view AS
SELECT
  user_id,
  username,
  firstname,
  lastname,
  'Business User' AS role_name,
  CAST(NULL AS STRING) AS store_id
FROM workspace.admin.users;


-- ------------------------------------------------------------
-- 2. CHECK_PASSWORD procedure (optional)
--    Requires Databricks Runtime 17.0+ and Unity Catalog.
--    If you get "syntax error" or "procedure not supported", skip this;
--    the app will still work using a query-based password check.
-- ------------------------------------------------------------

CREATE OR REPLACE PROCEDURE workspace.admin.check_password(p_username STRING, p_password STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS result
  FROM workspace.admin.users
  WHERE username = p_username AND password_hash = sha2(p_password, 256);
END;
