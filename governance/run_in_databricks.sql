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
-- 1. AUTH_VIEW (users JOIN roles so role_name comes from chosen role_id)
--    Run this so the app shows the correct role after login.
-- ------------------------------------------------------------

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
