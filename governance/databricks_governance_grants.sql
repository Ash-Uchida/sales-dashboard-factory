-- ============================================================
-- Databricks Governance & Security (Unity Catalog)
-- Run these in Databricks SQL Editor to enforce governance at the data layer.
-- Replace uch24004@byui.edu with your app's service principal or user
--   (e.g. the identity that owns the token in .env DATABRICKS_TOKEN).
-- ============================================================

-- ------------------------------------------------------------
-- 1. GRANTS: Least-privilege access for the app principal
--    The app connects with one token; grant only what it needs.
-- ------------------------------------------------------------

-- Admin schema: app needs to read auth_view, roles, stores; read/write users
USE CATALOG workspace;
USE SCHEMA admin;

-- App can read auth_view (login, list users, get role)
GRANT SELECT ON VIEW workspace.admin.auth_view TO `uch24004@byui.edu`;

-- App can read roles (signup dropdown, user management)
GRANT SELECT ON TABLE workspace.admin.roles TO `uch24004@byui.edu`;

-- App can read stores (dropdowns, dashboard)
GRANT SELECT ON TABLE workspace.admin.stores TO `uch24004@byui.edu`;

-- App can read/write users (register, reset password, list users)
-- Restrict this to app principal only; revoke from others if needed
GRANT SELECT, MODIFY ON TABLE workspace.admin.users TO `uch24004@byui.edu`;

-- App can insert into stores (Manager adds store via app)
GRANT MODIFY ON TABLE workspace.admin.stores TO `uch24004@byui.edu`;

-- App can call check_password procedure (if you use it)
GRANT EXECUTE ON PROCEDURE workspace.admin.check_password TO `uch24004@byui.edu`;

-- Sales schema: app needs to read and insert transactions, customers, products
USE SCHEMA sales;

GRANT SELECT, MODIFY ON TABLE workspace.sales.transactions TO `uch24004@byui.edu`;
GRANT SELECT, MODIFY ON TABLE workspace.sales.customers TO `uch24004@byui.edu`;
GRANT SELECT, MODIFY ON TABLE workspace.sales.products TO `uch24004@byui.edu`;

-- If you use secure_dashboard_view
GRANT SELECT ON VIEW workspace.sales.secure_dashboard_view TO `uch24004@byui.edu`;

-- ------------------------------------------------------------
-- 2. REVOKE from others (optional, for defense-in-depth)
--    If your workspace has a "users" or "account users" group,
--    revoke admin table access so only the app can touch them.
-- ------------------------------------------------------------

-- Example: revoke users table from all except app (run only if you want strict isolation)
-- REVOKE ALL PRIVILEGES ON TABLE workspace.admin.users FROM `users`;
-- REVOKE ALL PRIVILEGES ON TABLE workspace.admin.users FROM `account users`;

-- ------------------------------------------------------------
-- 3. COLUMN MASKING: Hide password_hash from ad-hoc queries
--    Even if someone runs SELECT * FROM users, password_hash shows as masked.
--    Requires Unity Catalog and supported runtime.
-- ------------------------------------------------------------

-- Create a mask function (returns constant for unauthorized viewers)
CREATE OR REPLACE FUNCTION workspace.admin.mask_password()
RETURNS STRING
RETURN '********';

-- Apply mask to password_hash column
-- (Syntax may vary by Databricks version; use if supported)
-- ALTER TABLE workspace.admin.users
--   ALTER COLUMN password_hash SET MASK workspace.admin.mask_password();

-- Alternative: If column masking is not supported, rely on auth_view
-- which never selects password_hash. Never grant SELECT on users
-- to principals that might run ad-hoc queries; only the app needs it.

-- ------------------------------------------------------------
-- 4. DATA QUALITY: Constraints (optional)
--    Enforce non-null and format rules at the table level.
-- ------------------------------------------------------------

-- Add constraints if your Delta/Unity Catalog version supports them
-- ALTER TABLE workspace.admin.users ALTER COLUMN username SET NOT NULL;
-- ALTER TABLE workspace.admin.users ALTER COLUMN password_hash SET NOT NULL;
-- ALTER TABLE workspace.admin.users ALTER COLUMN role_id SET NOT NULL;

-- ------------------------------------------------------------
-- 5. AUDIT: Unity Catalog system tables
--    Access is logged automatically. Query audit logs:
-- ------------------------------------------------------------

-- SELECT * FROM system.access.audit
-- WHERE table_name IN ('users', 'transactions', 'customers', 'products')
-- ORDER BY event_time DESC
-- LIMIT 100;

-- ------------------------------------------------------------
-- 6. ROW-LEVEL SECURITY (advanced, if you use multiple principals)
--    If you later connect with different tokens per app role,
--    you can add row filters so Business User/Manager see only their store.
-- ------------------------------------------------------------

-- Example row filter for transactions (store-scoped access):
-- CREATE OR REPLACE FUNCTION workspace.sales.store_filter(store_id STRING)
-- RETURNS BOOLEAN
-- RETURN store_id IN (
--   SELECT s.store_id FROM workspace.admin.stores s
--   JOIN workspace.admin.user_stores us ON us.store_id = s.store_id
--   WHERE us.username = current_user()
-- );
-- ALTER TABLE workspace.sales.transactions
--   SET ROW FILTER workspace.sales.store_filter ON (store_id);

-- For a single-token app, row-level filtering is enforced in app code
-- (SQL validator adds store_id filter for Business User/Manager).
