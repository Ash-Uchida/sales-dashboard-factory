-- Sales schema and tables (run in Databricks SQL)
-- Use your catalog if not workspace (e.g. replace workspace with your catalog name).

CREATE SCHEMA IF NOT EXISTS workspace.sales;

CREATE TABLE IF NOT EXISTS workspace.sales.transactions (
  order_id STRING,
  order_date DATE,
  store_id STRING,
  product_name STRING,
  quantity INT,
  unit_price DOUBLE,
  revenue DOUBLE,
  customer_id STRING
);

CREATE TABLE IF NOT EXISTS workspace.sales.customers (
  customer_id STRING,
  customer_name STRING,
  segment STRING,
  store_id STRING
);

CREATE TABLE IF NOT EXISTS workspace.sales.products (
  product_id STRING,
  product_name STRING,
  category STRING
);

-- View: links transactions to stores and manager (join on store_id, not region).
CREATE OR REPLACE VIEW workspace.sales.secure_dashboard_view AS
SELECT
  t.order_id,
  t.order_date,
  t.store_id,
  t.product_name,
  t.quantity,
  t.unit_price,
  t.revenue,
  t.customer_id,
  s.store_name,
  s.location,
  u.username AS manager_name
FROM workspace.sales.transactions t
LEFT JOIN workspace.admin.stores s ON t.store_id = s.store_id
LEFT JOIN workspace.admin.users u ON s.user_id = u.user_id;
