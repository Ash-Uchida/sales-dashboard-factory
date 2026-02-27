# Sales Dashboard Factory

A Streamlit-based governed analytics app for HackUSU 2026.

## Quick Start
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Environment Variables
Create `.env` (local only, do not commit):

```env
DATABRICKS_SERVER_HOSTNAME=dbc-<workspace-hostname>
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
DATABRICKS_TOKEN=<personal-access-token>
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=sales
```

## Data Setup (Databricks SQL Editor)
```sql
CREATE SCHEMA IF NOT EXISTS workspace.sales;

CREATE TABLE IF NOT EXISTS workspace.sales.transactions (
  order_id STRING,
  order_date DATE,
  region STRING,
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
  region STRING
);

CREATE TABLE IF NOT EXISTS workspace.sales.products (
  product_name STRING,
  category STRING
);
```

## 5-Minute Demo Outline
1. Problem (30s)
- Business users need fast answers from governed data without writing SQL.

2. Dashboard Walkthrough (90s)
- Show KPIs, date/region filters, and core charts.
- Confirm insights update from filtered data.

3. AI Chat Demo (90s)
- Ask a business question in natural language.
- Show generated SQL and returned result.

4. Governance Controls (60s)
- Explain approved tables, SELECT-only validation, row limit guardrails, and blocked unsafe SQL.
- Show audit log for traceability.

5. Close (30s)
- "We empower non-technical users while maintaining IT oversight through governed, auditable SQL workflows."

## Team Roles
- Person A: Data + dashboard
- Person B: AI + validation
- Person C: Governance + demo
