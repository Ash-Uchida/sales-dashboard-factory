# Sales Dashboard Factory

Governed analytics app for HackUSU 2026 built with Streamlit + Databricks SQL. Implements Data App Factory requirements: governed data access, visual dashboard, conversational interface, steering documents, and governance framework.

## Project Structure

```
sales-dashboard-factory/
├── app.py                    # Main application (dashboard, chat, data entry, auth)
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables (not committed)
├── components/
│   └── auth.py               # Authentication, login, signup, role-based access
├── governance/
│   ├── steering_doc.md       # App template documentation and guardrails
│   ├── guardrails.yaml       # Security policies (SQL rules, limits)
│   ├── roles.yaml            # User roles and permissions
│   ├── admin_schema.sql      # Admin schema (users, roles, stores, auth_view)
│   ├── sales_schema_and_view.sql  # Sales tables and secure view
│   └── run_in_databricks.sql # Migration and view scripts for Databricks
├── utils/
│   └── databricks_auth.py    # Databricks auth (login, register, password reset)
├── docs/
│   ├── ROLE_ACCESS_AND_DATABRICKS.md  # Role matrix and Databricks setup
│   └── ELEVATION_STRATEGY.md # Demo and differentiation notes
└── README.md                 # This file
```

## Project Goal
Build a demo-ready app that lets business users:
- View KPI dashboard and charts
- Ask natural-language sales questions
- Get governed SQL + results
- Keep IT oversight through audit logs and guardrails

## Stack
- Streamlit
- Databricks SQL Connector
- Pandas
- Plotly
- Python Dotenv
- OpenAI (optional)

## Team Roles
- A (Data + Dashboard): KPIs, filters, charts, Databricks connection
- B (AI + Validation): NL-to-SQL, SQL validation, chat flow
- C (Governance + Demo): steering doc, audit/log UX, demo script

## Quick Start

### macOS / Linux
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

### Windows (PowerShell)
```powershell
py -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

### Windows (Command Prompt)
```bat
py -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## Governance & Security (Implemented)

| Control | Who | What |
|--------|-----|------|
| **Password / User management** | IT Admin only | Create users (role + store), reset passwords, list users. Assign Business Users a store for store-only access. Passwords stored as hashes. |
| **Add Store** | Manager only | Add new stores to the system. |
| **Audit Log** | Data Analyst, IT Admin | View chat query logs (question, SQL, role, status, outcome). |
| **Row-level scope** | Business User, Manager | Chat queries scoped to their store; Analyst/Admin see all. |
| **Table access** | Role-based | Business User/Manager: transactions only; Analyst/Admin: transactions, customers, products. |

See `docs/ROLE_ACCESS_AND_DATABRICKS.md` for full role matrix and Databricks setup.

## Environment Variables
Create a local `.env` file (never commit secrets):

```env
DATABRICKS_SERVER_HOSTNAME=dbc-xxxxxxxx.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxxxxxxxxxx
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXX
DATABRICKS_CATALOG=workspace
DATABRICKS_SCHEMA=sales
DATABRICKS_SCHEMA_ADMIN=admin
```


Optional chat mode:
- Default is demo-safe execution for reliability.
- To force live Databricks chat execution:

```bash
export CHAT_EXECUTION_MODE=databricks
```

## Databricks SQL Setup
Run in SQL Editor:

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

Seed sample data:

```sql
INSERT INTO workspace.sales.transactions VALUES
('O1001','2026-01-05','West','Alpha',3,100,300,'C001'),
('O1002','2026-01-06','East','Beta',2,120,240,'C002'),
('O1003','2026-01-07','North','Gamma',5,80,400,'C003'),
('O1004','2026-01-08','South','Alpha',4,100,400,'C004');

INSERT INTO workspace.sales.customers VALUES
('C001','Acme Co','SMB','West'),
('C002','Bright Inc','Enterprise','East'),
('C003','Northwind','SMB','North'),
('C004','Sunrise LLC','Mid-Market','South');

INSERT INTO workspace.sales.products VALUES
('Alpha','Core'),
('Beta','Addon'),
('Gamma','Core');
```

## What the App Includes
- 5 KPIs: Revenue, Orders, Customers, AOV, Units
- Core charts: Revenue trend, Revenue by region, Product mix
- Filters: Date range and region
- Conversational analytics: question -> SQL -> validate -> execute/render
  - Default mode is demo-safe result rendering for stability
  - Live Databricks execution is opt-in via `CHAT_EXECUTION_MODE=databricks`
- SQL guardrails:
  - SELECT-only
  - Approved table allowlist
  - Dangerous keyword blocking
  - Row-limit enforcement
  - Role-aware table restrictions
- Audit logging for chat actions:
  - timestamp
  - role
  - question
  - generated SQL
  - status (`RECEIVED`, `SUCCESS`, `BLOCKED`, `ERROR`)
  - outcome
  - Audit log view restricted to **Data Analyst** and **IT Admin**
- User management (create user, reset password) restricted to **IT Admin**
- Add Store restricted to **Manager**

## Validator Test Cases
Use these to verify behavior quickly:

1. `revenue by region` -> `SUCCESS`
2. `top 5 products by revenue` -> `SUCCESS`
3. `what was last month's average revenue` -> `SUCCESS`
4. `delete my data` -> `BLOCKED`
5. (Business User) `show customer count by segment` -> `BLOCKED`
6. (Data Analyst) `show customer count by segment` -> `SUCCESS`
7. `mello mello helo jello` -> clarification/block
8. query with excessive limit request -> capped/blocked per rules
9. non-approved table reference -> `BLOCKED`
10. multi-statement input -> `BLOCKED`

## Demo Script (5 Minutes)
1. Problem: Business users need governed self-service analytics.
2. Dashboard: KPIs, filters, and charts.
3. Chat: Ask a natural-language question, show generated SQL and result.
4. Governance: Show blocked unsafe request + audit log entry.
5. Close: "We empower non-technical users while maintaining IT oversight through validated, auditable SQL workflows."

## Security Notes
- Do not commit `.env` or tokens.
- Each teammate uses their own local token.
- Rotate token immediately if exposed.

## Troubleshooting

### `streamlit: command not found`
Your virtual environment is not active.

macOS/Linux:
```bash
source venv/bin/activate
streamlit run app.py
```

Windows (PowerShell):
```powershell
.\\venv\\Scripts\\Activate.ps1
streamlit run app.py
```

### Warehouse pauses / query feels stuck
Databricks Free Edition warehouse auto-stops after inactivity.
- Go to Databricks SQL Warehouses
- Start `Serverless Starter Warehouse`
- Run a simple warm-up query in SQL Editor:
```sql
SELECT 1;
```

### Chat shows SQL but no Databricks result
Use reliable demo-safe mode (default), or explicitly choose mode:

```bash
# reliable demo behavior (default)
unset CHAT_EXECUTION_MODE

# force live Databricks execution
export CHAT_EXECUTION_MODE=databricks
```

If live mode is unstable, switch back to demo-safe mode for presentation reliability.

### `.env` values not loading
Check required keys are present:
- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN`
- `DATABRICKS_CATALOG`
- `DATABRICKS_SCHEMA`

Quick check:
```bash
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print(os.getenv('DATABRICKS_SERVER_HOSTNAME')); print(os.getenv('DATABRICKS_HTTP_PATH')); print(os.getenv('DATABRICKS_CATALOG')); print(os.getenv('DATABRICKS_SCHEMA'))"
```

### Token/auth errors
- Generate a new Databricks personal access token.
- Update local `.env`.
- Restart Streamlit app.

### Audit log not visible
By design, Audit Log is visible only for `Data Analyst` and `IT Admin` roles.

## Repo Workflow
```bash
git checkout -b codex/<feature-name>
git add .
git commit -m "<message>"
git push -u origin codex/<feature-name>
```
