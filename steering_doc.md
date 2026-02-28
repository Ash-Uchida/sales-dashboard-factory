# Sales Dashboard Factory
## Steering Document

## 1. Purpose
This document defines standards, guardrails, and governance controls for building Sales Dashboard applications in the Sales Dashboard Factory.

**Problem:** Store managers and merchandisers need same-day answers from governed sales data without writing SQL or exceeding their access. Report backlogs and shadow spreadsheets slow decisions and create compliance risk.

**Goal:** Empower non-technical users to analyze business performance while preserving security, compliance, and IT oversight through governed Databricks data access.

## 2. Approved Data Sources
All applications must query governed Unity Catalog assets only.

Approved catalog:
- `workspace`

Approved schema:
- `sales`

Approved tables:
- `workspace.sales.transactions`
- `workspace.sales.customers`
- `workspace.sales.products`

Data access rules:
- No external datasets.
- No unmanaged storage access.
- Only approved tables and views are allowed.
- Query lineage and access controls must remain visible to IT.

## 3. Role-Based Access Control (RBAC)

**Business User:**
- Can view KPIs, charts, and filters.
- Can ask business questions in chat (store-scoped, transactions only).
- Cannot run raw SQL directly.
- Query scope is restricted to approved transactions-level analytics.
- Cannot access Audit Log view.
- Can add transactions, customers, and products.

**Manager:**
- Same as Business User, plus:
- **Add Store:** Only Manager can add new stores to the system.
- Cannot access Audit Log or User management.

**Data Analyst:**
- Can extend dashboard metrics and visualization logic.
- Can tune approved analytical queries (all sales tables).
- **Audit Log:** Can access Audit Log view for review and QA.
- Cannot modify core governance policy in production.
- Cannot add stores or manage users/passwords.

**IT Admin:**
- Can review query logs and generated SQL.
- **Audit Log:** Can access Audit Log view and governance evidence.
- **User management:** Only IT Admin can create users, reset passwords, and manage accounts.
- Can disable non-compliant behavior.
- Can manage access and governance policies.

## 4. Application Template Requirements
Every Sales Dashboard Factory app must include:
- Minimum 5 KPIs.
- Minimum 3 visualizations.
- Date filter.
- Region filter.
- Conversational analytics interface.
- Query/audit log visibility for **Data Analyst** and **IT Admin** only.
- Optional AI insight summary that can be toggled on/off.

Dashboard standards:
- KPI formulas must be reproducible.
- Visuals must be sourced from governed tables.
- App must run in a sandbox/demo-safe environment before presentation.

## 5. Guardrails
The system enforces the following guardrails:
- SELECT-only SQL execution.
- Blocked operations include INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/MERGE/GRANT/REVOKE.
- Only approved table references are accepted.
- Query row limits are enforced.
- AI-generated SQL must pass validation prior to execution.
- Unsafe SQL is blocked and logged.
- Business User role receives stricter table/query scope than Analyst/Admin roles.

## 6. AI Governance Policy
AI is used for natural-language-to-SQL translation only.

AI policy:
- Generate SQL for governed business questions.
- Do not access external APIs or external datasets.
- Produce auditable SQL output for every prompt.
- Defer execution to the validation and governance layer.
- When live warehouse execution is unstable, demo-safe fallback execution may be used to preserve user experience while keeping validation and logging active.

This allows speed and accessibility without bypassing policy controls.

## 7. IT Oversight and Auditability
For operational control and compliance:
- User question, generated SQL, timestamp, role, status, and outcome are logged.
- Status values include: `RECEIVED`, `SUCCESS`, `BLOCKED`, `ERROR`.
- Blocked queries are retained for review.
- Admin role can inspect activity and intervene when needed.

This provides traceability and supports responsible AI use.

## 8. Governance & Security Guidelines

### Access Control (Implemented)
- **Role-Based Access:** Four roles—Business User, Manager, Data Analyst, IT Admin (see Section 3).
- **Row-Level Security:** Business User and Manager queries are scoped to their store; Data Analyst and IT Admin see all stores.
- **Audit Logging:** All chat queries (question, SQL, role, status, outcome) are logged. Audit Log view is restricted to **Data Analyst** and **IT Admin** only.
- **Password Management:** Only **IT Admin** can create users, reset passwords, and access User management. Passwords are stored as hashes; plaintext is never exposed.
- **Store Management:** Only **Manager** can add new stores via the Add Store page.

### Application Guardrails
- Template restrictions: SELECT-only SQL; approved tables only.
- Resource limits: Row limits enforced per role (1K for Business User/Manager, 100K for Analyst/Admin).
- AI-generated SQL must pass validation before execution.
- Blocked operations: INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/MERGE/GRANT/REVOKE in chat.

### IT Oversight
- Visibility: Audit Log shows all chat activity.
- Blocked queries are retained for review.
- IT Admin can inspect activity and intervene when needed.

## 9. Summary
Sales Dashboard Factory combines:
- Streamlit application templates,
- Databricks SQL execution,
- Unity Catalog governance,
- role-aware controls,
- and validated AI query translation.

Result: non-technical users can self-serve insights while IT maintains oversight, policy enforcement, and auditability.
