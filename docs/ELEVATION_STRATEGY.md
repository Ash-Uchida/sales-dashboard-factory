# Sales Dashboard Factory — Elevation Strategy (HackUSU 2026)

**Goal:** Turn "a dashboard that tracks transactions" into **a solution that solves a real problem** and stands out to judges. This doc reframes the problem, adds prize-worthy differentiators, and keeps your existing schedule intact.

---

## 1. The Real Problem (Use This in Your Pitch)

**Current framing (generic):**  
"Business users need fast answers from governed data without writing SQL."

**Stronger framing (problem-first, judge-friendly):**

> **Store managers and merchandisers can’t wait for IT or analysts to run reports.**  
> They need **same-day answers** from the same governed sales data that finance and IT trust—without writing SQL and **without seeing data they’re not allowed to see**.  
> We give them a **single place**: a governed dashboard + natural-language questions, with **every query validated and logged** so IT keeps full oversight.

**Why this wins:**
- **Who it helps:** Store managers, regional leads, merchandisers (real personas).
- **What breaks today:** Report backlogs, access requests, shadow spreadsheets.
- **What you deliver:** Self-serve analytics that stay inside Unity Catalog and audit trails.

Use this in your **Problem (30s)** and **Close** so the demo tells a story, not just a feature list.

---

## 2. What “More” Means — Differentiators That Don’t Break Your Schedule

These layer on top of your current plan. Pick **2–3** that fit your remaining hours.

### A. **Visible data lineage (Governance requirement + low effort)**

**Requirement:** Hackathon asks for "Demonstrate data lineage."

**Idea:** In the UI, show where numbers come from.

- Add an expander: **"Where does this data come from?"**
  - e.g. "Revenue KPI: `workspace.sales.transactions` → `SUM(revenue)` with date/region filters."
  - List approved tables and schema.

**Why it wins:** Judges see governance in action; lineage is explicit, not assumed.

---

### B. **Actionable insights (Not just charts — “So what?”)**

**Requirement:** Advanced feature: "AI-generated insights or recommendations."

**Idea:** After KPIs/charts, show **2–3 bullet insights** derived from the same governed data:

- "**West** is the top region by revenue in the selected period."
- "**Epsilon** and **Delta** drive ~60% of revenue; consider promotion in underperforming regions."
- "Average order value is **above** the overall median in **North**."

Rules: insights are **computed from the filtered dataframe** (no new SQL), optional (can be disabled), and **never bypass** the SQL validator for chat.

**Why it wins:** Shows "data → insight → action," not just "data → chart."

---

### C. **“Why did this number change?” (Period-over-period)**

**Idea:** Add a simple comparison: **This period vs previous period** (e.g. last 7 days vs prior 7 days).

- Show: Revenue change %, Orders change %, and **biggest contributor** (e.g. "Revenue down 5%; North region contributed most to the drop").
- Uses only `transactions` + date filters; no new tables.

**Why it wins:** Every business user asks "why did it go up/down?"—you answer it with governed data.

---

### D. **Persona-driven defaults (Role simulation 2.0)**

**Idea:** When role = "Business User" and they pick region "West," pre-fill or hint:

- "You’re viewing **West** region. Queries will be scoped to your region."
- Optionally: default the chat placeholder to "What are my top products in West?"

**Why it wins:** Makes row-level / region scoping **tangible** for judges ("different users see different data").

---

### E. **Export with audit (Export + compliance)**

**Requirement:** "Export functionality (CSV, PDF)" + audit.

**Idea:**  
- **Export current view (CSV):** Button that exports the **filtered** KPI summary or chart data to CSV and **logs** "Export requested: dashboard summary, role, timestamp" in the audit log.
- No need for PDF in the first version; CSV + log is enough to show "export + governance."

**Why it wins:** Demonstrates export and that exports are auditable.

---

### F. **Data quality indicator (Governance: “Validate data before visualization”)**

**Idea:** One line in the sidebar or under KPIs:

- "Data quality: Last transaction date **2025-08-28**. **X** rows in selected range."
- If you have a simple rule (e.g. "no future dates"), show "Checks passed" or "1 warning."

**Why it wins:** Shows you thought about "data quality" in the governance framework.

---

## 3. Recommended Order of Implementation (If You Have Time)

| Priority | Item | Rough effort | Hackathon alignment |
|----------|------|--------------|---------------------|
| 1 | **Visible data lineage** (expander with table/source) | ~15 min | Governance / lineage |
| 2 | **2–3 actionable insight bullets** (from filtered df) | ~30 min | AI insights |
| 3 | **Export CSV + audit log entry** | ~20 min | Export + audit |
| 4 | Period-over-period "why did it change?" | ~45 min | Advanced analytics |
| 5 | Data quality line (last date, row count) | ~15 min | Data quality |
| 6 | Persona placeholder / hint for Business User | ~10 min | Role simulation |

**Cut order if behind:** Do **1 + 2**; they give the most impact for the least risk. Keep dashboard, governed chat, guardrails, logging, and steering doc as non-negotiable.

---

## 4. Updated Demo Script Hooks (5-Minute Version)

- **Problem (30s):** Use the "Store managers can’t wait for IT…" framing above.
- **Dashboard (90s):** Show KPIs, filters, 3 charts, then **open "Where does this data come from?"** and show lineage. Optionally show **insight bullets** ("West is top region…").
- **Chat (90s):** Ask a business question → show SQL → show result. Then ask something that **gets blocked** (e.g. "DELETE from transactions") and show the log.
- **Governance (60s):** Audit log, role switch (Business User vs Analyst), mention export audit if you added it.
- **Close (30s):**  
  *"We empower non-technical users while maintaining IT oversight. Governance is enforced through Unity Catalog with validated, auditable SQL workflows—and every export and query is logged."*

---

## 5. One-Line Summary for Judges

**"Sales Dashboard Factory lets store managers and merchandisers get same-day answers from governed sales data via natural language and dashboards—with every query validated, scoped by role and region, and logged for IT oversight."**

---

## 6. What Stays the Same

- Your **technical plan** (Streamlit, Databricks, Unity Catalog, validator, audit log, roles) stays.
- Your **hour-by-hour schedule** still works; these ideas are **add-ons**.
- **Security:** No real tokens in repo; .env per teammate; no new risk.

Use this doc in your **steering document** and **README** as the "Problem" and "Differentiators" section so judges see a clear story: real problem → governed solution → visible lineage, insights, and audit.
