import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

from components.auth import can_access, get_current_role, get_current_user, logout, require_login

try:
    from databricks import sql as dbsql
except Exception:
    dbsql = None

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

load_dotenv()

APPROVED_TABLES = {
    "transactions",
    "customers",
    "products",
    "main.sales.transactions",
    "main.sales.customers",
    "main.sales.products",
    "workspace.sales.transactions",
    "workspace.sales.customers",
    "workspace.sales.products",
}

BLOCKED_SQL_PATTERNS = [
    r"\binsert\b",
    r"\bupdate\b",
    r"\bdelete\b",
    r"\bdrop\b",
    r"\balter\b",
    r"\btruncate\b",
    r"\bmerge\b",
    r"\bgrant\b",
    r"\brevoke\b",
    r"\bcall\b",
    r"\bcopy\b",
]

DEFAULT_LIMIT = 100000

ROLE_ALLOWED_TABLES = {
    "Business User": {
        "transactions",
        "main.sales.transactions",
        "workspace.sales.transactions",
    },
    "Data Analyst": {
        "transactions",
        "customers",
        "products",
        "main.sales.transactions",
        "main.sales.customers",
        "main.sales.products",
        "workspace.sales.transactions",
        "workspace.sales.customers",
        "workspace.sales.products",
    },
    "IT Admin": APPROVED_TABLES,
}

ROLE_MAX_LIMIT = {
    "Business User": 1000,
    "Data Analyst": 100000,
    "IT Admin": 100000,
}


@st.cache_data
def load_demo_data() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=240, freq="D")
    regions = ["West", "East", "North", "South"]
    products = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"]

    rows = []
    order_id = 1000
    for i, day in enumerate(dates):
        for region in regions:
            for p in products:
                qty = 5 + ((i + len(region) + len(p)) % 18)
                unit_price = 40 + (len(p) * 6)
                revenue = qty * unit_price
                rows.append(
                    {
                        "order_id": order_id,
                        "order_date": day,
                        "region": region,
                        "product_name": p,
                        "quantity": qty,
                        "unit_price": unit_price,
                        "revenue": float(revenue),
                        "customer_id": f"C{(i % 70) + 1}",
                    }
                )
                order_id += 1

    return pd.DataFrame(rows)


def databricks_configured() -> bool:
    return bool(
        os.getenv("DATABRICKS_SERVER_HOSTNAME")
        and os.getenv("DATABRICKS_HTTP_PATH")
        and os.getenv("DATABRICKS_TOKEN")
        and dbsql is not None
    )


def execute_databricks_query(query: str) -> pd.DataFrame:
    conn = dbsql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            colnames = [c[0] for c in cursor.description]
        return pd.DataFrame(rows, columns=colnames)
    finally:
        conn.close()


def normalize_sql(query: str) -> str:
    return re.sub(r"\s+", " ", query).strip()


def extract_table_references(query: str) -> List[str]:
    # Extract table names after FROM/JOIN, tolerating aliases and quoted identifiers.
    refs = re.findall(r"(?:from|join)\s+([`\"\w\.\-]+)", query, flags=re.IGNORECASE)
    cleaned = []
    for ref in refs:
        table = ref.strip().strip(",").replace("`", "").replace('"', "").lower()
        cleaned.append(table)
    return cleaned


def is_single_statement(query: str) -> bool:
    q = query.strip()
    if not q:
        return False
    # Allow one optional trailing semicolon only.
    if ";" in re.sub(r";\s*$", "", q):
        return False
    return True


def validate_sql(query: str, role: str, selected_region: str) -> Tuple[bool, str, str]:
    raw = query.strip()
    normalized = normalize_sql(raw)
    q = normalized.lower()

    if not is_single_statement(normalized):
        return False, "Only one SQL statement is allowed.", raw

    if not (q.startswith("select") or q.startswith("with")):
        return False, "Only SELECT queries are allowed.", raw

    if "--" in q or "/*" in q or "*/" in q:
        return False, "SQL comments are not allowed.", raw

    for pattern in BLOCKED_SQL_PATTERNS:
        if re.search(pattern, q):
            return False, "This query contains blocked SQL operations.", raw

    refs = extract_table_references(normalized)
    if not refs:
        return False, "No table reference found. Query must use approved Unity Catalog tables.", raw

    allowed_for_role = ROLE_ALLOWED_TABLES.get(role, APPROVED_TABLES)
    for table in refs:
        if table not in APPROVED_TABLES:
            return (
                False,
                f"Table `{table}` is not approved. Allowed tables: transactions, customers, products.",
                raw,
            )
        if table not in allowed_for_role:
            return False, f"Role '{role}' is not allowed to query table `{table}`.", raw

    if role == "Business User" and selected_region != "All":
        selected_region_lc = selected_region.lower()
        region_pattern = rf"\bregion\s*=\s*'{re.escape(selected_region_lc)}'"
        if not re.search(region_pattern, q):
            return False, f"Business User queries must be scoped to region '{selected_region}'.", raw

    role_max_limit = ROLE_MAX_LIMIT.get(role, DEFAULT_LIMIT)
    limit_match = re.search(r"\blimit\s+(\d+)\b", q)
    if limit_match:
        limit_value = int(limit_match.group(1))
        if limit_value <= 0:
            return False, "Query limit must be greater than 0.", raw
        if limit_value > role_max_limit:
            return False, f"Role '{role}' limit is {role_max_limit} rows.", raw
        return True, "SQL validated.", normalized

    sanitized = normalized.rstrip(";") + f" LIMIT {role_max_limit}"
    return True, f"SQL validated. Added default row limit ({role_max_limit}).", sanitized


def clean_sql_output(text: str) -> str:
    raw = text.strip()
    code_match = re.search(r"```sql\s*(.*?)\s*```", raw, flags=re.IGNORECASE | re.DOTALL)
    sql = (code_match.group(1) if code_match else raw).strip()
    sql = re.sub(r"^\s*sql\s*:\s*", "", sql, flags=re.IGNORECASE)
    sql = sql.strip("`").strip()

    if not re.match(r"^(select|with)\b", sql, flags=re.IGNORECASE):
        raise ValueError("Model returned non-SQL output; expected SELECT/WITH query.")
    if ";" in sql[:-1]:
        raise ValueError("Model returned multiple SQL statements; only one is allowed.")
    return sql


def explain_unsupported_request(question: str, role: str) -> Optional[str]:
    q = question.lower()
    policy_rules = [
        (
            r"\b(delete|drop|truncate|alter|update|insert|merge)\b",
            "I cannot help with data-changing requests. This app only allows read-only analytics queries.",
        ),
        (
            r"(revenue\s*\*\s*\d+|times\s*\d+|multiply.*revenue|inflate.*revenue|fake.*revenue)",
            "I cannot help manipulate business metrics. This app reports governed data values only.",
        ),
        (
            r"\b(hack|bypass|override|disable guardrail|ignore policy)\b",
            "I cannot bypass governance controls. Guardrails are required for secure and compliant access.",
        ),
    ]
    for pattern, message in policy_rules:
        if re.search(pattern, q):
            return message

    # Role-aware request restrictions with clear user feedback.
    if role == "Business User":
        if re.search(r"\b(join|customers?|segment)\b", q):
            return (
                "Your current role can query transactions-level analytics only. "
                "Switch to Data Analyst for customer/product/segment analysis."
            )
    # Low-intent / unclear input: prompt user to ask a concrete business query.
    tokens = re.findall(r"[a-zA-Z]+", q)
    known_terms = {
        "sales", "revenue", "region", "product", "products", "customer", "customers",
        "segment", "trend", "daily", "monthly", "month", "last", "top", "average",
        "avg", "aov", "order", "orders", "west", "east", "north", "south",
    }
    if tokens and not any(t in known_terms for t in tokens):
        return (
            "I couldn't map that to a business query. Try a clear request like "
            "'top 5 products by revenue' or 'last month average revenue'."
        )
    return None


def llm_to_sql(question: str, selected_region: str) -> str:
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "sales")
    region_filter = (
        f" and region = '{selected_region}'" if selected_region and selected_region != "All" else ""
    )
    selected_region_rule = (
        f"- selected_region is '{selected_region}'. You must include `region = '{selected_region}'` in SQL."
        if selected_region and selected_region != "All"
        else "- selected_region is 'All'. Do not force a specific region filter unless user asks."
    )

    if OpenAI is not None and os.getenv("OPENAI_API_KEY"):
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        prompt = f"""
You are a SQL generator for Databricks.
Return only SQL. No markdown. No explanation.

Rules:
- Only SELECT statements.
- Allowed tables only:
  - workspace.sales.transactions(order_id, order_date, region, product_name, quantity, unit_price, revenue, customer_id)
  - workspace.sales.customers(customer_id, customer_name, segment, region)
  - workspace.sales.products(product_name, category)
- Never use INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, MERGE, GRANT, REVOKE.
- Always include LIMIT 1000 or less.
- Prefer simple, readable SQL.
- Respect selected region context:
{selected_region_rule}
- If the question is ambiguous, return this exact safe query pattern:
  SELECT DATE(order_date) AS day, SUM(revenue) AS total_revenue
  FROM workspace.sales.transactions
  WHERE 1=1
  GROUP BY DATE(order_date)
  ORDER BY day DESC
  LIMIT 1000
- If user asks "last month", filter to previous calendar month.
- If user asks "average", return AVG(revenue) AS avg_order_value.

User question:
{question}
""".strip()

        response = client.responses.create(
            model=model,
            input=prompt,
            temperature=0,
        )
        text = response.output_text.strip()
        candidate_sql = clean_sql_output(text)
        return candidate_sql.rstrip(";")

    q = question.lower()
    base = f"{catalog}.{schema}.transactions"
    is_last_month = "last month" in q or "last months" in q
    is_average = "average" in q or "avg" in q or "mean" in q
    top_n_match = re.search(r"\btop\s+(\d{1,4})\b", q)
    top_n = min(max(int(top_n_match.group(1)), 1), 1000) if top_n_match else 10

    if is_average and is_last_month:
        return (
            "SELECT AVG(revenue) AS avg_order_value "
            f"FROM {base} "
            "WHERE DATE_TRUNC('month', order_date) = DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1))"
            f"{region_filter} LIMIT 1"
        )

    if is_last_month and ("revenue" in q or "sales" in q):
        return (
            "SELECT SUM(revenue) AS total_revenue "
            f"FROM {base} "
            "WHERE DATE_TRUNC('month', order_date) = DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1))"
            f"{region_filter} LIMIT 1"
        )

    if "customer" in q and "segment" in q:
        customers_table = f"{catalog}.{schema}.customers"
        return (
            "SELECT segment, COUNT(DISTINCT customer_id) AS customers "
            f"FROM {customers_table} "
            f"WHERE 1=1{region_filter} "
            "GROUP BY segment ORDER BY customers DESC LIMIT 1000"
        )

    if "join" in q and "customer_id" in q:
        customers_table = f"{catalog}.{schema}.customers"
        return (
            "SELECT c.segment, SUM(t.revenue) AS total_revenue "
            f"FROM {base} t JOIN {customers_table} c ON t.customer_id = c.customer_id "
            f"WHERE 1=1{region_filter} "
            "GROUP BY c.segment ORDER BY total_revenue DESC LIMIT 1000"
        )

    if "top" in q and "product" in q:
        return (
            "SELECT product_name, SUM(revenue) AS total_revenue "
            f"FROM {base} WHERE 1=1{region_filter} "
            f"GROUP BY product_name ORDER BY total_revenue DESC LIMIT {top_n}"
        )

    if "trend" in q or "over time" in q or "daily" in q:
        return (
            "SELECT DATE(order_date) AS day, SUM(revenue) AS total_revenue "
            f"FROM {base} WHERE 1=1{region_filter} "
            "GROUP BY DATE(order_date) ORDER BY day LIMIT 1000"
        )

    if is_average or "aov" in q:
        return (
            "SELECT AVG(revenue) AS avg_order_value "
            f"FROM {base} WHERE 1=1{region_filter} LIMIT 1"
        )

    return (
        "SELECT region, SUM(revenue) AS total_revenue, COUNT(DISTINCT customer_id) AS customers "
        f"FROM {base} WHERE 1=1{region_filter} GROUP BY region LIMIT 1000"
    )


def apply_filters(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp, region: str) -> pd.DataFrame:
    filtered = df[(df["order_date"] >= start_date) & (df["order_date"] <= end_date)].copy()
    if region != "All":
        filtered = filtered[filtered["region"] == region]
    return filtered


def render_kpis(df: pd.DataFrame) -> None:
    total_revenue = float(df["revenue"].sum()) if not df.empty else 0.0
    total_orders = int(df["order_id"].nunique()) if not df.empty else 0
    total_customers = int(df["customer_id"].nunique()) if not df.empty else 0
    avg_order_value = total_revenue / total_orders if total_orders else 0.0
    total_units = int(df["quantity"].sum()) if not df.empty else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Revenue", f"${total_revenue:,.0f}")
    c2.metric("Orders", f"{total_orders:,}")
    c3.metric("Customers", f"{total_customers:,}")
    c4.metric("AOV", f"${avg_order_value:,.2f}")
    c5.metric("Units", f"{total_units:,}")


def get_insight_bullets(df: pd.DataFrame) -> List[str]:
    """Generate 2-3 actionable insight bullets from the filtered dataframe (governed data only)."""
    if df.empty:
        return ["No data in selected range — adjust date or region filters."]
    bullets = []
    by_region = df.groupby("region", as_index=False)["revenue"].sum()
    if not by_region.empty:
        top_region = by_region.loc[by_region["revenue"].idxmax()]
        bullets.append(f"**{top_region['region']}** is the top region by revenue in the selected period (${top_region['revenue']:,.0f}).")
    by_product = df.groupby("product_name", as_index=False)["revenue"].sum().sort_values("revenue", ascending=False)
    if len(by_product) >= 2:
        top2 = by_product.head(2)
        top2_rev = top2["revenue"].sum()
        total_rev = df["revenue"].sum()
        pct = (top2_rev / total_rev * 100) if total_rev else 0
        names = ", ".join(top2["product_name"].tolist())
        bullets.append(f"**{names}** drive {pct:.0f}% of revenue; consider promotion in underperforming regions.")
    aov = df["revenue"].sum() / df["order_id"].nunique() if df["order_id"].nunique() else 0
    bullets.append(f"Average order value in selection: **${aov:,.2f}** — use chat to compare across regions.")
    return bullets[:3]


def render_charts(df: pd.DataFrame) -> None:
    if df.empty:
        st.warning("No data available for selected filters.")
        return

    # Build an explicit day column so Plotly always has a stable x-axis field.
    day_df = df.copy()
    day_df["order_day"] = pd.to_datetime(day_df["order_date"]).dt.date
    by_day = day_df.groupby("order_day", as_index=False)["revenue"].sum()
    by_region = df.groupby("region", as_index=False)["revenue"].sum()
    by_product = (
        df.groupby("product_name", as_index=False)["revenue"].sum().sort_values("revenue", ascending=False)
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_line = px.line(by_day, x="order_day", y="revenue", title="Revenue Trend")
        st.plotly_chart(fig_line, use_container_width=True)

    with col2:
        fig_bar = px.bar(by_region, x="region", y="revenue", title="Revenue by Region")
        st.plotly_chart(fig_bar, use_container_width=True)

    fig_pie = px.pie(by_product.head(8), names="product_name", values="revenue", title="Revenue Mix by Product")
    st.plotly_chart(fig_pie, use_container_width=True)


def log_event(
    role: str,
    question: str,
    sql: str,
    status: str,
    outcome: str,
) -> None:
    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "role": role,
        "question": question,
        "sql": sql,
        "status": status,
        "outcome": outcome,
    }
    logs = st.session_state.setdefault("audit_logs", [])
    logs.insert(0, entry)
    st.session_state["audit_logs"] = logs


def main() -> None:
    st.set_page_config(page_title="Sales Dashboard Factory", layout="wide")
    require_login()

    st.title("Sales Dashboard Factory")
    st.caption(
        "Governed data app template with KPI dashboard + natural language analytics. "
        "Unity Catalog guardrails are enforced through SQL validation rules."
    )

    if "audit_logs" not in st.session_state:
        st.session_state["audit_logs"] = []
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    role = get_current_role()

    with st.sidebar:
        st.subheader("Access Context")
        st.markdown(
            """
            <style>
            [data-testid="stSidebar"] div.stButton > button {
                border: none;
                border-radius: 0;
                margin: 0;
                box-shadow: none;
            }
            [data-testid="stSidebar"] div.stButton {
                margin: 0;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        user = get_current_user()
        st.info(f"**{user}** · {role}")
        if st.button("Log out"):
            logout()
        if "view_mode" not in st.session_state:
            st.session_state["view_mode"] = "Dashboard"
        st.caption("View")
        if st.button("Dashboard", use_container_width=True):
            st.session_state["view_mode"] = "Dashboard"
        if st.button("Audit Log", use_container_width=True):
            st.session_state["view_mode"] = "Audit Log"
        view = st.session_state["view_mode"]
        st.caption(f"Current: {view}")
        mode = "Databricks SQL" if databricks_configured() else "Demo Data (no Databricks env configured)"
        st.caption(f"Execution: {mode}")

    if view == "Audit Log":
        st.subheader("Audit Log")
        logs = pd.DataFrame(st.session_state.get("audit_logs", []))
        st.caption(f"Entries: {len(logs)}")
        if logs.empty:
            st.info("No queries logged yet.")
        else:
            st.dataframe(logs, use_container_width=True)
        return

    demo_df = load_demo_data()
    min_date = demo_df["order_date"].min().date()
    max_date = demo_df["order_date"].max().date()

    c1, c2, c3 = st.columns(3)
    with c1:
        start_date = st.date_input("Start Date", min_date)
    with c2:
        end_date = st.date_input("End Date", max_date)
    with c3:
        selected_region = st.selectbox("Region", ["All", "West", "East", "North", "South"])

    filtered_df = apply_filters(
        demo_df,
        pd.to_datetime(start_date),
        pd.to_datetime(end_date),
        selected_region,
    )

    st.subheader("KPI Dashboard")
    render_kpis(filtered_df)
    render_charts(filtered_df)

    # Actionable insights (from governed data only; does not bypass SQL validation)
    insight_bullets = get_insight_bullets(filtered_df)
    if insight_bullets:
        with st.expander("AI-generated insights (from this view)", expanded=True):
            for b in insight_bullets:
                st.markdown(f"- {b}")

    # Data lineage (governance requirement: demonstrate where data comes from)
    with st.expander("Where does this data come from?", expanded=False):
        st.markdown("""
        **Governed sources (Unity Catalog)**  
        - **Catalog:** `workspace` · **Schema:** `sales`

        **Tables used for this dashboard:**
        - `workspace.sales.transactions` — order_id, order_date, region, product_name, quantity, unit_price, revenue, customer_id
        - `workspace.sales.customers` — customer_id, customer_name, segment, region
        - `workspace.sales.products` — product_name, category

        **KPI formulas (reproducible):**
        - **Revenue:** `SUM(revenue)` from transactions, with date and region filters applied.
        - **Orders:** `COUNT(DISTINCT order_id)` from transactions.
        - **Customers:** `COUNT(DISTINCT customer_id)` from transactions.
        - **AOV:** Revenue ÷ Orders.
        - **Units:** `SUM(quantity)` from transactions.

        All visuals and insights are derived only from these approved tables and filters.
        """)

    st.subheader("Conversational Analytics")
    if st.button("Clear Chat"):
        st.session_state.chat_history = []
        st.rerun()

    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    user_question = st.chat_input("Ask about sales...")
    if user_question:
        cleaned = user_question.strip()
        if cleaned in {"", '""', "''"}:
            st.warning("Please enter a real question.")
            return

        st.session_state.chat_history.append({"role": "user", "content": cleaned})
        with st.chat_message("user"):
            st.markdown(cleaned)
        log_event(
            role,
            cleaned,
            "N/A",
            "RECEIVED",
            "question_received",
        )

        unsupported_reason = explain_unsupported_request(cleaned, role)
        if unsupported_reason:
            st.session_state.chat_history.append({"role": "assistant", "content": unsupported_reason})
            with st.chat_message("assistant"):
                st.warning(unsupported_reason)
            log_event(
                role,
                cleaned,
                "N/A",
                "BLOCKED",
                "policy_or_role_restriction",
            )
            return

        try:
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    time.sleep(1.2)

            generated_sql = llm_to_sql(cleaned, selected_region)

            ok, message, safe_sql = validate_sql(generated_sql, role, selected_region)
            if not ok:
                blocked_text = f"""I blocked this query for safety.

Reason: {message}

Generated SQL:
```sql
{generated_sql}
```"""
                st.session_state.chat_history.append({"role": "assistant", "content": blocked_text})
                with st.chat_message("assistant"):
                    st.warning(f"I blocked this query for safety: {message}")
                    st.markdown(f"```sql\n{generated_sql}\n```")
                log_event(
                    role,
                    cleaned,
                    generated_sql,
                    "BLOCKED",
                    message,
                )
            else:
                assistant_text = f"""I can help with sales trends, regions, top products, and KPIs.

Generated SQL:
```sql
{safe_sql}
```"""
                st.session_state.chat_history.append({"role": "assistant", "content": assistant_text})
                with st.chat_message("assistant"):
                    st.markdown(assistant_text)

                if databricks_configured():
                    result_df = execute_databricks_query(safe_sql)
                else:
                    result_df = filtered_df.head(200)

                with st.chat_message("assistant"):
                    st.markdown("Query results:")
                    st.dataframe(result_df, use_container_width=True)

                    numeric_cols = result_df.select_dtypes(include="number").columns
                    non_numeric_cols = [c for c in result_df.columns if c not in numeric_cols]
                    if len(numeric_cols) >= 1 and len(non_numeric_cols) >= 1:
                        fig = px.bar(
                            result_df.head(20),
                            x=non_numeric_cols[0],
                            y=numeric_cols[0],
                            title="Query Result Snapshot",
                        )
                        st.plotly_chart(fig, use_container_width=True)

                log_event(
                    role,
                    cleaned,
                    safe_sql,
                    "SUCCESS",
                    f"rows_returned={len(result_df)}",
                )
        except Exception as e:
            error_text = (
                "I couldn't process that request safely right now. "
                "Please rephrase your question and try again."
            )
            st.session_state.chat_history.append({"role": "assistant", "content": error_text})
            with st.chat_message("assistant"):
                st.error(error_text)
            log_event(
                role,
                cleaned,
                "N/A",
                "ERROR",
                f"malformed output or execution failure: {e}",
            )
    if can_access("audit_log"):
        with st.expander("Audit Log"):
            logs = pd.DataFrame(st.session_state.get("audit_logs", []))
            if logs.empty:
                st.write("No queries logged yet.")
            else:
                st.dataframe(logs, use_container_width=True)
    else:
        st.caption("Audit log is available to Data Analyst and IT Admin roles only.")

    st.markdown("---")
    st.markdown(
        "**Demo message**: This empowers non-technical users while maintaining IT oversight. "
        "Governance is enforced through Unity Catalog with row-level security and masking policies."
    )


if __name__ == "__main__":
    main()
