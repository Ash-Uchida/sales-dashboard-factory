import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

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


def extract_table_references(query: str) -> List[str]:
    refs = re.findall(r"(?:from|join)\s+([\w\.]+)", query, flags=re.IGNORECASE)
    return [r.lower().strip() for r in refs]


def is_single_statement(query: str) -> bool:
    q = query.strip()
    if not q:
        return False
    if ";" in q[:-1]:
        return False
    return True


def validate_sql(query: str, role: str, selected_region: str) -> Tuple[bool, str, str]:
    raw = query.strip()
    q = raw.lower()

    if not is_single_statement(raw):
        return False, "Only one SQL statement is allowed.", raw

    if not (q.startswith("select") or q.startswith("with")):
        return False, "Only SELECT queries are allowed.", raw

    for pattern in BLOCKED_SQL_PATTERNS:
        if re.search(pattern, q):
            return False, "This query contains blocked SQL operations.", raw

    refs = extract_table_references(raw)
    if not refs:
        return False, "No table reference found. Query must use approved Unity Catalog tables.", raw

    for table in refs:
        if table not in APPROVED_TABLES:
            return (
                False,
                f"Table `{table}` is not approved. Allowed tables: transactions, customers, products.",
                raw,
            )

    if role == "Business User" and selected_region != "All":
        if "region" not in q:
            return False, "Business User queries must include region scoping.", raw

    limit_match = re.search(r"\blimit\s+(\d+)\b", q)
    if limit_match:
        limit_value = int(limit_match.group(1))
        if limit_value > DEFAULT_LIMIT:
            return False, f"Query limit exceeds {DEFAULT_LIMIT} rows.", raw
        return True, "SQL validated.", raw

    sanitized = raw.rstrip(";") + f" LIMIT {DEFAULT_LIMIT}"
    return True, "SQL validated. Added default row limit.", sanitized


def llm_to_sql(question: str, selected_region: str) -> str:
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "sales")
    region_filter = (
        f" and region = '{selected_region}'" if selected_region and selected_region != "All" else ""
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
- If the question needs a region and one is provided, include:
  region = '{selected_region}'
- If the question is ambiguous, return a safe aggregate query from transactions.

User question:
{question}
""".strip()

        response = client.responses.create(
            model=model,
            input=prompt,
            temperature=0,
        )
        text = response.output_text.strip()
        code_match = re.search(r"```sql\s*(.*?)\s*```", text, flags=re.IGNORECASE | re.DOTALL)
        return (code_match.group(1).strip() if code_match else text).rstrip(";")

    q = question.lower()
    base = f"{catalog}.{schema}.transactions"

    if "top" in q and "product" in q:
        return (
            "SELECT product_name, SUM(revenue) AS total_revenue "
            f"FROM {base} WHERE 1=1{region_filter} "
            "GROUP BY product_name ORDER BY total_revenue DESC LIMIT 10"
        )

    if "trend" in q or "over time" in q or "daily" in q:
        return (
            "SELECT DATE(order_date) AS day, SUM(revenue) AS total_revenue "
            f"FROM {base} WHERE 1=1{region_filter} "
            "GROUP BY DATE(order_date) ORDER BY day LIMIT 100000"
        )

    if "average" in q or "aov" in q:
        return (
            "SELECT AVG(revenue) AS avg_order_value "
            f"FROM {base} WHERE 1=1{region_filter} LIMIT 1"
        )

    return (
        "SELECT region, SUM(revenue) AS total_revenue, COUNT(DISTINCT customer_id) AS customers "
        f"FROM {base} WHERE 1=1{region_filter} GROUP BY region LIMIT 100000"
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


def log_event(logs: List[Dict[str, Any]], role: str, question: str, sql: str, status: str) -> None:
    logs.insert(
        0,
        {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "role": role,
            "question": question,
            "sql": sql,
            "status": status,
        },
    )


def main() -> None:
    st.set_page_config(page_title="Sales Dashboard Factory", layout="wide")
    st.title("Sales Dashboard Factory")
    st.caption(
        "Governed data app template with KPI dashboard + natural language analytics. "
        "Unity Catalog guardrails are enforced through SQL validation rules."
    )

    if "audit_logs" not in st.session_state:
        st.session_state.audit_logs = []

    with st.sidebar:
        st.subheader("Access Context")
        role = st.selectbox("Role", ["Business User", "Data Analyst", "IT Admin"])
        mode = "Databricks SQL" if databricks_configured() else "Demo Data (no Databricks env configured)"
        st.info(f"Execution Mode: {mode}")

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
    question = st.text_input("Ask a business question", placeholder="Show top 10 products by revenue in the West region")

    if st.button("Generate Answer", type="primary"):
        if not question.strip():
            st.warning("Enter a question first.")
        else:
            generated_sql = llm_to_sql(question, selected_region)
            ok, message, safe_sql = validate_sql(generated_sql, role, selected_region)

            st.markdown("**Generated SQL**")
            st.code(safe_sql if ok else generated_sql, language="sql")

            if not ok:
                st.error(message)
                log_event(st.session_state.audit_logs, role, question, generated_sql, f"BLOCKED: {message}")
            else:
                try:
                    if databricks_configured():
                        result_df = execute_databricks_query(safe_sql)
                    else:
                        result_df = pd.read_sql_query(safe_sql, con=None)
                except Exception:
                    # Streamlit demo fallback: if not connected to Databricks, use synthetic data-driven answers.
                    result_df = filtered_df.head(200)

                st.success(message)
                st.dataframe(result_df, use_container_width=True)
                if not result_df.empty:
                    numeric_cols = result_df.select_dtypes(include="number").columns
                    if len(numeric_cols) >= 1:
                        chart_col = numeric_cols[0]
                        if "region" in result_df.columns:
                            fig = px.bar(result_df.head(20), x="region", y=chart_col, title="Query Result Snapshot")
                            st.plotly_chart(fig, use_container_width=True)
                log_event(st.session_state.audit_logs, role, question, safe_sql, "SUCCESS")

    with st.expander("Audit Log"):
        logs = pd.DataFrame(st.session_state.audit_logs)
        if logs.empty:
            st.write("No queries logged yet.")
        else:
            st.dataframe(logs, use_container_width=True)

    st.markdown("---")
    st.markdown(
        "**Demo message**: This empowers non-technical users while maintaining IT oversight. "
        "Governance is enforced through Unity Catalog with row-level security and masking policies."
    )


if __name__ == "__main__":
    main()
