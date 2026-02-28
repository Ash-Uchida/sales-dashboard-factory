import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

from components.auth import can_access, get_current_role, get_current_store_id, get_current_user, logout, require_login

try:
    from utils import databricks_auth as db_auth
except Exception:
    db_auth = None

try:
    from databricks import sql as dbsql
except Exception:
    dbsql = None

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

load_dotenv()

# Validator compares lowercased table refs; include both forms.
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
    *(s.lower() for s in (
        "main.sales.transactions", "main.sales.customers", "main.sales.products",
        "workspace.sales.transactions", "workspace.sales.customers", "workspace.sales.products",
    )),
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
    "Manager": {
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
    "Manager": 1000,
    "Data Analyst": 100000,
    "IT Admin": 100000,
}


@st.cache_data
def load_demo_data() -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=240, freq="D")
    stores = [("S1", "Store West"), ("S2", "Store East"), ("S3", "Store North"), ("S4", "Store South")]
    products = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"]

    rows = []
    order_id = 1000
    for i, day in enumerate(dates):
        for store_id, _ in stores:
            for p in products:
                qty = 5 + ((i + len(store_id) + len(p)) % 18)
                unit_price = 40 + (len(p) * 6)
                revenue = qty * unit_price
                rows.append(
                    {
                        "order_id": order_id,
                        "order_date": day,
                        "store_id": store_id,
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


def execute_databricks_query(query: str, timeout_seconds: int = 25) -> pd.DataFrame:
    def _run() -> pd.DataFrame:
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

    pool = ThreadPoolExecutor(max_workers=1)
    future = pool.submit(_run)
    try:
        return future.result(timeout=timeout_seconds)
    except FuturesTimeoutError as exc:
        future.cancel()
        pool.shutdown(wait=False, cancel_futures=True)
        raise TimeoutError(
            f"Databricks query timed out after {timeout_seconds} seconds. "
            "Check warehouse state or retry."
        ) from exc
    finally:
        # Avoid blocking shutdown when connector calls stall.
        pool.shutdown(wait=False, cancel_futures=True)


def execute_databricks_statement(query: str, timeout_seconds: int = 25) -> None:
    """Run INSERT/UPDATE etc.; no result set returned."""
    def _run() -> None:
        conn = dbsql.connect(
            server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
        finally:
            conn.close()

    pool = ThreadPoolExecutor(max_workers=1)
    future = pool.submit(_run)
    try:
        future.result(timeout=timeout_seconds)
    except Exception:
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def _escape(s: str) -> str:
    return (s or "").replace("'", "''")


def insert_transaction(
    order_id: str,
    order_date: str,
    store_id: str,
    product_name: str,
    quantity: int,
    unit_price: float,
    customer_id: str,
) -> Tuple[bool, Optional[str]]:
    """Insert one row into workspace.sales.transactions. revenue = quantity * unit_price."""
    if not databricks_configured():
        return False, "Databricks not configured."
    revenue = quantity * unit_price
    table = _transactions_table()
    q = (
        f"INSERT INTO {table} (order_id, order_date, store_id, product_name, quantity, unit_price, revenue, customer_id) "
        f"VALUES ('{_escape(order_id)}', CAST('{order_date}' AS DATE), '{_escape(store_id)}', "
        f"'{_escape(product_name)}', {int(quantity)}, {float(unit_price)}, {float(revenue)}, '{_escape(customer_id)}')"
    )
    try:
        execute_databricks_statement(q)
        return True, None
    except Exception as e:
        return False, str(e)


def insert_customer(customer_id: str, customer_name: str, segment: str, store_id: str) -> Tuple[bool, Optional[str]]:
    """Insert one row into workspace.sales.customers."""
    if not databricks_configured():
        return False, "Databricks not configured."
    table = _sales_table("customers")
    q = (
        f"INSERT INTO {table} (customer_id, customer_name, segment, store_id) "
        f"VALUES ('{_escape(customer_id)}', '{_escape(customer_name)}', '{_escape(segment)}', '{_escape(store_id)}')"
    )
    try:
        execute_databricks_statement(q)
        return True, None
    except Exception as e:
        return False, str(e)


def insert_product(
    product_id: str,
    product_name: str,
    product_price: float,
    category: str,
) -> Tuple[bool, Optional[str]]:
    """Insert one row into workspace.sales.products (product_id, product_name, product_price, category)."""
    if not databricks_configured():
        return False, "Databricks not configured."
    table = _sales_table("products")
    q = (
        f"INSERT INTO {table} (product_id, product_name, product_price, category) "
        f"VALUES ('{_escape(product_id)}', '{_escape(product_name)}', {float(product_price)}, '{_escape(category)}')"
    )
    try:
        execute_databricks_statement(q)
        return True, None
    except Exception as e:
        return False, str(e)


def insert_store(store_id: str, store_name: str, location: str) -> Tuple[bool, Optional[str]]:
    """Insert one row into workspace.admin.stores (Manager-only in app; Databricks can restrict table too)."""
    if not databricks_configured():
        return False, "Databricks not configured."
    table = _admin_table("stores")
    q = (
        f"INSERT INTO {table} (store_id, store_name, location, user_id, created_at) "
        f"VALUES ('{_escape(store_id)}', '{_escape(store_name)}', '{_escape(location)}', NULL, CURRENT_TIMESTAMP)"
    )
    try:
        execute_databricks_statement(q)
        return True, None
    except Exception as e:
        return False, str(e)


def normalize_sql(query: str) -> str:
    return re.sub(r"\s+", " ", query).strip()


def _sales_table(name: str) -> str:
    """Fully qualified sales table from env (e.g. transactions, customers, products)."""
    catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
    schema = os.getenv("DATABRICKS_SCHEMA", "sales")
    return f"{catalog}.{schema}.{name}"


def _transactions_table() -> str:
    return _sales_table("transactions")


def _admin_table(name: str) -> str:
    """Fully qualified admin table (e.g. stores) from env."""
    catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
    schema = os.getenv("DATABRICKS_SCHEMA_ADMIN", "admin")
    return f"{catalog}.{schema}.{name}"


@st.cache_data(ttl=120)
def get_databricks_date_range() -> Tuple[Optional[datetime], Optional[datetime]]:
    """Return (min_date, max_date) from transactions in Databricks. Returns (None, None) if empty or error."""
    try:
        table = _transactions_table()
        df = execute_databricks_query(
            f"SELECT MIN(order_date) AS min_d, MAX(order_date) AS max_d FROM {table}"
        )
        if df.empty or pd.isna(df.at[0, "min_d"]) or pd.isna(df.at[0, "max_d"]):
            return None, None
        min_d, max_d = df.at[0, "min_d"], df.at[0, "max_d"]
        if hasattr(min_d, "date"):
            min_d, max_d = min_d.date(), max_d.date()
        return min_d, max_d
    except Exception:
        return None, None


@st.cache_data(ttl=60)
def get_databricks_products() -> List[Dict[str, Any]]:
    """Return list of {product_id, product_name, product_price, category} from workspace.sales.products."""
    if not databricks_configured():
        return []
    try:
        table = _sales_table("products")
        df = execute_databricks_query(
            f"SELECT product_id, product_name, product_price, category FROM {table} ORDER BY product_name"
        )
        if df.empty:
            return []
        df["product_price"] = pd.to_numeric(df["product_price"], errors="coerce").fillna(0.0)
        return df.to_dict("records")
    except Exception:
        return []


def get_databricks_stores() -> List[Tuple[str, str]]:
    """Return [(store_id, store_name), ...] for filter dropdown; first entry is ('All', 'All')."""
    out = [("All", "All")]
    if not databricks_configured():
        return out + [("S1", "Store West"), ("S2", "Store East"), ("S3", "Store North"), ("S4", "Store South")]
    try:
        if db_auth and db_auth.databricks_auth_configured():
            stores = db_auth.get_stores()
            if stores:
                for s in stores:
                    out.append((str(s.get("store_id", "")), str(s.get("store_name", s.get("store_id", "?")))))
                return out
        table = _transactions_table()
        df = execute_databricks_query(
            f"SELECT DISTINCT store_id FROM {table} WHERE store_id IS NOT NULL ORDER BY store_id"
        )
        if not df.empty and "store_id" in df.columns:
            for sid in df["store_id"].astype(str).str.strip():
                if sid and sid != "None":
                    out.append((sid, sid))
    except Exception:
        pass
    if len(out) == 1:
        out += [("S1", "Store 1"), ("S2", "Store 2")]
    return out


@st.cache_data(ttl=120)
def get_databricks_regions() -> List[str]:
    """Return ['All'] + distinct regions from transactions (legacy). Prefer get_databricks_stores()."""
    try:
        table = _transactions_table()
        df = execute_databricks_query(
            f"SELECT DISTINCT region FROM {table} ORDER BY region"
        )
        if df.empty or "region" not in df.columns:
            return ["All"]
        return ["All"] + df["region"].astype(str).str.strip().tolist()
    except Exception:
        return ["All", "West", "East", "North", "South"]


def load_dashboard_data_from_databricks(
    start_date: datetime,
    end_date: datetime,
    selected_store_id: str,
) -> pd.DataFrame:
    """Load transactions from Databricks with date and optional store filter."""
    table = _transactions_table()
    s, e = start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    where = f"order_date BETWEEN CAST('{s}' AS DATE) AND CAST('{e}' AS DATE)"
    if selected_store_id and selected_store_id != "All":
        safe_id = str(selected_store_id).replace("'", "''")
        where += f" AND store_id = '{safe_id}'"
    query = f"SELECT * FROM {table} WHERE {where} LIMIT {DEFAULT_LIMIT}"
    return execute_databricks_query(query)


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


def validate_sql(query: str, role: str, selected_store_id: str) -> Tuple[bool, str, str]:
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

    if role == "Business User" and selected_store_id and selected_store_id != "All":
        store_pattern = rf"\bstore_id\s*=\s*'{re.escape(selected_store_id)}'"
        if not re.search(store_pattern, q):
            return False, f"Business User queries must be scoped to store_id '{selected_store_id}'.", raw

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


def llm_to_sql(question: str, selected_store_id: str) -> str:
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "sales")
    safe_store = (selected_store_id or "").replace("'", "''")
    store_filter = (
        f" AND store_id = '{safe_store}'"
        if selected_store_id and selected_store_id != "All"
        else ""
    )
    store_rule = (
        f"- selected store_id is '{selected_store_id}'. You must include `store_id = '{selected_store_id}'` in SQL."
        if selected_store_id and selected_store_id != "All"
        else "- selected store is 'All'. Do not force a specific store filter unless user asks."
    )

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if OpenAI is not None and api_key:
        try:
            client = OpenAI(api_key=api_key)
            model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            prompt = f"""
You are a SQL generator for Databricks.
Return only SQL. No markdown. No explanation.

Rules:
- Only SELECT statements.
- Allowed tables only:
  - workspace.sales.transactions(order_id, order_date, store_id, product_name, quantity, unit_price, revenue, customer_id)
  - workspace.sales.customers(customer_id, customer_name, segment, region)
  - workspace.sales.products(product_name, category)
- Never use INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, MERGE, GRANT, REVOKE.
- Always include LIMIT 1000 or less.
- Prefer simple, readable SQL.
- Respect selected store context:
{store_rule}
- If the question is ambiguous, return this exact safe query pattern:
  SELECT DATE(order_date) AS day, SUM(revenue) AS total_revenue
  FROM workspace.sales.transactions
  WHERE 1=1
  GROUP BY DATE(order_date)
  ORDER BY day DESC
  LIMIT 1000
- If user asks "last month", filter to previous calendar month.
- If user asks "average", return AVG(revenue) AS avg_order_value.
- Use store_id (not region) for store-level filtering and grouping.

User question:
{question}
""".strip()
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            if response.choices and response.choices[0].message.content:
                text = response.choices[0].message.content.strip()
                candidate_sql = clean_sql_output(text)
                return candidate_sql.rstrip(";")
        except Exception:
            pass  # Fall back to rule-based SQL (e.g. 429 quota, network, or empty response)

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
            f"{store_filter} LIMIT 1"
        )

    if is_last_month and ("revenue" in q or "sales" in q):
        return (
            "SELECT SUM(revenue) AS total_revenue "
            f"FROM {base} "
            "WHERE DATE_TRUNC('month', order_date) = DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -1))"
            f"{store_filter} LIMIT 1"
        )

    if "customer" in q and "segment" in q:
        customers_table = f"{catalog}.{schema}.customers"
        return (
            "SELECT segment, COUNT(DISTINCT customer_id) AS customers "
            f"FROM {customers_table} "
            f"WHERE 1=1{store_filter} "
            "GROUP BY segment ORDER BY customers DESC LIMIT 1000"
        )

    if "join" in q and "customer_id" in q:
        customers_table = f"{catalog}.{schema}.customers"
        return (
            "SELECT c.segment, SUM(t.revenue) AS total_revenue "
            f"FROM {base} t JOIN {customers_table} c ON t.customer_id = c.customer_id "
            f"WHERE 1=1{store_filter} "
            "GROUP BY c.segment ORDER BY total_revenue DESC LIMIT 1000"
        )

    if "top" in q and "product" in q:
        return (
            "SELECT product_name, SUM(revenue) AS total_revenue "
            f"FROM {base} WHERE 1=1{store_filter} "
            f"GROUP BY product_name ORDER BY total_revenue DESC LIMIT {top_n}"
        )

    if "trend" in q or "over time" in q or "daily" in q:
        return (
            "SELECT DATE(order_date) AS day, SUM(revenue) AS total_revenue "
            f"FROM {base} WHERE 1=1{store_filter} "
            "GROUP BY DATE(order_date) ORDER BY day LIMIT 1000"
        )

    if is_average or "aov" in q:
        return (
            "SELECT AVG(revenue) AS avg_order_value "
            f"FROM {base} WHERE 1=1{store_filter} LIMIT 1"
        )

    return (
        "SELECT store_id, SUM(revenue) AS total_revenue, COUNT(DISTINCT customer_id) AS customers "
        f"FROM {base} WHERE 1=1{store_filter} GROUP BY store_id LIMIT 1000"
    )


def apply_filters(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    selected_store_id: str,
) -> pd.DataFrame:
    filtered = df[(df["order_date"] >= start_date) & (df["order_date"] <= end_date)].copy()
    if selected_store_id and selected_store_id != "All" and "store_id" in filtered.columns:
        filtered = filtered[filtered["store_id"] == selected_store_id]
    elif selected_store_id and selected_store_id != "All" and "region" in filtered.columns:
        filtered = filtered[filtered["region"] == selected_store_id]
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
        return ["No data in selected range — adjust date or store filters."]
    bullets = []
    group_col = "store_id" if "store_id" in df.columns else "region"
    if group_col in df.columns:
        by_store = df.groupby(group_col, as_index=False)["revenue"].sum()
        if not by_store.empty:
            top = by_store.loc[by_store["revenue"].idxmax()]
            label = str(top[group_col])
            bullets.append(f"**{label}** is the top store by revenue in the selected period (${top['revenue']:,.0f}).")
    by_product = df.groupby("product_name", as_index=False)["revenue"].sum().sort_values("revenue", ascending=False)
    if "product_name" in df.columns and len(by_product) >= 2:
        top2 = by_product.head(2)
        top2_rev = top2["revenue"].sum()
        total_rev = df["revenue"].sum()
        pct = (top2_rev / total_rev * 100) if total_rev else 0
        names = ", ".join(top2["product_name"].tolist())
        bullets.append(f"**{names}** drive {pct:.0f}% of revenue; consider promotion in underperforming stores.")
    aov = df["revenue"].sum() / df["order_id"].nunique() if df["order_id"].nunique() else 0
    bullets.append(f"Average order value in selection: **${aov:,.2f}** — use chat to compare across stores.")
    return bullets[:3]


def render_charts(df: pd.DataFrame) -> None:
    if df.empty:
        st.warning("No data available for selected filters.")
        return

    # Build an explicit day column so Plotly always has a stable x-axis field.
    day_df = df.copy()
    day_df["order_day"] = pd.to_datetime(day_df["order_date"]).dt.date
    by_day = day_df.groupby("order_day", as_index=False)["revenue"].sum()
    group_col = "store_id" if "store_id" in df.columns else "region"
    by_store = df.groupby(group_col, as_index=False)["revenue"].sum() if group_col in df.columns else pd.DataFrame()
    by_product = (
        df.groupby("product_name", as_index=False)["revenue"].sum().sort_values("revenue", ascending=False)
        if "product_name" in df.columns
        else pd.DataFrame()
    )

    col1, col2 = st.columns(2)
    with col1:
        fig_line = px.line(by_day, x="order_day", y="revenue", title="Revenue Trend")
        st.plotly_chart(fig_line, use_container_width=True)

    with col2:
        if not by_store.empty:
            fig_bar = px.bar(by_store, x=group_col, y="revenue", title="Revenue by Store")
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.caption("No store breakdown (add store_id to data for Revenue by Store).")

    if not by_product.empty:
        fig_pie = px.pie(by_product.head(8), names="product_name", values="revenue", title="Revenue Mix by Product")
        st.plotly_chart(fig_pie, use_container_width=True)


def build_demo_fallback_result(sql_text: str, df: pd.DataFrame) -> pd.DataFrame:
    q = (sql_text or "").lower()
    if df.empty:
        return pd.DataFrame()

    group_col = "store_id" if "store_id" in df.columns else "region"
    if ("group by store_id" in q or "group by region" in q) and "sum(revenue)" in q and group_col in df.columns:
        return (
            df.groupby(group_col, as_index=False)
            .agg(total_revenue=("revenue", "sum"), customers=("customer_id", "nunique"))
            .sort_values("total_revenue", ascending=False)
        )

    if "group by product_name" in q and "sum(revenue)" in q:
        limit_match = re.search(r"\blimit\s+(\d+)\b", q)
        limit_n = int(limit_match.group(1)) if limit_match else 10
        return (
            df.groupby("product_name", as_index=False)["revenue"]
            .sum()
            .rename(columns={"revenue": "total_revenue"})
            .sort_values("total_revenue", ascending=False)
            .head(limit_n)
        )

    if "avg(revenue)" in q:
        return pd.DataFrame(
            [{"avg_order_value": float(df["revenue"].mean()) if not df.empty else 0.0}]
        )

    if "sum(revenue)" in q and "group by" not in q:
        return pd.DataFrame(
            [{"total_revenue": float(df["revenue"].sum()) if not df.empty else 0.0}]
        )

    return df.head(200).copy()


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


def _render_add_transaction_page() -> None:
    st.subheader("Add Transaction")
    st.caption("Insert a new row into Databricks **transactions**. Order ID is auto-generated.")
    if not databricks_configured():
        st.warning("Databricks is not configured. Set .env and connect to save data.")
        return

    products_list = get_databricks_products()
    store_options = [(s[0], s[1]) for s in get_databricks_stores() if s[0] != "All"]
    # Business User: only their store
    _role, _user_store = get_current_role(), get_current_store_id()
    if _role == "Business User" and _user_store:
        store_options = [(s[0], s[1]) for s in store_options if s[0] == _user_store]
        if not store_options:
            store_options = [(_user_store, f"Your store ({_user_store})")]
    store_names = [s[1] for s in store_options]
    product_names = [p.get("product_name") or "" for p in products_list if p.get("product_name")]

    if not product_names:
        st.warning("No products in catalog. Add products first from **Add Product**.")
    if not store_names:
        st.warning("No stores configured. Add stores in Databricks workspace.admin.stores or enter Store ID below.")

    col1, col2 = st.columns(2)
    with col1:
        order_date = st.date_input("Order Date", value=datetime.now().date())
        selected_product_name = st.selectbox(
            "Product",
            options=product_names,
            key="tx_product",
        ) if product_names else None
        quantity = st.number_input("Quantity", min_value=1, value=1, key="tx_qty")

    unit_price = 0.0
    if selected_product_name and products_list:
        for p in products_list:
            if (p.get("product_name") or "") == selected_product_name:
                unit_price = float(p.get("product_price") or 0)
                break
    total = quantity * unit_price

    with col2:
        if store_names:
            _disabled = (_role == "Business User" and _user_store and len(store_options) == 1)
            selected_store_name = st.selectbox("Store", options=store_names, key="tx_store", disabled=_disabled)
            store_id = next((s[0] for s in store_options if s[1] == selected_store_name), store_options[0][0] if store_options else None)
        else:
            store_id = st.text_input("Store ID", placeholder="e.g. S1", key="tx_store_id")
        customer_id = st.text_input("Customer ID", placeholder="e.g. C001", key="tx_customer")

    st.divider()
    st.caption("Unit price comes from the selected product; total = quantity × unit price.")
    r1, r2, r3 = st.columns(3)
    with r1:
        st.metric("Unit Price", f"${unit_price:,.2f}")
    with r2:
        st.metric("Quantity", quantity)
    with r3:
        st.metric("Total", f"${total:,.2f}")

    if st.button("Save to Databricks", type="primary", key="tx_save"):
        if not selected_product_name and not product_names:
            st.error("Select a product (or add products first).")
        elif not customer_id or not customer_id.strip():
            st.error("Customer ID is required.")
        else:
            order_id = "ORD-" + uuid.uuid4().hex[:12].upper()
            sid = store_id if isinstance(store_id, str) else (store_options[0][0] if store_options else "")
            ok, err = insert_transaction(
                order_id,
                order_date.strftime("%Y-%m-%d"),
                sid,
                selected_product_name or "",
                int(quantity),
                float(unit_price),
                customer_id.strip(),
            )
            if ok:
                st.success(f"Transaction saved. Order ID: **{order_id}**")
            else:
                st.error("Failed to save.")
                if err:
                    st.code(err, language="text")


def _render_add_customer_page() -> None:
    st.subheader("Add Customer")
    st.caption("Insert a new row into Databricks **customers** (workspace.sales.customers).")
    if not databricks_configured():
        st.warning("Databricks is not configured. Set .env and connect to save data.")
        return
    store_options = [(s[0], s[1]) for s in get_databricks_stores() if s[0] != "All"]
    # Business User: only their store
    _role, _user_store = get_current_role(), get_current_store_id()
    if _role == "Business User" and _user_store:
        store_options = [(s[0], s[1]) for s in store_options if s[0] == _user_store]
        if not store_options:
            store_options = [(_user_store, f"Your store ({_user_store})")]
    store_names = [s[1] for s in store_options]
    with st.form("add_customer_form"):
        customer_id = st.text_input("Customer ID", placeholder="e.g. C001")
        customer_name = st.text_input("Customer Name", placeholder="e.g. Acme Corp")
        segment = st.text_input("Segment", placeholder="e.g. Enterprise")
        if store_names:
            _disabled_cust = (_role == "Business User" and _user_store and len(store_options) == 1)
            selected_store_name = st.selectbox("Store", options=store_names, disabled=_disabled_cust)
            store_id = next((s[0] for s in store_options if s[1] == selected_store_name), store_options[0][0] if store_options else "")
        else:
            store_id = st.text_input("Store ID", placeholder="e.g. S1")
        submitted = st.form_submit_button("Save to Databricks")
    if submitted:
        if not customer_id:
            st.error("Customer ID is required.")
        else:
            sid = store_id if isinstance(store_id, str) else (store_options[0][0] if store_options else "")
            ok, err = insert_customer(
                customer_id.strip(),
                (customer_name or "").strip(),
                (segment or "").strip(),
                sid,
            )
            if ok:
                st.success("Customer saved to Databricks.")
            else:
                st.error("Failed to save.")
                if err:
                    st.code(err, language="text")


def _render_add_product_page() -> None:
    st.subheader("Add Product")
    st.caption("Insert a new row into Databricks **products** (product_id, product_name, product_price, category).")
    if not databricks_configured():
        st.warning("Databricks is not configured. Set .env and connect to save data.")
        return
    with st.form("add_product_form"):
        product_id = st.text_input("Product ID", placeholder="e.g. P001")
        product_name = st.text_input("Product Name", placeholder="e.g. Alpha")
        product_price = st.number_input("Product Price", min_value=0.0, value=0.0, format="%.2f", step=0.01)
        category = st.text_input("Category", placeholder="e.g. Electronics")
        submitted = st.form_submit_button("Save to Databricks")
    if submitted:
        if not product_id or not product_name:
            st.error("Product ID and Product Name are required.")
        else:
            ok, err = insert_product(
                product_id.strip(),
                product_name.strip(),
                float(product_price),
                (category or "").strip(),
            )
            if ok:
                st.success("Product saved to Databricks.")
            else:
                st.error("Failed to save.")
                if err:
                    st.code(err, language="text")


def _render_add_store_page() -> None:
    """Add Store page: only visible and submittable by Manager role (enforced in code; optional in Databricks)."""
    if not can_access("add_store"):
        st.subheader("Add Store")
        st.warning("Only **Manager** role can add stores. Your role does not have access.")
        return
    st.subheader("Add Store")
    st.caption("Insert a new store into Databricks **admin.stores** (Manager only).")
    if not databricks_configured():
        st.warning("Databricks is not configured. Set .env and connect to save data.")
        return
    with st.form("add_store_form"):
        store_id = st.text_input("Store ID", placeholder="e.g. S5")
        store_name = st.text_input("Store Name", placeholder="e.g. Store Central")
        location = st.text_input("Location", placeholder="e.g. Salt Lake City")
        submitted = st.form_submit_button("Save to Databricks")
    if submitted:
        if not store_id or not store_name:
            st.error("Store ID and Store Name are required.")
        else:
            ok, err = insert_store(
                store_id.strip(),
                store_name.strip(),
                (location or "").strip(),
            )
            if ok:
                st.success("Store saved to Databricks.")
            else:
                st.error("Failed to save.")
                if err:
                    st.code(err, language="text")


def _render_user_management_page() -> None:
    """User management: list users and create new users (set password). IT Admin only."""
    if not can_access("user_management"):
        st.subheader("User management")
        st.warning("Only **IT Admin** can access User management (create users and set passwords).")
        return
    st.subheader("User management")
    st.caption("List users, reset passwords, and create new accounts. Only IT Admin can access this page.")
    if not db_auth or not db_auth.databricks_auth_configured():
        st.warning("Databricks auth is not configured. Set .env and connect to list or create users.")
        return
    users = db_auth.list_users()
    if users:
        st.caption(f"**{len(users)}** user(s)")
        df = pd.DataFrame(users)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.markdown("---")
        with st.expander("Reset password (for user who forgot password)"):
            st.caption("Set a new password for an existing user. Share the new password with them manually.")
            with st.form("user_mgmt_reset_pwd"):
                usernames = [u["username"] for u in users]
                reset_username = st.selectbox("User", usernames, key="reset_username")
                new_password = st.text_input("New password", type="password", placeholder="••••••••", key="reset_pwd")
                new_confirm = st.text_input("Confirm new password", type="password", placeholder="••••••••", key="reset_confirm")
                reset_submitted = st.form_submit_button("Reset password")
            if reset_submitted:
                if not new_password:
                    st.error("Enter a new password.")
                elif new_password != new_confirm:
                    st.error("Passwords do not match.")
                else:
                    ok, err_msg = db_auth.reset_user_password(reset_username, new_password)
                    if ok:
                        st.success(f"Password updated for **{reset_username}**. They can log in with the new password.")
                        st.rerun()
                    else:
                        st.error("Failed to reset password.")
                        if err_msg:
                            st.code(err_msg, language="text")
    else:
        st.info("No users in Databricks yet. Create the first user below (e.g. an IT Admin).")
    st.markdown("---")
    st.caption("Create new user (password is required and stored securely as hash)")
    with st.form("user_mgmt_create"):
        firstname = st.text_input("First name", placeholder="e.g. Jane")
        lastname = st.text_input("Last name", placeholder="e.g. Smith")
        username = st.text_input("Username", placeholder="e.g. jane.smith")
        password = st.text_input("Password", type="password", placeholder="••••••••")
        confirm = st.text_input("Confirm password", type="password", placeholder="••••••••")
        roles_list = db_auth.get_roles()
        if roles_list:
            role_options = [r["role_name"] for r in roles_list]
            role_ids = [r["role_id"] for r in roles_list]
            role_display = st.selectbox("Role", role_options)
            role_id = role_ids[role_options.index(role_display)] if role_display in role_options else role_ids[0]
        else:
            role_display = "Business User"
            role_id = "role_business"
        stores_list = db_auth.get_stores()
        if stores_list:
            store_options = ["— None —"] + [s["store_name"] for s in stores_list]
            store_ids = [None] + [s["store_id"] for s in stores_list]
            store_display = st.selectbox("Store (optional)", store_options)
            store_id = store_ids[store_options.index(store_display)] if store_display in store_options else None
        else:
            store_id = None
        submitted = st.form_submit_button("Create user")
    if submitted:
        firstname = (firstname or "").strip()
        lastname = (lastname or "").strip()
        username = (username or "").strip()
        if not username:
            st.error("Username is required.")
        elif not password:
            st.error("Password is required.")
        elif password != confirm:
            st.error("Passwords do not match.")
        elif db_auth.username_exists(username):
            st.error("That username is already taken.")
        else:
            ok, err_msg = db_auth.register_user(firstname, lastname, username, password, role_id, store_id)
            if ok:
                st.success(f"User **{username}** created. They can log in with that password.")
                st.rerun()
            else:
                st.error("Failed to create user.")
                if err_msg:
                    st.code(err_msg, language="text")


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
    if "latest_query_result" not in st.session_state:
        st.session_state["latest_query_result"] = None
    if "latest_query_sql" not in st.session_state:
        st.session_state["latest_query_sql"] = None

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
        if st.button("Add Transaction", use_container_width=True):
            st.session_state["view_mode"] = "Add Transaction"
        if st.button("Add Customer", use_container_width=True):
            st.session_state["view_mode"] = "Add Customer"
        if st.button("Add Product", use_container_width=True):
            st.session_state["view_mode"] = "Add Product"
        if can_access("add_store"):
            if st.button("Add Store", use_container_width=True):
                st.session_state["view_mode"] = "Add Store"
        elif st.session_state.get("view_mode") == "Add Store":
            st.session_state["view_mode"] = "Dashboard"
        if can_access("user_management"):
            if st.button("User management", use_container_width=True):
                st.session_state["view_mode"] = "User management"
        elif st.session_state.get("view_mode") == "User management":
            st.session_state["view_mode"] = "Dashboard"
        if can_access("audit_log"):
            if st.button("Audit Log", use_container_width=True):
                st.session_state["view_mode"] = "Audit Log"
        elif st.session_state.get("view_mode") == "Audit Log":
            st.session_state["view_mode"] = "Dashboard"
        view = st.session_state["view_mode"]
        st.caption(f"Current: {view}")
        mode = "Databricks SQL" if databricks_configured() else "Demo Data (no Databricks env configured)"
        st.caption(f"Execution: {mode}")
        show_insights = st.checkbox("Show AI Insights", value=True)

    if view == "Audit Log":
        st.subheader("Audit Log")
        logs = pd.DataFrame(st.session_state.get("audit_logs", []))
        st.caption(f"Entries: {len(logs)}")
        if st.button("Clear Audit Log"):
            st.session_state["audit_logs"] = []
            st.rerun()
        if logs.empty:
            st.info("No queries logged yet.")
        else:
            st.dataframe(logs, use_container_width=True)
        return

    if view == "Add Transaction":
        _render_add_transaction_page()
        return
    if view == "Add Customer":
        _render_add_customer_page()
        return
    if view == "Add Product":
        _render_add_product_page()
        return
    if view == "Add Store":
        _render_add_store_page()
        return
    if view == "User management":
        _render_user_management_page()
        return

    # Dashboard data: Databricks when configured, else demo data
    if databricks_configured():
        db_min, db_max = get_databricks_date_range()
        if db_min is not None and db_max is not None:
            min_date, max_date = db_min, db_max
        else:
            from datetime import date, timedelta
            today = date.today()
            min_date = today - timedelta(days=365)
            max_date = today
        store_options = get_databricks_stores()
    else:
        demo_df = load_demo_data()
        min_date = demo_df["order_date"].min().date()
        max_date = demo_df["order_date"].max().date()
        store_options = get_databricks_stores()

    # Business User: restrict to their assigned store only (row-level access)
    user_store_id = get_current_store_id()
    if role == "Business User" and user_store_id:
        store_options = [(s[0], s[1]) for s in store_options if s[0] == user_store_id]
        if not store_options:
            store_options = [(user_store_id, f"Your store ({user_store_id})")]
        selected_store_id = user_store_id
        store_display_names = [s[1] for s in store_options]
        c1, c2, c3 = st.columns(3)
        with c1:
            start_date = st.date_input("Start Date", min_date)
        with c2:
            end_date = st.date_input("End Date", max_date)
        with c3:
            st.selectbox("Store", store_display_names, disabled=True, key="store_bu_locked")
    else:
        store_display_names = [s[1] for s in store_options]
        c1, c2, c3 = st.columns(3)
        with c1:
            start_date = st.date_input("Start Date", min_date)
        with c2:
            end_date = st.date_input("End Date", max_date)
        with c3:
            selected_store_label = st.selectbox("Store", store_display_names)
            selected_store_id = store_options[store_display_names.index(selected_store_label)][0] if selected_store_label in store_display_names else "All"

    if databricks_configured():
        try:
            filtered_df = load_dashboard_data_from_databricks(
                pd.to_datetime(start_date),
                pd.to_datetime(end_date),
                selected_store_id,
            )
        except Exception as e:
            st.error(f"Could not load data from Databricks: {e}")
            filtered_df = pd.DataFrame()
    else:
        filtered_df = apply_filters(
            load_demo_data(),
            pd.to_datetime(start_date),
            pd.to_datetime(end_date),
            selected_store_id,
        )

    st.subheader("KPI Dashboard")
    render_kpis(filtered_df)
    render_charts(filtered_df)

    # Actionable insights (from governed data only; does not bypass SQL validation)
    insight_bullets = get_insight_bullets(filtered_df)
    if show_insights and insight_bullets:
        with st.expander("AI-generated insights (from this view)", expanded=True):
            for b in insight_bullets:
                st.markdown(f"- {b}")

    # Data lineage (governance requirement: demonstrate where data comes from)
    with st.expander("Where does this data come from?", expanded=False):
        st.markdown("""
        **Governed sources (Unity Catalog)**  
        - **Catalog:** `workspace` · **Schema:** `sales`

        **Tables used for this dashboard:**
        - `workspace.sales.transactions` — order_id, order_date, store_id, product_name, quantity, unit_price, revenue, customer_id
        - `workspace.sales.customers` — customer_id, customer_name, segment, region
        - `workspace.sales.products` — product_name, category

        **KPI formulas (reproducible):**
        - **Revenue:** `SUM(revenue)` from transactions, with date and store filters applied.
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
        # Always write an initial audit row so in-flight requests are visible.
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

        generated_sql = ""
        safe_sql = ""
        try:
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    time.sleep(1.2)

            generated_sql = llm_to_sql(cleaned, selected_store_id)

            ok, message, safe_sql = validate_sql(generated_sql, role, selected_store_id)
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
                assistant_text = f"""I can help with sales trends, stores, top products, and KPIs.

Generated SQL:
```sql
{safe_sql}
```"""
                st.session_state.chat_history.append({"role": "assistant", "content": assistant_text})
                with st.chat_message("assistant"):
                    st.markdown(assistant_text)

                chat_execution_mode = os.getenv("CHAT_EXECUTION_MODE", "demo").strip().lower()
                use_databricks_for_chat = databricks_configured() and chat_execution_mode == "databricks"

                if use_databricks_for_chat:
                    try:
                        result_df = execute_databricks_query(safe_sql)
                    except Exception as db_err:
                        result_df = build_demo_fallback_result(safe_sql, filtered_df)
                        with st.chat_message("assistant"):
                            st.info(
                                "Databricks query execution is slow/unavailable right now. "
                                "Showing demo fallback result so you can continue."
                            )
                        log_event(
                            role,
                            cleaned,
                            safe_sql,
                            "SUCCESS",
                            f"fallback_demo_result (db_error={str(db_err)[:120]}) rows_returned={len(result_df)}",
                        )
                else:
                    result_df = build_demo_fallback_result(safe_sql, filtered_df)
                st.session_state["latest_query_result"] = result_df
                st.session_state["latest_query_sql"] = safe_sql

                with st.chat_message("assistant"):
                    st.markdown("Query results:")
                    if result_df.empty:
                        st.info(
                            "No rows returned for this query with current filters. "
                            "Try changing date range, store, or question."
                        )
                    else:
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

                existing = st.session_state.get("audit_logs", [])
                already_logged_fallback = (
                    len(existing) > 0
                    and existing[0].get("status") == "SUCCESS"
                    and "fallback_demo_result" in str(existing[0].get("outcome", ""))
                    and existing[0].get("question") == cleaned
                )
                if not already_logged_fallback:
                    log_event(
                        role,
                        cleaned,
                        safe_sql,
                        "SUCCESS",
                        "empty_result_set" if result_df.empty else f"rows_returned={len(result_df)}",
                    )
        except Exception as e:
            error_detail = str(e)
            if "429" in error_detail or "insufficient_quota" in error_detail.lower():
                error_text = (
                    "**OpenAI API quota exceeded.** Your API key has hit its usage or billing limit. "
                    "Check [OpenAI usage and billing](https://platform.openai.com/account/usage) or add payment method. "
                    "To use chat without OpenAI, remove `OPENAI_API_KEY` from your `.env` — the app will use built-in rules instead."
                )
            else:
                error_text = (
                    "I couldn't process that request safely right now. "
                    "Please rephrase your question and try again."
                )
            st.session_state.chat_history.append({"role": "assistant", "content": error_text})
            with st.chat_message("assistant"):
                st.error(error_text)
                with st.expander("Technical details (for debugging)"):
                    st.code(error_detail, language="text")
            log_event(
                role,
                cleaned,
                safe_sql or generated_sql or "N/A",
                "ERROR",
                f"malformed output or execution failure: {e}",
            )

    st.markdown("**Latest Query Result**")
    latest_df = st.session_state.get("latest_query_result")
    if latest_df is None:
        st.caption("Run a chat query to see result data and chart here.")
    else:
        st.dataframe(latest_df, use_container_width=True)
        numeric_cols = latest_df.select_dtypes(include="number").columns
        non_numeric_cols = [c for c in latest_df.columns if c not in numeric_cols]
        if len(numeric_cols) >= 1 and len(non_numeric_cols) >= 1:
            fig = px.bar(
                latest_df.head(20),
                x=non_numeric_cols[0],
                y=numeric_cols[0],
                title="Latest Query Chart",
            )
            st.plotly_chart(fig, use_container_width=True)
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
