"""
Databricks-backed auth: roles, stores, password check, register.
Uses workspace.admin.roles, workspace.admin.users, workspace.admin.stores.
Password hashes stored as SHA256 hex; check via query (hash never returned) or procedure.
"""
import hashlib
import os
import uuid
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

try:
    from databricks import sql as dbsql
except Exception:
    dbsql = None

_ADMIN_CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
_ADMIN_SCHEMA = os.getenv("DATABRICKS_SCHEMA_ADMIN", "admin")


def _conn():
    """Connection to Databricks SQL (caller must close or use as context)."""
    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def databricks_auth_configured() -> bool:
    return bool(
        dbsql is not None
        and os.getenv("DATABRICKS_SERVER_HOSTNAME")
        and os.getenv("DATABRICKS_HTTP_PATH")
        and os.getenv("DATABRICKS_TOKEN")
    )


def _run_query(query: str, params: Optional[Dict[str, Any]] = None) -> List[tuple]:
    """Run a query and return rows. No param placeholder for safety we build query with literals for auth."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    finally:
        conn.close()


def _run_query_df(query: str):
    """Run query and return a small DataFrame (columns from cursor.description)."""
    import pandas as pd
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def hash_password(password: str) -> str:
    """SHA256 hex (match Databricks sha2(..., 256)). For production prefer bcrypt/argon2."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def get_roles() -> List[Dict[str, str]]:
    """List of {role_id, role_name} from workspace.admin.roles."""
    if not databricks_auth_configured():
        return []
    try:
        q = f"SELECT role_id, role_name FROM {_ADMIN_CATALOG}.{_ADMIN_SCHEMA}.roles ORDER BY role_name"
        df = _run_query_df(q)
        if df.empty:
            return []
        return df.to_dict("records")
    except Exception:
        return []


def get_stores() -> List[Dict[str, str]]:
    """List of {store_id, store_name, location} from workspace.admin.stores."""
    if not databricks_auth_configured():
        return []
    try:
        q = f"SELECT store_id, store_name, location FROM {_ADMIN_CATALOG}.{_ADMIN_SCHEMA}.stores ORDER BY store_name"
        df = _run_query_df(q)
        if df.empty:
            return []
        return df.to_dict("records")
    except Exception:
        return []


def _check_password_via_procedure(username: str, password: str) -> Optional[bool]:
    """Call workspace.admin.check_password procedure if it exists. Returns None if procedure unavailable."""
    if not databricks_auth_configured():
        return None
    safe_user = username.replace("'", "''")
    safe_pwd = password.replace("'", "''")
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(
                f"CALL {_ADMIN_CATALOG}.{_ADMIN_SCHEMA}.check_password('{safe_user}', '{safe_pwd}')"
            )
            rows = cur.fetchall()
        if rows and len(rows) > 0:
            return int(rows[0][0]) == 1
        return False
    except Exception:
        return None


def check_password(username: str, password: str) -> bool:
    """
    Verify password without exposing stored hash.
    Tries procedure check_password(username, plaintext) first; if not available,
    app hashes password and runs SELECT 1 FROM users WHERE username=? AND password_hash=?.
    """
    if not databricks_auth_configured():
        return False
    ok = _check_password_via_procedure(username, password)
    if ok is not None:
        return ok
    pwd_hash = hash_password(password)
    safe_user = username.replace("'", "''")
    safe_hash = pwd_hash.replace("'", "''")
    q = f"""
    SELECT 1 AS ok FROM {_ADMIN_CATALOG}.{_ADMIN_SCHEMA}.users
    WHERE username = '{safe_user}' AND password_hash = '{safe_hash}'
    LIMIT 1
    """
    try:
        df = _run_query_df(q)
        return not df.empty
    except Exception:
        return False


def get_user_after_login(username: str):
    """
    Return user info for session (no password_hash). Requires auth_view with
    user_id, username, firstname, lastname, role_name, store_id.
    """
    if not databricks_auth_configured():
        return None
    safe_user = username.replace("'", "''")
    q = f"""
    SELECT user_id, username, firstname, lastname, role_name AS role, store_id
    FROM {_ADMIN_CATALOG}.{_ADMIN_SCHEMA}.auth_view
    WHERE username = '{safe_user}'
    LIMIT 1
    """
    try:
        df = _run_query_df(q)
        if df.empty:
            return None
        return df.iloc[0].to_dict()
    except Exception:
        return None


def username_exists(username: str) -> bool:
    try:
        safe_user = username.replace("'", "''")
        q = f"SELECT 1 FROM {_ADMIN_CATALOG}.{_ADMIN_SCHEMA}.users WHERE username = '{safe_user}' LIMIT 1"
        df = _run_query_df(q)
        return not df.empty
    except Exception:
        return False


def register_user(
    firstname: str,
    lastname: str,
    username: str,
    password: str,
    role_id: str,
    store_id: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Insert into workspace.admin.users; password is hashed before store.
    Returns (True, None) on success, (False, error_message) on failure.
    """
    if not databricks_auth_configured():
        return False, "Databricks auth not configured (check .env)."
    user_id = str(uuid.uuid4())
    pwd_hash = hash_password(password)
    safe = lambda s: (s or "").replace("'", "''")
    firstname_s, lastname_s = safe(firstname), safe(lastname)
    username_s = safe(username)
    # Insert only columns that exist: user_id, firstname, lastname, username, password_hash.
    # If your table has role_id, store_id, created_at, add them in Databricks and we can extend the INSERT.
    q = f"""
    INSERT INTO {_ADMIN_CATALOG}.{_ADMIN_SCHEMA}.users
    (user_id, firstname, lastname, username, password_hash)
    VALUES
    ('{user_id}', '{firstname_s}', '{lastname_s}', '{username_s}', '{pwd_hash}')
    """
    try:
        conn = _conn()
        try:
            with conn.cursor() as cur:
                cur.execute(q)
            if hasattr(conn, "commit"):
                conn.commit()
        finally:
            conn.close()
        return True, None
    except Exception as e:
        return False, str(e)
