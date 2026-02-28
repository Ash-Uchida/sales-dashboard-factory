"""
Login and role-based page access for Sales Dashboard Factory.
Uses Databricks workspace.admin (users, roles, stores, auth_view) when configured;
otherwise falls back to in-code demo users and in-session signups.
"""
from typing import Any, Dict, List, Optional

import streamlit as st

try:
    from utils import databricks_auth as db_auth
except Exception:
    db_auth = None

# Demo users when Databricks auth is not configured.
DEMO_USERS: Dict[str, Dict[str, str]] = {
    "store_manager": {"password": "demo123", "role": "Business User"},
    "analyst": {"password": "demo123", "role": "Data Analyst"},
    "admin": {"password": "demo123", "role": "IT Admin"},
}

# Which role can access which page (logical page names used in the app).
PAGE_ACCESS: Dict[str, List[str]] = {
    "Business User": ["dashboard", "chat"],
    "Data Analyst": ["dashboard", "chat", "audit_log"],
    "IT Admin": ["dashboard", "chat", "audit_log"],
}

# Page IDs used in the app (must match can_access(page) calls).
PAGES = ["dashboard", "chat", "audit_log"]


def _get_signups() -> Dict[str, Dict[str, str]]:
    """Return in-session signups (persist only until app restart for demo)."""
    if "auth_signups" not in st.session_state:
        st.session_state["auth_signups"] = {}
    return st.session_state["auth_signups"]


def _get_user_store() -> Dict[str, Dict[str, str]]:
    """Return merged user store: demo users + signups."""
    merged = dict(DEMO_USERS)
    merged.update(_get_signups())
    return merged


def _session_user() -> Optional[Dict[str, Any]]:
    """Return current user dict from session_state or None."""
    return st.session_state.get("auth_user")


def get_current_user() -> Optional[str]:
    """Return current username or None if not logged in."""
    u = _session_user()
    return u.get("username") if u else None


def get_current_role() -> str:
    """Return current role (role_name); defaults to Business User if not logged in."""
    u = _session_user()
    return u.get("role", "Business User") if u else "Business User"


def get_current_firstname() -> str:
    u = _session_user()
    return u.get("firstname", "") if u else ""


def get_current_lastname() -> str:
    u = _session_user()
    return u.get("lastname", "") if u else ""


def get_current_store_id() -> Optional[str]:
    u = _session_user()
    return u.get("store_id") if u else None


def can_access(page: str) -> bool:
    """Return True if the current user's role is allowed to access the given page."""
    role = get_current_role()
    allowed = PAGE_ACCESS.get(role, [])
    return page in allowed


def _auth_view() -> str:
    """Current auth view: 'login' or 'signup'."""
    if "auth_view" not in st.session_state:
        st.session_state["auth_view"] = "login"
    return st.session_state["auth_view"]


def _set_auth_view(view: str) -> None:
    st.session_state["auth_view"] = view
    st.rerun()


def render_login_form() -> bool:
    """
    Render login form. When Databricks auth is configured, uses check_password and auth_view;
    otherwise uses in-memory demo users.
    """
    if _session_user():
        return True

    st.title("Sales Dashboard Factory")
    st.markdown("Sign in to access the dashboard and analytics.")
    with st.form("login_form"):
        username = st.text_input("Username", placeholder="e.g. store_manager")
        password = st.text_input("Password", type="password", placeholder="••••••••")
        submitted = st.form_submit_button("Log in")

    if submitted and username and password:
        username = username.strip()
        if db_auth and db_auth.databricks_auth_configured():
            if db_auth.check_password(username, password):
                user_info = db_auth.get_user_after_login(username)
                if user_info:
                    st.session_state["auth_user"] = user_info
                    st.success(f"Logged in as **{user_info.get('firstname', '')} {user_info.get('lastname', '')}** ({user_info.get('role', '')}).")
                    st.rerun()
            st.error("Invalid username or password.")
        else:
            users = _get_user_store()
            if username in users and users[username]["password"] == password:
                st.session_state["auth_user"] = {
                    "username": username,
                    "role": users[username]["role"],
                }
                st.success(f"Logged in as **{username}** ({users[username]['role']}).")
                st.rerun()
            else:
                st.error("Invalid username or password.")

    st.markdown("---")
    if st.button("Don't have an account? **Sign up**", key="go_signup"):
        _set_auth_view("signup")
    return _session_user() is not None


def render_signup_form() -> bool:
    """
    Render sign-up form. When Databricks auth is configured, shows firstname, lastname,
    username, password, role (from roles table), store (from stores table); otherwise
    shows simplified demo signup.
    """
    if _session_user():
        return True

    st.title("Sales Dashboard Factory")
    st.markdown("Create an account to access the dashboard and analytics.")

    use_databricks = db_auth and db_auth.databricks_auth_configured()
    roles_list = db_auth.get_roles() if use_databricks else []
    stores_list = db_auth.get_stores() if use_databricks else []
    use_db_signup = use_databricks and len(roles_list) > 0

    with st.form("signup_form"):
        firstname = st.text_input("First name", placeholder="e.g. Jane")
        lastname = st.text_input("Last name", placeholder="e.g. Smith")
        username = st.text_input("Username", placeholder="e.g. jane.smith")
        password = st.text_input("Password", type="password", placeholder="••••••••")
        confirm = st.text_input("Confirm password", type="password", placeholder="••••••••")
        if use_db_signup:
            role_options = [r["role_name"] for r in roles_list]
            role_ids = [r["role_id"] for r in roles_list]
            role_display = st.selectbox("Role", role_options, help="Determines which pages you can access.")
            role_id = role_ids[role_options.index(role_display)] if role_display in role_options else role_ids[0]
        else:
            role_display = st.selectbox(
                "Role",
                ["Business User", "Data Analyst", "IT Admin"],
                help="Determines which pages you can access.",
            )
            role_id = role_display
        if use_db_signup and stores_list:
            store_options = ["— None —"] + [s["store_name"] for s in stores_list]
            store_ids = [None] + [s["store_id"] for s in stores_list]
            store_display = st.selectbox("Store (optional)", store_options)
            store_id = store_ids[store_options.index(store_display)] if store_display in store_options else None
        else:
            store_id = None
        submitted = st.form_submit_button("Sign up")

    if submitted:
        firstname = (firstname or "").strip()
        lastname = (lastname or "").strip()
        username = (username or "").strip()
        if not username:
            st.error("Please enter a username.")
        elif not password:
            st.error("Please enter a password.")
        elif password != confirm:
            st.error("Passwords do not match.")
        elif use_db_signup:
            if db_auth.username_exists(username):
                st.error("That username is already taken.")
            else:
                ok, err_msg = db_auth.register_user(firstname, lastname, username, password, role_id, store_id)
                if ok:
                    user_info = db_auth.get_user_after_login(username)
                    if user_info:
                        st.session_state["auth_user"] = user_info
                        st.success(f"Account **{firstname} {lastname}** created. You are now logged in.")
                        st.rerun()
                else:
                    st.error("Registration failed. Check Databricks connection and admin schema.")
                    if err_msg:
                        st.code(err_msg, language="text")
        else:
            users = _get_user_store()
            if username in users:
                st.error("That username is already taken.")
            else:
                _get_signups()[username] = {"password": password, "role": role_display}
                st.session_state["auth_user"] = {"username": username, "role": role_display}
                st.success(f"Account **{username}** created. You are now logged in.")
                st.rerun()

    st.markdown("---")
    if st.button("Already have an account? **Log in**", key="go_login"):
        _set_auth_view("login")
    return _session_user() is not None


def require_login() -> None:
    """
    Ensure user is logged in. If not, render login or sign-up form and stop.
    Call this at the top of your main app.
    """
    if "auth_user" not in st.session_state:
        st.session_state["auth_user"] = None

    if not _session_user():
        view = _auth_view()
        if view == "signup":
            if not render_signup_form():
                st.stop()
        else:
            if not render_login_form():
                st.stop()
        return
    # Already logged in; main app continues


def logout() -> None:
    """Clear auth state and switch to login view, then rerun."""
    if "auth_user" in st.session_state:
        del st.session_state["auth_user"]
    st.session_state["auth_view"] = "login"
    st.rerun()
