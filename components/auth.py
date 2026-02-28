"""
Login and role-based page access for Sales Dashboard Factory.
Demo only: credentials are in-code; use env or secrets in production.
"""
from typing import Any, Dict, List, Optional

import streamlit as st

# Demo users: username -> { password, role }
# In production, use environment variables or a secure secrets store.
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
    """Return current role; defaults to Business User if not logged in (should not happen after require_login)."""
    u = _session_user()
    return u.get("role", "Business User") if u else "Business User"


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
    Render login form in the main area. Return True if user is already logged in
    or just logged in successfully; False otherwise (caller should st.stop()).
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
    Render sign-up form. Return True if user is already logged in or just
    signed up (and logged in); False otherwise.
    """
    if _session_user():
        return True

    st.title("Sales Dashboard Factory")
    st.markdown("Create an account to access the dashboard and analytics.")
    with st.form("signup_form"):
        username = st.text_input("Username", placeholder="e.g. my_username")
        password = st.text_input("Password", type="password", placeholder="••••••••")
        confirm = st.text_input("Confirm password", type="password", placeholder="••••••••")
        role = st.selectbox(
            "Role",
            ["Business User", "Data Analyst", "IT Admin"],
            help="Determines which pages you can access (e.g. Audit Log for Analyst/Admin).",
        )
        submitted = st.form_submit_button("Sign up")

    if submitted:
        username = (username or "").strip()
        if not username:
            st.error("Please enter a username.")
        elif not password:
            st.error("Please enter a password.")
        elif password != confirm:
            st.error("Passwords do not match.")
        else:
            users = _get_user_store()
            if username in users:
                st.error("That username is already taken.")
            else:
                _get_signups()[username] = {"password": password, "role": role}
                st.session_state["auth_user"] = {"username": username, "role": role}
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
