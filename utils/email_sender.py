"""
Send email (e.g. password-reset notification). Uses SMTP env vars when set.
Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, EMAIL_FROM in .env to enable.
"""
import os
from typing import Optional, Tuple

from dotenv import load_dotenv

load_dotenv()


def email_configured() -> bool:
    """True if SMTP is configured enough to send mail."""
    return bool(
        os.getenv("SMTP_HOST")
        and os.getenv("SMTP_PORT")
        and os.getenv("EMAIL_FROM")
    )


def send_email(to_email: str, subject: str, body_plain: str) -> Tuple[bool, Optional[str]]:
    """
    Send an email. Returns (True, None) on success, (False, error_message) on failure.
    If SMTP is not configured, returns (False, "Email not configured").
    """
    if not email_configured():
        return False, "Email not configured (set SMTP_HOST, SMTP_PORT, EMAIL_FROM in .env)."
    to_email = (to_email or "").strip()
    if not to_email:
        return False, "Recipient email is required."
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    host = os.getenv("SMTP_HOST", "")
    port = int(os.getenv("SMTP_PORT", "587"))
    from_addr = os.getenv("EMAIL_FROM", "")
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASSWORD")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_email
    msg.attach(MIMEText(body_plain, "plain"))

    try:
        with smtplib.SMTP(host, port) as server:
            if user and password:
                server.starttls()
                server.login(user, password)
            server.sendmail(from_addr, [to_email], msg.as_string())
        return True, None
    except Exception as e:
        return False, str(e)


def send_password_reset_email(to_email: str, username: str, new_password: str) -> Tuple[bool, Optional[str]]:
    """Send a notification that the user's password was reset (IT Admin flow)."""
    subject = "Your password was reset - Sales Dashboard"
    body = (
        f"Hello,\n\n"
        f"Your password for account '{username}' was reset by an administrator.\n\n"
        f"Your new temporary password is: {new_password}\n\n"
        f"Please log in and change it if desired.\n\n"
        f"— Sales Dashboard"
    )
    return send_email(to_email, subject, body)
