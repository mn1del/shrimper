from __future__ import annotations

from typing import Optional

from flask import current_app
from flask_mail import Mail, Message

mail = Mail()


def send_password_reset_email(recipient: str, reset_url: str) -> bool:
    """Send a password reset email. Returns True on success."""
    if not recipient or not reset_url:
        return False

    subject = current_app.config.get("PASSWORD_RESET_SUBJECT", "Shrimper password reset")
    sender = current_app.config.get("MAIL_DEFAULT_SENDER")

    body_lines = [
        "Hello,",
        "",
        "We received a request to reset the password for your Shrimper admin account.",
        "If you made this request, choose a new password using the link below:",
        "",
        reset_url,
        "",
        "This link expires in one hour. If you did not request a reset, you can ignore this message.",
        "",
        "— Shrimper League Results",
    ]
    body = "\n".join(body_lines)

    msg = Message(subject=subject, recipients=[recipient], body=body, sender=sender)

    try:
        mail.send(msg)
        return True
    except Exception:  # pragma: no cover - the caller logs failure
        current_app.logger.exception("Failed to send password reset email to %%s", recipient)
        return False
