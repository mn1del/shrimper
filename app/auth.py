from __future__ import annotations

from typing import Any, Optional

from flask_login import LoginManager, UserMixin


class AdminUser(UserMixin):
    """Lightweight wrapper for admin records used by Flask-Login."""

    def __init__(self, payload: dict[str, Any]):
        self._data = dict(payload or {})

    @property
    def data(self) -> dict[str, Any]:
        return self._data

    def get_id(self) -> str:
        return str(self._data.get("id"))

    @property
    def email(self) -> Optional[str]:
        return self._data.get("email")

    @property
    def is_superadmin(self) -> bool:
        return bool(self._data.get("is_superadmin"))

    def is_active(self) -> bool:  # type: ignore[override]
        return bool(self._data.get("is_active", True))


login_manager = LoginManager()
login_manager.login_view = "main.login"


def build_user(record: Optional[dict[str, Any]]) -> Optional[AdminUser]:
    if not record:
        return None
    return AdminUser(record)
