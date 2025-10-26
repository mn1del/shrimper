import os
from flask import Flask


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def create_app():
    app = Flask(__name__)

    secret_key = os.environ.get("SECRET_KEY", "").strip()
    if not secret_key:
        # Development/test fallback; production should always set SECRET_KEY explicitly
        secret_key = "development-secret-key"
    app.config["SECRET_KEY"] = secret_key
    app.config["SECURITY_PASSWORD_SALT"] = os.environ.get("SECURITY_PASSWORD_SALT", "change-me-salt")

    # Mail configuration (used for password reset emails)
    mail_defaults = {
        "MAIL_SERVER": os.environ.get("MAIL_SERVER", ""),
        "MAIL_PORT": int(os.environ.get("MAIL_PORT", "0") or 0),
        "MAIL_USERNAME": os.environ.get("MAIL_USERNAME", ""),
        "MAIL_PASSWORD": os.environ.get("MAIL_PASSWORD", ""),
        "MAIL_USE_TLS": _env_bool("MAIL_USE_TLS", True),
        "MAIL_USE_SSL": _env_bool("MAIL_USE_SSL", False),
        "MAIL_DEFAULT_SENDER": os.environ.get("MAIL_DEFAULT_SENDER", ""),
        "MAIL_SUPPRESS_SEND": _env_bool("MAIL_SUPPRESS_SEND", False),
    }
    app.config.update(mail_defaults)

    # PostgreSQL-only configuration (JSON backend retired on this branch)
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL is required. This branch is PostgreSQL-only and does not use data.json."
        )

    # Initialize connection pool early (optional; direct connect works if pool init fails)
    try:
        from . import datastore_pg as _pg
        try:
            minconn = int(os.environ.get("DB_POOL_MIN", "1"))
        except ValueError:
            minconn = 1
        try:
            maxconn = int(os.environ.get("DB_POOL_MAX", "10"))
        except ValueError:
            maxconn = 10
        _pg.init_pool(minconn=minconn, maxconn=maxconn)
    except Exception:  # pragma: no cover
        app.logger.exception("PostgreSQL pool initialization failed; continuing without pool")

    # Routes use datastore proxies that now target PostgreSQL
    from . import routes  # type: ignore
    app.register_blueprint(routes.bp)

    # Optionally skip full recalculation on startup for faster boot
    if os.environ.get("RECALC_ON_STARTUP", "1") not in ("0", "false", "False"):
        app.logger.info("Starting handicap recalculation")
        try:
            routes.recalculate_handicaps()
        except Exception:  # pylint: disable=broad-except
            app.logger.exception("Error recalculating handicaps")

    return app


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    create_app().run(host='0.0.0.0', port=port)
