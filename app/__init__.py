import os
import click
from flask import Flask, jsonify, redirect, request, url_for

from .auth import build_user, login_manager
from .email import mail
from . import datastore as datastore_proxy


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

    mail.init_app(app)

    login_manager.init_app(app)

    @login_manager.user_loader
    def _load_user(user_id: str):
        try:
            admin = datastore_proxy.get_admin_by_id(int(user_id))
        except Exception:
            return None
        if not admin or not bool(admin.get("is_active", True)):
            return None
        return build_user(admin)

    @login_manager.unauthorized_handler
    def _unauthorized():
        if request.path.startswith("/api/") or request.is_json:
            return jsonify({"error": "authentication_required"}), 401
        next_url = request.url
        return redirect(url_for("main.login", next=next_url))

    @app.cli.command("auth-create-admin")
    @click.option("--email", prompt=True, help="Email address for the admin account")
    @click.option(
        "--password",
        prompt=True,
        hide_input=True,
        confirmation_prompt=True,
        help="Initial password. The admin can change this later.",
    )
    @click.option("--superadmin/--no-superadmin", default=False, help="Grant super-admin privileges")
    def auth_create_admin(email: str, password: str, superadmin: bool) -> None:
        """Create an admin account from the CLI."""
        try:
            record = datastore_proxy.create_admin(email, password, is_superadmin=superadmin)
        except ValueError as exc:  # duplicate or validation error
            click.echo(f"Error: {exc}", err=True)
            raise click.Abort() from exc
        click.echo(
            f"Created admin {record['email']} (superadmin={bool(record.get('is_superadmin'))})"
        )

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
