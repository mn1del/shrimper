import os
from flask import Flask


def create_app():
    app = Flask(__name__)

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

    app.logger.info("Starting handicap recalculation")
    try:
        routes.recalculate_handicaps()
    except Exception:  # pylint: disable=broad-except
        app.logger.exception("Error recalculating handicaps")

    return app


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    create_app().run(host='0.0.0.0', port=port)
