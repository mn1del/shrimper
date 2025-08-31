import os
from flask import Flask


def create_app():
    app = Flask(__name__)

    # Auto-select backend: PostgreSQL if DATABASE_URL is set, else JSON file
    routes = None
    use_pg = bool(os.environ.get("DATABASE_URL"))
    if use_pg:
        try:
            from . import routes_pg as routes  # type: ignore
            app.logger.info("Using PostgreSQL backend (routes_pg)")
        except Exception:  # pragma: no cover
            app.logger.exception("Falling back to JSON backend; failed to init Postgres routes")
            from . import routes  # type: ignore
    else:
        from . import routes  # type: ignore
        app.logger.info("Using JSON file backend (routes)")

    app.register_blueprint(routes.bp)

    app.logger.info("Starting handicap recalculation")
    try:
        routes.recalculate_handicaps()
    except Exception:  # pylint: disable=broad-except
        app.logger.exception("Error recalculating handicaps")

    return app

app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
