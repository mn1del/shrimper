from flask import Flask


def create_app():
    app = Flask(__name__)

    from . import routes
    app.register_blueprint(routes.bp)

    app.logger.info("Starting handicap recalculation")
    try:
        routes.recalculate_handicaps()
    except Exception:  # pylint: disable=broad-except
        app.logger.exception("Error recalculating handicaps")

    return app

app = create_app()
