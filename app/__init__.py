import os
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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
