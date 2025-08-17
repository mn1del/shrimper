from flask import Flask


def create_app():
    app = Flask(__name__)

    from . import routes
    app.register_blueprint(routes.bp)

    routes.recalculate_handicaps()

    return app

app = create_app()
