import os
from flask import Flask


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        DATABASE=os.path.join(app.instance_path, 'data', 'airline.parquet'),
        CANCEL_CODES=os.path.join(app.instance_path, 'data', 'L_CANCELLATION.parquet'),
        AIRPORT_NAMES=os.path.join(app.instance_path, 'data', 'L_AIRPORT_ID.parquet'),
        AIRLINE_NAMES=os.path.join(app.instance_path, 'data', 'L_AIRLINE_ID.parquet')
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    if not os.path.exists(app.instance_path):
        os.makedirs(app.instance_path)

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'
    
    from . import part1
    app.register_blueprint(part1.bp)

    ## Register Spark Connection Manager
    from .Flask_Spark import SparkConnectionManager

    flask_spark = SparkConnectionManager(app)

    return app