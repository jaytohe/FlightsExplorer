from pyspark.sql import SparkSession
class SparkConnectionManager:
    def __init__(self, app):
        if app is not None:
            self.init_app(app)
     
    def init_app(self, app):

        # Single spark session used for all requests.
        self.spark = SparkSession.builder.master("local").appName("FlightsExplorer").getOrCreate()

        # Read-in main flights dataset
        self.flights_db = self.spark.read.parquet(app.config["DATABASE"])

        # Supplementary tables used
        self.cancel_codes = self.spark.read.parquet(app.config["CANCEL_CODES"])
        self.airport_names = self.spark.read.parquet(app.config["AIRPORT_NAMES"])
        self.airline_names = self.spark.read.parquet(app.config["AIRLINE_NAMES"])

        #Create SQL table global views
        self.flights_db.createOrReplaceGlobalTempView("flights_db")
        self.cancel_codes.createOrReplaceGlobalTempView("cancel_codes")
        self.airport_names.createOrReplaceGlobalTempView("airport_names")
        self.airline_names.createOrReplaceGlobalTempView("airline_names")
    
        # register extension with app
        app.extensions = getattr(app, 'extensions', {})
        app.extensions['spark_connection_manager'] = self

    def get_db(self):
        return self.flights_db
    
    def get_spark(self):
        return self.spark
    
    def execute_query(self, sql_query_string, collect=True):
        return self.get_spark().sql(sql_query_string).collect() if collect else self.get_spark().sql(sql_query_string)