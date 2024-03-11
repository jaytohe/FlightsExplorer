from pyspark.sql import SparkSession
class SparkConnectionManager:
    def __init__(self, app):
        if app is not None:
            self.init_app(app)
     
    def init_app(self, app):

        self.spark = SparkSession.builder.master("local").appName("FlightsExplorer").getOrCreate()
        self.flights_db = self.spark.read.parquet(app.config["DATABASE"])
        self.cancel_codes = self.spark.read.parquet(app.config["CANCEL_CODES"])

        #Create SQL table global views
        self.flights_db.createOrReplaceGlobalTempView("flights_db")
        self.cancel_codes.createOrReplaceGlobalTempView("cancel_codes")
    
        # register extension with app
        app.extensions = getattr(app, 'extensions', {})
        app.extensions['spark_connection_manager'] = self

    def get_db(self):
        return self.flights_db
    
    def get_spark(self):
        return self.spark
    
    def execute_query(self, sql_query_string, collect=True):
        return self.get_spark().sql(sql_query_string).collect() if collect else self.get_spark().sql(sql_query_string)