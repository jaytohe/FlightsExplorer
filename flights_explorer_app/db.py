from flask import current_app, g
import pyspark.sql.functions as F # type: ignore
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("FlightsExplorer").getOrCreate()



def get_db():
    """Connect to the application's configured database. The connection
    is unique for each request and will be reused if this is called
    again.
    """
    if "db" not in g:
        print("Reading parquet database")
        g.db = get_spark().read.parquet(current_app.config["DATABASE"])
    return g.db


def get_spark():
    if "spark" not in g:
        print("creating spark session")
        g.spark =  SparkSession.builder.master("local").appName("FlightsExplorer").getOrCreate()
    return g.spark