from flask import current_app, g
import pyspark.sql.functions as F # type: ignore
from pyspark.sql import SparkSession



def get_db():
    """Connect to the application's configured database. The connection
    is unique for each request and will be reused if this is called
    again.
    """
    if "db" not in g:
        g.db =  SparkSession.builder.master("local").appName("FlightsExplorer").getOrCreate()
    return g.db