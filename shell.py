from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.master("local").appName("Shell").getOrCreate()

flights_db = spark.read.parquet("instance/data/airline.parquet")
flights_db.createOrReplaceGlobalTempView("flights_db")

cancel_codes = spark.read.parquet("instance/data/L_CANCELLATION.parquet")
cancel_codes.createOrReplaceGlobalTempView("cancel_codes")


