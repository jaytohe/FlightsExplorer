from flask import (
    Blueprint, request, current_app
)
from flask.json import jsonify
from .utils import json_required_params
#from .db import get_db, get_spark, execute_query

import pyspark.sql.functions as F # type: ignore

bp = Blueprint('part1', __name__, url_prefix='/part1')


@bp.route('/num_flights_took_place', methods=('POST',))
@json_required_params({"years": list, "isRange" : bool})
def num_flights_took_place():
    """
    A function to calculate the number of flights that took place (i.e. were NOT cancelled) within a specified range of years or in specific years.
    Takes no parameters.
    Returns a dictionary containing the years, a boolean indicating if a range was used, and the number of flights that took place, along with an HTTP status code.

    """

    flights_db = current_app.extensions['spark_connection_manager'].flights_db

    data = request.get_json()
    years = data["years"]
    years = sorted(years)
    isRange = data["isRange"]
    results_list = flights_db.where(
        (F.col('Year').between(years[0], years[1]) if isRange else F.col('Year').isin(years)) 
        & (F.col('Cancelled') == 0)
    ).groupby("Year").count().collect()
    return jsonify([row.asDict() for row in results_list]), 200
 #   [
 #     {
 #      "year" : xxxx,
 #      "num_flights" : yyyy,
 #     },
 #   ]

@bp.route('/num_flights_ontime_early_late/<int:year>', methods=('GET',))
#@json_required_params({"year": int})
def num_flights_ontime_early_late(year):
    """
    Retrieves the number of flights that were on time, early, and late for a given year.
    """
    execute_query = current_app.extensions['spark_connection_manager'].execute_query

#    data = request.get_json()
#    year = data["year"]
    results_list = execute_query(f"""
        SELECT
            Year as year,
            AVG(CASE WHEN DepDelayMinutes = 0 THEN 1 ELSE 0 END) AS num_early,
            AVG(DepDel15) AS num_late,
            AVG(CASE WHEN (DepDelayMinutes > 0) AND (DepDelayMinutes < 15) THEN 1 ELSE 0 END) AS num_ontime
        FROM global_temp.flights_db
        WHERE DepDelayMinutes IS NOT NULL AND DepDel15 IS NOT NULL AND Year = {year}
        GROUP BY Year;
    """)
    if len(results_list) == 0:
        return {
            "year": year,
            "num_early": 0,
            "num_late": 0,
            "num_ontime": 0
        }, 200
    else:
        return next(iter(results_list)).asDict(), 200 #get first row from list output and convert to dict
    
    

@bp.route('/get_top_cancel_reason/<int:year>', methods=('GET',))
def get_top_cancel_reason(year):
    """
    Retrieves the top cancellation reason for a given year.
    """


    if (year < 1987) or (year > 2020):
        return {
            "year": year,
            "cancellation_reason": "Unknown",
            "count": 0
        }, 200

    flights_db = current_app.extensions['spark_connection_manager'].flights_db
    cancel_codes = current_app.extensions['spark_connection_manager'].cancel_codes

    #join cancelation code description table with flights table on code
    # filter by provided year
    # count how many times each code appears and save it as countDesc
    # sort in descending order.
    results_list = flights_db \
        .join(cancel_codes, F.col("CancellationCode") == cancel_codes["Code"]) \
        .where(F.col("Year") == year).groupby(["Year", "Description"]) \
        .agg(F.count(F.col("Description")).alias("count")) \
        .withColumnsRenamed({"Year" : "year", "Description" : "cancellation_reason"}) \
        .sort("count", ascending=False).collect() #TODO: Add limit(1) to abide to task exactly.


    if len(results_list) == 0:
        return {
            "year": year,
            "cancellation_reason": "Unknown",
            "count": 0
        }, 200
    
    else:
        return next(iter(results_list)).asDict(), 200


@bp.route('/get_top_three_punctual_airlines', methods=('GET',))
def get_top_three_punctual_airlines():
    execute_query = current_app.extensions['spark_connection_manager'].execute_query
    
    results_list = execute_query("""
    SELECT year, SPLIT(airport_desc, ":")[0] as airport_location, TRIM(SPLIT(airport_desc, ":")[1]) as airport_name, proportion_punctual_takeoffs, punctuality_rank
    FROM (
        SELECT
            Year AS year,
            Description AS airport_desc, 
            AVG(CASE WHEN DepDelay = 0 THEN 1 ELSE 0 END) AS proportion_punctual_takeoffs,
            ROW_NUMBER() OVER (PARTITION BY Year ORDER BY AVG(CASE WHEN DepDelay = 0 THEN 1 ELSE 0 END) DESC) AS punctuality_rank
        FROM global_temp.flights_db
        JOIN global_temp.airport_names ON global_temp.flights_db.OriginAirportID = global_temp.airport_names.Code
        WHERE YEAR IN (1987, 1997, 2007, 2017) AND DepDelay IS NOT NULL
        GROUP BY Year, Description
    ) AS ranks
    WHERE punctuality_rank <= 3;
    """)

    return jsonify([row.asDict() for row in results_list]), 200


@bp.route('/get_top_three_worst_airlines', methods=('GET',))
def get_top_three_worst_airlines():
    flights_db = current_app.extensions['spark_connection_manager'].flights_db
    airline_names = current_app.extensions['spark_connection_manager'].airline_names

    results_list = flights_db \
    .join(airline_names, F.col("DOT_ID_Reporting_Airline") == airline_names["Code"])\
    .where((F.col("Year") >= 1987) & (F.col("Year") <= 1999)) \
    .groupby("Description") \
    .agg({"Cancelled" : "avg", "DepDel15" : "avg", "ArrDel15" : "avg"}) \
    .withColumn("performance_penalty", 5*F.col("avg(Cancelled)") + 2*F.col("avg(DepDel15)") + 3*F.col("avg(ArrDel15)")) \
    .withColumnRenamed("Description", "airline_name") \
    .sort("performance_penalty", ascending=False) \
    .limit(3) \
    .collect()

    return jsonify([row.asDict() for row in results_list]), 200

