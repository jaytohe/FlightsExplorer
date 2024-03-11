from flask import (
    Blueprint, request, current_app
)
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
    get_db = current_app.extensions['spark_connection_manager'].get_db
    get_spark = current_app.extensions['spark_connection_manager'].get_spark
    execute_query = current_app.extensions['spark_connection_manager'].execute_query

    data = request.get_json()
    years = data["years"]
    isRange = data["isRange"]
    flights_took_place = get_db().where(
        (F.col('Year').between(years[0], years[1]) if isRange else F.col('Year').isin(years)) 
        & (F.col('Cancelled') == 0)
    ).count()
    return {
        "years": years,
        "isRange": isRange,
        "flights_took_place": flights_took_place
    }, 200

@bp.route('/num_flights_ontime_early_late', methods=('POST',))
@json_required_params({"year": int})
def num_flights_ontime_early_late():
    """
    Retrieves the number of flights that were on time, early, and late for a given year.
    """

    get_db = current_app.extensions['spark_connection_manager'].get_db
    get_spark = current_app.extensions['spark_connection_manager'].get_spark
    execute_query = current_app.extensions['spark_connection_manager'].execute_query

    data = request.get_json()
    year = data["year"]
    results_list = execute_query(f"""
        SELECT
            Year as year,
            AVG(CASE WHEN DepDelayMinutes = 0 THEN 1 ELSE 0 END) AS num_early,
            AVG(DepDel15) AS num_late,
            AVG(CASE WHEN (DepDel15 == 0) AND (DepDelayMinutes > 0) THEN 1 ELSE 0 END) AS num_ontime
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
        return next(iter(results_list)).asDict() #get first row from list output and convert to dict
    
    

@bp.route('/get_top_cancel_reason/<int:year>', methods=('GET',))
def get_top_cancel_reason(year):
    """
    Retrieves the top cancellation reason for a given year.
    """

    get_db = current_app.extensions['spark_connection_manager'].get_db
    get_spark = current_app.extensions['spark_connection_manager'].get_spark
    execute_query = current_app.extensions['spark_connection_manager'].execute_query

    cancel_codes = get_spark().read.parquet(current_app.config["CANCEL_CODES"])

    #join cancelation code description table with flights table on code
    # filter by provided year
    # count how many times each code appears and save it as countDesc
    # sort in descending order.
    results_list = get_db() \
        .join(cancel_codes, F.col("CancellationCode") == cancel_codes["Code"]) \
        .where(F.col("Year") == year).groupby(["Year", "Description"]) \
        .agg(F.count(F.col("Description")).alias("countDesc")) \
        .sort("countDesc", ascending=False).collect()


    if len(results_list) == 0:
        return {
            "year": year,
            "cancellation_reason": "Unknown",
            "num_cancelled": 0
        }
    
    else:
        return next(iter(results_list)).asDict()

"""

"""

