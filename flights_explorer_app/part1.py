from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from .utils import json_required_params
from .db import get_db, get_spark
import pyspark.sql.functions as F # type: ignore

bp = Blueprint('part1', __name__, url_prefix='/part1')


@bp.route('/num_flights_took_place', methods=('POST',))
@json_required_params({"years": list, "isRange" : bool})
def num_flights_took_place():
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
    data = request.get_json()
    year = data["year"]

    get_db().createOrReplaceGlobalTempView("flights_db")
    output_df = get_spark().sql(f"""
        SELECT
            Year as year,
            AVG(CASE WHEN DepDelayMinutes = 0 THEN 1 ELSE 0 END) AS num_early,
            AVG(DepDel15) AS num_late,
            AVG(CASE WHEN (DepDel15 == 0) AND (DepDelayMinutes > 0) THEN 1 ELSE 0 END) AS num_ontime
        FROM global_temp.flights_db
        WHERE DepDelayMinutes IS NOT NULL AND DepDel15 IS NOT NULL AND Year = {year}
        GROUP BY Year;
    """).collect()

    return next(iter(output_df)).asDict() #TODO: handle no output row; get first row from list output and convert to dict


