"""

import os
from datetime import datetime
from math import isnan

from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text, select, func

# Optional: load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:password@localhost:3306/nyc_taxi")
# Example: "mysql+pymysql://etl_user:pass@localhost:3306/nyc_taxi"

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

app = Flask(__name__)


# -------------------------
# Utilities
# -------------------------
def parse_date_param(name):
    val = request.args.get(name)
    if not val:
        return None
    try:
        # Accept YYYY-MM-DD or ISO datetime
        if len(val) == 10:
            return datetime.strptime(val, "%Y-%m-%d")
        return datetime.fromisoformat(val)
    except Exception:
        return None


def date_filter_clause(params, start, end):
    """
    Returns SQL clause string and adds to params dict.
    """
    clause = ""
    if start:
        clause += " AND pickup_datetime >= :start"
        params["start"] = start
    if end:
        clause += " AND pickup_datetime <= :end"
        params["end"] = end
    return clause


# -------------------------
# Endpoints
# -------------------------

@app.route("/api/summary", methods=["GET"])
def summary():
    """
    Aggregated summary: trips, avg_distance_km, avg_fare, avg_tip, avg_speed_kmh
    Optional query params: start (YYYY-MM-DD or ISO), end
    """
    start = parse_date_param("start")
    end = parse_date_param("end")
    params = {}
    clause = date_filter_clause(params, start, end)

    sql = text(f"""
        SELECT
            COUNT(*) AS total_trips,
            ROUND(AVG(trip_distance_km), 3) AS avg_distance_km,
            ROUND(AVG(fare_amount), 2) AS avg_fare,
            ROUND(AVG(tip_amount), 2) AS avg_tip,
            ROUND(AVG(trip_speed_kmh), 2) AS avg_speed_kmh
        FROM trips
        WHERE 1=1 {clause}
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, params).fetchone()
    return jsonify(dict(row))


@app.route("/api/time-series", methods=["GET"])
def time_series():
    """
    Time series endpoint.
    Params:
      - start, end
      - granularity: 'hour' (default), 'day'
    Returns list of { period: 'YYYY-MM-DD HH:00:00', trips: N }
    """
    gran = request.args.get("granularity", "hour")
    start = parse_date_param("start")
    end = parse_date_param("end")
    params = {}
    clause = date_filter_clause(params, start, end)

    if gran == "day":
        period_expr = "DATE(pickup_datetime)"
        fmt = "%Y-%m-%d"
    else:
        # hour granularity
        period_expr = "DATE_FORMAT(pickup_datetime, '%Y-%m-%d %H:00:00')"
        fmt = "%Y-%m-%d %H:00:00"

    sql = text(f"""
        SELECT {period_expr} AS period, COUNT(*) AS trips
        FROM trips
        WHERE 1=1 {clause}
        GROUP BY period
        ORDER BY period
        LIMIT 10000
    """)
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return jsonify(rows)


@app.route("/api/hotspots", methods=["GET"])
def hotspots():
    """
    Top-K pickup zones or grid cells.
    Optional params:
      - k (default 20)
      - start, end
      - zone: if trips.pickup_zone_id exists, we operate on zone; otherwise fallback to pickup_lat/pickup_lon clustering in DB (not implemented).
    Returns list of {zone_id, zone_name (if available), trips}
    """
    k = int(request.args.get("k", 20))
    start = parse_date_param("start")
    end = parse_date_param("end")
    params = {}
    clause = date_filter_clause(params, start, end)

    # Prefer pickup_zone_id join with zones table if present
    sql = text(f"""
        SELECT z.zone_id, z.zone_name, COUNT(t.id) AS trips
        FROM trips t
        LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
        WHERE 1=1 {clause} AND t.pickup_zone_id IS NOT NULL
        GROUP BY z.zone_id, z.zone_name
        ORDER BY trips DESC
        LIMIT :k
    """)
    params["k"] = k
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

    # If zones not available or returned empty, fallback to top pickup lat/lon clusters (coarse)
    if not rows:
        # coarse grid bucketing query (round lat/lon to 2 decimal -> ~1.1km)
        sql2 = text(f"""
            SELECT
              ROUND(pickup_lat, 2) AS lat_grid,
              ROUND(pickup_lon, 2) AS lon_grid,
              COUNT(*) AS trips
            FROM trips
            WHERE 1=1 {clause}
            GROUP BY lat_grid, lon_grid
            ORDER BY trips DESC
            LIMIT :k
        """)
        with engine.connect() as conn:
            rows = [dict(r) for r in conn.execute(sql2, params).fetchall()]
    return jsonify(rows)


@app.route("/api/fare-stats", methods=["GET"])
def fare_stats():
    """
    Fare statistics and distribution summaries.
    Optional params: start, end
    Returns: avg fare, median fare, avg fare_per_km, fare percentiles
    """
    start = parse_date_param("start")
    end = parse_date_param("end")
    params = {}
    clause = date_filter_clause(params, start, end)

    # MySQL does not have a built-in median; approximate using percentile with ORDER BY and LIMIT.
    # We'll compute some percentiles via a subquery using variables (works in many MySQL versions).
    # Simpler approach: return AVG, STDDEV, MIN, MAX and quartiles using window functions if MySQL 8+.
    sql = text(f"""
        SELECT
            ROUND(AVG(fare_amount), 2) AS avg_fare,
            ROUND(STDDEV_SAMP(fare_amount), 2) AS stddev_fare,
            ROUND(MIN(fare_amount), 2) AS min_fare,
            ROUND(MAX(fare_amount), 2) AS max_fare,
            ROUND(AVG(fare_per_km), 2) AS avg_fare_per_km
        FROM trips
        WHERE 1=1 {clause}
    """)
    with engine.connect() as conn:
        summary = dict(conn.execute(sql, params).fetchone())

        # Quartiles using window function (MySQL 8+)
        quartile_sql = text(f"""
            SELECT
              ROUND( (SELECT fare_amount FROM (SELECT fare_amount, ROW_NUMBER() OVER (ORDER BY fare_amount) AS rn, COUNT(*) OVER() AS cnt FROM trips WHERE 1=1 {clause}) t WHERE rn = FLOOR(cnt*0.25) OR rn = CEIL(cnt*0.25) LIMIT 1), 2 ) AS q1,
              ROUND( (SELECT fare_amount FROM (SELECT fare_amount, ROW_NUMBER() OVER (ORDER BY fare_amount) AS rn, COUNT(*) OVER() AS cnt FROM trips WHERE 1=1 {clause}) t WHERE rn = FLOOR(cnt*0.5) OR rn = CEIL(cnt*0.5) LIMIT 1), 2 ) AS median,
              ROUND( (SELECT fare_amount FROM (SELECT fare_amount, ROW_NUMBER() OVER (ORDER BY fare_amount) AS rn, COUNT(*) OVER() AS cnt FROM trips WHERE 1=1 {clause}) t WHERE rn = FLOOR(cnt*0.75) OR rn = CEIL(cnt*0.75) LIMIT 1), 2 ) AS q3
        """)
        try:
            quartiles = dict(conn.execute(quartile_sql, params).fetchone())
        except Exception:
            quartiles = {"q1": None, "median": None, "q3": None}

    out = {"summary": summary, "quartiles": quartiles}
    return jsonify(out)


@app.route("/api/top-routes", methods=["GET"])
def top_routes():
    """
    Top N routes: pickup_zone -> dropoff_zone counts.
    Params: n (default 20), start, end
    """
    n = int(request.args.get("n", 20))
    start = parse_date_param("start")
    end = parse_date_param("end")
    params = {}
    clause = date_filter_clause(params, start, end)

    sql = text(f"""
        SELECT
           t.pickup_zone_id,
           p.zone_name AS pickup_zone_name,
           t.dropoff_zone_id,
           d.zone_name AS dropoff_zone_name,
           COUNT(*) AS trips
        FROM trips t
        LEFT JOIN zones p ON t.pickup_zone_id = p.zone_id
        LEFT JOIN zones d ON t.dropoff_zone_id = d.zone_id
        WHERE 1=1 {clause}
        GROUP BY t.pickup_zone_id, t.dropoff_zone_id, p.zone_name, d.zone_name
        ORDER BY trips DESC
        LIMIT :n
    """)
    params["n"] = n
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return jsonify(rows)


@app.route("/api/trips", methods=["GET"])
def trips():
    """
    Paginated trip detail drill-down.
    Filters: start, end, min_distance, max_distance, min_fare, max_fare, passenger_count, pickup_zone, dropoff_zone
    Pagination: page, limit
    """
    start = parse_date_param("start")
    end = parse_date_param("end")
    params = {}
    clause = date_filter_clause(params, start, end)

    # optional filters
    if request.args.get("min_distance"):
        clause += " AND trip_distance_km >= :min_distance"
        params["min_distance"] = float(request.args.get("min_distance"))
    if request.args.get("max_distance"):
        clause += " AND trip_distance_km <= :max_distance"
        params["max_distance"] = float(request.args.get("max_distance"))
    if request.args.get("min_fare"):
        clause += " AND fare_amount >= :min_fare"
        params["min_fare"] = float(request.args.get("min_fare"))
    if request.args.get("max_fare"):
        clause += " AND fare_amount <= :max_fare"
        params["max_fare"] = float(request.args.get("max_fare"))
    if request.args.get("passenger_count"):
        clause += " AND passenger_count = :passenger_count"
        params["passenger_count"] = int(request.args.get("passenger_count"))
    if request.args.get("pickup_zone"):
        clause += " AND pickup_zone_id = :pickup_zone"
        params["pickup_zone"] = int(request.args.get("pickup_zone"))
    if request.args.get("dropoff_zone"):
        clause += " AND dropoff_zone_id = :dropoff_zone"
        params["dropoff_zone"] = int(request.args.get("dropoff_zone"))

    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 100))
    offset = (page - 1) * limit

    sql = text(f"""
        SELECT id, vendor_code, pickup_datetime, dropoff_datetime,
               pickup_lat, pickup_lon, dropoff_lat, dropoff_lon,
               passenger_count, trip_distance_km, trip_duration_seconds,
               fare_amount, tip_amount, trip_speed_kmh, fare_per_km, tip_pct, hour_of_day, day_of_week
        FROM trips
        WHERE 1=1 {clause}
        ORDER BY pickup_datetime DESC
        LIMIT :limit OFFSET :offset
    """)
    params["limit"] = limit
    params["offset"] = offset

    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        # quick total count (could be expensive; optional)
        count_sql = text(f"SELECT COUNT(*) AS total FROM trips WHERE 1=1 {clause}")
        total = conn.execute(count_sql, params).fetchone()["total"]

    return jsonify({"page": page, "limit": limit, "total": total, "rows": rows})


@app.route("/api/insights", methods=["GET"])
def insights():
    """
    Return three meaningful insights with the SQL/logic and the query results (for the report).
    1) Rush-hour peaks (trips by hour_of_day)
    2) Spatial hotspots (top pickup zones morning vs evening)
    3) Fare efficiency (fare_per_km vs tip_pct per zone)
    Optional params: start, end
    """
    start = parse_date_param("start")
    end = parse_date_param("end")
    params = {}
    clause = date_filter_clause(params, start, end)

    with engine.connect() as conn:
        # 1) Rush-hour peaks
        sql1 = text(f"""
            SELECT hour_of_day, COUNT(*) AS trips
            FROM trips
            WHERE 1=1 {clause}
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """)
        rush_rows = [dict(r) for r in conn.execute(sql1, params).fetchall()]

        # 2) Spatial hotspots: compare morning (7-9) and evening (17-19) top 10 pickup zones
        sql2_morning = text(f"""
            SELECT t.pickup_zone_id, z.zone_name, COUNT(*) AS trips
            FROM trips t
            LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
            WHERE 1=1 {clause} AND hour_of_day BETWEEN 7 AND 9
            GROUP BY t.pickup_zone_id, z.zone_name
            ORDER BY trips DESC
            LIMIT 10
        """)
        sql2_evening = text(f"""
            SELECT t.pickup_zone_id, z.zone_name, COUNT(*) AS trips
            FROM trips t
            LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
            WHERE 1=1 {clause} AND hour_of_day BETWEEN 17 AND 19
            GROUP BY t.pickup_zone_id, z.zone_name
            ORDER BY trips DESC
            LIMIT 10
        """)
        morning_hotspots = [dict(r) for r in conn.execute(sql2_morning, params).fetchall()]
        evening_hotspots = [dict(r) for r in conn.execute(sql2_evening, params).fetchall()]

        # 3) Fare efficiency: compute avg fare_per_km and avg tip_pct per pickup_zone
        sql3 = text(f"""
            SELECT t.pickup_zone_id, z.zone_name,
                   ROUND(AVG(t.fare_per_km), 2) AS avg_fare_per_km,
                   ROUND(AVG(t.tip_pct)*100, 2) AS avg_tip_pct,
                   COUNT(*) AS trips
            FROM trips t
            LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
            WHERE 1=1 {clause}
            GROUP BY t.pickup_zone_id, z.zone_name
            HAVING COUNT(*) > 50
            ORDER BY avg_fare_per_km DESC
            LIMIT 20
        """)
        fare_efficiency = [dict(r) for r in conn.execute(sql3, params).fetchall()]

    # Explanations for the report (short)
    explanations = {
        "rush_hour": "Trips aggregated by hour_of_day show demand peaks. Use this to plan fleet allocation.",
        "spatial_hotspots": "Top pickup zones compared between morning (7-9) and evening (17-19) to show directional demand.",
        "fare_efficiency": "Zones ranked by average fare_per_km and average tip percent; helps identify pricing/tipping patterns by area."
    }

    payload = {
        "insight_1_rush_hour": {"explanation": explanations["rush_hour"], "data": rush_rows, "sql": str(sql1)},
        "insight_2_spatial_hotspots": {
            "explanation": explanations["spatial_hotspots"],
            "morning_top10": morning_hotspots,
            "evening_top10": evening_hotspots,
            "sql_morning": str(sql2_morning),
            "sql_evening": str(sql2_evening)
        },
        "insight_3_fare_efficiency": {"explanation": explanations["fare_efficiency"], "data": fare_efficiency, "sql": str(sql3)}
    }
    return jsonify(payload)


# Root health
@app.route("/", methods=["GET"])
def root():
    return {"status": "ok", "message": "NYC Taxi Analytics API", "database": DATABASE_URL}


if __name__ == "__main__":
    # run locally
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true"))
