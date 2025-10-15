import os
from datetime import datetime
from math import isnan

from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Optional: load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:password@localhost:3306/nyc_taxi")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

app = Flask(__name__)


# -------------------------
# Error Handlers
# -------------------------
@app.errorhandler(Exception)
def handle_exception(e):
    """Global error handler for debugging"""
    import traceback
    app.logger.error(f"Error: {str(e)}")
    app.logger.error(traceback.format_exc())
    return jsonify({
        "error": str(e),
        "type": type(e).__name__
    }), 500


# -------------------------
# Utilities
# -------------------------
def parse_date_param(name):
    val = request.args.get(name)
    if not val:
        return None
    try:
        if len(val) == 10:
            return datetime.strptime(val, "%Y-%m-%d")
        return datetime.fromisoformat(val.replace('Z', '+00:00'))
    except Exception as e:
        app.logger.warning(f"Date parse error for {name}={val}: {e}")
        return None


def date_filter_clause(params, start, end):
    """Returns SQL clause string and adds to params dict."""
    clause = ""
    if start:
        clause += " AND pickup_datetime >= :start"
        params["start"] = start
    if end:
        clause += " AND pickup_datetime <= :end"
        params["end"] = end
    return clause


def safe_dict(row):
    """Safely convert SQLAlchemy row to dict, handling None values"""
    if row is None:
        return {}
    try:
        return dict(row._mapping)
    except:
        return dict(row)


# -------------------------
# Endpoints
# -------------------------

@app.route("/api/summary", methods=["GET"])
def summary():
    """Aggregated summary with error handling"""
    try:
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)

        sql = text(f"""
            SELECT
                COUNT(*) AS total_trips,
                ROUND(COALESCE(AVG(trip_distance_km), 0), 3) AS avg_distance_km,
                ROUND(COALESCE(AVG(fare_amount), 0), 2) AS avg_fare,
                ROUND(COALESCE(AVG(tip_amount), 0), 2) AS avg_tip,
                ROUND(COALESCE(AVG(trip_speed_kmh), 0), 2) AS avg_speed_kmh
            FROM trips
            WHERE 1=1 {clause}
        """)
        
        with engine.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            if row is None:
                return jsonify({"error": "No data found"}), 404
            return jsonify(safe_dict(row))
    
    except SQLAlchemyError as e:
        app.logger.error(f"Database error in /api/summary: {e}")
        return jsonify({"error": "Database error", "details": str(e)}), 500
    except Exception as e:
        app.logger.error(f"Error in /api/summary: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/time-series", methods=["GET"])
def time_series():
    """Time series endpoint with error handling"""
    try:
        gran = request.args.get("granularity", "hour")
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)

        if gran == "day":
            period_expr = "DATE(pickup_datetime)"
        else:
            period_expr = "DATE_FORMAT(pickup_datetime, '%Y-%m-%d %H:00:00')"

        sql = text(f"""
            SELECT {period_expr} AS period, COUNT(*) AS trips
            FROM trips
            WHERE 1=1 {clause}
            GROUP BY period
            ORDER BY period
            LIMIT 10000
        """)
        
        with engine.connect() as conn:
            rows = [safe_dict(r) for r in conn.execute(sql, params).fetchall()]
            return jsonify(rows)
    
    except Exception as e:
        app.logger.error(f"Error in /api/time-series: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/hotspots", methods=["GET"])
def hotspots():
    """Top-K pickup zones with better error handling"""
    try:
        k = min(int(request.args.get("k", 20)), 100)  # Cap at 100
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)

        # First try with zones
        sql = text(f"""
            SELECT 
                COALESCE(z.zone_id, 0) AS zone_id,
                COALESCE(z.zone_name, 'Unknown') AS zone_name,
                COUNT(t.id) AS trips
            FROM trips t
            LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
            WHERE 1=1 {clause}
            GROUP BY z.zone_id, z.zone_name
            HAVING trips > 0
            ORDER BY trips DESC
            LIMIT :k
        """)
        params["k"] = k
        
        with engine.connect() as conn:
            rows = [safe_dict(r) for r in conn.execute(sql, params).fetchall()]

        # Fallback to coordinates if no zones
        if not rows:
            params = {}
            clause = date_filter_clause(params, start, end)
            sql2 = text(f"""
                SELECT
                    ROUND(pickup_lat, 2) AS lat_grid,
                    ROUND(pickup_lon, 2) AS lon_grid,
                    COUNT(*) AS trips
                FROM trips
                WHERE 1=1 {clause}
                    AND pickup_lat IS NOT NULL
                    AND pickup_lon IS NOT NULL
                GROUP BY lat_grid, lon_grid
                ORDER BY trips DESC
                LIMIT :k
            """)
            params["k"] = k
            with engine.connect() as conn:
                rows = [safe_dict(r) for r in conn.execute(sql2, params).fetchall()]
        
        return jsonify(rows)
    
    except Exception as e:
        app.logger.error(f"Error in /api/hotspots: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/fare-stats", methods=["GET"])
def fare_stats():
    """Simplified fare stats to avoid complex subqueries"""
    try:
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)

        # Basic stats
        sql = text(f"""
            SELECT
                ROUND(COALESCE(AVG(fare_amount), 0), 2) AS avg_fare,
                ROUND(COALESCE(STDDEV(fare_amount), 0), 2) AS stddev_fare,
                ROUND(COALESCE(MIN(fare_amount), 0), 2) AS min_fare,
                ROUND(COALESCE(MAX(fare_amount), 0), 2) AS max_fare,
                ROUND(COALESCE(AVG(fare_per_km), 0), 2) AS avg_fare_per_km
            FROM trips
            WHERE 1=1 {clause} AND fare_amount IS NOT NULL
        """)
        
        with engine.connect() as conn:
            summary = safe_dict(conn.execute(sql, params).fetchone())

        # Simplified percentiles
        quartiles = {"q1": None, "median": None, "q3": None}
        try:
            perc_sql = text(f"""
                SELECT
                    fare_amount,
                    PERCENT_RANK() OVER (ORDER BY fare_amount) AS pct_rank
                FROM trips
                WHERE 1=1 {clause} AND fare_amount IS NOT NULL
                LIMIT 10000
            """)
            with engine.connect() as conn:
                rows = conn.execute(perc_sql, params).fetchall()
                if rows:
                    fares = [r[0] for r in rows]
                    fares.sort()
                    n = len(fares)
                    if n > 0:
                        quartiles["q1"] = round(fares[int(n * 0.25)], 2)
                        quartiles["median"] = round(fares[int(n * 0.50)], 2)
                        quartiles["q3"] = round(fares[int(n * 0.75)], 2)
        except Exception as e:
            app.logger.warning(f"Could not compute quartiles: {e}")

        return jsonify({"summary": summary, "quartiles": quartiles})
    
    except Exception as e:
        app.logger.error(f"Error in /api/fare-stats: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/top-routes", methods=["GET"])
def top_routes():
    """Top routes with null handling"""
    try:
        n = min(int(request.args.get("n", 20)), 100)
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)

        sql = text(f"""
            SELECT
                t.pickup_zone_id,
                COALESCE(p.zone_name, 'Unknown') AS pickup_zone_name,
                t.dropoff_zone_id,
                COALESCE(d.zone_name, 'Unknown') AS dropoff_zone_name,
                COUNT(*) AS trips
            FROM trips t
            LEFT JOIN zones p ON t.pickup_zone_id = p.zone_id
            LEFT JOIN zones d ON t.dropoff_zone_id = d.zone_id
            WHERE 1=1 {clause}
            GROUP BY t.pickup_zone_id, t.dropoff_zone_id, p.zone_name, d.zone_name
            HAVING trips > 0
            ORDER BY trips DESC
            LIMIT :n
        """)
        params["n"] = n
        
        with engine.connect() as conn:
            rows = [safe_dict(r) for r in conn.execute(sql, params).fetchall()]
            return jsonify(rows)
    
    except Exception as e:
        app.logger.error(f"Error in /api/top-routes: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trips", methods=["GET"])
def trips():
    """Paginated trip details with better validation"""
    try:
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)

        # Optional filters with validation
        if request.args.get("min_distance"):
            try:
                params["min_distance"] = float(request.args.get("min_distance"))
                clause += " AND trip_distance_km >= :min_distance"
            except ValueError:
                pass
        
        if request.args.get("max_distance"):
            try:
                params["max_distance"] = float(request.args.get("max_distance"))
                clause += " AND trip_distance_km <= :max_distance"
            except ValueError:
                pass
        
        if request.args.get("min_fare"):
            try:
                params["min_fare"] = float(request.args.get("min_fare"))
                clause += " AND fare_amount >= :min_fare"
            except ValueError:
                pass
        
        if request.args.get("max_fare"):
            try:
                params["max_fare"] = float(request.args.get("max_fare"))
                clause += " AND fare_amount <= :max_fare"
            except ValueError:
                pass

        page = max(int(request.args.get("page", 1)), 1)
        limit = min(int(request.args.get("limit", 100)), 1000)  # Cap at 1000
        offset = (page - 1) * limit

        sql = text(f"""
            SELECT 
                id, vendor_id, pickup_datetime, dropoff_datetime,
                pickup_lat, pickup_lon, dropoff_lat, dropoff_lon,
                passenger_count, trip_distance_km, trip_duration_seconds,
                fare_amount, tip_amount, trip_speed_kmh, fare_per_km, 
                tip_pct, hour_of_day, day_of_week
            FROM trips
            WHERE 1=1 {clause}
            ORDER BY pickup_datetime DESC
            LIMIT :limit OFFSET :offset
        """)
        params["limit"] = limit
        params["offset"] = offset

        with engine.connect() as conn:
            rows = [safe_dict(r) for r in conn.execute(sql, params).fetchall()]
            
            # Get total count (with timeout protection)
            try:
                count_sql = text(f"SELECT COUNT(*) AS total FROM trips WHERE 1=1 {clause}")
                total = conn.execute(count_sql, params).fetchone()[0]
            except:
                total = -1  # Unknown if query times out

        return jsonify({"page": page, "limit": limit, "total": total, "rows": rows})
    
    except Exception as e:
        app.logger.error(f"Error in /api/trips: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/insights", methods=["GET"])
def insights():
    """Simplified insights endpoint"""
    try:
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)

        with engine.connect() as conn:
            # 1) Rush-hour peaks
            sql1 = text(f"""
                SELECT COALESCE(hour_of_day, 0) AS hour_of_day, COUNT(*) AS trips
                FROM trips
                WHERE 1=1 {clause} AND hour_of_day IS NOT NULL
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """)
            rush_rows = [safe_dict(r) for r in conn.execute(sql1, params).fetchall()]

            # 2) Morning hotspots
            sql2_morning = text(f"""
                SELECT t.pickup_zone_id, COALESCE(z.zone_name, 'Unknown') AS zone_name, COUNT(*) AS trips
                FROM trips t
                LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
                WHERE 1=1 {clause} AND hour_of_day BETWEEN 7 AND 9
                GROUP BY t.pickup_zone_id, z.zone_name
                HAVING trips > 0
                ORDER BY trips DESC
                LIMIT 10
            """)
            morning_hotspots = [safe_dict(r) for r in conn.execute(sql2_morning, params).fetchall()]

            # 3) Evening hotspots
            sql2_evening = text(f"""
                SELECT t.pickup_zone_id, COALESCE(z.zone_name, 'Unknown') AS zone_name, COUNT(*) AS trips
                FROM trips t
                LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
                WHERE 1=1 {clause} AND hour_of_day BETWEEN 17 AND 19
                GROUP BY t.pickup_zone_id, z.zone_name
                HAVING trips > 0
                ORDER BY trips DESC
                LIMIT 10
            """)
            evening_hotspots = [safe_dict(r) for r in conn.execute(sql2_evening, params).fetchall()]

            # 4) Fare efficiency
            sql3 = text(f"""
                SELECT 
                    t.pickup_zone_id,
                    COALESCE(z.zone_name, 'Unknown') AS zone_name,
                    ROUND(AVG(t.fare_per_km), 2) AS avg_fare_per_km,
                    ROUND(AVG(t.tip_pct)*100, 2) AS avg_tip_pct,
                    COUNT(*) AS trips
                FROM trips t
                LEFT JOIN zones z ON t.pickup_zone_id = z.zone_id
                WHERE 1=1 {clause} 
                    AND t.fare_per_km IS NOT NULL
                    AND t.tip_pct IS NOT NULL
                GROUP BY t.pickup_zone_id, z.zone_name
                HAVING COUNT(*) > 50
                ORDER BY avg_fare_per_km DESC
                LIMIT 20
            """)
            fare_efficiency = [safe_dict(r) for r in conn.execute(sql3, params).fetchall()]

        payload = {
            "insight_1_rush_hour": {
                "explanation": "Trips by hour showing demand peaks",
                "data": rush_rows
            },
            "insight_2_spatial_hotspots": {
                "explanation": "Top pickup zones: morning (7-9am) vs evening (5-7pm)",
                "morning_top10": morning_hotspots,
                "evening_top10": evening_hotspots
            },
            "insight_3_fare_efficiency": {
                "explanation": "Zones by fare per km and tip percentage",
                "data": fare_efficiency
            }
        }
        return jsonify(payload)
    
    except Exception as e:
        app.logger.error(f"Error in /api/insights: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status": "ok",
        "message": "NYC Taxi Analytics API",
        "endpoints": [
            "/api/summary",
            "/api/time-series",
            "/api/hotspots",
            "/api/fare-stats",
            "/api/top-routes",
            "/api/trips",
            "/api/insights"
        ]
    })


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        debug=os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true")
    )
