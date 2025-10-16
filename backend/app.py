import os
from datetime import datetime, timedelta
from math import isnan

from flask import Flask, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import random
import json

# Optional: load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:password@localhost:3306/nyc_taxi")

# Try to create engine, but fall back to mock data if database is not available
try:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    USE_MOCK_DATA = False
except Exception as e:
    print(f"Database connection failed: {e}")
    print("Using mock data instead...")
    engine = None
    USE_MOCK_DATA = True

app = Flask(__name__)
CORS(app)


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
# Mock Data Generation
# -------------------------
def generate_mock_trips(count=200):
    """Generate mock trip data for demonstration"""
    trips = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(count):
        # Random pickup time within last 30 days
        pickup_time = base_date.replace(
            day=random.randint(1, 30),
            hour=random.randint(0, 23),
            minute=random.randint(0, 59)
        )
        
        # Trip duration between 5 minutes and 2 hours
        duration_seconds = random.randint(300, 7200)
        dropoff_time = pickup_time + timedelta(seconds=duration_seconds)
        
        # NYC coordinates (approximate bounds)
        pickup_lat = round(random.uniform(40.4774, 40.9176), 6)
        pickup_lon = round(random.uniform(-74.2591, -73.7004), 6)
        dropoff_lat = round(random.uniform(40.4774, 40.9176), 6)
        dropoff_lon = round(random.uniform(-74.2591, -73.7004), 6)
        
        # Distance and fare calculations
        distance_km = round(random.uniform(0.5, 25.0), 2)
        fare_amount = round(random.uniform(5.0, 80.0), 2)
        tip_amount = round(random.uniform(0.0, fare_amount * 0.3), 2)
        speed_kmh = round(distance_km / (duration_seconds / 3600), 1) if duration_seconds > 0 else 0
        fare_per_km = round(fare_amount / distance_km, 2) if distance_km > 0 else 0
        tip_pct = round(tip_amount / fare_amount, 3) if fare_amount > 0 else 0
        
        trip = {
            'id': i + 1,
            'vendor_id': random.randint(1, 3),
            'pickup_datetime': pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
            'dropoff_datetime': dropoff_time.strftime('%Y-%m-%d %H:%M:%S'),
            'pickup_lat': pickup_lat,
            'pickup_lon': pickup_lon,
            'dropoff_lat': dropoff_lat,
            'dropoff_lon': dropoff_lon,
            'passenger_count': random.randint(1, 6),
            'trip_distance_km': distance_km,
            'trip_duration_seconds': duration_seconds,
            'fare_amount': fare_amount,
            'tip_amount': tip_amount,
            'trip_speed_kmh': speed_kmh,
            'fare_per_km': fare_per_km,
            'tip_pct': tip_pct,
            'hour_of_day': pickup_time.hour,
            'day_of_week': pickup_time.strftime('%A')
        }
        trips.append(trip)
    
    return trips

# Generate mock data once at startup
MOCK_TRIPS = generate_mock_trips(1000) if USE_MOCK_DATA else []

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
        if USE_MOCK_DATA:
            # Use mock data
            trips = MOCK_TRIPS
            if not trips:
                return jsonify({"error": "No mock data available"}), 404
            
            total_trips = len(trips)
            avg_distance_km = round(sum(t['trip_distance_km'] for t in trips) / total_trips, 3)
            avg_fare = round(sum(t['fare_amount'] for t in trips) / total_trips, 2)
            avg_tip = round(sum(t['tip_amount'] for t in trips) / total_trips, 2)
            avg_speed_kmh = round(sum(t['trip_speed_kmh'] for t in trips) / total_trips, 2)
            
            return jsonify({
                'total_trips': total_trips,
                'avg_distance_km': avg_distance_km,
                'avg_fare': avg_fare,
                'avg_tip': avg_tip,
                'avg_speed_kmh': avg_speed_kmh
            })
        
        # Database mode
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
        if USE_MOCK_DATA:
            # Use mock data
            limit = min(int(request.args.get("limit", 200)), 1000)
            offset = int(request.args.get("offset", 0))
            
            # Simple pagination of mock data
            total = len(MOCK_TRIPS)
            start_idx = offset
            end_idx = min(offset + limit, total)
            page_data = MOCK_TRIPS[start_idx:end_idx]
            
            return jsonify({
                "data": page_data,
                "pagination": {
                    "page": (offset // limit) + 1,
                    "limit": limit,
                    "total": total,
                    "offset": offset
                }
            })
        
        # Original database code
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

        return jsonify({
            "data": rows,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "offset": offset
            }
        })
    
    except Exception as e:
        app.logger.error(f"Error in /api/trips: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/heatmap-manual", methods=["GET"])
def heatmap_manual():
    """Manual heatmap endpoint for pickup locations"""
    try:
        if USE_MOCK_DATA:
            # Generate mock heatmap data
            k = min(int(request.args.get("k", 100)), 500)
            precision = int(request.args.get("precision", 3))
            
            # Group mock trips by rounded coordinates
            coord_counts = {}
            for trip in MOCK_TRIPS:
                lat = round(trip['pickup_lat'], precision)
                lon = round(trip['pickup_lon'], precision)
                key = (lat, lon)
                coord_counts[key] = coord_counts.get(key, 0) + 1
            
            # Convert to list and sort by count
            heatmap_data = [
                {"lat": lat, "lon": lon, "count": count}
                for (lat, lon), count in coord_counts.items()
            ]
            heatmap_data.sort(key=lambda x: x['count'], reverse=True)
            
            return jsonify({
                "precision": precision,
                "sampled": len(heatmap_data[:k]),
                "k": k,
                "data": heatmap_data[:k]
            })
        
        # Original database code
        precision = int(request.args.get("precision", 3))
        limit_rows = min(int(request.args.get("limitRows", 50000)), 100000)
        k = min(int(request.args.get("k", 10000)), 50000)
        
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)
        
        # Round coordinates to create a grid
        sql = text(f"""
            SELECT 
                ROUND(pickup_lat, :precision) AS lat,
                ROUND(pickup_lon, :precision) AS lon,
                COUNT(*) AS count
            FROM trips
            WHERE 1=1 {clause}
                AND pickup_lat IS NOT NULL 
                AND pickup_lon IS NOT NULL
                AND pickup_lat BETWEEN 40.4 AND 40.9
                AND pickup_lon BETWEEN -74.3 AND -73.7
            GROUP BY lat, lon
            HAVING count > 0
            ORDER BY count DESC
            LIMIT :k
        """)
        params.update({"precision": precision, "k": k})
        
        with engine.connect() as conn:
            rows = [safe_dict(r) for r in conn.execute(sql, params).fetchall()]
            
        return jsonify({
            "precision": precision,
            "sampled": len(rows),
            "k": k,
            "data": rows
        })
    
    except Exception as e:
        app.logger.error(f"Error in /api/heatmap-manual: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/top-routes-manual", methods=["GET"])
def top_routes_manual():
    """Manual top routes endpoint"""
    try:
        if USE_MOCK_DATA:
            # Generate mock top routes data
            k = min(int(request.args.get("k", 10)), 50)
            precision = int(request.args.get("precision", 3))
            
            # Group mock trips by rounded route coordinates
            route_counts = {}
            for trip in MOCK_TRIPS:
                pickup_lat = round(trip['pickup_lat'], precision)
                pickup_lon = round(trip['pickup_lon'], precision)
                dropoff_lat = round(trip['dropoff_lat'], precision)
                dropoff_lon = round(trip['dropoff_lon'], precision)
                
                key = (pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
                if key not in route_counts:
                    route_counts[key] = {
                        'count': 0,
                        'total_distance': 0,
                        'total_fare': 0
                    }
                
                route_counts[key]['count'] += 1
                route_counts[key]['total_distance'] += trip['trip_distance_km']
                route_counts[key]['total_fare'] += trip['fare_amount']
            
            # Convert to list and calculate averages
            routes_data = []
            for (pickup_lat, pickup_lon, dropoff_lat, dropoff_lon), stats in route_counts.items():
                if stats['count'] > 1:  # Only routes with multiple trips
                    routes_data.append({
                        "pickup_lat": pickup_lat,
                        "pickup_lon": pickup_lon,
                        "dropoff_lat": dropoff_lat,
                        "dropoff_lon": dropoff_lon,
                        "count": stats['count'],
                        "avg_distance": round(stats['total_distance'] / stats['count'], 2),
                        "avg_fare": round(stats['total_fare'] / stats['count'], 2)
                    })
            
            # Sort by count and return top k
            routes_data.sort(key=lambda x: x['count'], reverse=True)
            
            return jsonify({
                "precision": precision,
                "sampled": len(routes_data[:k]),
                "k": k,
                "data": routes_data[:k]
            })
        
        # Original database code
        precision = int(request.args.get("precision", 3))
        limit_rows = min(int(request.args.get("limitRows", 50000)), 100000)
        k = min(int(request.args.get("k", 10)), 100)
        
        start = parse_date_param("start")
        end = parse_date_param("end")
        params = {}
        clause = date_filter_clause(params, start, end)
        
        sql = text(f"""
            SELECT 
                ROUND(pickup_lat, :precision) AS pickup_lat,
                ROUND(pickup_lon, :precision) AS pickup_lon,
                ROUND(dropoff_lat, :precision) AS dropoff_lat,
                ROUND(dropoff_lon, :precision) AS dropoff_lon,
                COUNT(*) AS count,
                ROUND(AVG(trip_distance_km), 2) AS avg_distance,
                ROUND(AVG(fare_amount), 2) AS avg_fare
            FROM trips
            WHERE 1=1 {clause}
                AND pickup_lat IS NOT NULL 
                AND pickup_lon IS NOT NULL
                AND dropoff_lat IS NOT NULL 
                AND dropoff_lon IS NOT NULL
                AND pickup_lat BETWEEN 40.4 AND 40.9
                AND pickup_lon BETWEEN -74.3 AND -73.7
                AND dropoff_lat BETWEEN 40.4 AND 40.9
                AND dropoff_lon BETWEEN -74.3 AND -73.7
            GROUP BY pickup_lat, pickup_lon, dropoff_lat, dropoff_lon
            HAVING count > 1
            ORDER BY count DESC
            LIMIT :k
        """)
        params.update({"precision": precision, "k": k})
        
        with engine.connect() as conn:
            rows = [safe_dict(r) for r in conn.execute(sql, params).fetchall()]
            
        return jsonify({
            "precision": precision,
            "sampled": len(rows),
            "k": k,
            "data": rows
        })
    
    except Exception as e:
        app.logger.error(f"Error in /api/top-routes-manual: {e}")
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
    if USE_MOCK_DATA:
        return jsonify({
            "status": "healthy", 
            "database": "mock_data", 
            "trips_count": len(MOCK_TRIPS)
        })
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


if __name__ == "__main__":
    # For development, run only on localhost for security
    # In production, use a proper WSGI server like gunicorn
    app.run(
        host="127.0.0.1",  # Only bind to localhost for security
        port=int(os.getenv("PORT", 5001)),
        debug=os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true"),
        threaded=True  # Enable threading for better performance
    )
