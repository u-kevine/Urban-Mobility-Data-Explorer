from flask import Blueprint, jsonify, request
from sqlalchemy import create_engine, text
from config import DB_URI

trip_bp = Blueprint("trip", __name__)
engine = create_engine(DB_URI)

@trip_bp.route("/summary", methods=["GET"])
def summary():
    query = """
    SELECT 
        AVG(trip_distance) AS avg_distance,
        AVG(fare_amount) AS avg_fare,
        AVG(tip_amount) AS avg_tip
    FROM trips;
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    return jsonify(dict(result))

@trip_bp.route("/top_speeds", methods=["GET"])
def top_speeds():
    query = "SELECT * FROM trips ORDER BY avg_speed_kmh DESC LIMIT 10;"
    with engine.connect() as conn:
        rows = conn.execute(text(query)).fetchall()
    return jsonify([dict(row) for row in rows])

