#!/usr/bin/env python3
"""

import argparse
import os
import sys
import csv
from datetime import datetime
from math import isfinite

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode

# Geographic bounds (NYC approx)
MIN_LAT, MAX_LAT = 40.4, 40.95
MIN_LON, MAX_LON = -74.35, -73.7

# Output log path
LOG_DIR = "data/logs"
os.makedirs(LOG_DIR, exist_ok=True)
CLEANING_LOG = os.path.join(LOG_DIR, "cleaning_log.csv")

# Canonical columns we'll insert into MySQL trips table
CANONICAL_COLS = [
    "pickup_datetime", "dropoff_datetime",
    "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon",
    "passenger_count", "trip_distance_km", "trip_duration_seconds",
    "fare_amount", "tip_amount", "trip_speed_kmh", "fare_per_km",
    "tip_pct", "hour_of_day", "day_of_week", "vendor_code"
]


def is_valid_coordinate(lat, lon):
    try:
        if pd.isna(lat) or pd.isna(lon):
            return False
        lat = float(lat); lon = float(lon)
        return (MIN_LAT <= lat <= MAX_LAT) and (MIN_LON <= lon <= MAX_LON)
    except Exception:
        return False


def safe_div(a, b):
    try:
        if b is None:
            return None
        if pd.isna(a) or pd.isna(b):
            return None
        if float(b) == 0.0:
            return None
        return float(a) / float(b)
    except Exception:
        return None


def normalize_columns(df):
    # minimal normalization (lowercase names & strip)
    df = df.rename(columns={c: c.strip() for c in df.columns})
    df.columns = [c.lower() for c in df.columns]
    return df


def detect_and_assign_columns(df):
    """
    Ensure presence of key columns by mapping common variants.
    Returns df with canonical names where possible.
    """
    df = normalize_columns(df)

    # map pickup/dropoff datetime
    for c in ["tpep_pickup_datetime", "pickup_datetime", "pickup_time", "pickup_ts"]:
        if c in df.columns:
            df["pickup_datetime"] = pd.to_datetime(df[c], errors="coerce")
            break
    for c in ["tpep_dropoff_datetime", "dropoff_datetime", "dropoff_time", "dropoff_ts"]:
        if c in df.columns:
            df["dropoff_datetime"] = pd.to_datetime(df[c], errors="coerce")
            break

    # coords
    for c in ["pickup_longitude", "pickup_lon", "pickup_long"]:
        if c in df.columns:
            df["pickup_lon"] = pd.to_numeric(df[c], errors="coerce")
            break
    for c in ["pickup_latitude", "pickup_lat", "pickup_latitude_decimal"]:
        if c in df.columns:
            df["pickup_lat"] = pd.to_numeric(df[c], errors="coerce")
            break
    for c in ["dropoff_longitude", "dropoff_lon", "dropoff_long"]:
        if c in df.columns:
            df["dropoff_lon"] = pd.to_numeric(df[c], errors="coerce")
            break
    for c in ["dropoff_latitude", "dropoff_lat", "dropoff_latitude_decimal"]:
        if c in df.columns:
            df["dropoff_lat"] = pd.to_numeric(df[c], errors="coerce")
            break

    # distance
    for c in ["trip_distance", "distance", "tripdistance"]:
        if c in df.columns:
            df["trip_distance_km"] = pd.to_numeric(df[c], errors="coerce")
            break

    # fare
    for c in ["fare_amount", "fare", "fareamount"]:
        if c in df.columns:
            df["fare_amount"] = pd.to_numeric(df[c], errors="coerce")
            break

    # tip
    for c in ["tip_amount", "tip", "tipamount"]:
        if c in df.columns:
            df["tip_amount"] = pd.to_numeric(df[c], errors="coerce")
            break
    if "tip_amount" not in df.columns:
        df["tip_amount"] = 0.0

    # passenger count
    if "passenger_count" in df.columns:
        df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").fillna(1).astype(int)
    else:
        df["passenger_count"] = 1

    # vendor
    if "vendor_id" in df.columns:
        df["vendor_code"] = df["vendor_id"].astype(str)
    elif "vendor" in df.columns:
        df["vendor_code"] = df["vendor"].astype(str)
    else:
        df["vendor_code"] = None

    # Convert trip_distance_km from miles to km if heuristic suggests miles
    if "trip_distance_km" in df.columns:
        s = df["trip_distance_km"].dropna()
        if len(s) > 0 and s.mean() < 200 and s.median() < 30:
            df["trip_distance_km"] = df["trip_distance_km"] * 1.60934

    return df


def clean_chunk(df):
    df = detect_and_assign_columns(df)

    # compute duration if timestamps exist
    if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
        df["trip_duration_seconds"] = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds()
    else:
        df["trip_duration_seconds"] = np.nan

    # derived features
    def compute_speed(row):
        try:
            td = row["trip_duration_seconds"]
            dist = row["trip_distance_km"]
            if pd.isna(td) or td <= 0 or pd.isna(dist) or dist <= 0:
                return np.nan
            hours = td / 3600.0
            return dist / hours
        except Exception:
            return np.nan

    df["trip_speed_kmh"] = df.apply(compute_speed, axis=1)
    df["fare_per_km"] = df.apply(lambda r: safe_div(r.get("fare_amount", np.nan), r.get("trip_distance_km", np.nan)), axis=1)
    df["tip_pct"] = df.apply(lambda r: safe_div(r.get("tip_amount", 0.0), r.get("fare_amount", np.nan)), axis=1)
    df["hour_of_day"] = df["pickup_datetime"].dt.hour.where(df["pickup_datetime"].notnull(), None)
    df["day_of_week"] = df["pickup_datetime"].dt.day_name().where(df["pickup_datetime"].notnull(), None)

    # validate rows and split clean/excluded
    clean_rows = []
    excluded_rows = []

    for _, row in df.iterrows():
        reasons = []
        # timestamps
        if pd.isna(row.get("pickup_datetime")) or pd.isna(row.get("dropoff_datetime")):
            reasons.append("missing_timestamps")
        else:
            if row["dropoff_datetime"] < row["pickup_datetime"]:
                reasons.append("dropoff_before_pickup")

        # coords
        if not is_valid_coordinate(row.get("pickup_lat"), row.get("pickup_lon")):
            reasons.append("invalid_pickup_coord")
        if not is_valid_coordinate(row.get("dropoff_lat"), row.get("dropoff_lon")):
            reasons.append("invalid_dropoff_coord")

        # distance & duration
        if pd.isna(row.get("trip_distance_km")) or row.get("trip_distance_km") < 0:
            reasons.append("invalid_distance")
        if pd.isna(row.get("trip_duration_seconds")) or row.get("trip_duration_seconds") <= 0:
            reasons.append("invalid_duration")

        # fare
        if pd.isna(row.get("fare_amount")) or row.get("fare_amount") < 0:
            reasons.append("invalid_fare")

        # speed
        speed = row.get("trip_speed_kmh")
        if (speed is not None) and (pd.notna(speed)) and (isfinite(speed)) and (speed > 200):
            reasons.append("unrealistic_speed")

        if len(reasons) == 0:
            # compose canonical row
            canonical = {c: (row[c] if c in row.index else None) for c in CANONICAL_COLS}
            # Ensure datetimes are strings in proper format for MySQL insertion
            if canonical.get("pickup_datetime") is not None and not pd.isna(canonical["pickup_datetime"]):
                canonical["pickup_datetime"] = canonical["pickup_datetime"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                canonical["pickup_datetime"] = None
            if canonical.get("dropoff_datetime") is not None and not pd.isna(canonical["dropoff_datetime"]):
                canonical["dropoff_datetime"] = canonical["dropoff_datetime"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                canonical["dropoff_datetime"] = None

            # Ensure numeric types
            for ncol in ["pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon",
                         "passenger_count", "trip_distance_km", "trip_duration_seconds",
                         "fare_amount", "tip_amount", "trip_speed_kmh", "fare_per_km", "tip_pct", "hour_of_day"]:
                if ncol in canonical:
                    v = canonical[ncol]
                    if pd.isna(v):
                        canonical[ncol] = None
            clean_rows.append(canonical)
        else:
            r = {"reasons": ";".join(reasons)}
            excluded_rows.append(r)

    return clean_rows, excluded_rows


def create_table_if_not_exists_mysql(conn, table_name="trips"):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        vendor_code VARCHAR(50),
        pickup_datetime DATETIME NOT NULL,
        dropoff_datetime DATETIME NOT NULL,
        pickup_lat DOUBLE,
        pickup_lon DOUBLE,
        dropoff_lat DOUBLE,
        dropoff_lon DOUBLE,
        passenger_count INT,
        trip_distance_km DOUBLE,
        trip_duration_seconds DOUBLE,
        fare_amount DOUBLE,
        tip_amount DOUBLE,
        trip_speed_kmh DOUBLE,
        fare_per_km DOUBLE,
        tip_pct DOUBLE,
        hour_of_day TINYINT,
        day_of_week VARCHAR(16),
        INDEX idx_pickup_datetime (pickup_datetime),
        INDEX idx_hour_of_day (hour_of_day),
        INDEX idx_fare_amount (fare_amount)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cur = conn.cursor()
    cur.execute(create_sql)
    conn.commit()
    cur.close()


def insert_rows_mysql(conn, table_name, rows, batch_size=1000):
    if not rows:
        return 0
    cols = CANONICAL_COLS
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join([f"`{c}`" for c in cols])
    insert_sql = f"INSERT INTO `{table_name}` ({collist}) VALUES ({placeholders})"
    cur = conn.cursor()
    count = 0
    # insert in smaller batches
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        vals = []
        for r in batch:
            vals.append(tuple(r.get(c) for c in cols))
        cur.executemany(insert_sql, vals)
        conn.commit()
        count += len(batch)
    cur.close()
    return count


def parse_args():
    p = argparse.ArgumentParser(description="ETL (clean + load) into MySQL for NYC Taxi dataset")
    p.add_argument("--input", required=True, help="Path to raw CSV (train.csv)")
    p.add_argument("--mysql-host", default="localhost")
    p.add_argument("--mysql-port", type=int, default=3306)
    p.add_argument("--mysql-user", required=True)
    p.add_argument("--mysql-password", required=True)
    p.add_argument("--mysql-db", required=True)
    p.add_argument("--table", default="trips")
    p.add_argument("--create-table", action="store_true", help="Create trips table if not exists")
    p.add_argument("--chunksize", type=int, default=200000)
    p.add_argument("--batch-size", type=int, default=1000)
    return p.parse_args()


def main():
    args = parse_args()

    # connect to mysql
    try:
        conn = mysql.connector.connect(
            host=args.mysql_host,
            port=args.mysql_port,
            user=args.mysql_user,
            password=args.mysql_password,
            database=args.mysql_db,
            autocommit=False,
            charset="utf8mb4"
        )
    except mysql.connector.Error as err:
        print("MySQL connection error:", err)
        sys.exit(1)

    if args.create_table:
        print("Ensuring target table exists...")
        create_table_if_not_exists_mysql(conn, table_name=args.table)

    total_in = 0
    total_clean = 0
    total_excluded = 0
    excluded_summary = []

    # ensure log header
    if not os.path.exists(CLEANING_LOG):
        with open(CLEANING_LOG, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["chunk_index", "excluded_count", "sample_reason"])

    chunk_index = 0
    for chunk in pd.read_csv(args.input, chunksize=args.chunksize, low_memory=False):
        chunk_index += 1
        total_in += len(chunk)
        print(f"[Chunk {chunk_index}] read {len(chunk)} rows")
        clean_rows, excluded_rows = clean_chunk(chunk)
        inserted = insert_rows_mysql(conn, args.table, clean_rows, batch_size=args.batch_size)
        total_clean += inserted
        total_excluded += len(excluded_rows)
        excluded_summary.append((chunk_index, len(excluded_rows), excluded_rows[:3]))

        # append excluded summary to log file
        with open(CLEANING_LOG, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            sample_reason = excluded_rows[0]["reasons"] if excluded_rows else ""
            writer.writerow([chunk_index, len(excluded_rows), sample_reason])

        print(f"[Chunk {chunk_index}] cleaned={len(clean_rows)} inserted={inserted} excluded={len(excluded_rows)}")

    conn.close()
    print("ETL complete.")
    print(f"Total rows read: {total_in}")
    print(f"Total cleaned & inserted: {total_clean}")
    print(f"Total excluded: {total_excluded}")
    print(f"Cleaning log: {CLEANING_LOG}")


if __name__ == "__main__":
    main()
