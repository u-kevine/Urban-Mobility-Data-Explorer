import pandas as pd
import numpy as np

def clean_data(filepath):
    df = pd.read_csv(filepath)
    df.dropna(subset=["pickup_datetime", "dropoff_datetime", "trip_distance", "fare_amount"], inplace=True)
    df.drop_duplicates(inplace=True)

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
    df["trip_duration_min"] = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60

    # Derived features
    df["avg_speed_kmh"] = df["trip_distance"] / (df["trip_duration_min"] / 60)
    df["fare_per_km"] = df["fare_amount"] / df["trip_distance"]

    # Remove invalid speeds and fares
    df = df[(df["avg_speed_kmh"] < 150) & (df["fare_amount"] > 0)]

    df.to_csv("data/processed/cleaned_trips.csv", index=False)
    return df

