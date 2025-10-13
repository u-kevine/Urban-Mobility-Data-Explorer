from sqlalchemy import Column, Integer, Float, String, DateTime
from database import Base

class Trip(Base):
    __tablename__ = "trips"

    id = Column(Integer, primary_key=True)
    pickup_datetime = Column(DateTime)
    dropoff_datetime = Column(DateTime)
    pickup_longitude = Column(Float)
    pickup_latitude = Column(Float)
    dropoff_longitude = Column(Float)
    dropoff_latitude = Column(Float)
    passenger_count = Column(Integer)
    trip_distance = Column(Float)
    fare_amount = Column(Float)
    tip_amount = Column(Float)
    total_amount = Column(Float)
    avg_speed_kmh = Column(Float)
    fare_per_km = Column(Float)

