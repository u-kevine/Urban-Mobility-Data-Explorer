**# Urban-Mobility-Data-Explorer
**
# Project Overview

The Trip Data Dashboard is a web-based tool for analyzing urban taxi trip data. It allows users to explore trip patterns, fare trends, trip durations, busiest hours, and top routes. The dashboard includes interactive charts, filters, and custom algorithms to generate insights for urban mobility planning.

**#Features**

-Display trips in a dynamic table.

-Histogram charts: Trip duration, distance, and speed.

-Busiest hours chart (top 5).

-Daily fare trend line chart.

-Heatmap visualization of trip locations (manual clustering).

-Top routes identification (manual aggregation).

-Filters: Date range, hour, distance, fare.

-Light/dark theme toggle.

#Dataset

-Source: Urban taxi trip records.

-Key fields:

-pickup_datetime, dropoff_datetime

-trip_duration (seconds)

-trip_distance (km)

-trip_speed_kmh (km/h)

-fare_amount, fare_per_km

-passenger_count

#Challenges:
Missing values, outliers, inconsistent timestamps.

Cleaning assumptions: Invalid or negative values removed from stats/charts.

System Architecture
[Frontend: HTML/CSS/JS + Chart.js] → [Backend: Node.js/Express API] → [Data: CSV/JSON]


Frontend: Interactive charts and filters using Chart.js and vanilla JS.

Backend: Node.js with Express serving trip and algorithm endpoints.

Data: CSV/JSON files read into memory, filtered, and served via API.

Manual Algorithms: Implemented top routes aggregation and heatmap clustering without libraries.

Installation

Clone repository:

git clone https://github.com/yourusername/trip-dashboard.git
cd trip-dashboard


Install dependencies:

npm install


Start the server:

npm run start


Open index.html in a browser or navigate to http://localhost:3000.

Usage

Use filters to select specific trips by date, hour, distance, and fare.

Hover over charts for details.

Toggle light/dark mode with the button in the top-right.

Heatmap and top routes dynamically update via manual aggregation endpoints.

Manual Algorithms

Top Routes Aggregation:

Round pickup/dropoff coordinates to a precision (e.g., 3 decimal places).

Use a Map object:

Key = pickup_lat,pickup_lon,dropoff_lat,dropoff_lon

Value = trip count.

Select top k routes by count.

Pseudo-code:

routeCount = {}
for trip in trips:
    key = round(trip.pickup_lat,3)+","+round(trip.pickup_lon,3)+
          ","+round(trip.dropoff_lat,3)+","+round(trip.dropoff_lon,3)
    routeCount[key] = routeCount.get(key,0)+1
topRoutes = select k keys with highest counts


Complexity: O(n log k) time, O(n) space.

Insights

Busiest Hours: 8–9 AM and 5–7 PM.

Visualization: Bar chart.

Insight: Peak commuting times for city planning.

Fare Trends: Daily average fare_per_km higher on weekends.

Visualization: Line chart.

Insight: Reflects demand-driven pricing.

Trip Duration Distribution: Most trips 5–15 minutes; long tail beyond 45 minutes.

Visualization: Histogram.

Insight: Short trips dominate urban mobility; long trips may indicate congestion or anomalies.

Technical Decisions

Stack Choice: Lightweight vanilla JS frontend + Node.js backend.

Trade-offs: Flat file data for simplicity; may not scale beyond 50,000 rows.

Manual Algorithms: Avoided built-in sorting or counting libraries to demonstrate algorithmic understanding.

Charts: Chart.js chosen for responsive and interactive visualizations.

Future Work

Integrate a database for scalability (PostgreSQL or MongoDB).

Stream real-time trip data and visualize live heatmaps.

Predictive analytics: trip duration, fare estimation.

Export filtered datasets for further analysis.

Enhance UI/UX for mobile devices.
