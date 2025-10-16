#!/bin/bash

# NYC Taxi Dashboard - Production Server Startup Script

echo "Starting NYC Taxi Dashboard API Server..."

# Check if gunicorn is installed
if ! command -v gunicorn &> /dev/null; then
    echo "Gunicorn not found. Installing..."
    pip3 install gunicorn
fi

# Kill any existing processes on port 5001
echo "Stopping any existing servers on port 5001..."
lsof -ti:5001 | xargs kill -9 2>/dev/null || true

# Start the server with gunicorn for production
echo "Starting production server with gunicorn..."
cd backend
python3 -m gunicorn \
    --bind 127.0.0.1:5001 \
    --workers 2 \
    --threads 2 \
    --timeout 60 \
    --keep-alive 2 \
    --max-requests 1000 \
    --max-requests-jitter 50 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    wsgi:app

echo "Server stopped."
