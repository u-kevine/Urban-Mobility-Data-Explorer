#!/bin/bash

# NYC Taxi Dashboard - Development Server Startup Script

echo "Starting NYC Taxi Dashboard in development mode..."

# Kill any existing processes on port 5001
echo "Stopping any existing servers on port 5001..."
lsof -ti:5001 | xargs kill -9 2>/dev/null || true

# Set development environment
export FLASK_DEBUG=true

# Start the development server
echo "Starting development server..."
cd backend
python3 app.py
