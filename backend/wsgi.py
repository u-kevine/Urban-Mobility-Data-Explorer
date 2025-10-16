#!/usr/bin/env python3
"""
WSGI entry point for production deployment
Usage: gunicorn --bind 127.0.0.1:5001 wsgi:app
"""

from app import app

if __name__ == "__main__":
    app.run()
