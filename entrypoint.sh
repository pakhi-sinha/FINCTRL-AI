#!/bin/bash
set -e

echo "Running Database Migrations..."
cd /app/finctrl/backend
alembic upgrade head
cd /app

echo "Starting Application..."
uvicorn finctrl.backend.api.main:app --host 0.0.0.0 --port 8000 --proxy-headers
