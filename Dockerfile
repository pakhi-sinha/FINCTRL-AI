# FINCTRL-AI Production Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY finctrl/backend/requirements.txt /app/finctrl/backend/

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/finctrl/backend/requirements.txt

# Copy application code
COPY finctrl /app/finctrl
COPY alembic /app/alembic
COPY alembic.ini /app/

# Create non-root user
RUN useradd -m -u 1000 finctrl && chown -R finctrl:finctrl /app
USER finctrl

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

# Run migrations then start server
CMD ["sh", "-c", "alembic upgrade head && uvicorn finctrl.backend.api.main:app --host 0.0.0.0 --port 8000"]
