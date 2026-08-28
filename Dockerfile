FROM python:3.11-slim

WORKDIR /app

COPY finctrl/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8000

# Start script
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]
