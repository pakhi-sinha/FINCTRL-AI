# FINCTRL-AI

FINCTRL is an AI-augmented reconciliation engine for financial operations.

## Phase 5 - Production Hardening

This phase hardens FINCTRL for production deployment.

### Features

- **Dockerized Deployment**: Fully containerized using Docker and Docker Compose for easy deployment.
- **PostgreSQL Database**: Configured to use PostgreSQL in production for robust data integrity and performance.
- **Alembic Migrations**: Structured database schema management using Alembic.
- **Authentication & Authorization**: Role-Based Access Control (RBAC) via API Keys (`X-API-Key`).
- **Webhook Idempotency & Replay**: Hardened webhook endpoints designed to handle duplicate deliveries gracefully, and replay endpoints available for admins.
- **Observability**: Structured application logging with correlation IDs for tracing.

### Deployment Instructions

#### 1. Setup Environment
Copy the example environment file and configure your secrets:
```bash
cp .env.example .env
```
Ensure you provide valid values for all required variables in `.env` (like `DATABASE_URL`, API Keys, and Razorpay credentials). For production, `APP_MODE` must be set to `production`.

#### 2. Start Services
Build and start the application along with the PostgreSQL database using Docker Compose:
```bash
docker-compose up --build -d
```
The application will be exposed on port `8000`.

#### 3. Database Migrations
The database schema migrations are automatically executed before the Uvicorn application server starts in the provided `entrypoint.sh` script via `alembic upgrade head`.

To generate new migrations after modifying SQLAlchemy models:
```bash
docker-compose exec backend alembic revision --autogenerate -m "Description"
```

#### 4. Testing
In-memory SQLite is used by default when running pytest locally. Run the test suite using:
```bash
python -m pytest finctrl/backend/tests
```
