# FINCTRL-AI

Financial Reconciliation AI System with Production-Grade Reliability

A production-ready financial reconciliation system that automatically matches transactions from ERP systems, Razorpay payment gateway, and bank records using deterministic rules and AI-powered investigation for ambiguous cases.

## Features

- **Multi-Stage Reconciliation Pipeline**: Deterministic rule-based matching with AI fallback for complex cases
- **Production-Grade Security**: X-API-Key authentication with role-based access control (ADMIN/READ_ONLY)
- **Webhook Reliability**: Idempotent webhook processing with signature verification and replay capabilities
- **Observability**: Structured JSON logging with correlation IDs for request tracing
- **Database Migrations**: Alembic-based schema migrations with PostgreSQL (production) and SQLite (test) support
- **Razorpay Integration**: Official SDK integration with test mode support
- **AI-Powered Investigation**: OpenRouter/OpenAI integration for complex reconciliation scenarios
- **Docker Support**: Production-ready containerization with docker-compose

## Architecture

### Core Components

1. **API Layer** (`finctrl/backend/api/`)
   - FastAPI-based REST API
   - X-API-Key authentication with RBAC
   - Webhook endpoints with signature verification
   - Health and readiness probes

2. **Reconciliation Engine** (`finctrl/backend/reconciliation/`)
   - Multi-stage deterministic matching
   - Handles ERP ↔ Razorpay ↔ Bank record reconciliation
   - Refund-aware settlement calculations
   - Exception detection and reporting

3. **AI Engine** (`finctrl/backend/engine/`)
   - AI agent for investigating ambiguous cases
   - Policy layer with strict validation rules
   - Hallucination detection and confidence thresholds
   - Tool-use loop with database and Razorpay API access

4. **Database** (`finctrl/backend/database/`)
   - SQLAlchemy async models
   - Alembic migrations
   - PostgreSQL (production) / SQLite (test) support
   - Audit logging and provenance tracking

## Quick Start

### Operational dashboard

The Phase 6E React/TypeScript dashboard lives in `frontend/` and uses the existing
`X-API-Key` authentication contract.

```bash
cd frontend
cp .env.example .env
npm install
npm run dev
```

Use `npm test` and `npm run build` for validation. `VITE_API_BASE_URL` is the only
required browser configuration; never place backend, Razorpay, Gemini, or
OpenRouter credentials in `VITE_*` variables. API keys are entered at runtime.
The backend allows `http://localhost:5173` by default; configure comma-separated
`CORS_ORIGINS` for other deployments.

### Prerequisites

- Python 3.12+
- PostgreSQL 16+ (for production)
- Docker & Docker Compose (optional, for containerized deployment)

### Local Development Setup

1. **Clone the repository**
   ```bash
   cd FINCTRL-AI
   ```

2. **Install dependencies**
   ```bash
   pip install -r finctrl/backend/requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Run database migrations**
   ```bash
   # For test mode (SQLite)
   export APP_MODE=test
   alembic upgrade head

   # For production mode (PostgreSQL)
   export APP_MODE=production
   export DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/finctrl
   alembic upgrade head
   ```

5. **Start the server**
   ```bash
   uvicorn finctrl.backend.api.main:app --reload --host 0.0.0.0 --port 8000
   ```

6. **Test the API**
   ```bash
   curl http://localhost:8000/health
   # Response: {"status":"ok"}
   ```

### Docker Deployment

1. **Set environment variables**
   ```bash
   export ADMIN_API_KEY=your_admin_key_here
   export READ_ONLY_API_KEY=your_readonly_key_here
   export RAZORPAY_KEY_ID=your_razorpay_key
   export RAZORPAY_KEY_SECRET=your_razorpay_secret
   ```

2. **Start services**
   ```bash
   docker-compose up --build
   ```

3. **Access the API**
   - API: http://localhost:8000
   - Health: http://localhost:8000/health
   - API Docs: http://localhost:8000/docs

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `APP_MODE` | Yes | `test` | Application mode: `test` or `production` |
| `DATABASE_URL` | Production | - | PostgreSQL connection string (e.g., `postgresql+asyncpg://user:pass@host:5432/db`) |
| `ADMIN_API_KEY` | Production | - | Admin API key for write operations |
| `READ_ONLY_API_KEY` | Production | - | Read-only API key for read operations |
| `RAZORPAY_MODE` | No | `test` | Razorpay mode: `test` or `live` |
| `RAZORPAY_KEY_ID` | Production | - | Razorpay API key ID |
| `RAZORPAY_KEY_SECRET` | Production | - | Razorpay API secret (also used for webhook verification) |
| `AI_PROVIDER` | No | `openrouter` | AI provider: `openrouter`, `openai`, or `mock` |
| `OPENROUTER_API_KEY` | If using OpenRouter | - | OpenRouter API key |
| `OPENAI_API_KEY` | If using OpenAI | - | OpenAI API key |

### Production Configuration

In production mode (`APP_MODE=production`), the system **fails fast** if required configuration is missing:
- `DATABASE_URL` must be a PostgreSQL connection string
- `ADMIN_API_KEY` and `READ_ONLY_API_KEY` must be set
- `RAZORPAY_KEY_ID` and `RAZORPAY_KEY_SECRET` must be set

### Razorpay Test Mode

For development and testing, you can use Razorpay's test mode:

1. Sign up at https://razorpay.com/
2. Go to Settings → API Keys → Generate Test Key
3. Set environment variables:
   ```bash
   export RAZORPAY_MODE=test
   export RAZORPAY_KEY_ID=rzp_test_xxxxxxxxxxxxx
   export RAZORPAY_KEY_SECRET=your_test_secret
   ```

Test mode allows you to simulate payments, settlements, and webhooks without real transactions.

## API Endpoints

### Public Endpoints

- `GET /health` - Health check
- `GET /ready` - Readiness check
- `POST /webhooks/razorpay` - Razorpay webhook (signature-verified, no API key required)

### Admin Endpoints (Require `X-API-Key: <ADMIN_API_KEY>`)

- `POST /ingest/erp` - Bulk ingest ERP records
- `POST /ingest/rzp` - Bulk ingest Razorpay records
- `POST /ingest/bank` - Bulk ingest bank records
- `POST /reconciliation/run` - Trigger reconciliation
- `POST /ai/investigate/{candidate_id}` - AI investigation
- `POST /ai/process/{candidate_id}` - AI processing
- `POST /ai/process-pending` - Process pending candidates
- `POST /webhooks/replay/{event_id}` - Replay failed webhook

### Read-Only Endpoints (Require `X-API-Key: <READ_ONLY_API_KEY>` or `<ADMIN_API_KEY>`)

- `GET /matches` - List reconciliation matches
- `GET /candidates` - List reconciliation candidates
- `GET /cash-position` - Get current cash position
- `GET /metrics` - Get system metrics
- `GET /ai/investigations/{candidate_id}` - Get investigation logs

### Authentication

All endpoints except `/health`, `/ready`, and `/webhooks/razorpay` require an `X-API-Key` header:

```bash
# Admin request
curl -H "X-API-Key: your_admin_key" http://localhost:8000/reconciliation/run

# Read-only request
curl -H "X-API-Key: your_readonly_key" http://localhost:8000/metrics
```

## Database Migrations

### Create a New Migration

```bash
alembic revision --autogenerate -m "description_of_changes"
```

### Apply Migrations

```bash
# Upgrade to latest
alembic upgrade head

# Upgrade by 1 version
alembic upgrade +1

# Downgrade by 1 version
alembic downgrade -1

# Downgrade to base
alembic downgrade base
```

### View Migration History

```bash
alembic history
alembic current
```

## Testing

Run the full test suite:

```bash
# Run all tests
python -m pytest finctrl/backend/tests

# Run with verbose output
python -m pytest finctrl/backend/tests -v

# Run specific test file
python -m pytest finctrl/backend/tests/test_webhooks.py

# Run with coverage
python -m pytest finctrl/backend/tests --cov=finctrl/backend --cov-report=html
```

Tests automatically use SQLite in-memory database for isolation.

## Webhook Replay

Failed webhooks can be replayed via the admin-only replay endpoint:

```bash
curl -X POST \
  -H "X-API-Key: your_admin_key" \
  http://localhost:8000/webhooks/replay/{event_id}
```

**Replay Rules:**
- Only `FAILED` events can be replayed
- Maximum 5 retry attempts per event
- Successful replay → status becomes `PROCESSED`
- Failed replay → status remains `FAILED`, attempt count increments
- All replay attempts are logged in audit logs

## Observability

### Structured Logging

All logs are JSON-formatted with correlation IDs:

```json
{
  "timestamp": "2026-08-28T16:04:46.120Z",
  "level": "INFO",
  "logger": "finctrl.middleware",
  "message": "Request completed: POST /reconciliation/run - 200",
  "correlation_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "method": "POST",
  "path": "/reconciliation/run",
  "status_code": 200,
  "duration_ms": 1234.56
}
```

### Correlation IDs

Every request gets a unique correlation ID:
- Automatically generated if not provided
- Can be passed via `X-Correlation-ID` header
- Returned in `X-Correlation-ID` response header
- Included in all log entries for request tracing

### Health Checks

- **`/health`**: Basic liveness check (always returns 200 if the app is running)
- **`/ready`**: Readiness check (returns 200 when ready to serve traffic)

Use these endpoints for Kubernetes liveness/readiness probes or load balancer health checks.

## Security Best Practices

1. **Never commit secrets**: Always use environment variables for API keys and credentials
2. **Rotate API keys**: Regularly rotate `ADMIN_API_KEY` and `READ_ONLY_API_KEY`
3. **Use strong keys**: Generate cryptographically secure random keys (minimum 32 characters)
4. **Webhook signature verification**: Razorpay webhooks are verified using HMAC-SHA256 signatures
5. **Least privilege**: Use `READ_ONLY_API_KEY` when write access is not needed
6. **HTTPS in production**: Always use HTTPS/TLS for API communication
7. **Database access**: Restrict database access to the application only
8. **Audit logging**: All sensitive operations are logged with audit trails

## Troubleshooting

### Connection Issues

**Problem**: `asyncpg.exceptions.InvalidPasswordError`

**Solution**: Check `DATABASE_URL` format and credentials:
```bash
export DATABASE_URL=postgresql+asyncpg://user:password@host:5432/database
```

### Migration Issues

**Problem**: `Target database is not up to date`

**Solution**: Run migrations:
```bash
alembic upgrade head
```

### Webhook Verification Failures

**Problem**: `Invalid signature` errors on webhook endpoint

**Solution**: Verify `RAZORPAY_KEY_SECRET` matches your Razorpay dashboard webhook secret.

### Test Mode Not Working

**Problem**: Production validation errors in test environment

**Solution**: Ensure `APP_MODE=test` is set:
```bash
export APP_MODE=test
```

## License

MIT

## Support

For issues, questions, or contributions, please open an issue on the repository.
