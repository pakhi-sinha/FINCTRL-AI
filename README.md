# FINCTRL-AI

FINCTRL-AI is a financial-operations control system for reconciling ERP records, Razorpay payments and settlements, and bank transactions. It combines deterministic reconciliation with an exception workbench, evidence-bound AI investigation, mandatory human approval, and an auditable React/TypeScript control room.

The system is designed around a control loop: ingest authoritative financial facts, reconcile what can be resolved deterministically, retain ambiguity as an exception, investigate that exception without changing source facts, and require a person to approve or reject the recommendation.

## Why it exists

Payment operations rarely line up as a simple one-to-one join. ERP references may be incomplete, settlements can consolidate multiple payments, fees and tax change net amounts, refunds reverse earlier activity, and bank credits can arrive later than the underlying payment. A useful reconciliation system must explain unresolved cases without silently rewriting financial history.

FINCTRL-AI implements that workflow end to end:

1. **Ingest** ERP, Razorpay, and bank facts through batch APIs, Razorpay API synchronization, or signed Razorpay webhooks.
2. **Reconcile** the three sources through deterministic matching stages, including consolidated settlements, fee differences, partial references, timing differences, and refunds.
3. **Create exceptions** for unresolved or discrepant records, with links back to their authoritative evidence.
4. **Investigate with AI** using only the evidence assembled for an exception. Provider output must pass a strict structured schema and reference evidence that exists in the case.
5. **Approve or reject** the advisory investigation through admin-only endpoints. Approval state is separate from financial facts and does not mutate them.
6. **Audit and recover** through correlation IDs, audit records, idempotent operations, bounded retries, durable leases, and a recovery worker.

## Architecture

```text
 ERP batches       Razorpay API + webhooks       Bank batches
     |                       |                        |
     +-----------------------+------------------------+
                             v
                  Authoritative source ledger
                             |
                             v
              Deterministic reconciliation engine
                    | matches       | candidates
                    |               v
                    |       Exception + evidence
                    |               |
                    |               v
                    |      Structured AI investigation
                    |               |
                    |               v
                    |       Human approve / reject
                    |               |
                    +---------------+------------------+
                                    v
                         Reports, metrics, cash view
                                    |
                                    v
                       FINCTRL Control Room (React)

 PostgreSQL stores operational state and audit history; the recovery worker
 reclaims expired reconciliation, investigation, and webhook leases.
```

## Core capabilities

### ERP, Razorpay, and bank reconciliation

The reconciliation engine preserves source records and produces matches, candidates, evidence, and exceptions as separate operational records. Controlled runs expose stage status and counts, accept an idempotency key, support bounded linked retries, and can be scoped by Unix timestamp. Reconciliation periods add reporting, close-readiness checks, close/reopen controls, and protection against ingesting or retrying activity into a closed period.

Razorpay data can arrive through legacy batch ingestion, paginated API synchronization, or webhooks. The connector synchronizes orders, payments, settlements, and refunds, records per-resource sync state, uses provider identities for deduplication, and retries transient connector failures with backoff.

### Reliable Razorpay webhooks

`POST /webhooks/razorpay` is public only in the API-key sense; every request still requires `X-Razorpay-Signature` and `X-Razorpay-Event-Id`. Before processing, the API:

- limits the request body to 256 KiB;
- verifies the raw body with HMAC-SHA256 and the dedicated `RAZORPAY_WEBHOOK_SECRET` using constant-time comparison;
- rejects malformed JSON, reused event IDs with conflicting payloads, and provider-identity conflicts;
- persists the raw payload and hash, and makes repeated delivery idempotent;
- coordinates concurrent processing and records processing state.

Failed events can be replayed with the admin-only `POST /webhooks/replay/{event_id}` endpoint. Replay keeps the original event identity and payload, only accepts failed events, and stops after five attempts. The recovery worker can reclaim expired webhook work as well as interrupted reconciliation runs and AI investigations using database-time leases and fenced ownership.

### Evidence-bound AI investigation

An admin can start an investigation for a reconciliation exception. The service constructs a case from persisted exception evidence, calls the configured Gemini or OpenRouter provider, and validates the response against a closed schema. The stored result contains:

- classification: `MATCHING_ERROR`, `MISSING_RECORD`, `TIMING_DIFFERENCE`, `AMOUNT_DIFFERENCE`, `DUPLICATE`, `REFUND_OR_SETTLEMENT`, or `UNDETERMINED`;
- root cause and summary;
- recommended action: `MANUAL_REVIEW`, `REQUEST_EVIDENCE`, `DETERMINISTIC_REPROCESS`, or `DISMISS_IF_VERIFIED`;
- confidence from 0 to 1;
- evidence references that must resolve to evidence supplied in the case;
- `requires_human_approval: true`.

Input and result hashes, provider/model details, timestamps, status, and sanitized failure codes are persisted. The current exception-investigation workflow is advisory: it cannot edit ERP, Razorpay, bank, match, candidate, or exception facts. Older candidate AI endpoints remain exposed for test-mode compatibility and are rejected in production; production users should use the exception investigation endpoints.

### Human approval and auditability

Approval and rejection are separate admin-only actions. The decision stores its status, actor, reason, decision time, and correlation ID, while the investigation retains its evidence and hashes. Exception lifecycle actions (`investigate`, `resolve`, and `dismiss`) are also explicit transitions with audit entries. API middleware accepts or creates an `X-Correlation-ID`, returns it to the caller, and includes it in structured JSON logs.

### FINCTRL Control Room

The `frontend/` application is a Vite-powered React/TypeScript dashboard. It reads the authenticated backend APIs in parallel and presents:

- historical cash movement and deterministic cash forecast;
- reconciliation run health and match/candidate/exception counts;
- open exceptions by severity and recent high/critical exceptions;
- pending, approved, and rejected AI investigation decisions;
- Razorpay synchronization status;
- selectable 7/30/90-day history windows and 7/14/30-day forecast horizons.

The API key is entered at runtime and stored only in browser `sessionStorage`. The frontend currently displays INR.

> Dashboard screenshot placeholder: add `docs/screenshots/finctrl-control-room.png` when a captured dashboard image is available.

## API

FastAPI interactive documentation is available at `http://localhost:8000/docs` while the backend is running. Except for the health, readiness, and signed webhook routes, endpoints require `X-API-Key`. An admin key can read and write; a read-only key can call read endpoints only.

### Public and webhook routes

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Process liveness |
| `GET` | `/ready` | Database readiness |
| `POST` | `/webhooks/razorpay` | Receive a signature-verified Razorpay event |

### Admin routes

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/ingest/erp` | Ingest an ERP batch |
| `POST` | `/ingest/rzp` | Ingest a legacy Razorpay batch |
| `POST` | `/ingest/bank` | Ingest a bank batch |
| `POST` | `/reconciliation/run` | Run reconciliation (legacy response shape) |
| `POST` | `/reconciliation/runs` | Create a controlled reconciliation run |
| `POST` | `/reconciliation/runs/{run_id}/retry` | Retry a failed run |
| `POST` | `/reconciliation/periods` | Create a reporting period |
| `POST` | `/reconciliation/periods/{period_id}/close` | Close a ready period |
| `POST` | `/reconciliation/periods/{period_id}/reopen` | Reopen a period |
| `POST` | `/razorpay/sync` | Synchronize all supported Razorpay resources |
| `POST` | `/razorpay/sync/{resource}` | Synchronize one Razorpay resource |
| `POST` | `/reconciliation/exceptions/{exception_id}/investigations` | Create an AI investigation |
| `POST` | `/reconciliation/investigations/{investigation_id}/approve` | Approve an investigation |
| `POST` | `/reconciliation/investigations/{investigation_id}/reject` | Reject an investigation |
| `POST` | `/exceptions/{exception_id}/investigate` | Move an exception into investigation |
| `POST` | `/exceptions/{exception_id}/resolve` | Resolve an exception |
| `POST` | `/exceptions/{exception_id}/dismiss` | Dismiss an exception |
| `POST` | `/webhooks/replay/{event_id}` | Replay a failed webhook |
| `POST` | `/ai/investigate/{candidate_id}` | Legacy candidate AI (test mode only) |
| `POST` | `/ai/process/{candidate_id}` | Legacy candidate AI processing (test mode only) |
| `POST` | `/ai/process-pending` | Legacy pending-candidate processing (test mode only) |

### Read routes

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/forecast/cash` | Cash history and deterministic forecast |
| `GET` | `/forecast/cash/summary` | Compact cash forecast summary |
| `GET` | `/reconciliation/runs` | List controlled runs |
| `GET` | `/reconciliation/runs/{run_id}` | Get a run |
| `GET` | `/reconciliation/runs/{run_id}/stages` | Get run-stage details |
| `GET` | `/reconciliation/periods` | List periods |
| `GET` | `/reconciliation/periods/{period_id}` | Get a period |
| `GET` | `/reconciliation/reports` | List period reports |
| `GET` | `/reconciliation/reports/{period_id}` | Get a period report |
| `GET` | `/reconciliation/reports/{period_id}/exceptions` | Get filtered period exceptions |
| `GET` | `/reconciliation/reports/{period_id}/runs` | Get period runs |
| `GET` | `/reconciliation/reports/{period_id}/close-readiness` | Evaluate close readiness |
| `GET` | `/razorpay/sync-status` | Get per-resource sync state |
| `GET` | `/matches` | List reconciliation matches |
| `GET` | `/candidates` | List reconciliation candidates |
| `GET` | `/exceptions` | List reconciliation exceptions |
| `GET` | `/exceptions/{exception_id}` | Get an exception and its audit/evidence links |
| `GET` | `/exceptions/{exception_id}/candidates` | Get candidates linked to an exception |
| `GET` | `/exceptions/{exception_id}/evidence` | Resolve an exception's evidence facts |
| `GET` | `/reconciliation/exceptions/{exception_id}/investigations` | List exception investigations |
| `GET` | `/reconciliation/investigations/{investigation_id}` | Get an investigation and approval |
| `GET` | `/cash-position` | Get realized and projected cash position |
| `GET` | `/metrics` | Get processing and reconciliation metrics |
| `GET` | `/ai/investigations/{candidate_id}` | Get legacy candidate investigation logs |

Query parameters and request/response schemas are documented by OpenAPI at `/docs`.

## Setup

### Backend with Docker Compose

Prerequisites: Docker with Compose support. From the repository root, create a local `.env` file (it is intentionally not provided by the repository) with values suitable for your environment:

```dotenv
APP_MODE=test
DATABASE_URL=postgresql+asyncpg://finctrl:replace-this-password@postgres:5432/finctrl
POSTGRES_USER=finctrl
POSTGRES_PASSWORD=replace-this-password
POSTGRES_DB=finctrl

ADMIN_API_KEY=replace-with-a-long-admin-key
READ_ONLY_API_KEY=replace-with-a-different-long-read-key

RAZORPAY_MODE=test
RAZORPAY_KEY_ID=rzp_test_replace_me
RAZORPAY_KEY_SECRET=replace-with-test-api-secret
RAZORPAY_WEBHOOK_SECRET=replace-with-separate-webhook-secret

AI_PROVIDER=gemini
GEMINI_API_KEY=
OPENROUTER_API_KEY=
OPENAI_API_KEY=
```

Then start PostgreSQL, migrations, the API, and the durable recovery worker:

```bash
docker compose up --build
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

This test-mode configuration starts the core backend without a live AI call. To run exception investigations, provide the selected provider key. For production, set `APP_MODE=production`, use a PostgreSQL URL, use distinct non-placeholder API keys, set `RAZORPAY_MODE=live`, provide live Razorpay credentials, use a separate webhook secret, and configure the selected AI provider key. Production configuration fails fast when required values are absent or unsafe.

Stop the stack with `docker compose down`. The named PostgreSQL volume is retained.

### Frontend

Prerequisites: Node.js/npm compatible with the locked dependencies. The frontend is run locally; this repository does not define a frontend container.

```bash
cd frontend
npm ci
```

The default backend URL is `http://localhost:8000`. To override it, create `frontend/.env.local`:

```dotenv
VITE_API_BASE_URL=http://localhost:8000
```

Start the dashboard:

```bash
npm run dev
```

Open `http://localhost:5173` and enter the read-only or admin API key. For a different frontend origin, add it to the backend's comma-separated `CORS_ORIGINS`. Never put backend, Razorpay, database, or AI secrets in a `VITE_*` variable because Vite exposes those values to the browser.

## Testing and evaluation

Install backend dependencies in a Python 3.12 environment when running outside Docker:

```bash
python -m pip install -r finctrl/backend/requirements.txt
```

Run backend tests and frontend checks:

```bash
python -m pytest finctrl/backend/tests

cd frontend
npm ci
npm test
npm run build
```

Run the fixed offline evaluation splits from the repository root:

```bash
python -m finctrl.backend.evaluation_runner --dataset validation
python -m finctrl.backend.evaluation_runner --dataset held_out
python -m finctrl.backend.evaluation_runner --dataset held_out --production-check
```

Final held-out result:

```text
HELD_OUT: 100/100 correct; readiness=PASS
```

This is **not a claim of 100% accurate AI**. It is the deterministic reconciliation result on the repository's fixed, synthetic held-out corpus. The offline runner uses an isolated in-memory SQLite database and makes no external Razorpay, Gemini, or OpenRouter calls. Its readiness checks cover dataset integrity, source immutability, idempotency, evidence validity, AI schema safety, and deterministic forecast invariants; `--production-check` also runs the selected safety tests plus frontend tests/build. It does not establish live integration performance or forecast predictive accuracy.

Do not edit or regenerate `finctrl/backend/data/held_out/dataset.json` or its ground truth to tune behavior.

## Five-minute demo

1. Start the Docker stack with `docker compose up --build`; confirm `/health` and `/ready`.
2. In another terminal, start the dashboard with `cd frontend`, `npm ci`, and `npm run dev`.
3. Open `http://localhost:5173`, enter the runtime read-only API key, and review cash, run health, exception severity, AI approvals, and Razorpay sync state.
4. Open `http://localhost:8000/docs` to inspect the real API contract. Use the admin key to ingest ERP/Razorpay/bank sample payloads or trigger a controlled reconciliation run, then inspect matches, candidates, exceptions, and evidence.
5. Demonstrate the control boundary by creating an exception investigation and explicitly approving or rejecting its structured recommendation. Finish with `python -m finctrl.backend.evaluation_runner --dataset held_out --production-check` to reproduce the offline held-out and readiness checks.

Live Razorpay synchronization, webhook delivery, and AI investigation require valid external credentials and are not exercised by the offline evaluation.

## Project structure

```text
FINCTRL-AI/
|-- finctrl/backend/
|   |-- api/                 # FastAPI routes, schemas, authentication
|   |-- database/            # SQLAlchemy models and async sessions
|   |-- engine/ai/           # Legacy candidate AI agent and policy controls
|   |-- integrations/
|   |   `-- razorpay/        # Razorpay client, schemas, synchronization
|   |-- reconciliation/      # Matching, runs, exceptions, investigation, reports, forecast
|   |-- recovery/            # Durable lease recovery worker
|   |-- synthetic_data/      # Reproducible corpus generation utilities
|   |-- data/                # Dev, validation, and held-out corpora
|   |-- tests/               # Backend and evaluation tests
|   `-- evaluation_runner.py # Offline evaluation/readiness CLI
|-- frontend/                # React/TypeScript FINCTRL Control Room
|-- alembic/                 # Database migrations
|-- Dockerfile               # Backend image
|-- docker-compose.yml       # PostgreSQL, migration, API, recovery worker
`-- README.md
```

## Technology stack

- **API:** Python 3.12, FastAPI, Pydantic, Uvicorn
- **Data:** SQLAlchemy async, PostgreSQL in production, SQLite/aiosqlite in tests, Alembic migrations
- **Payments:** Razorpay Python SDK plus signed webhook handling
- **AI investigation:** Gemini or OpenRouter for the production exception workflow; OpenAI support remains in the legacy candidate path
- **Frontend:** React, TypeScript, Vite
- **Testing:** pytest, pytest-asyncio, Vitest, Testing Library, jsdom
- **Operations:** Docker Compose, structured JSON logging, correlation IDs, database-backed recovery leases

## Security notes

- Never commit `.env` files, API keys, database passwords, or webhook secrets.
- Keep `RAZORPAY_WEBHOOK_SECRET` distinct from `RAZORPAY_KEY_SECRET`.
- Use the read-only API key for dashboards and other read-only clients; reserve the admin key for controlled actions.
- Use long, random, distinct API keys and rotate them through your deployment secret manager.
- Terminate TLS in front of the API in production and restrict database network access.
- Treat `VITE_*` settings as public browser configuration, never as secret storage.
- Preserve raw webhook bytes for signature verification and configure the exact webhook secret registered with Razorpay.
- Review audit records and correlation IDs when approving investigations, resolving exceptions, replaying webhooks, or reopening periods.

## License

MIT
