# FINCTRL-AI

### Financial reconciliation that keeps the facts deterministic, the investigation evidence-bound, and the final decision human.

FINCTRL-AI is a financial-operations control system for teams working across ERP records, Razorpay activity, and bank transactions. It turns fragmented payment data into matches, reviewable exceptions, structured AI investigations, and auditable human decisions—without allowing AI to rewrite authoritative financial records.

**Held-out evaluation: 100/100 correct · Production readiness: PASS**

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React + TypeScript](https://img.shields.io/badge/React%20%2B%20TypeScript-Control%20Room-3178C6?logo=react&logoColor=white)](https://react.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Razorpay](https://img.shields.io/badge/Razorpay-Payments-0C2451)](https://razorpay.com/)
[![Gemini](https://img.shields.io/badge/Gemini-AI%20Investigation-8E75B2?logo=googlegemini&logoColor=white)](https://ai.google.dev/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

[Explore the repository](https://github.com/pakhi-sinha/FINCTRL-AI) · [Quick start](#quick-start) · [API reference](#api) · [Five-minute product demo](#five-minute-product-demo)

## FINCTRL Control Room

![FINCTRL Control Room](docs/screenshots/finctrl-control-room.png)

*The FINCTRL Control Room brings reconciliation health, exceptions, AI decisions, cash visibility, and Razorpay synchronization into one operational view.*

## Why FINCTRL?

Financial truth is usually split across systems: an ERP records what should have happened, a payment gateway records what it processed, and a bank records what actually moved. Those records do not always form clean one-to-one pairs—settlements consolidate payments, fees and tax alter net amounts, refunds reverse activity, and references or timing can differ.

When deterministic matching stops, a discrepancy needs an explanation backed by evidence. FINCTRL-AI lets AI investigate that evidence, but never silently change the underlying facts. A person remains accountable for approving or rejecting the recommendation.

## The FINCTRL control loop

```text
Ingest
  ↓
Normalize
  ↓
Deterministic Reconciliation
  ↓
Match / Candidate / Exception
  ↓
Evidence-bound AI Investigation
  ↓
Human Approval
  ↓
Auditable Decision
  ↓
Recovery + Monitoring
```

| Stage | What happens |
| --- | --- |
| **Ingest** | ERP, Razorpay, and bank facts enter through batch APIs, Razorpay synchronization, or signed webhooks. |
| **Normalize** | Provider records are converted into stable internal representations while source facts remain authoritative. |
| **Reconcile** | Controlled deterministic stages evaluate references, amounts, fees, timing, refunds, and consolidated settlements. |
| **Classify** | Resolved records become matches; ambiguity is retained as candidates or exceptions with linked evidence. |
| **Investigate** | Gemini or OpenRouter receives only the assembled case and must return a strict structured result. |
| **Approve** | An admin approves or rejects the advisory result; the actor, reason, time, and correlation context are stored. |
| **Audit and recover** | Structured logs, idempotency, durable leases, bounded retries, and a recovery worker keep operations traceable. |

## Built for controlled financial operations

| Capability | What FINCTRL-AI implements |
| --- | --- |
| ⚡ **Deterministic reconciliation** | Reconciles ERP, Razorpay, and bank data through controlled stages, preserving matches, candidates, evidence, and exceptions as distinct records. |
| 💳 **Razorpay control loop** | Synchronizes orders, payments, refunds, and settlements; verifies signed webhooks; deduplicates deliveries; and supports bounded replay and recovery. |
| 🧠 **Evidence-bound AI investigation** | Uses Gemini or OpenRouter for exception analysis with a closed schema, confidence score, recommended action, and references that must resolve to case evidence. |
| 👤 **Human in the loop** | Restricts approval and rejection to admins and persists decision status, actor, reason, timestamp, and correlation metadata. |
| 🛡️ **Reliability and auditability** | Carries correlation IDs through JSON logs and audit records, protects source immutability, and uses idempotent operations, fenced durable leases, and bounded retries. |
| 📊 **FINCTRL Control Room** | Presents net cash movement, forecast state, reconciliation health, exception severity, AI decisions, Razorpay sync state, recent critical exceptions, and selectable history/forecast windows. |

### From exception to decision

The tested investigation lifecycle demonstrates the complete control boundary:

```text
Razorpay payment
  → MISSING_ERP exception
  → structured AI investigation
  → MISSING_RECORD classification
  → REQUEST_EVIDENCE recommendation
  → 0.91 confidence
  → human review
  → APPROVED
```

The recommendation and approval are persisted, while the original ERP amount and other financial source records remain unchanged.

## Why the AI is safe by design

> **AI investigates. People decide. Authoritative financial facts stay authoritative.**

- The model receives evidence assembled for one exception, not unrestricted write access.
- Its response must pass a closed schema and cite evidence that exists in the case.
- The stored investigation includes provider/model details, input and result hashes, timestamps, confidence, and sanitized failure codes.
- Approval or rejection is a separate, admin-only action where applicable.
- Decision state is stored separately from ERP, Razorpay, bank, match, candidate, and exception facts.

## Architecture

```text
 ERP ─────────┐
              ├──> Authoritative records ──> PostgreSQL
 Razorpay ────┤              │                    │
              │              ▼                    │
 Bank ────────┘    Deterministic reconciliation   │
                             │                    │
                             ▼                    │
               Matches / Candidates / Exceptions │
                             │                    │
                             ▼                    │
                    AI investigation              │
                             │                    │
                             ▼                    │
                     Human approval               │
                             │                    │
                             ▼                    │
                      Audit + recovery <──────────┘
                             ▲
                             │
                  Durable recovery worker
                             │
                             ▼
                   FINCTRL Control Room
```

PostgreSQL holds operational state and audit history. The recovery worker reclaims expired reconciliation, investigation, and webhook leases using database-time leases and fenced ownership.

## Technology stack

| Layer | Technologies |
| --- | --- |
| Backend | Python 3.12, FastAPI, SQLAlchemy |
| Data | PostgreSQL, Alembic |
| Payments | Razorpay SDK and signed webhooks |
| AI investigation | Gemini or OpenRouter |
| Control Room | React, TypeScript, Vite |
| Operations | Docker Compose |
| Testing | Pytest, Vitest, Testing Library |

## Quick start

### Backend with Docker

Prerequisite: Docker with Compose support. Copy `.env.example` to `.env`, then set environment-appropriate values. At minimum, configure distinct API keys and database credentials:

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
```

Start PostgreSQL, migrations, the API, and the recovery worker:

```bash
docker compose up --build
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

The core backend runs in test mode without a live AI call. Live Razorpay synchronization and exception investigation require the corresponding credentials. Stop the stack with `docker compose down`; the named PostgreSQL volume is retained.

### Control Room

The frontend runs locally and defaults to `http://localhost:8000` for its API:

```bash
cd frontend
npm ci
npm run dev
```

Open [http://localhost:5173](http://localhost:5173) and enter a read-only or admin API key. To use another API URL, set `VITE_API_BASE_URL` in `frontend/.env.local`. To use another frontend origin, add it to the backend's comma-separated `CORS_ORIGINS` setting.

## API

OpenAPI documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs) while the backend is running. Except for liveness, readiness, and the signature-verified webhook, routes require `X-API-Key`. Admin keys can read and write; read-only keys can call read routes.

<details>
<summary><strong>Health and readiness</strong></summary>

| Method | Path | Access | Purpose |
| --- | --- | --- | --- |
| `GET` | `/health` | Public | Process liveness |
| `GET` | `/ready` | Public | Database readiness |

</details>

<details>
<summary><strong>Razorpay and webhooks</strong></summary>

| Method | Path | Access | Purpose |
| --- | --- | --- | --- |
| `POST` | `/ingest/rzp` | Admin | Ingest a legacy Razorpay batch |
| `POST` | `/razorpay/sync` | Admin | Synchronize every supported resource |
| `POST` | `/razorpay/sync/{resource}` | Admin | Synchronize one resource |
| `GET` | `/razorpay/sync-status` | Read | Get per-resource sync state |
| `POST` | `/webhooks/razorpay` | Signed webhook | Receive a verified Razorpay event |
| `POST` | `/webhooks/replay/{event_id}` | Admin | Replay a failed event |

</details>

<details>
<summary><strong>Reconciliation and reporting</strong></summary>

| Method | Path | Access | Purpose |
| --- | --- | --- | --- |
| `POST` | `/ingest/erp` | Admin | Ingest an ERP batch |
| `POST` | `/ingest/bank` | Admin | Ingest a bank batch |
| `POST` | `/reconciliation/run` | Admin | Run reconciliation with the legacy response shape |
| `POST` | `/reconciliation/runs` | Admin | Create a controlled run |
| `GET` | `/reconciliation/runs` | Read | List controlled runs |
| `GET` | `/reconciliation/runs/{run_id}` | Read | Get one run |
| `GET` | `/reconciliation/runs/{run_id}/stages` | Read | Get run-stage details |
| `POST` | `/reconciliation/runs/{run_id}/retry` | Admin | Retry a failed run |
| `GET` | `/matches` | Read | List matches |
| `GET` | `/candidates` | Read | List candidates |
| `POST` | `/reconciliation/periods` | Admin | Create a reporting period |
| `GET` | `/reconciliation/periods` | Read | List periods |
| `GET` | `/reconciliation/periods/{period_id}` | Read | Get a period |
| `POST` | `/reconciliation/periods/{period_id}/close` | Admin | Close a ready period |
| `POST` | `/reconciliation/periods/{period_id}/reopen` | Admin | Reopen a period |
| `GET` | `/reconciliation/reports` | Read | List period reports |
| `GET` | `/reconciliation/reports/{period_id}` | Read | Get a period report |
| `GET` | `/reconciliation/reports/{period_id}/exceptions` | Read | Get filtered period exceptions |
| `GET` | `/reconciliation/reports/{period_id}/runs` | Read | Get period runs |
| `GET` | `/reconciliation/reports/{period_id}/close-readiness` | Read | Evaluate close readiness |

</details>

<details>
<summary><strong>Exceptions, AI investigation, and approvals</strong></summary>

| Method | Path | Access | Purpose |
| --- | --- | --- | --- |
| `GET` | `/exceptions` | Read | List exceptions |
| `GET` | `/exceptions/{exception_id}` | Read | Get an exception with audit/evidence links |
| `GET` | `/exceptions/{exception_id}/candidates` | Read | Get linked candidates |
| `GET` | `/exceptions/{exception_id}/evidence` | Read | Resolve evidence facts |
| `POST` | `/exceptions/{exception_id}/investigate` | Admin | Move an exception into investigation |
| `POST` | `/exceptions/{exception_id}/resolve` | Admin | Resolve an exception |
| `POST` | `/exceptions/{exception_id}/dismiss` | Admin | Dismiss an exception |
| `POST` | `/reconciliation/exceptions/{exception_id}/investigations` | Admin | Create a structured AI investigation |
| `GET` | `/reconciliation/exceptions/{exception_id}/investigations` | Read | List investigations for an exception |
| `GET` | `/reconciliation/investigations/{investigation_id}` | Read | Get an investigation and decision |
| `POST` | `/reconciliation/investigations/{investigation_id}/approve` | Admin | Approve an investigation |
| `POST` | `/reconciliation/investigations/{investigation_id}/reject` | Admin | Reject an investigation |

Legacy candidate-AI compatibility routes remain available in test mode: `GET /ai/investigations/{candidate_id}`, `POST /ai/investigate/{candidate_id}`, `POST /ai/process/{candidate_id}`, and `POST /ai/process-pending`. Production rejects the legacy write routes; use exception investigations instead.

</details>

<details>
<summary><strong>Forecast and metrics</strong></summary>

| Method | Path | Access | Purpose |
| --- | --- | --- | --- |
| `GET` | `/forecast/cash` | Read | Get cash history and deterministic forecast |
| `GET` | `/forecast/cash/summary` | Read | Get a compact forecast summary |
| `GET` | `/cash-position` | Read | Get realized and projected cash position |
| `GET` | `/metrics` | Read | Get processing and reconciliation metrics |

</details>

Query parameters and complete request/response schemas are documented by OpenAPI at `/docs`.

## Security

- Use `X-API-Key` with the read-only role for dashboards and read clients; reserve the admin role for controlled writes.
- Razorpay webhooks require `X-Razorpay-Signature` and `X-Razorpay-Event-Id`. The server verifies the raw body with HMAC-SHA256 and constant-time comparison before processing.
- Keep `RAZORPAY_WEBHOOK_SECRET` separate from `RAZORPAY_KEY_SECRET`.
- Never commit `.env` files, database passwords, API keys, or webhook secrets.
- Never place backend, Razorpay, database, or AI secrets in `VITE_*` variables; Vite exposes them to the browser.
- Production configuration fails fast when keys are missing, duplicated, unsafe, or incompatible with live mode.
- Terminate TLS in front of the production API, restrict database network access, and rotate secrets through the deployment secret manager.

Webhook bodies are limited to 256 KiB. Events are payload-hashed and idempotent, identity conflicts are rejected, failed events can be replayed up to five attempts, and expired webhook work can be reclaimed by the recovery worker.

## Testing and final validation

> **HELD_OUT: 100/100 correct**<br>
> **readiness=PASS**

This result belongs to the deterministic reconciliation engine on the repository's fixed synthetic held-out corpus. It is **not** a claim of 100% AI accuracy or real-world forecast accuracy.

Install and run the backend regression suite:

**Latest verified result:** `229 passed`

```bash
python -m pip install -r finctrl/backend/requirements.txt
python -m pytest finctrl/backend/tests
```

Run frontend tests and the production build:

**Latest verified result:** `7 passed` across 2 test files; production build succeeded.

```bash
cd frontend
npm ci
npm test
npm run build
```

Run the fixed offline evaluations from the repository root:

```bash
python -m finctrl.backend.evaluation_runner --dataset validation
python -m finctrl.backend.evaluation_runner --dataset held_out
python -m finctrl.backend.evaluation_runner --dataset held_out --production-check
```

The offline runner uses isolated in-memory SQLite and makes no external Razorpay, Gemini, or OpenRouter calls. Readiness covers dataset integrity, source immutability, idempotency, evidence validity, AI schema safety, and deterministic forecast invariants. The production check additionally runs selected safety tests plus frontend tests/build. Do not edit or regenerate the held-out dataset or ground truth to tune behavior.

## Five-minute product demo

This is the recommended product pitch:

1. Open the **FINCTRL Control Room**.
2. Show reconciliation health, exception severity, AI decision state, cash visibility, and Razorpay synchronization.
3. Open a `MISSING_ERP` exception and trace it to its persisted Razorpay evidence.
4. Show the structured investigation: classification, root cause, recommendation, confidence, and evidence references.
5. Approve the result and show the persisted actor, reason, timestamp, and correlation context—alongside unchanged source facts.
6. Briefly explain signed webhooks, idempotency, durable recovery leases, structured logs, and the human decision boundary.
7. Finish with **held-out 100/100** and **readiness PASS**, clearly framed as deterministic offline evaluation.

## Current demo state

- If the demo dataset has no authoritative bank movements, forecast output is intentionally unavailable rather than fabricated.
- Live Razorpay synchronization, webhook delivery, and AI investigation require configured external credentials.
- The fixed held-out evaluator measures deterministic reconciliation behavior; it does not establish live-integration performance or predictive forecast accuracy.
- The Control Room displays INR in its current implementation.

## Project structure

```text
FINCTRL-AI/
├── finctrl/backend/
│   ├── api/                 # FastAPI routes, schemas, and authentication
│   ├── database/            # SQLAlchemy models and async sessions
│   ├── engine/ai/           # Legacy candidate-AI compatibility path
│   ├── integrations/
│   │   └── razorpay/        # Client, schemas, and synchronization
│   ├── reconciliation/      # Matching, runs, exceptions, AI, reports, forecast
│   ├── recovery/            # Durable lease recovery worker
│   ├── synthetic_data/      # Reproducible corpus utilities
│   ├── data/                # Dev, validation, and held-out corpora
│   ├── tests/               # Backend and evaluation tests
│   └── evaluation_runner.py # Offline evaluation/readiness CLI
├── frontend/                # React/TypeScript FINCTRL Control Room
├── alembic/                 # Database migrations
├── Dockerfile               # Backend image
├── docker-compose.yml       # PostgreSQL, migrations, API, recovery worker
└── README.md
```

## License

MIT
