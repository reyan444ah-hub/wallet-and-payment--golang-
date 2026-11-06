# Distributed Wallet & Payment Ledger Platform with Saga-Based Transfers

A high-performance, resilient, distributed multi-service backend built in **Go (Golang)**, designed for rigorous financial accuracy using a **double-entry ledger**, **saga-based distributed orchestrator**, **transactional outbox/inbox messaging**, and **automated continuous reconciliation**.

---

## 1. Key Architectural Features

- **Double-Entry Bookkeeping**: Strict invariant that total debits (`DR`) equal total credits (`CR`) for every journal. Enforced at the domain and database levels.
- **Append-Only Ledger**: Database triggers prohibit `UPDATE` or `DELETE` on ledger entries.
- **Saga Orchestrator**: Multi-step state machine with optimistic fencing tokens, automated crash-recovery, and compensating rollback actions.
- **Transactional Outbox & Inbox**: DB-committed event outbox with NATS JetStream publisher relay and message deduplication inbox.
- **Ordered Wallet Locking**: Deterministic UUID lock ordering reduces deadlock risk for transfers that touch the same wallets.
- **Authentication & Multi-Tenancy**: JWT with refresh token rotation and reuse detection, Argon2id password hashing, and scoped hashed API keys.
- **Resilient Operations**: Lua-based atomic sliding window rate limiter with fail-open payment degradation mode.
- **Automated Integrity Reconciliation**: Periodic auditing job cross-checking recorded balances against the sum of append-only ledger entries.
- **Observability**: Prometheus metrics and correlation IDs are provided; OpenTelemetry tracing is not currently implemented.

---

## 2. Repository Structure

```
├── cmd/
│   ├── api-service/          # REST API server (Gin), JWT & API key auth, wallets, deposits, transfers
│   ├── saga-orchestrator/    # Saga consumer worker, Outbox relay, crash recovery
│   ├── risk-service/         # gRPC server for automated risk evaluation
│   ├── reconciliation/       # Scheduled integrity audit daemon
│   └── cli/                  # Admin CLI for DLQ management and on-demand audits
├── proto/
│   └── risk/v1/              # Protobuf contracts and gRPC stubs
├── pkg/
│   ├── api/                  # Gin routing, middleware (Auth, Idempotency, RBAC)
│   ├── auth/                 # Argon2id, JWT rotation, API key hashing
│   ├── database/             # PostgreSQL connection pooling and migrations
│   ├── domain/               # Ledger domain models, transfer saga states, outbox models
│   ├── inbox/                # Idempotent deduplication store
│   ├── lock/                 # Redis distributed lock with lease verification
│   ├── outbox/               # Transactional outbox relay
│   ├── ratelimit/            # Redis Lua sliding window rate limiter
│   ├── reconciliation/       # Audit engine verifying balance and journal invariants
│   ├── risk/                 # gRPC risk service implementation
│   ├── telemetry/            # Prometheus metrics and correlation IDs
│   └── webhooks/             # HMAC-SHA256 signing and exponential backoff retry dispatcher
├── migrations/               # PostgreSQL schema with append-only triggers and constraints
├── deployments/              # Docker Compose and container definitions
├── tests/                    # PostgreSQL-backed service tests (requires TEST_DATABASE_URL)
└── docs/                     # Design document, threat model, runbook, architecture blueprint
```

---

## 3. Quick Start with Docker Compose

Ensure Docker and Docker Compose are installed. Copy `.env.example` to `.env` and replace every placeholder with a unique secret before starting. Do not commit `.env`.

```bash
# Validate the Compose configuration
docker compose --env-file .env -f deployments/docker-compose.yml config --quiet

# Build and start PostgreSQL, Redis, NATS JetStream, API, risk, saga,
# reconciliation, and Prometheus
docker compose --env-file .env -f deployments/docker-compose.yml up -d --build

# Check service health
docker compose --env-file .env -f deployments/docker-compose.yml ps
```

The services will be exposed at:
- **REST API**: `http://localhost:8080`
- **gRPC Risk Service**: `localhost:50051`
- **Prometheus Metrics**: `http://localhost:8080/api/v1/metrics` and `http://localhost:9090`
- **NATS JetStream**: `localhost:4222`
- **PostgreSQL**: `localhost:5432`
- **Redis**: `localhost:6379`

---

## 4. Running Tests

Unit tests run without external services. PostgreSQL-backed HTTP/API tests are skipped unless `TEST_DATABASE_URL` points to a disposable PostgreSQL database. Each test creates and removes an isolated schema; the supplied database user needs permission to create schemas. Set `TEST_NATS_URL` as well to enable the JetStream relay and saga-redelivery integration tests.

```bash
# Run unit tests and any enabled integration tests
go test ./...

# Run the database-backed tests (PowerShell example)
$env:TEST_DATABASE_URL = "postgres://user:password@localhost:5432/wallet_test?sslmode=disable"
go test -v ./tests/...

# Run the race detector where a working CGO C compiler is available
go test -race ./...
```

---

## 5. Frontend Dashboard

The React dashboard lives in the existing `forntend\` directory. Start the
backend stack as described above, then open a separate PowerShell window:

```powershell
Set-Location 'C:\Users\Hashim Traders\Documents\Task (Go lang)\wallet and payment\forntend'
npm.cmd ci
npm.cmd run dev
```

Open `http://localhost:5173`. Vite proxies `/api` requests to the local backend
at `http://localhost:8080`. Frontend test and production build commands:

```powershell
npm.cmd test
npm.cmd run build
npm.cmd audit
```

The API also exposes tenant-scoped paginated collections at
`GET /api/v1/wallets` and `GET /api/v1/transfers`; transfer listing accepts an
optional `status` filter.

### Isolated integration infrastructure

The database, outbox, webhook, and NATS integration tests can use an isolated
test-only Compose project. It has no persistent volumes and does not read the
development `.env` file:

```powershell
docker compose -p wallet-test -f deployments/docker-compose.test.yml up -d --wait
$env:TEST_DATABASE_URL = "postgres://wallet_test:wallet_test_only@127.0.0.1:55439/wallet_test?sslmode=disable"
$env:TEST_NATS_URL = "nats://127.0.0.1:54229"
go test ./...
go test -race ./...
docker compose -p wallet-test -f deployments/docker-compose.test.yml down --volumes
```

The integration tests create a unique PostgreSQL schema per test and remove it
afterward. The 1,000-transfer test executes both concurrent transfer
reservations and concurrent saga completion, then verifies balances and
double-entry totals. The container-kill chaos test builds and runs a separate
full Compose project with disposable volumes and test-only credentials:

```powershell
.\tests\chaos_test.ps1
```

It submits concurrent idempotent transfers while killing and restarting the
API, saga/outbox worker, reconciliation daemon, and NATS, then verifies transfer
completion and ledger reconciliation before removing its test project.

---

## 6. REST API Endpoints Overview

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/auth/register` | Register new organization and named user with Argon2id |
| `POST` | `/api/v1/auth/login` | Login and receive JWT access and refresh token pair |
| `POST` | `/api/v1/auth/refresh` | Rotate refresh token with reuse detection |
| `POST` | `/api/v1/auth/logout` | Revoke session |
| `GET` | `/api/v1/users` | Admin-only, organization-scoped paginated user list |
| `PATCH` | `/api/v1/users/:id/status` | Admin-only deactivate/reactivate account while retaining financial history |
| `GET` | `/api/v1/organizations/:id/profile` | Authenticated company details and paginated members; organization ID must match the JWT |
| `POST` | `/api/v1/api-keys` | Create scoped API key (hashed in database) |
| `DELETE` | `/api/v1/api-keys/:id` | Revoke API key |
| `POST` | `/api/v1/wallets` | Create currency-specific wallet (`USD`, `EUR`) |
| `GET` | `/api/v1/wallets` | List caller-owned customer wallets for user sessions (cursor pagination) |
| `GET` | `/api/v1/wallets/:id` | Get wallet balance and hold balance |
| `DELETE` | `/api/v1/wallets/:id` | Permanently remove only an unused, empty wallet with no ledger/transfer history |
| `POST` | `/api/v1/wallets/:id/close` | Close an empty wallet while preserving its financial history |
| `POST` | `/api/v1/wallets/:id/reopen` | Reopen a closed, empty customer wallet while preserving its financial history |
| `POST` | `/api/v1/wallets/:id/deposit` | Record an external-funding ledger credit (Requires `Idempotency-Key`; does not collect money) |
| `POST` | `/api/v1/transfers` | Create internal or user-to-user transfer saga (Returns `202 Accepted`, Requires `Idempotency-Key`) |
| `GET` | `/api/v1/transfers` | List transfers involving the caller's wallets (cursor pagination and status filter) |
| `GET` | `/api/v1/transfers/:id` | Query transaction status, sender/receiver wallets, and organization-scoped debit/credit ledger entries |
| `POST` | `/api/v1/transfers/:id/reverse` | Reverse a completed transfer |
| `GET` | `/api/v1/wallets/:id/ledger` | Cursor-based paginated ledger entries |
| `GET` | `/api/v1/reports/reconciliation` | Run on-demand ledger reconciliation audit |
| `POST` | `/api/v1/webhooks` | Register signed HMAC webhook endpoint |
| `GET` | `/api/v1/metrics` | Prometheus metrics |

The deposit endpoint is a manual/demo ledger operation. It credits the wallet
and records the counter-entry against an internal external-funding account;
it is not connected to a bank, card processor, or settlement provider. Do not
expose it as real-money funding until a payment provider, verified webhook
flow, and authorization/reconciliation controls are integrated.

Every successful registration creates an active, zero-balance USD customer
wallet owned by the new user in the same database transaction. This applies to
viewer accounts as well as organization founders, so users can receive
transfers without elevated wallet-creation permissions.

Company profile requests return the organization's name and member name, email,
and role. Member rows are filtered by the authenticated JWT organization, and
the requested organization ID must match that claim. New registrations can
provide a full name; existing accounts without one use the email prefix until
their display name is supplied.

Admins can deactivate or reactivate users in their own organization. Deactivated
users cannot sign in, use existing access tokens, refresh sessions, or receive
new transfers. Their wallets and ledger history are retained. Admins cannot
deactivate themselves, and the organization must retain an active admin.

Transfer requests require `source_wallet_id`, `currency`, and a positive
integer `amount` in minor units, plus exactly one of `destination_wallet_id`
or `recipient_email`. A user session can transfer only from a wallet owned by
that user. Email recipients resolve to one active wallet owned by a registered
user in any organization with the same currency; if the recipient has multiple
matching wallets, supply the destination wallet ID instead. User sessions can
view transaction history when they own either the source or destination wallet,
including in the recipient's organization. Transfer details expose the sender
and receiver wallet IDs and only ledger entries belonging to the caller's
organization; they do not disclose the other organization's journal accounts.
Organization admins can inspect transfer history and journal entries across
their organization's authorized accounts. Every transfer uses the existing
hold, outbox, and saga processing flow. Cross-organization transfers post
balanced sender and recipient journals atomically, each constrained to its
organization's wallets, and offset through per-organization clearing accounts.
Reversal posts an inverse journal in each affected organization in one
transaction and is rejected if the recipient cannot cover it.

---

## 7. Admin CLI Commands

```bash
# Run full reconciliation audit
go run cmd/cli/main.go reconcile

# List Dead Letter Queue messages
go run cmd/cli/main.go dlq:list

# Replay Dead Letter Queue messages
go run cmd/cli/main.go dlq:replay all

```

Ledger and wallet constraints deliberately reject direct balance corruption; exercise corruption detection using a dedicated disposable database and controlled privileged test setup rather than a production CLI command.
