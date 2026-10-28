# Distributed Wallet & Payment Ledger Platform with Saga-Based Transfers

A high-performance, resilient, distributed multi-service backend built in **Go (Golang)**, designed for rigorous financial accuracy using a **double-entry ledger**, **saga-based distributed orchestrator**, **transactional outbox/inbox messaging**, and **automated continuous reconciliation**.

---

## 1. Key Architectural Features

- **Double-Entry Bookkeeping**: Strict invariant that total debits (`DR`) equal total credits (`CR`) for every journal. Enforced at the domain and database levels.
- **Append-Only Ledger**: Database triggers prohibit `UPDATE` or `DELETE` on ledger entries.
- **Saga Orchestrator**: Multi-step state machine with optimistic fencing tokens, automated crash-recovery, and compensating rollback actions.
- **Transactional Outbox & Inbox**: DB-committed event outbox with NATS JetStream publisher relay and message deduplication inbox.
- **Zero-Deadlock Concurrency**: Deterministic UUID lock ordering prevents cyclic deadlocks under heavy concurrent load (e.g. 1,000 parallel transfers).
- **Authentication & Multi-Tenancy**: JWT with refresh token rotation and reuse detection, Argon2id password hashing, and scoped hashed API keys.
- **Resilient Operations**: Lua-based atomic sliding window rate limiter with fail-open payment degradation mode.
- **Automated Integrity Reconciliation**: Periodic auditing job cross-checking recorded balances against the sum of append-only ledger entries.

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
│   ├── telemetry/            # Prometheus metrics and OTel tracking
│   └── webhooks/             # HMAC-SHA256 signing and exponential backoff retry dispatcher
├── migrations/               # PostgreSQL schema with append-only triggers and constraints
├── deployments/              # Docker Compose and container definitions
├── tests/                    # 1,000 concurrent transfers, 50+ concurrent idempotency checks
└── docs/                     # Design document, threat model, runbook, architecture blueprint
```

---

## 3. Quick Start with Docker Compose

Ensure Docker and Docker Compose are installed:

```bash
# 1. Start all services, databases, message broker, and Prometheus
docker compose -f deployments/docker-compose.yml up -d --build

# 2. Verify all containers are healthy
docker compose -f deployments/docker-compose.yml ps
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

Run the complete test suite verifying double-entry invariants, authentication, 1,000 concurrent transfers, and 50+ concurrent idempotency requests:

```bash
# Run unit and domain tests
go test -v ./pkg/...

# Run concurrency and idempotency tests
go test -v ./tests/...
```

---

## 5. REST API Endpoints Overview

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/auth/register` | Register new organization and user with Argon2id |
| `POST` | `/api/v1/auth/login` | Login and receive JWT access and refresh token pair |
| `POST` | `/api/v1/auth/refresh` | Rotate refresh token with reuse detection |
| `POST` | `/api/v1/auth/logout` | Revoke session |
| `POST` | `/api/v1/api-keys` | Create scoped API key (hashed in database) |
| `DELETE` | `/api/v1/api-keys/:id` | Revoke API key |
| `POST` | `/api/v1/wallets` | Create currency-specific wallet (`USD`, `EUR`) |
| `GET` | `/api/v1/wallets/:id` | Get wallet balance and hold balance |
| `POST` | `/api/v1/wallets/:id/deposit` | Credit wallet from external funding (Requires `Idempotency-Key`) |
| `POST` | `/api/v1/transfers` | Create transfer saga (Returns `202 Accepted`, Requires `Idempotency-Key`) |
| `GET` | `/api/v1/transfers/:id` | Query transfer status and saga step |
| `POST` | `/api/v1/transfers/:id/reverse` | Reverse a completed transfer |
| `GET` | `/api/v1/wallets/:id/ledger` | Cursor-based paginated ledger entries |
| `GET` | `/api/v1/reports/reconciliation` | Run on-demand ledger reconciliation audit |
| `POST` | `/api/v1/webhooks` | Register signed HMAC webhook endpoint |
| `GET` | `/api/v1/metrics` | Prometheus metrics |

---

## 6. Admin CLI Commands

```bash
# Run full reconciliation audit
go run cmd/cli/main.go reconcile

# List Dead Letter Queue messages
go run cmd/cli/main.go dlq:list

# Replay Dead Letter Queue messages
go run cmd/cli/main.go dlq:replay all

# Deliberately inject corruption to test reconciliation detection
go run cmd/cli/main.go corrupt:balance <WALLET_UUID>
```
