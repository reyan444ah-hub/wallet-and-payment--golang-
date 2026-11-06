# Security Threat Model
## Distributed Wallet & Payment Platform

### 1. Scope and Assets
- **Critical Assets**: Customer funds, ledger entries, wallet balances, authentication credentials, API keys, webhook signing secrets.
- **Trust Boundaries**:
  - External Client $\to$ REST API Gateway
  - API Gateway $\to$ Internal Services (gRPC Risk Service, PostgreSQL, Redis, NATS)
  - Saga Orchestrator $\to$ External Webhook Endpoints

---

### 2. Threat Analysis & Mitigations

| Threat | STRIDE Category | Attack Vector | Technical Mitigation |
|---|---|---|---|
| **Token Theft & Session Hijacking** | Spoofing | Adversary intercepts refresh token | Refresh token family rotation with automatic reuse detection: any reused token invalidates the entire session family immediately. Access tokens have short 15-minute lifetimes. |
| **Transaction Replay Attack** | Tampering / Repudiation | Adversary replays identical transfer request | Mandatory `Idempotency-Key` header with payload SHA-256 hash validation stored in PostgreSQL. Webhook verification checks timestamps with a strict tolerance window. |
| **Privilege Escalation** | Elevation of Privilege | Viewer attempts financial transfers or key creation | RBAC middleware enforces strict role hierarchies (`admin`, `finance`, `viewer`) alongside organization-level boundary enforcement on every database query. |
| **Double-Spending via Concurrency** | Tampering | Concurrent parallel requests drain funds before balance check commits | Explicit database row locks (`SELECT ... FOR UPDATE`), `CHECK (balance >= 0)` constraint, and hold reservation pattern during initial saga phase. |
| **Ledger Record Tampering** | Tampering | Malicious actor or rogue insider executes SQL updates | PostgreSQL trigger `trg_prevent_ledger_update_delete` unconditionally aborts any `UPDATE` or `DELETE` statement on `ledger_entries`. |
| **Webhook Spoofing & Replay** | Spoofing / Tampering | Man-in-the-middle forges event notifications to merchant | Every webhook is cryptographically signed with HMAC-SHA256 computed over timestamp + payload body. Receivers verify timestamps against current time. |
| **Poison Message Loop** | Denial of Service | Malformed message causes crash-looping in message consumers | Dead-Letter Queue (DLQ) moves events failing more than 5 times to `DEAD_LETTER` state, preventing pipeline stalling and alerting on-call operators. |

---

### 3. Residual Risks & Hardening Recommendations
1. Enforce TLS 1.3 across all gRPC and HTTP communication in production environments.
2. Store database and JWT encryption secrets in a dedicated Key Management Service (AWS KMS, HashiCorp Vault).
3. Enable PostgreSQL Write-Ahead Log (WAL) archiving with point-in-time recovery.
