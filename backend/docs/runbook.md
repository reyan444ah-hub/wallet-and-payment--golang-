# Operations Runbook
## Distributed Wallet & Payment Platform

This runbook outlines operational procedures for investigating stuck sagas, replaying Dead Letter Queue (DLQ) messages, running ledger integrity audits, and diagnosing failure scenarios.

---

### 1. Redis Outage Behavior

Redis is used for API rate limiting and is not the source of truth for balances,
holds, transfers, or ledger entries. If Redis becomes unavailable, the API logs
a warning and fails open for rate limiting; valid operations continue using
PostgreSQL transactions and row locks. This increases exposure to request floods
while Redis is down, so monitor API traffic and restore Redis promptly. If
PostgreSQL is unavailable, financial writes fail rather than falling back to
Redis or an in-memory balance.

The standalone Redis distributed-lock helper is not used to authorize or
serialize financial writes; PostgreSQL row locks remain authoritative.

---

### 2. Investigating Stuck Sagas

#### Symptom:
A transfer remains in `PENDING` or `PROCESSING` state for longer than 30 seconds.

#### Inspection Commands:
```bash
# Query stuck transfers directly
psql -U postgres -d wallet_ledger -c "
  SELECT id, source_wallet_id, destination_wallet_id, amount, status, saga_step, updated_at 
  FROM transfers 
  WHERE status IN ('PENDING', 'PROCESSING') AND updated_at < NOW() - INTERVAL '30 seconds';
"

# Inspect saga step logs for the affected transfer
psql -U postgres -d wallet_ledger -c "
  SELECT step, status, fencing_token, error_message, created_at 
  FROM saga_step_logs 
  WHERE transfer_id = '<TRANSFER_UUID>' 
  ORDER BY created_at ASC;
"
```

#### Automated & Manual Remediation:
1. The **Saga Orchestrator** automatically detects and resumes stuck sagas during service restart and via periodic recovery sweeps.
2. If the Risk service was unresponsive and has recovered, restarting the orchestrator container triggers immediate recovery:
   ```bash
   docker compose restart saga-orchestrator
   ```

---

### 3. Inspecting & Replaying Dead-Letter Queue (DLQ) Messages

When an outbox event exceeds 5 publication retries, it is placed in the `DEAD_LETTER` state to prevent message pipeline blockage.

#### List DLQ Events:
```bash
# Using the CLI
go run cmd/cli/main.go dlq:list
```

#### Replay Specific Event:
```bash
go run cmd/cli/main.go dlq:replay <EVENT_UUID>
```

#### Replay All Dead Letter Events:
```bash
go run cmd/cli/main.go dlq:replay all
```

---

### 4. Running Ledger Reconciliation Audits

The reconciliation engine verifies the four core invariants:
1. Every journal entry balances ($\sum \text{DR} = \sum \text{CR}$).
2. Wallet recorded balance matches the exact sum of append-only ledger entries.
3. No completed transfer lacks ledger entries.
4. No saga is stuck beyond the 5-minute threshold.

#### Run Audit via CLI:
```bash
go run cmd/cli/main.go reconcile
```

#### Run Audit via REST API:
```bash
curl -X GET http://localhost:8080/api/v1/reports/reconciliation \
  -H "Authorization: Bearer <ADMIN_OR_FINANCE_JWT>"
```

#### Deliberately Test Discrepancy Detection:
```bash
# Artificially alter a wallet's recorded balance
go run cmd/cli/main.go corrupt:balance <WALLET_UUID>

# Run audit to confirm the anomaly is detected and reported
go run cmd/cli/main.go reconcile
```
