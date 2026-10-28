-- PostgreSQL Migration Schema for Distributed Wallet & Payment Ledger Platform
-- Enforces:
-- 1. Multi-tenancy (organizations, users, API keys, audit logs)
-- 2. Wallets with non-negative balance constraint
-- 3. Double-entry ledger: journal_entries, ledger_entries (append-only via triggers)
-- 4. Saga state storage with optimistic versioning / fencing tokens
-- 5. Transactional outbox and consumer inbox tables
-- 6. Webhooks, delivery logs, and reconciliation logs

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- -------------------------------------------------------------
-- 1. ORGANIZATIONS, USERS, API KEYS & AUDIT LOGS
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL CHECK (role IN ('admin', 'finance', 'viewer')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_users_org ON users(organization_id);

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    family_id UUID NOT NULL,
    token_hash VARCHAR(255) NOT NULL UNIQUE,
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family ON refresh_tokens(family_id);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user ON refresh_tokens(user_id);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    key_prefix VARCHAR(16) NOT NULL,
    key_hash VARCHAR(255) NOT NULL UNIQUE,
    scopes TEXT[] NOT NULL DEFAULT '{}',
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_api_keys_org ON api_keys(organization_id);

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    actor_type VARCHAR(50) NOT NULL, -- 'user' or 'api_key'
    actor_id UUID NOT NULL,
    action VARCHAR(100) NOT NULL,   -- 'login', 'create_key', 'transfer', 'reversal', etc.
    resource_type VARCHAR(100) NOT NULL,
    resource_id VARCHAR(255) NOT NULL,
    ip_address VARCHAR(45),
    details JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_audit_logs_org_created ON audit_logs(organization_id, created_at DESC);

-- -------------------------------------------------------------
-- 2. WALLETS & SYSTEM ACCOUNTS
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    currency VARCHAR(3) NOT NULL CHECK (currency IN ('USD', 'EUR')),
    account_type VARCHAR(50) NOT NULL DEFAULT 'customer' CHECK (account_type IN ('customer', 'external_funding', 'fees', 'suspense')),
    balance BIGINT NOT NULL DEFAULT 0,
    hold_balance BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'closed')),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_wallet_balance_non_negative CHECK (
        account_type != 'customer' OR balance >= 0
    ),
    CONSTRAINT chk_wallet_hold_non_negative CHECK (hold_balance >= 0),
    CONSTRAINT chk_wallet_available_funds CHECK (
        account_type != 'customer' OR (balance - hold_balance) >= 0
    )
);
CREATE INDEX IF NOT EXISTS idx_wallets_org_currency ON wallets(organization_id, currency);

-- -------------------------------------------------------------
-- 3. DOUBLE-ENTRY JOURNAL & LEDGER ENTRIES
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS journal_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    reference_id UUID,          -- links to transfer_id or deposit_id
    reference_type VARCHAR(50) NOT NULL, -- 'transfer', 'deposit', 'reversal', 'fee'
    currency VARCHAR(3) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_journal_entries_ref ON journal_entries(reference_id, reference_type);
CREATE INDEX IF NOT EXISTS idx_journal_entries_org ON journal_entries(organization_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    journal_entry_id UUID NOT NULL REFERENCES journal_entries(id) ON DELETE RESTRICT,
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
    direction VARCHAR(2) NOT NULL CHECK (direction IN ('DR', 'CR')), -- DR = Debit, CR = Credit
    amount BIGINT NOT NULL CHECK (amount > 0), -- Amount in minor units (cents)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ledger_wallet_created ON ledger_entries(wallet_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ledger_journal ON ledger_entries(journal_entry_id);

-- STRICT APPEND-ONLY TRIGGER FOR LEDGER ENTRIES (Prevents updates and deletes)
CREATE OR REPLACE FUNCTION prevent_ledger_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Ledger entries are strictly append-only. Updates and deletes are prohibited.';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_ledger_update_delete ON ledger_entries;
CREATE TRIGGER trg_prevent_ledger_update_delete
BEFORE UPDATE OR DELETE ON ledger_entries
FOR EACH ROW EXECUTE FUNCTION prevent_ledger_modification();

-- -------------------------------------------------------------
-- 4. TRANSFERS & SAGA STATE MACHINE
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transfers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    source_wallet_id UUID NOT NULL REFERENCES wallets(id),
    destination_wallet_id UUID NOT NULL REFERENCES wallets(id),
    currency VARCHAR(3) NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),
    fee_amount BIGINT NOT NULL DEFAULT 0 CHECK (fee_amount >= 0),
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'REVERSED')),
    saga_step VARCHAR(50) NOT NULL DEFAULT 'RESERVE_FUNDS',
    failure_reason TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 1,
    idempotency_key VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_different_wallets CHECK (source_wallet_id != destination_wallet_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_transfers_org_idempotency ON transfers(organization_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers(status, saga_step);
CREATE INDEX IF NOT EXISTS idx_transfers_source_dest ON transfers(source_wallet_id, destination_wallet_id);

CREATE TABLE IF NOT EXISTS saga_step_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transfer_id UUID NOT NULL REFERENCES transfers(id) ON DELETE CASCADE,
    step VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL, -- 'STARTED', 'COMPLETED', 'FAILED', 'COMPENSATED'
    payload JSONB DEFAULT '{}'::jsonb,
    error_message TEXT,
    fencing_token BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_saga_logs_transfer ON saga_step_logs(transfer_id, created_at ASC);

-- -------------------------------------------------------------
-- 5. IDEMPOTENCY STORE
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key VARCHAR(255) NOT NULL,
    organization_id UUID NOT NULL,
    request_path VARCHAR(255) NOT NULL,
    request_hash VARCHAR(64) NOT NULL,
    response_code INT,
    response_body JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until TIMESTAMPTZ,
    PRIMARY KEY (organization_id, key)
);

-- -------------------------------------------------------------
-- 6. TRANSACTIONAL OUTBOX & INBOX
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255) NOT NULL, -- e.g. wallet_id for ordering
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED', 'DEAD_LETTER')),
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(status, next_retry_at) WHERE status = 'PENDING';

CREATE TABLE IF NOT EXISTS inbox_records (
    message_id VARCHAR(255) PRIMARY KEY,
    consumer_group VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- 7. WEBHOOKS & DELIVERY LOGS
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    secret VARCHAR(255) NOT NULL,
    events TEXT[] NOT NULL DEFAULT '{"transfer.completed","transfer.failed"}',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_webhooks_org ON webhooks(organization_id);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id UUID NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    signature VARCHAR(255) NOT NULL,
    response_status INT,
    error_message TEXT,
    attempt INT NOT NULL DEFAULT 1,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED')),
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_pending ON webhook_deliveries(status, next_retry_at) WHERE status IN ('PENDING', 'PROCESSING');

-- -------------------------------------------------------------
-- 8. RECONCILIATION RUNS & AUDIT DISCREPANCIES
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status VARCHAR(50) NOT NULL, -- 'SUCCESS', 'DISCREPANCIES_FOUND'
    total_wallets_checked INT NOT NULL DEFAULT 0,
    total_journals_checked INT NOT NULL DEFAULT 0,
    discrepancies JSONB DEFAULT '[]'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
