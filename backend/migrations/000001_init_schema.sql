-- PostgreSQL Migration Schema for Distributed Wallet & Payment Ledger Platform
-- Enforces:
-- 1. Multi-tenancy (organizations, users, API keys, audit logs)
-- 2. Wallets with non-negative balance constraint
-- 3. Double-entry ledger: journal_entries, ledger_entries (append-only via triggers)
-- 4. Saga state storage with optimistic versioning / fencing tokens
-- 5. Transactional outbox and consumer inbox tables
-- 6. Webhooks, delivery logs, and reconciliation logs

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
    full_name VARCHAR(255) NOT NULL DEFAULT '',
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL CHECK (role IN ('admin', 'finance', 'viewer')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name VARCHAR(255) NOT NULL DEFAULT '';
UPDATE users SET full_name = split_part(email, '@', 1) WHERE full_name = '';
CREATE INDEX IF NOT EXISTS idx_users_org ON users(organization_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_id_org ON users(id, organization_id);

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
    owner_user_id UUID,
    currency VARCHAR(3) NOT NULL CHECK (currency IN ('USD', 'EUR')),
    account_type VARCHAR(50) NOT NULL DEFAULT 'customer' CHECK (account_type IN ('customer', 'external_funding', 'fees', 'suspense', 'inter_org_clearing')),
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
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'wallets'
          AND column_name = 'owner_user_id'
    ) THEN
        ALTER TABLE wallets ADD COLUMN owner_user_id UUID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_wallet_owner_org'
          AND conrelid = 'wallets'::regclass
    ) THEN
        ALTER TABLE wallets
            ADD CONSTRAINT fk_wallet_owner_org
            FOREIGN KEY (owner_user_id, organization_id)
            REFERENCES users(id, organization_id) ON DELETE RESTRICT;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_wallets_org_currency ON wallets(organization_id, currency);
CREATE INDEX IF NOT EXISTS idx_wallets_owner ON wallets(owner_user_id, created_at DESC)
    WHERE owner_user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_wallets_id_org ON wallets(id, organization_id);
DO $$
DECLARE
    account_type_constraint TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO account_type_constraint
    FROM pg_constraint
    WHERE conrelid = 'wallets'::regclass
      AND conname = 'wallets_account_type_check';
    IF account_type_constraint IS NULL
       OR position('inter_org_clearing' IN account_type_constraint) = 0 THEN
        ALTER TABLE wallets DROP CONSTRAINT IF EXISTS wallets_account_type_check;
        ALTER TABLE wallets ADD CONSTRAINT wallets_account_type_check
            CHECK (account_type IN ('customer', 'external_funding', 'fees', 'suspense', 'inter_org_clearing'));
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema()
          AND indexname = 'idx_wallets_system_account'
          AND position('inter_org_clearing' IN indexdef) > 0
    ) THEN
        DROP INDEX IF EXISTS idx_wallets_system_account;
        CREATE UNIQUE INDEX idx_wallets_system_account
            ON wallets(organization_id, currency, account_type)
            WHERE account_type IN ('external_funding', 'fees', 'suspense', 'inter_org_clearing');
    END IF;
END $$;

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
DROP INDEX IF EXISTS idx_journal_entries_reference_unique;
CREATE UNIQUE INDEX idx_journal_entries_reference_unique
    ON journal_entries(organization_id, reference_id, reference_type)
    WHERE reference_id IS NOT NULL;

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

CREATE OR REPLACE FUNCTION validate_ledger_entry_account()
RETURNS TRIGGER AS $$
DECLARE
    journal_org UUID;
    journal_currency VARCHAR(3);
    wallet_org UUID;
    wallet_currency VARCHAR(3);
BEGIN
    IF TG_TABLE_NAME = 'journal_entries' THEN
        RETURN NEW;
    END IF;

    SELECT organization_id, currency INTO journal_org, journal_currency
    FROM journal_entries WHERE id = NEW.journal_entry_id;
    SELECT organization_id, currency INTO wallet_org, wallet_currency
    FROM wallets WHERE id = NEW.wallet_id;

    IF journal_org IS NULL OR wallet_org IS NULL
       OR journal_org <> wallet_org OR journal_currency <> wallet_currency THEN
        RAISE EXCEPTION 'ledger entry account must match journal organization and currency';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_ledger_entry_account ON ledger_entries;
CREATE TRIGGER trg_validate_ledger_entry_account
BEFORE INSERT ON ledger_entries
FOR EACH ROW EXECUTE FUNCTION validate_ledger_entry_account();

CREATE OR REPLACE FUNCTION validate_journal_balance()
RETURNS TRIGGER AS $$
DECLARE
    entry_count BIGINT;
    total_debit NUMERIC;
    total_credit NUMERIC;
    target_journal_id UUID;
BEGIN
    IF TG_TABLE_NAME = 'journal_entries' THEN
        target_journal_id := NEW.id;
    ELSE
        target_journal_id := NEW.journal_entry_id;
    END IF;

    SELECT COUNT(*),
           COALESCE(SUM(amount) FILTER (WHERE direction = 'DR'), 0),
           COALESCE(SUM(amount) FILTER (WHERE direction = 'CR'), 0)
    INTO entry_count, total_debit, total_credit
    FROM ledger_entries
    WHERE journal_entry_id = target_journal_id;

    IF entry_count < 2 OR total_debit <> total_credit THEN
        RAISE EXCEPTION 'journal is not a balanced double-entry journal';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_journal_balance ON journal_entries;
CREATE CONSTRAINT TRIGGER trg_validate_journal_balance
AFTER INSERT ON journal_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_journal_balance();

DROP TRIGGER IF EXISTS trg_validate_ledger_journal_balance ON ledger_entries;
CREATE CONSTRAINT TRIGGER trg_validate_ledger_journal_balance
AFTER INSERT ON ledger_entries
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_journal_balance();

CREATE OR REPLACE FUNCTION prevent_journal_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'journal entries are immutable';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_journal_update_delete ON journal_entries;
CREATE TRIGGER trg_prevent_journal_update_delete
BEFORE UPDATE OR DELETE ON journal_entries
FOR EACH ROW EXECUTE FUNCTION prevent_journal_modification();

CREATE OR REPLACE FUNCTION validate_wallet_ledger_balance()
RETURNS TRIGGER AS $$
DECLARE
    ledger_balance NUMERIC;
    recorded_balance BIGINT;
BEGIN
    SELECT COALESCE(SUM(
        CASE WHEN direction = 'CR' THEN amount ELSE -amount END
    ), 0)
    INTO ledger_balance
    FROM ledger_entries
    WHERE wallet_id = NEW.id;

    SELECT balance INTO recorded_balance FROM wallets WHERE id = NEW.id;
    IF ledger_balance <> recorded_balance THEN
        RAISE EXCEPTION 'wallet % balance % does not match ledger balance %',
            NEW.id, recorded_balance, ledger_balance;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = 'wallets'::regclass
          AND tgname = 'trg_validate_wallet_ledger_balance'
          AND NOT tgisinternal
    ) THEN
        CREATE CONSTRAINT TRIGGER trg_validate_wallet_ledger_balance
        AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_wallet_ledger_balance();
    END IF;
END $$;

-- -------------------------------------------------------------
-- 4. TRANSFERS & SAGA STATE MACHINE
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transfers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    source_wallet_id UUID NOT NULL REFERENCES wallets(id),
    destination_wallet_id UUID NOT NULL REFERENCES wallets(id),
    destination_organization_id UUID NOT NULL REFERENCES organizations(id),
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
    CONSTRAINT chk_different_wallets CHECK (source_wallet_id != destination_wallet_id),
    CONSTRAINT fk_transfer_source_org FOREIGN KEY (source_wallet_id, organization_id)
        REFERENCES wallets(id, organization_id),
    CONSTRAINT fk_transfer_destination_org FOREIGN KEY (destination_wallet_id, destination_organization_id)
        REFERENCES wallets(id, organization_id)
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
    correlation_id VARCHAR(64),
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED', 'DEAD_LETTER')),
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(64);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(status, next_retry_at) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_outbox_processing_claimed ON outbox_events(claimed_at)
    WHERE status = 'PROCESSING';

CREATE TABLE IF NOT EXISTS inbox_records (
    message_id VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (message_id, consumer_group)
);

CREATE TABLE IF NOT EXISTS dead_letter_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source VARCHAR(100) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    payload BYTEA NOT NULL,
    delivery_count BIGINT NOT NULL,
    error_message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at TIMESTAMPTZ,
    UNIQUE (source, message_id)
);
CREATE INDEX IF NOT EXISTS idx_dead_letter_created ON dead_letter_messages(created_at DESC);

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
    claimed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_pending ON webhook_deliveries(status, next_retry_at) WHERE status IN ('PENDING', 'PROCESSING');
CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_transfer_delivery_unique
    ON webhook_deliveries(webhook_id, event_type, (payload->>'transfer_id'))
    WHERE payload ? 'transfer_id';

-- -------------------------------------------------------------
-- 8. RECONCILIATION RUNS & AUDIT DISCREPANCIES
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id),
    status VARCHAR(50) NOT NULL, -- 'SUCCESS', 'DISCREPANCIES_FOUND'
    total_wallets_checked INT NOT NULL DEFAULT 0,
    total_journals_checked INT NOT NULL DEFAULT 0,
    discrepancies JSONB DEFAULT '[]'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(64);
ALTER TABLE webhook_deliveries ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE reconciliation_runs ADD COLUMN IF NOT EXISTS organization_id UUID REFERENCES organizations(id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'inbox_records'::regclass
          AND conname = 'inbox_records_pkey'
          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (message_id, consumer_group)'
    ) THEN
        ALTER TABLE inbox_records DROP CONSTRAINT IF EXISTS inbox_records_pkey;
        ALTER TABLE inbox_records ADD CONSTRAINT inbox_records_pkey
            PRIMARY KEY (message_id, consumer_group);
    END IF;
END;
$$;

ALTER TABLE transfers ADD COLUMN IF NOT EXISTS destination_organization_id UUID;
UPDATE transfers t
SET destination_organization_id = w.organization_id
FROM wallets w
WHERE t.destination_wallet_id = w.id
  AND t.destination_organization_id IS NULL;
ALTER TABLE transfers ALTER COLUMN destination_organization_id SET NOT NULL;
ALTER TABLE transfers DROP CONSTRAINT IF EXISTS fk_transfer_destination_org;
ALTER TABLE transfers ADD CONSTRAINT fk_transfer_destination_org
    FOREIGN KEY (destination_wallet_id, destination_organization_id)
    REFERENCES wallets(id, organization_id);

UPDATE wallets w
SET owner_user_id = u.id
FROM users u
WHERE w.owner_user_id IS NULL
  AND w.account_type = 'customer'
  AND u.organization_id = w.organization_id
  AND (SELECT COUNT(*) FROM users org_users WHERE org_users.organization_id = w.organization_id) = 1;
