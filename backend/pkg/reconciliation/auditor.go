package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type Discrepancy struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	Description string `json:"description"`
}

type Auditor struct {
	db     *pgxpool.Pool
	logger *zap.Logger
}

func NewAuditor(db *pgxpool.Pool, logger *zap.Logger) *Auditor {
	return &Auditor{db: db, logger: logger}
}

func (a *Auditor) RunAudit(ctx context.Context) ([]Discrepancy, error) {
	return a.runAudit(ctx, nil)
}

func (a *Auditor) RunAuditForOrganization(ctx context.Context, organizationID uuid.UUID) ([]Discrepancy, error) {
	return a.runAudit(ctx, &organizationID)
}

func (a *Auditor) runAudit(ctx context.Context, organizationID *uuid.UUID) ([]Discrepancy, error) {
	var discrepancies []Discrepancy
	var scope any
	if organizationID != nil {
		scope = *organizationID
	}

	rows, err := a.db.Query(ctx, `
		SELECT j.id, COUNT(le.id),
		       COALESCE(SUM(le.amount) FILTER (WHERE le.direction = 'DR'), 0),
		       COALESCE(SUM(le.amount) FILTER (WHERE le.direction = 'CR'), 0)
		FROM journal_entries j
		LEFT JOIN ledger_entries le ON le.journal_entry_id = j.id
		WHERE ($1::uuid IS NULL OR j.organization_id = $1)
		GROUP BY j.id
		HAVING COUNT(le.id) < 2
		    OR COALESCE(SUM(le.amount) FILTER (WHERE le.direction = 'DR'), 0)
		       <> COALESCE(SUM(le.amount) FILTER (WHERE le.direction = 'CR'), 0)
	`, scope)
	if err != nil {
		return nil, fmt.Errorf("query journal discrepancies: %w", err)
	}
	for rows.Next() {
		var journalID uuid.UUID
		var entries, debits, credits int64
		if err := rows.Scan(&journalID, &entries, &debits, &credits); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan journal discrepancy: %w", err)
		}
		discrepancies = append(discrepancies, Discrepancy{
			Type:        "UNBALANCED_JOURNAL",
			ID:          journalID.String(),
			Description: fmt.Sprintf("Journal %s has %d entries: DR=%d, CR=%d", journalID, entries, debits, credits),
		})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate journal discrepancies: %w", err)
	}
	rows.Close()

	walletRows, err := a.db.Query(ctx, `
		SELECT w.id, w.currency, w.balance,
		       COALESCE(SUM(CASE WHEN le.direction = 'CR' THEN le.amount ELSE -le.amount END), 0)
		FROM wallets w
		LEFT JOIN ledger_entries le ON le.wallet_id = w.id
		WHERE ($1::uuid IS NULL OR w.organization_id = $1)
		GROUP BY w.id, w.currency, w.balance
		HAVING w.balance <> COALESCE(SUM(CASE WHEN le.direction = 'CR' THEN le.amount ELSE -le.amount END), 0)
	`, scope)
	if err != nil {
		return nil, fmt.Errorf("query wallet balance discrepancies: %w", err)
	}
	for walletRows.Next() {
		var walletID uuid.UUID
		var currency string
		var recorded, calculated int64
		if err := walletRows.Scan(&walletID, &currency, &recorded, &calculated); err != nil {
			walletRows.Close()
			return nil, fmt.Errorf("scan wallet discrepancy: %w", err)
		}
		discrepancies = append(discrepancies, Discrepancy{
			Type:        "BALANCE_MISMATCH",
			ID:          walletID.String(),
			Description: fmt.Sprintf("Wallet %s (%s) balance mismatch: recorded=%d, ledger_sum=%d", walletID, currency, recorded, calculated),
		})
	}
	if err := walletRows.Err(); err != nil {
		walletRows.Close()
		return nil, fmt.Errorf("iterate wallet discrepancies: %w", err)
	}
	walletRows.Close()

	if organizationID == nil {
		clearingRows, err := a.db.Query(ctx, `
			SELECT currency
			FROM wallets
			WHERE account_type = 'inter_org_clearing'
			GROUP BY currency
			HAVING SUM(balance) <> 0
		`)
		if err != nil {
			return nil, fmt.Errorf("query inter-organization clearing discrepancies: %w", err)
		}
		for clearingRows.Next() {
			var currency string
			if err := clearingRows.Scan(&currency); err != nil {
				clearingRows.Close()
				return nil, fmt.Errorf("scan inter-organization clearing discrepancy: %w", err)
			}
			discrepancies = append(discrepancies, Discrepancy{
				Type:        "INTER_ORG_CLEARING_MISMATCH",
				ID:          currency,
				Description: fmt.Sprintf("Inter-organization clearing balances do not net to zero for %s", currency),
			})
		}
		if err := clearingRows.Err(); err != nil {
			clearingRows.Close()
			return nil, fmt.Errorf("iterate inter-organization clearing discrepancies: %w", err)
		}
		clearingRows.Close()
	}

	missingLedgerRows, err := a.db.Query(ctx, `
		SELECT t.id
		FROM transfers t
		WHERE t.status IN ('COMPLETED', 'REVERSED')
		  AND ($1::uuid IS NULL OR t.organization_id = $1 OR t.destination_organization_id = $1)
		  AND (
		      NOT EXISTS (
		          SELECT 1 FROM journal_entries j
		          WHERE j.reference_id = t.id AND j.reference_type = 'transfer'
		            AND j.organization_id = t.organization_id
		      )
		      OR (
		          t.destination_organization_id <> t.organization_id
		          AND NOT EXISTS (
		              SELECT 1 FROM journal_entries j
		              WHERE j.reference_id = t.id AND j.reference_type = 'transfer'
		                AND j.organization_id = t.destination_organization_id
		          )
		      )
		  )
	`, scope)
	if err != nil {
		return nil, fmt.Errorf("query transfers missing ledger: %w", err)
	}
	for missingLedgerRows.Next() {
		var transferID uuid.UUID
		if err := missingLedgerRows.Scan(&transferID); err != nil {
			missingLedgerRows.Close()
			return nil, fmt.Errorf("scan transfer missing ledger: %w", err)
		}
		discrepancies = append(discrepancies, Discrepancy{
			Type:        "MISSING_LEDGER_TRANSFER",
			ID:          transferID.String(),
			Description: fmt.Sprintf("Transfer %s has no corresponding journal entry", transferID),
		})
	}
	if err := missingLedgerRows.Err(); err != nil {
		missingLedgerRows.Close()
		return nil, fmt.Errorf("iterate transfers missing ledger: %w", err)
	}
	missingLedgerRows.Close()

	stuckSagaRows, err := a.db.Query(ctx, `
		SELECT id, saga_step, updated_at
		FROM transfers
		WHERE status IN ('PENDING', 'PROCESSING')
		  AND ($1::uuid IS NULL OR organization_id = $1 OR destination_organization_id = $1)
		  AND updated_at < NOW() - INTERVAL '5 minutes'
	`, scope)
	if err != nil {
		return nil, fmt.Errorf("query stuck sagas: %w", err)
	}
	for stuckSagaRows.Next() {
		var transferID uuid.UUID
		var step string
		var updatedAt time.Time
		if err := stuckSagaRows.Scan(&transferID, &step, &updatedAt); err != nil {
			stuckSagaRows.Close()
			return nil, fmt.Errorf("scan stuck saga: %w", err)
		}
		discrepancies = append(discrepancies, Discrepancy{
			Type:        "STUCK_SAGA",
			ID:          transferID.String(),
			Description: fmt.Sprintf("Transfer saga %s stuck in step %s since %s", transferID, step, updatedAt.Format(time.RFC3339)),
		})
	}
	if err := stuckSagaRows.Err(); err != nil {
		stuckSagaRows.Close()
		return nil, fmt.Errorf("iterate stuck sagas: %w", err)
	}
	stuckSagaRows.Close()

	discrepancyBytes, err := json.Marshal(discrepancies)
	if err != nil {
		return nil, fmt.Errorf("marshal reconciliation discrepancies: %w", err)
	}
	status := "SUCCESS"
	if len(discrepancies) > 0 {
		status = "DISCREPANCIES_FOUND"
	}
	if _, err := a.db.Exec(ctx, `
		INSERT INTO reconciliation_runs (organization_id, status, discrepancies, started_at, completed_at)
		VALUES ($1, $2, $3, NOW(), NOW())
	`, scope, status, discrepancyBytes); err != nil {
		return nil, fmt.Errorf("record reconciliation run: %w", err)
	}

	return discrepancies, nil
}
