package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrCurrencyMismatch     = errors.New("currencies do not match")
	ErrInvalidAmount        = errors.New("amount must be strictly positive")
	ErrInsufficientBalance  = errors.New("insufficient available balance in wallet")
	ErrSelfTransfer         = errors.New("cannot transfer money to the same wallet")
	ErrUnbalancedJournal    = errors.New("journal entries do not balance: sum of debits must equal sum of credits")
	ErrInvalidDirection     = errors.New("direction must be DR (Debit) or CR (Credit)")
	ErrUnsupportedCurrency  = errors.New("unsupported currency: only USD and EUR allowed")
	ErrWalletSuspended      = errors.New("wallet is not active")
)

type Direction string

const (
	DirectionDebit  Direction = "DR"
	DirectionCredit Direction = "CR"
)

type AccountType string

const (
	AccountCustomer        AccountType = "customer"
	AccountExternalFunding AccountType = "external_funding"
	AccountFees            AccountType = "fees"
	AccountSuspense        AccountType = "suspense"
)

type WalletStatus string

const (
	WalletStatusActive    WalletStatus = "active"
	WalletStatusSuspended WalletStatus = "suspended"
	WalletStatusClosed    WalletStatus = "closed"
)

// Wallet represents a single currency financial account
type Wallet struct {
	ID             uuid.UUID    `json:"id"`
	OrganizationID uuid.UUID    `json:"organization_id"`
	Currency       string       `json:"currency"`
	AccountType    AccountType  `json:"account_type"`
	Balance        int64        `json:"balance"`      // Stored in minor units (e.g. cents)
	HoldBalance    int64        `json:"hold_balance"` // Funds reserved for in-flight sagas
	Status         WalletStatus `json:"status"`
	Version        int64        `json:"version"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

func (w *Wallet) AvailableBalance() int64 {
	if w.AccountType != AccountCustomer {
		return w.Balance
	}
	return w.Balance - w.HoldBalance
}

// JournalEntry groups atomic ledger entries
type JournalEntry struct {
	ID             uuid.UUID     `json:"id"`
	OrganizationID uuid.UUID     `json:"organization_id"`
	ReferenceID    *uuid.UUID    `json:"reference_id,omitempty"`
	ReferenceType  string        `json:"reference_type"` // "transfer", "deposit", "reversal", "fee"
	Currency       string        `json:"currency"`
	Description    string        `json:"description"`
	CreatedAt      time.Time     `json:"created_at"`
	Entries        []LedgerEntry `json:"entries,omitempty"`
}

// LedgerEntry represents an atomic single-direction entry
type LedgerEntry struct {
	ID             uuid.UUID `json:"id"`
	JournalEntryID uuid.UUID `json:"journal_entry_id"`
	WalletID       uuid.UUID `json:"wallet_id"`
	Direction      Direction `json:"direction"`
	Amount         int64     `json:"amount"` // Strictly positive minor unit
	CreatedAt      time.Time `json:"created_at"`
}

// ValidateJournalBalance validates that total DR == total CR
func ValidateJournalBalance(entries []LedgerEntry) error {
	if len(entries) < 2 {
		return errors.New("a double-entry journal requires at least two ledger entries")
	}

	var totalDebit, totalCredit int64
	for _, e := range entries {
		if e.Amount <= 0 {
			return ErrInvalidAmount
		}
		switch e.Direction {
		case DirectionDebit:
			totalDebit += e.Amount
		case DirectionCredit:
			totalCredit += e.Amount
		default:
			return ErrInvalidDirection
		}
	}

	if totalDebit != totalCredit {
		return ErrUnbalancedJournal
	}
	return nil
}
