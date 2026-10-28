package domain_test

import (
	"testing"
	"time"

	"github.com/distributed-wallet/platform/pkg/domain"
	"github.com/google/uuid"
)

func TestValidateJournalBalance_Balanced(t *testing.T) {
	journalID := uuid.New()
	walletA := uuid.New()
	walletB := uuid.New()

	entries := []domain.LedgerEntry{
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       walletA,
			Direction:      domain.DirectionDebit,
			Amount:         5000, // $50.00
			CreatedAt:      time.Now(),
		},
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       walletB,
			Direction:      domain.DirectionCredit,
			Amount:         5000, // $50.00
			CreatedAt:      time.Now(),
		},
	}

	err := domain.ValidateJournalBalance(entries)
	if err != nil {
		t.Fatalf("expected journal to be balanced, got error: %v", err)
	}
}

func TestValidateJournalBalance_WithFee(t *testing.T) {
	journalID := uuid.New()
	srcWallet := uuid.New()
	dstWallet := uuid.New()
	feeWallet := uuid.New()

	entries := []domain.LedgerEntry{
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       srcWallet,
			Direction:      domain.DirectionDebit,
			Amount:         5010, // $50.00 + $0.10 fee
			CreatedAt:      time.Now(),
		},
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       dstWallet,
			Direction:      domain.DirectionCredit,
			Amount:         5000, // $50.00
			CreatedAt:      time.Now(),
		},
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       feeWallet,
			Direction:      domain.DirectionCredit,
			Amount:         10, // $0.10 fee
			CreatedAt:      time.Now(),
		},
	}

	err := domain.ValidateJournalBalance(entries)
	if err != nil {
		t.Fatalf("expected 3-legged fee journal to balance, got: %v", err)
	}
}

func TestValidateJournalBalance_UnbalancedFails(t *testing.T) {
	journalID := uuid.New()
	entries := []domain.LedgerEntry{
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       uuid.New(),
			Direction:      domain.DirectionDebit,
			Amount:         5000,
			CreatedAt:      time.Now(),
		},
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       uuid.New(),
			Direction:      domain.DirectionCredit,
			Amount:         4999, // Off by 1 cent!
			CreatedAt:      time.Now(),
		},
	}

	err := domain.ValidateJournalBalance(entries)
	if err != domain.ErrUnbalancedJournal {
		t.Fatalf("expected ErrUnbalancedJournal, got: %v", err)
	}
}

func TestValidateJournalBalance_NegativeAmountFails(t *testing.T) {
	journalID := uuid.New()
	entries := []domain.LedgerEntry{
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       uuid.New(),
			Direction:      domain.DirectionDebit,
			Amount:         -100,
			CreatedAt:      time.Now(),
		},
		{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			WalletID:       uuid.New(),
			Direction:      domain.DirectionCredit,
			Amount:         -100,
			CreatedAt:      time.Now(),
		},
	}

	err := domain.ValidateJournalBalance(entries)
	if err != domain.ErrInvalidAmount {
		t.Fatalf("expected ErrInvalidAmount, got: %v", err)
	}
}
