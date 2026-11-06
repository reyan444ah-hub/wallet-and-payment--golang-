package auth_test

import (
	"testing"

	"github.com/distributed-wallet/platform/pkg/auth"
	"github.com/google/uuid"
)

func TestArgon2PasswordHashing(t *testing.T) {
	password := "SecurePaymentPassword!2026"

	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("failed hashing password: %v", err)
	}

	match, err := auth.ComparePassword(password, hash)
	if err != nil || !match {
		t.Fatalf("expected password match, got match=%v err=%v", match, err)
	}

	wrongMatch, _ := auth.ComparePassword("WrongPassword123", hash)
	if wrongMatch {
		t.Fatalf("wrong password should not match")
	}
}

func TestJWTTokenLifecycle(t *testing.T) {
	secret := "secret-jwt-key-for-testing"
	userID := uuid.New()
	orgID := uuid.New()
	role := "finance"

	access, refresh, familyID, err := auth.GenerateTokenPair(secret, userID, orgID, role, nil)
	if err != nil {
		t.Fatalf("token pair generation failed: %v", err)
	}

	claims, err := auth.ValidateToken(secret, access)
	if err != nil {
		t.Fatalf("access token validation failed: %v", err)
	}

	if claims.UserID != userID || claims.Role != "finance" || claims.TokenType != "access" {
		t.Fatalf("claims mismatch: %+v", claims)
	}

	refreshClaims, err := auth.ValidateToken(secret, refresh)
	if err != nil {
		t.Fatalf("refresh token validation failed: %v", err)
	}
	if refreshClaims.FamilyID != familyID || refreshClaims.TokenType != "refresh" {
		t.Fatalf("refresh claims mismatch: %+v", refreshClaims)
	}
}

func TestAPIKeyGenerationAndHashing(t *testing.T) {
	rawKey, prefix, hash, err := auth.GenerateAPIKey()
	if err != nil {
		t.Fatalf("api key generation failed: %v", err)
	}

	if len(rawKey) < 20 || len(prefix) < 8 {
		t.Fatalf("invalid key length or prefix")
	}

	computedHash := auth.HashAPIKey(rawKey)
	if computedHash != hash {
		t.Fatalf("hash mismatch: expected %s, got %s", hash, computedHash)
	}
}

func TestJWTTokenValidation_InvalidOrWrongSecret(t *testing.T) {
	secret := "secret-jwt-key-1"
	wrongSecret := "secret-jwt-key-2"
	userID := uuid.New()
	orgID := uuid.New()

	access, _, _, err := auth.GenerateTokenPair(secret, userID, orgID, "viewer", nil)
	if err != nil {
		t.Fatalf("failed generating token: %v", err)
	}

	// Validate with wrong secret
	_, err = auth.ValidateToken(wrongSecret, access)
	if err == nil {
		t.Fatal("expected token validation to fail when using incorrect secret")
	}

	// Validate malformed token string
	_, err = auth.ValidateToken(secret, "malformed.jwt.token")
	if err == nil {
		t.Fatal("expected malformed token validation to fail")
	}
}
