package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/argon2"
)

var (
	ErrInvalidToken     = errors.New("invalid or expired token")
	ErrInvalidPassword  = errors.New("invalid credentials")
	ErrTokenRevoked     = errors.New("token has been revoked")
	ErrTokenReused      = errors.New("refresh token reuse detected! Revoking session family")
	ErrInsufficientRole = errors.New("unauthorized role action")
)

type TokenClaims struct {
	UserID         uuid.UUID `json:"user_id"`
	OrganizationID uuid.UUID `json:"organization_id"`
	Role           string    `json:"role"`
	TokenType      string    `json:"token_type"` // "access" or "refresh"
	FamilyID       uuid.UUID `json:"family_id,omitempty"`
	jwt.RegisteredClaims
}

type Argon2Params struct {
	Memory      uint32
	Iterations  uint32
	Parallelism uint8
	SaltLength  uint32
	KeyLength   uint32
}

var DefaultArgon2Params = &Argon2Params{
	Memory:      64 * 1024, // 64MB
	Iterations:  3,
	Parallelism: 2,
	SaltLength:  16,
	KeyLength:   32,
}

// HashPassword creates an Argon2id hash of a password
func HashPassword(password string) (string, error) {
	p := DefaultArgon2Params
	salt := make([]byte, p.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	hash := argon2.IDKey([]byte(password), salt, p.Iterations, p.Memory, p.Parallelism, p.KeyLength)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	encodedHash := fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, p.Memory, p.Iterations, p.Parallelism, b64Salt, b64Hash)

	return encodedHash, nil
}

// ComparePassword compares an Argon2id hash with a raw password
func ComparePassword(password, encodedHash string) (bool, error) {
	parts := strings.Split(encodedHash, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, errors.New("invalid hash format")
	}

	var version int
	var memory, iterations uint32
	var parallelism uint8
	_, err := fmt.Sscanf(parts[2], "v=%d", &version)
	if err != nil {
		return false, err
	}
	_, err = fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism)
	if err != nil {
		return false, err
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, err
	}
	decodedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, err
	}

	comparisonHash := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, uint32(len(decodedHash)))

	if subtle.ConstantTimeCompare(decodedHash, comparisonHash) == 1 {
		return true, nil
	}
	return false, nil
}

// GenerateTokenPair generates an access token (15m) and a refresh token (7d)
func GenerateTokenPair(secret string, userID, orgID uuid.UUID, role string, familyID *uuid.UUID) (accessToken string, refreshToken string, famID uuid.UUID, err error) {
	if familyID == nil {
		famID = uuid.New()
	} else {
		famID = *familyID
	}

	// 1. Access Token (15 mins)
	accessClaims := &TokenClaims{
		UserID:         userID,
		OrganizationID: orgID,
		Role:           role,
		TokenType:      "access",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ID:        uuid.New().String(),
		},
	}
	accessJwt := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	accessToken, err = accessJwt.SignedString([]byte(secret))
	if err != nil {
		return "", "", uuid.Nil, err
	}

	// 2. Refresh Token (7 days)
	refreshClaims := &TokenClaims{
		UserID:         userID,
		OrganizationID: orgID,
		Role:           role,
		TokenType:      "refresh",
		FamilyID:       famID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(7 * 24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ID:        uuid.New().String(),
		},
	}
	refreshJwt := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
	refreshToken, err = refreshJwt.SignedString([]byte(secret))
	if err != nil {
		return "", "", uuid.Nil, err
	}

	return accessToken, refreshToken, famID, nil
}

// ValidateToken parses and validates a JWT token
func ValidateToken(secret, tokenString string) (*TokenClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &TokenClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	})

	if err != nil {
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*TokenClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}
