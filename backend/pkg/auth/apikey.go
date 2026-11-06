package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// GenerateAPIKey creates a secure random API key with a prefix and its SHA256 hash
func GenerateAPIKey() (rawKey string, prefix string, hashedKey string, err error) {
	bytes := make([]byte, 24)
	if _, err := rand.Read(bytes); err != nil {
		return "", "", "", err
	}

	secret := hex.EncodeToString(bytes)
	prefix = "ak_" + secret[:8]
	rawKey = fmt.Sprintf("%s_%s", prefix, secret[8:])
	hashedKey = HashAPIKey(rawKey)

	return rawKey, prefix, hashedKey, nil
}

// HashAPIKey hashes the raw API key with SHA-256 for persistent database lookup
func HashAPIKey(rawKey string) string {
	hash := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(hash[:])
}
