package webhooks

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

const encryptedSecretPrefix = "enc:v1:"

func ParseEncryptionKey(value string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("decode webhook encryption key: %w", err)
	}
	if len(key) != 32 {
		return nil, errors.New("webhook encryption key must decode to exactly 32 bytes")
	}
	return key, nil
}

func EncryptSecret(key []byte, secret string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create webhook secret cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create webhook secret AEAD: %w", err)
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate webhook secret nonce: %w", err)
	}
	ciphertext := aead.Seal(nonce, nonce, []byte(secret), nil)
	return encryptedSecretPrefix + base64.RawStdEncoding.EncodeToString(ciphertext), nil
}

func DecryptSecret(key []byte, stored string) (string, error) {
	if !strings.HasPrefix(stored, encryptedSecretPrefix) {
		return stored, nil
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(stored, encryptedSecretPrefix))
	if err != nil {
		return "", fmt.Errorf("decode encrypted webhook secret: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create webhook secret cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create webhook secret AEAD: %w", err)
	}
	if len(ciphertext) < aead.NonceSize() {
		return "", errors.New("encrypted webhook secret is truncated")
	}
	nonce, body := ciphertext[:aead.NonceSize()], ciphertext[aead.NonceSize():]
	plaintext, err := aead.Open(nil, nonce, body, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt webhook secret: %w", err)
	}
	return string(plaintext), nil
}
