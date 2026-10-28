package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/distributed-wallet/platform/pkg/auth"
	"github.com/distributed-wallet/platform/pkg/domain"
	"github.com/distributed-wallet/platform/pkg/outbox"
	"github.com/distributed-wallet/platform/pkg/ratelimit"
	"github.com/distributed-wallet/platform/pkg/reconciliation"
	"github.com/distributed-wallet/platform/pkg/telemetry"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Server struct {
	router      *gin.Engine
	db          *pgxpool.Pool
	limiter     *ratelimit.Limiter
	metrics     *telemetry.Metrics
	auditor     *reconciliation.Auditor
	logger      *zap.Logger
	jwtSecret   string
	transferFee int64 // Minor units e.g. 10 = $0.10
}

func NewServer(
	db *pgxpool.Pool,
	limiter *ratelimit.Limiter,
	metrics *telemetry.Metrics,
	logger *zap.Logger,
	jwtSecret string,
) *Server {
	s := &Server{
		router:      gin.Default(),
		db:          db,
		limiter:     limiter,
		metrics:     metrics,
		auditor:     reconciliation.NewAuditor(db, logger),
		logger:      logger,
		jwtSecret:   jwtSecret,
		transferFee: 10, // 10 cents standard platform fee
	}

	s.setupRoutes()
	return s
}

func (s *Server) Router() *gin.Engine {
	return s.router
}

func (s *Server) setupRoutes() {
	// Prometheus metrics
	s.router.GET("/api/v1/metrics", gin.WrapH(promhttp.Handler()))

	v1 := s.router.Group("/api/v1")
	{
		// Authentication & Session
		authGroup := v1.Group("/auth")
		{
			authGroup.POST("/register", s.handleRegister)
			authGroup.POST("/login", s.handleLogin)
			authGroup.POST("/refresh", s.handleRefreshToken)
			authGroup.POST("/logout", s.authMiddleware(), s.handleLogout)
		}

		// API Keys (Requires 'api_keys:manage' scope or admin role)
		apiKeys := v1.Group("/api-keys")
		apiKeys.Use(s.authMiddleware(), s.requirePermission("api_keys:manage", "admin"))
		{
			apiKeys.POST("", s.handleCreateAPIKey)
			apiKeys.DELETE("/:id", s.handleRevokeAPIKey)
		}

		// Wallets & Accounts
		wallets := v1.Group("/wallets")
		wallets.Use(s.authMiddleware())
		{
			wallets.POST("", s.requirePermission("wallets:write", "admin", "finance"), s.handleCreateWallet)
			wallets.GET("/:id", s.requirePermission("wallets:read", "admin", "finance", "viewer"), s.handleGetWallet)
			wallets.POST("/:id/deposit", s.requirePermission("wallets:deposit", "admin", "finance"), s.idempotencyMiddleware(), s.handleDeposit)
			wallets.GET("/:id/ledger", s.requirePermission("ledger:read", "admin", "finance", "viewer"), s.handleGetLedgerEntries)
		}

		// Transfers (Saga)
		transfers := v1.Group("/transfers")
		transfers.Use(s.authMiddleware())
		{
			transfers.POST("", s.requirePermission("transfers:write", "admin", "finance"), s.idempotencyMiddleware(), s.handleCreateTransfer)
			transfers.GET("/:id", s.requirePermission("transfers:read", "admin", "finance", "viewer"), s.handleGetTransfer)
			transfers.POST("/:id/reverse", s.requirePermission("transfers:reverse", "admin"), s.idempotencyMiddleware(), s.handleReverseTransfer)
		}

		// Webhooks
		webhooks := v1.Group("/webhooks")
		webhooks.Use(s.authMiddleware(), s.requirePermission("webhooks:manage", "admin"))
		{
			webhooks.POST("", s.handleRegisterWebhook)
		}

		// Reconciliation Audit Reports
		reports := v1.Group("/reports")
		reports.Use(s.authMiddleware(), s.requirePermission("reports:read", "admin", "finance"))
		{
			reports.GET("/reconciliation", s.handleReconciliationReport)
		}
	}
}

// -------------------------------------------------------------
// AUTHENTICATION & MULTI-TENANCY MIDDLEWARE
// -------------------------------------------------------------
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. Try API Key Header first
		apiKey := c.GetHeader("X-API-Key")
		if apiKey != "" {
			keyHash := auth.HashAPIKey(apiKey)
			var orgID, keyID uuid.UUID
			var scopes []string
			var revoked bool
			err := s.db.QueryRow(c.Request.Context(), `
				SELECT id, organization_id, scopes, revoked FROM api_keys WHERE key_hash = $1
			`, keyHash).Scan(&keyID, &orgID, &scopes, &revoked)
			if err != nil || revoked {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or revoked api key"})
				return
			}

			c.Set("organization_id", orgID)
			c.Set("actor_id", keyID)
			c.Set("actor_type", "api_key")
			c.Set("scopes", scopes)
			c.Set("role", "api_client") // Do not escalate to admin! Scopes must be checked.
			c.Next()
			return
		}

		// 2. Bearer JWT Access Token
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authorization token required"})
			return
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		claims, err := auth.ValidateToken(s.jwtSecret, tokenStr)
		if err != nil || claims.TokenType != "access" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}

		c.Set("user_id", claims.UserID)
		c.Set("organization_id", claims.OrganizationID)
		c.Set("role", claims.Role)
		c.Set("actor_id", claims.UserID)
		c.Set("actor_type", "user")
		c.Next()
	}
}

func (s *Server) requirePermission(requiredScope string, allowedRoles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		actorTypeVal, _ := c.Get("actor_type")
		actorType, _ := actorTypeVal.(string)

		if actorType == "api_key" {
			scopesVal, exists := c.Get("scopes")
			if !exists {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "insufficient api key scope"})
				return
			}
			scopes, ok := scopesVal.([]string)
			if !ok {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "invalid api key scopes"})
				return
			}

			// Check if required scope or wildcard * or admin scope is present
			for _, sc := range scopes {
				if sc == requiredScope || sc == "*" || sc == "admin" {
					c.Next()
					return
				}
			}

			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": fmt.Sprintf("api key lacks required scope: %s", requiredScope),
			})
			return
		}

		// Human user: check role
		userRole, _ := c.Get("role")
		roleStr, ok := userRole.(string)
		if !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "insufficient privileges"})
			return
		}

		for _, r := range allowedRoles {
			if r == roleStr {
				c.Next()
				return
			}
		}

		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "unauthorized role action for this resource"})
	}
}

// -------------------------------------------------------------
// IDEMPOTENCY MIDDLEWARE
// -------------------------------------------------------------
func (s *Server) idempotencyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		idempotencyKey := c.GetHeader("Idempotency-Key")
		if idempotencyKey == "" {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required"})
			return
		}

		orgIDVal, _ := c.Get("organization_id")
		orgID := orgIDVal.(uuid.UUID)

		// Read, hash, and restore the request body so downstream handlers can parse it
		var bodyBytes []byte
		if c.Request.Body != nil {
			var err error
			bodyBytes, err = c.GetRawData()
			if err != nil {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
				return
			}
			c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		hash := sha256.Sum256(bodyBytes)
		reqHash := hex.EncodeToString(hash[:])

		// Check existing idempotency record
		var savedHash string
		var respCode *int
		var respBody []byte
		err := s.db.QueryRow(c.Request.Context(), `
			SELECT request_hash, response_code, response_body 
			FROM idempotency_keys 
			WHERE organization_id = $1 AND key = $2
		`, orgID, idempotencyKey).Scan(&savedHash, &respCode, &respBody)

		if err == nil {
			// Key exists: check payload match
			if savedHash != reqHash {
				c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
					"error": "Idempotency key reuse with different request payload is prohibited",
				})
				return
			}

			// If already completed, replay recorded response
			if respCode != nil && respBody != nil {
				var jsonResp any
				_ = json.Unmarshal(respBody, &jsonResp)
				c.AbortWithStatusJSON(*respCode, jsonResp)
				return
			}

			// In-flight concurrent request
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "concurrent request with same idempotency key is already in progress"})
			return
		}

		// Insert lock record
		_, err = s.db.Exec(c.Request.Context(), `
			INSERT INTO idempotency_keys (key, organization_id, request_path, request_hash, created_at, locked_until)
			VALUES ($1, $2, $3, $4, NOW(), NOW() + INTERVAL '1 minute')
		`, idempotencyKey, orgID, c.Request.URL.Path, reqHash)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "idempotency key conflict"})
			return
		}

		c.Set("idempotency_key", idempotencyKey)
		c.Set("request_hash", reqHash)
		c.Set("request_body_bytes", bodyBytes)

		c.Next()
	}
}

// -------------------------------------------------------------
// HANDLERS: AUTH
// -------------------------------------------------------------
type RegisterRequest struct {
	OrganizationName string `json:"organization_name" binding:"required"`
	Email            string `json:"email" binding:"required,email"`
	Password         string `json:"password" binding:"required,min=8"`
	Role             string `json:"role"` // Optional; defaults to 'viewer' unless creating a brand-new organization as founder
}

func (s *Server) handleRegister(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	pwHash, err := auth.HashPassword(req.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	// Check if organization already exists
	var orgID uuid.UUID
	var isNewOrg bool
	err = tx.QueryRow(c.Request.Context(), `
		SELECT id FROM organizations WHERE name = $1
	`, req.OrganizationName).Scan(&orgID)

	if err != nil {
		// Create new Organization: founder receives 'admin' role
		isNewOrg = true
		err = tx.QueryRow(c.Request.Context(), `
			INSERT INTO organizations (name) VALUES ($1) RETURNING id
		`, req.OrganizationName).Scan(&orgID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create organization"})
			return
		}
	}

	// Security Policy: Only the initial organization creator gets admin.
	// Any subsequent public registrant in an existing org is assigned 'viewer' to prevent privilege escalation.
	assignedRole := "viewer"
	if isNewOrg {
		assignedRole = "admin"
	}

	// Create User
	var userID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		INSERT INTO users (organization_id, email, password_hash, role)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, orgID, req.Email, pwHash, assignedRole).Scan(&userID)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "email already registered"})
		return
	}

	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"organization_id": orgID,
		"user_id":         userID,
		"email":           req.Email,
		"role":            assignedRole,
	})
	return
}

type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

func (s *Server) handleLogin(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var userID, orgID uuid.UUID
	var pwHash, role string
	err := s.db.QueryRow(c.Request.Context(), `
		SELECT id, organization_id, password_hash, role FROM users WHERE email = $1
	`, req.Email).Scan(&userID, &orgID, &pwHash, &role)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	match, err := auth.ComparePassword(req.Password, pwHash)
	if err != nil || !match {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}

	access, refresh, familyID, err := auth.GenerateTokenPair(s.jwtSecret, userID, orgID, role, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate tokens"})
		return
	}

	refHash := auth.HashAPIKey(refresh)
	_, _ = s.db.Exec(c.Request.Context(), `
		INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
		VALUES ($1, $2, $3, NOW() + INTERVAL '7 days')
	`, userID, familyID, refHash)

	// Log audit
	s.recordAudit(c.Request.Context(), orgID, "user", userID, "login", "users", userID.String(), c.ClientIP(), nil)

	c.JSON(http.StatusOK, gin.H{
		"access_token":  access,
		"refresh_token": refresh,
		"token_type":    "Bearer",
		"expires_in":    900,
	})
}

type RefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

func (s *Server) handleRefreshToken(c *gin.Context) {
	var req RefreshTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	claims, err := auth.ValidateToken(s.jwtSecret, req.RefreshToken)
	if err != nil || claims.TokenType != "refresh" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid refresh token"})
		return
	}

	tokenHash := auth.HashAPIKey(req.RefreshToken)
	var isRevoked bool
	var familyID uuid.UUID
	err = s.db.QueryRow(c.Request.Context(), `
		SELECT revoked, family_id FROM refresh_tokens WHERE token_hash = $1
	`, tokenHash).Scan(&isRevoked, &familyID)

	if err != nil || isRevoked {
		// REUSE DETECTED: Revoke entire session family immediately!
		_, _ = s.db.Exec(c.Request.Context(), `
			UPDATE refresh_tokens SET revoked = true WHERE family_id = $1
		`, claims.FamilyID)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "refresh token reuse detected: session revoked"})
		return
	}

	// Invalidate current token
	_, _ = s.db.Exec(c.Request.Context(), `UPDATE refresh_tokens SET revoked = true WHERE token_hash = $1`, tokenHash)

	// Issue rotated token pair under same family
	access, refresh, _, err := auth.GenerateTokenPair(s.jwtSecret, claims.UserID, claims.OrganizationID, claims.Role, &familyID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "token rotation failed"})
		return
	}

	newHash := auth.HashAPIKey(refresh)
	_, _ = s.db.Exec(c.Request.Context(), `
		INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
		VALUES ($1, $2, $3, NOW() + INTERVAL '7 days')
	`, claims.UserID, familyID, newHash)

	c.JSON(http.StatusOK, gin.H{
		"access_token":  access,
		"refresh_token": refresh,
		"token_type":    "Bearer",
		"expires_in":    900,
	})
}

func (s *Server) handleLogout(c *gin.Context) {
	userID, _ := c.Get("user_id")
	_, _ = s.db.Exec(c.Request.Context(), `
		UPDATE refresh_tokens SET revoked = true WHERE user_id = $1
	`, userID)
	c.JSON(http.StatusOK, gin.H{"message": "successfully logged out"})
}

// -------------------------------------------------------------
// HANDLERS: API KEYS
// -------------------------------------------------------------
type CreateAPIKeyRequest struct {
	Name   string   `json:"name" binding:"required"`
	Scopes []string `json:"scopes" binding:"required"`
}

func (s *Server) handleCreateAPIKey(c *gin.Context) {
	var req CreateAPIKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	rawKey, prefix, hash, err := auth.GenerateAPIKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate key"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	actorID := c.MustGet("actor_id").(uuid.UUID)

	var keyID uuid.UUID
	err = s.db.QueryRow(c.Request.Context(), `
		INSERT INTO api_keys (organization_id, name, key_prefix, key_hash, scopes)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, orgID, req.Name, prefix, hash, req.Scopes).Scan(&keyID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save key"})
		return
	}

	s.recordAudit(c.Request.Context(), orgID, "user", actorID, "create_api_key", "api_keys", keyID.String(), c.ClientIP(), nil)

	c.JSON(http.StatusCreated, gin.H{
		"id":         keyID,
		"name":       req.Name,
		"key_prefix": prefix,
		"api_key":    rawKey, // Only returned once
		"scopes":     req.Scopes,
	})
}

func (s *Server) handleRevokeAPIKey(c *gin.Context) {
	keyIDStr := c.Param("id")
	keyID, err := uuid.Parse(keyIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid key id"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	actorID := c.MustGet("actor_id").(uuid.UUID)

	_, err = s.db.Exec(c.Request.Context(), `
		UPDATE api_keys SET revoked = true WHERE id = $1 AND organization_id = $2
	`, keyID, orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to revoke key"})
		return
	}

	s.recordAudit(c.Request.Context(), orgID, "user", actorID, "revoke_api_key", "api_keys", keyID.String(), c.ClientIP(), nil)
	c.JSON(http.StatusOK, gin.H{"message": "api key revoked"})
}

// -------------------------------------------------------------
// HANDLERS: WALLETS
// -------------------------------------------------------------
type CreateWalletRequest struct {
	Currency    string             `json:"currency" binding:"required,oneof=USD EUR"`
	AccountType domain.AccountType `json:"account_type"`
}

func (s *Server) handleCreateWallet(c *gin.Context) {
	var req CreateWalletRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.AccountType == "" {
		req.AccountType = domain.AccountCustomer
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	var walletID uuid.UUID
	err := s.db.QueryRow(c.Request.Context(), `
		INSERT INTO wallets (organization_id, currency, account_type, balance, hold_balance)
		VALUES ($1, $2, $3, 0, 0)
		RETURNING id
	`, orgID, req.Currency, req.AccountType).Scan(&walletID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create wallet"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":           walletID,
		"currency":     req.Currency,
		"account_type": req.AccountType,
		"balance":      0,
		"hold_balance": 0,
		"status":       "active",
	})
}

func (s *Server) handleGetWallet(c *gin.Context) {
	walletIDStr := c.Param("id")
	walletID, err := uuid.Parse(walletIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet id"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	var w domain.Wallet
	err = s.db.QueryRow(c.Request.Context(), `
		SELECT id, organization_id, currency, account_type, balance, hold_balance, status, created_at, updated_at
		FROM wallets WHERE id = $1 AND organization_id = $2
	`, walletID, orgID).Scan(
		&w.ID, &w.OrganizationID, &w.Currency, &w.AccountType, &w.Balance, &w.HoldBalance, &w.Status, &w.CreatedAt, &w.UpdatedAt,
	)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":                w.ID,
		"currency":          w.Currency,
		"account_type":      w.AccountType,
		"balance":           w.Balance,
		"hold_balance":      w.HoldBalance,
		"available_balance": w.AvailableBalance(),
		"status":            w.Status,
	})
}

type DepositRequest struct {
	Amount int64 `json:"amount" binding:"required,gt=0"`
}

// handleDeposit credits the wallet from external funding using balanced double-entry journal
func (s *Server) handleDeposit(c *gin.Context) {
	walletIDStr := c.Param("id")
	walletID, err := uuid.Parse(walletIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet id"})
		return
	}

	var req DepositRequest
	bodyBytes, _ := c.Get("request_body_bytes")
	if err := json.Unmarshal(bodyBytes.([]byte), &req); err != nil || req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "amount must be greater than 0"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "transaction start failed"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	// Fetch wallet
	var currency string
	err = tx.QueryRow(c.Request.Context(), `
		SELECT currency FROM wallets WHERE id = $1 AND organization_id = $2 FOR UPDATE
	`, walletID, orgID).Scan(&currency)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}

	// Ensure external_funding system account
	var fundingWalletID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		SELECT id FROM wallets WHERE organization_id = $1 AND currency = $2 AND account_type = 'external_funding'
	`, orgID, currency).Scan(&fundingWalletID)
	if err != nil {
		fundingWalletID = uuid.New()
		_, _ = tx.Exec(c.Request.Context(), `
			INSERT INTO wallets (id, organization_id, currency, account_type, balance, hold_balance)
			VALUES ($1, $2, $3, 'external_funding', 0, 0)
			ON CONFLICT (id) DO NOTHING
		`, fundingWalletID, orgID, currency)
	}

	// Balanced Double-Entry:
	// DR: external_funding
	// CR: customer wallet
	journalID := uuid.New()
	_, err = tx.Exec(c.Request.Context(), `
		INSERT INTO journal_entries (id, organization_id, reference_type, currency, description)
		VALUES ($1, $2, 'deposit', $3, 'External wallet deposit')
	`, journalID, orgID, currency)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed creating journal"})
		return
	}

	// Append-only ledger entries
	_, err = tx.Exec(c.Request.Context(), `
		INSERT INTO ledger_entries (journal_entry_id, wallet_id, direction, amount)
		VALUES ($1, $2, 'DR', $3), ($1, $4, 'CR', $3)
	`, journalID, fundingWalletID, req.Amount, walletID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed posting ledger entries"})
		return
	}

	// Update Wallet Running Balances
	_, err = tx.Exec(c.Request.Context(), `
		UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE id = $2
	`, req.Amount, walletID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed updating wallet balance"})
		return
	}

	_, _ = tx.Exec(c.Request.Context(), `
		UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE id = $2
	`, req.Amount, fundingWalletID)

	// Save Idempotency response
	respPayload := gin.H{
		"journal_entry_id": journalID,
		"wallet_id":        walletID,
		"credited_amount":  req.Amount,
		"currency":         currency,
		"status":           "SUCCESS",
	}
	s.saveIdempotencyResponse(c.Request.Context(), tx, orgID, c.GetString("idempotency_key"), http.StatusOK, respPayload)

	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}

	c.JSON(http.StatusOK, respPayload)
}

// -------------------------------------------------------------
// HANDLERS: TRANSFERS (SAGA LAUNCHER - 202 ACCEPTED)
// -------------------------------------------------------------
type CreateTransferRequest struct {
	SourceWalletID      uuid.UUID `json:"source_wallet_id" binding:"required"`
	DestinationWalletID uuid.UUID `json:"destination_wallet_id" binding:"required"`
	Currency            string    `json:"currency" binding:"required,oneof=USD EUR"`
	Amount              int64     `json:"amount" binding:"required,gt=0"`
}

func (s *Server) handleCreateTransfer(c *gin.Context) {
	var req CreateTransferRequest
	bodyBytes, _ := c.Get("request_body_bytes")
	if err := json.Unmarshal(bodyBytes.([]byte), &req); err != nil || req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transfer request payload"})
		return
	}

	if req.SourceWalletID == req.DestinationWalletID {
		c.JSON(http.StatusBadRequest, gin.H{"error": domain.ErrSelfTransfer.Error()})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	idempotencyKey := c.GetString("idempotency_key")

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "tx failed"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	// Step 1 of Saga: Atomically validate & place HOLD on source wallet
	var sourceBalance, sourceHold int64
	var sourceCurrency, sourceStatus string
	err = tx.QueryRow(c.Request.Context(), `
		SELECT balance, hold_balance, currency, status 
		FROM wallets 
		WHERE id = $1 AND organization_id = $2 
		FOR UPDATE
	`, req.SourceWalletID, orgID).Scan(&sourceBalance, &sourceHold, &sourceCurrency, &sourceStatus)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "source wallet not found"})
		return
	}

	if sourceCurrency != req.Currency {
		c.JSON(http.StatusBadRequest, gin.H{"error": "currency mismatch with source wallet"})
		return
	}

	totalNeeded := req.Amount + s.transferFee
	if (sourceBalance - sourceHold) < totalNeeded {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": domain.ErrInsufficientBalance.Error()})
		return
	}

	// Verify destination wallet exists and matches currency
	var destCurrency, destStatus string
	err = tx.QueryRow(c.Request.Context(), `
		SELECT currency, status FROM wallets WHERE id = $1 AND organization_id = $2
	`, req.DestinationWalletID, orgID).Scan(&destCurrency, &destStatus)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "destination wallet not found"})
		return
	}
	if destCurrency != req.Currency {
		c.JSON(http.StatusBadRequest, gin.H{"error": "destination wallet currency does not match transfer currency"})
		return
	}

	// Place Hold
	_, err = tx.Exec(c.Request.Context(), `
		UPDATE wallets 
		SET hold_balance = hold_balance + $1, updated_at = NOW()
		WHERE id = $2
	`, totalNeeded, req.SourceWalletID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed placing hold on wallet"})
		return
	}

	// Insert Transfer row in PENDING state
	transferID := uuid.New()
	_, err = tx.Exec(c.Request.Context(), `
		INSERT INTO transfers (
			id, organization_id, source_wallet_id, destination_wallet_id, currency, amount, fee_amount,
			status, saga_step, fencing_token, idempotency_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', 'RESERVE_FUNDS', 1, $8)
	`, transferID, orgID, req.SourceWalletID, req.DestinationWalletID, req.Currency, req.Amount, s.transferFee, idempotencyKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed recording transfer"})
		return
	}

	// Transactional Outbox: Event publication guaranteed with DB transaction
	err = outbox.QueueEventInTx(c.Request.Context(), tx, "transfer.created", req.SourceWalletID.String(), transferID.String())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "outbox queuing failed"})
		return
	}

	// Record Idempotency Response: 202 Accepted
	respPayload := gin.H{
		"transfer_id": transferID,
		"status":      "ACCEPTED",
		"message":     "Transfer accepted for asynchronous saga processing",
		"amount":      req.Amount,
		"fee_amount":  s.transferFee,
		"currency":    req.Currency,
	}
	s.saveIdempotencyResponse(c.Request.Context(), tx, orgID, idempotencyKey, http.StatusAccepted, respPayload)

	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}

	// Return 202 Accepted
	c.JSON(http.StatusAccepted, respPayload)
}

func (s *Server) handleGetTransfer(c *gin.Context) {
	transferIDStr := c.Param("id")
	transferID, err := uuid.Parse(transferIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transfer id"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)

	var t domain.Transfer
	err = s.db.QueryRow(c.Request.Context(), `
		SELECT id, organization_id, source_wallet_id, destination_wallet_id, currency, amount, fee_amount,
		       status, saga_step, failure_reason, fencing_token, created_at, updated_at
		FROM transfers WHERE id = $1 AND organization_id = $2
	`, transferID, orgID).Scan(
		&t.ID, &t.OrganizationID, &t.SourceWalletID, &t.DestinationWalletID, &t.Currency, &t.Amount, &t.FeeAmount,
		&t.Status, &t.SagaStep, &t.FailureReason, &t.FencingToken, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "transfer not found"})
		return
	}

	c.JSON(http.StatusOK, t)
}

// handleReverseTransfer reverses a previously COMPLETED transfer by posting inverse balanced entries
func (s *Server) handleReverseTransfer(c *gin.Context) {
	transferIDStr := c.Param("id")
	transferID, err := uuid.Parse(transferIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transfer id"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	idempotencyKey := c.GetString("idempotency_key")

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "tx failed"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	var t domain.Transfer
	err = tx.QueryRow(c.Request.Context(), `
		SELECT id, source_wallet_id, destination_wallet_id, currency, amount, fee_amount, status
		FROM transfers WHERE id = $1 AND organization_id = $2 FOR UPDATE
	`, transferID, orgID).Scan(&t.ID, &t.SourceWalletID, &t.DestinationWalletID, &t.Currency, &t.Amount, &t.FeeAmount, &t.Status)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "transfer not found"})
		return
	}

	if t.Status != domain.TransferStatusCompleted {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only COMPLETED transfers can be reversed"})
		return
	}

	// Verify destination has sufficient balance to return
	var destBal int64
	_ = tx.QueryRow(c.Request.Context(), `SELECT balance FROM wallets WHERE id = $1 FOR UPDATE`, t.DestinationWalletID).Scan(&destBal)
	if destBal < t.Amount {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "destination wallet has insufficient funds to reverse transfer"})
		return
	}

	// Create Reversal Balanced Journal:
	// DR: Destination (Amount)
	// CR: Source (Amount)
	journalID := uuid.New()
	_, _ = tx.Exec(c.Request.Context(), `
		INSERT INTO journal_entries (id, organization_id, reference_id, reference_type, currency, description)
		VALUES ($1, $2, $3, 'reversal', $4, $5)
	`, journalID, orgID, t.ID, t.Currency, fmt.Sprintf("Reversal of Transfer %s", t.ID))

	_, _ = tx.Exec(c.Request.Context(), `
		INSERT INTO ledger_entries (journal_entry_id, wallet_id, direction, amount)
		VALUES ($1, $2, 'DR', $3), ($1, $4, 'CR', $3)
	`, journalID, t.DestinationWalletID, t.Amount, t.SourceWalletID)

	// Update Balances
	_, _ = tx.Exec(c.Request.Context(), `UPDATE wallets SET balance = balance - $1 WHERE id = $2`, t.Amount, t.DestinationWalletID)
	_, _ = tx.Exec(c.Request.Context(), `UPDATE wallets SET balance = balance + $1 WHERE id = $2`, t.Amount, t.SourceWalletID)

	// Mark Transfer REVERSED
	_, _ = tx.Exec(c.Request.Context(), `UPDATE transfers SET status = 'REVERSED', updated_at = NOW() WHERE id = $1`, t.ID)

	respPayload := gin.H{
		"reversal_journal_id": journalID,
		"transfer_id":         t.ID,
		"status":              "REVERSED",
	}
	s.saveIdempotencyResponse(c.Request.Context(), tx, orgID, idempotencyKey, http.StatusOK, respPayload)

	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}

	c.JSON(http.StatusOK, respPayload)
}

// -------------------------------------------------------------
// HANDLERS: LEDGER & RECONCILIATION
// -------------------------------------------------------------
func (s *Server) handleGetLedgerEntries(c *gin.Context) {
	walletIDStr := c.Param("id")
	walletID, err := uuid.Parse(walletIDStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet id"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)

	// Verify wallet belongs to the authenticated organization
	var walletExists bool
	err = s.db.QueryRow(c.Request.Context(), `
		SELECT EXISTS(SELECT 1 FROM wallets WHERE id = $1 AND organization_id = $2)
	`, walletID, orgID).Scan(&walletExists)
	if err != nil || !walletExists {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}

	cursorStr := c.Query("cursor")
	limitStr := c.DefaultQuery("limit", "20")
	limit, _ := strconv.Atoi(limitStr)
	if limit > 100 {
		limit = 100
	}

	var query string
	var args []any
	if cursorStr != "" {
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, cursorStr)
		if parseErr == nil {
			query = `
				SELECT id, journal_entry_id, wallet_id, direction, amount, created_at
				FROM ledger_entries
				WHERE wallet_id = $1 AND created_at < $2
				ORDER BY created_at DESC
				LIMIT $3
			`
			args = []any{walletID, cursorTime, limit}
		}
	}

	if query == "" {
		query = `
			SELECT id, journal_entry_id, wallet_id, direction, amount, created_at
			FROM ledger_entries
			WHERE wallet_id = $1
			ORDER BY created_at DESC
			LIMIT $2
		`
		args = []any{walletID, limit}
	}

	rows, err := s.db.Query(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying ledger"})
		return
	}
	defer rows.Close()

	var entries []domain.LedgerEntry
	var nextCursor string
	for rows.Next() {
		var e domain.LedgerEntry
		if err := rows.Scan(&e.ID, &e.JournalEntryID, &e.WalletID, &e.Direction, &e.Amount, &e.CreatedAt); err == nil {
			entries = append(entries, e)
			nextCursor = e.CreatedAt.Format(time.RFC3339Nano)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"entries":     entries,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleReconciliationReport(c *gin.Context) {
	discrepancies, err := s.auditor.RunAudit(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "audit failed: " + err.Error()})
		return
	}

	status := "BALANCED"
	if len(discrepancies) > 0 {
		status = "DISCREPANCIES_FOUND"
	}

	c.JSON(http.StatusOK, gin.H{
		"status":              status,
		"discrepancies_count": len(discrepancies),
		"discrepancies":       discrepancies,
		"timestamp":           time.Now(),
	})
}

// -------------------------------------------------------------
// HANDLERS: WEBHOOKS
// -------------------------------------------------------------
type RegisterWebhookRequest struct {
	URL    string   `json:"url" binding:"required,url"`
	Secret string   `json:"secret" binding:"required,min=16"`
	Events []string `json:"events"`
}

func (s *Server) handleRegisterWebhook(c *gin.Context) {
	var req RegisterWebhookRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.Events) == 0 {
		req.Events = []string{"transfer.completed", "transfer.failed"}
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	var webhookID uuid.UUID
	err := s.db.QueryRow(c.Request.Context(), `
		INSERT INTO webhooks (organization_id, url, secret, events)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, orgID, req.URL, req.Secret, req.Events).Scan(&webhookID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to register webhook"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":     webhookID,
		"url":    req.URL,
		"events": req.Events,
		"status": "active",
	})
}

// -------------------------------------------------------------
// HELPERS
// -------------------------------------------------------------
func (s *Server) saveIdempotencyResponse(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, key string, code int, body any) {
	bodyBytes, _ := json.Marshal(body)
	_, _ = tx.Exec(ctx, `
		UPDATE idempotency_keys
		SET response_code = $1, response_body = $2
		WHERE organization_id = $3 AND key = $4
	`, code, bodyBytes, orgID, key)
}

func (s *Server) recordAudit(ctx context.Context, orgID uuid.UUID, actorType string, actorID uuid.UUID, action, resType, resID, ip string, details any) {
	detBytes, _ := json.Marshal(details)
	_, _ = s.db.Exec(ctx, `
		INSERT INTO audit_logs (organization_id, actor_type, actor_id, action, resource_type, resource_id, ip_address, details)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, orgID, actorType, actorID, action, resType, resID, ip, detBytes)
}
