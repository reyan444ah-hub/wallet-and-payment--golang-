package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/distributed-wallet/platform/pkg/auth"
	"github.com/distributed-wallet/platform/pkg/domain"
	"github.com/distributed-wallet/platform/pkg/outbox"
	"github.com/distributed-wallet/platform/pkg/ratelimit"
	"github.com/distributed-wallet/platform/pkg/reconciliation"
	"github.com/distributed-wallet/platform/pkg/telemetry"
	"github.com/distributed-wallet/platform/pkg/webhooks"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Server struct {
	router               *gin.Engine
	db                   *pgxpool.Pool
	limiter              *ratelimit.Limiter
	metrics              *telemetry.Metrics
	auditor              *reconciliation.Auditor
	logger               *zap.Logger
	jwtSecret            string
	webhookEncryptionKey []byte
	transferFee          int64 // Minor units e.g. 10 = $0.10
}

func (s *Server) publicRateLimitMiddleware(limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		allowed, err := s.limiter.Allow(c.Request.Context(), "ip:"+c.ClientIP(), limit, window)
		if err != nil {
			s.logger.Warn("distributed rate limit unavailable; allowing request", zap.Error(err))
			c.Next()
			return
		}
		if !allowed {
			s.metrics.RateLimitRejections.WithLabelValues("ip").Inc()
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}

func (s *Server) rateLimitMiddleware(limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		orgID, ok := c.Get("organization_id")
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "organization context is required"})
			return
		}
		allowed, err := s.limiter.Allow(c.Request.Context(), fmt.Sprint(orgID), limit, window)
		if err != nil {
			s.logger.Warn("distributed rate limit unavailable; allowing request", zap.Error(err))
			c.Next()
			return
		}
		if !allowed {
			s.metrics.RateLimitRejections.WithLabelValues("organization").Inc()
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}

func NewServer(
	db *pgxpool.Pool,
	limiter *ratelimit.Limiter,
	metrics *telemetry.Metrics,
	logger *zap.Logger,
	jwtSecret string,
	webhookEncryptionKey []byte,
) *Server {
	s := &Server{
		router:               gin.Default(),
		db:                   db,
		limiter:              limiter,
		metrics:              metrics,
		auditor:              reconciliation.NewAuditor(db, logger),
		logger:               logger,
		jwtSecret:            jwtSecret,
		webhookEncryptionKey: webhookEncryptionKey,
		transferFee:          10, // 10 cents standard platform fee
	}

	s.router.Use(s.correlationMiddleware(), s.metricsMiddleware())
	s.setupRoutes()
	return s
}

func (s *Server) Router() *gin.Engine {
	return s.router
}

func (s *Server) metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()
		path := c.FullPath()
		if path == "" {
			path = "unmatched"
		}
		method := c.Request.Method
		status := strconv.Itoa(c.Writer.Status())
		s.metrics.HttpRequestsTotal.WithLabelValues(method, path, status).Inc()
		s.metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(started).Seconds())
	}
}

func (s *Server) correlationMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		correlationID, err := uuid.Parse(c.GetHeader("X-Correlation-ID"))
		if err != nil {
			correlationID = uuid.New()
		}
		id := correlationID.String()
		c.Set("correlation_id", id)
		c.Writer.Header().Set("X-Correlation-ID", id)
		c.Request = c.Request.WithContext(telemetry.ContextWithCorrelationID(c.Request.Context(), id))
		c.Next()
	}
}

func (s *Server) setupRoutes() {
	s.router.GET("/healthz", func(c *gin.Context) {
		if err := s.db.Ping(c.Request.Context()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	// Prometheus metrics
	s.router.GET("/api/v1/metrics", gin.WrapH(promhttp.Handler()))

	v1 := s.router.Group("/api/v1")
	{
		// Authentication & Session
		authGroup := v1.Group("/auth")
		{
			authGroup.POST("/register", s.publicRateLimitMiddleware(10, time.Minute), s.handleRegister)
			authGroup.POST("/login", s.publicRateLimitMiddleware(10, time.Minute), s.handleLogin)
			authGroup.POST("/refresh", s.publicRateLimitMiddleware(30, time.Minute), s.handleRefreshToken)
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
			wallets.GET("", s.requirePermission("wallets:read", "admin", "finance", "viewer"), s.handleListWallets)
			wallets.POST("", s.requirePermission("wallets:write", "admin", "finance"), s.handleCreateWallet)
			wallets.GET("/:id", s.requirePermission("wallets:read", "admin", "finance", "viewer"), s.handleGetWallet)
			wallets.DELETE("/:id", s.requirePermission("wallets:write", "admin", "finance"), s.handleDeleteUnusedWallet)
			wallets.POST("/:id/close", s.requirePermission("wallets:write", "admin", "finance"), s.handleCloseWallet)
			wallets.POST("/:id/reopen", s.requirePermission("wallets:write", "admin", "finance"), s.handleReopenWallet)
			wallets.POST("/:id/deposit", s.requirePermission("wallets:deposit", "admin", "finance"), s.rateLimitMiddleware(60, time.Minute), s.idempotencyMiddleware(), s.handleDeposit)
			wallets.GET("/:id/ledger", s.requirePermission("ledger:read", "admin", "finance", "viewer"), s.handleGetLedgerEntries)
		}

		// Transfers (Saga)
		transfers := v1.Group("/transfers")
		transfers.Use(s.authMiddleware())
		{
			transfers.GET("", s.requirePermission("transfers:read", "admin", "finance", "viewer"), s.handleListTransfers)
			transfers.POST("", s.requirePermission("transfers:write", "admin", "finance"), s.rateLimitMiddleware(60, time.Minute), s.idempotencyMiddleware(), s.handleCreateTransfer)
			transfers.GET("/:id", s.requirePermission("transfers:read", "admin", "finance", "viewer"), s.handleGetTransfer)
			transfers.POST("/:id/reverse", s.requirePermission("transfers:reverse", "admin"), s.rateLimitMiddleware(30, time.Minute), s.idempotencyMiddleware(), s.handleReverseTransfer)
		}

		users := v1.Group("/users")
		users.Use(s.authMiddleware(), s.requirePermission("users:manage", "admin"))
		{
			users.GET("", s.handleListOrganizationUsers)
			users.PATCH("/:id/status", s.handleUpdateUserStatus)
		}

		organizations := v1.Group("/organizations")
		organizations.Use(s.authMiddleware(), s.requirePermission("organization:read", "admin", "finance", "viewer"))
		{
			organizations.GET("/:id/profile", s.handleGetOrganizationProfile)
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
				SELECT id, organization_id, scopes, revoked FROM api_keys
				WHERE key_hash = $1 AND (expires_at IS NULL OR expires_at > NOW())
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

		var isActive bool
		if err := s.db.QueryRow(c.Request.Context(), `
			SELECT is_active FROM users WHERE id = $1 AND organization_id = $2
		`, claims.UserID, claims.OrganizationID).Scan(&isActive); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "user account is unavailable"})
			} else {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "failed to verify user account"})
			}
			return
		}
		if !isActive {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "user account is deactivated"})
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
				if sc == requiredScope {
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
		if idempotencyKey == "" || len(idempotencyKey) > 255 {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key must be between 1 and 255 characters"})
			return
		}

		orgIDVal, _ := c.Get("organization_id")
		orgID := orgIDVal.(uuid.UUID)

		// Read, hash, and restore the request body so downstream handlers can parse it
		var bodyBytes []byte
		if c.Request.Body != nil {
			var err error
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
			bodyBytes, err = c.GetRawData()
			if err != nil {
				var maxBytesErr *http.MaxBytesError
				if errors.As(err, &maxBytesErr) {
					c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body exceeds 1 MiB"})
				} else {
					c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
				}
				return
			}
			c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		hash := sha256.Sum256(bodyBytes)
		reqHash := hex.EncodeToString(hash[:])

		tx, err := s.db.Begin(c.Request.Context())
		if err != nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "idempotency store unavailable"})
			return
		}
		defer tx.Rollback(c.Request.Context())

		inserted, err := tx.Exec(c.Request.Context(), `
			INSERT INTO idempotency_keys (key, organization_id, request_path, request_hash, created_at, locked_until)
			VALUES ($1, $2, $3, $4, NOW(), NOW() + INTERVAL '1 minute')
			ON CONFLICT (organization_id, key) DO NOTHING
		`, idempotencyKey, orgID, c.Request.URL.Path, reqHash)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "idempotency store unavailable"})
			return
		}

		if inserted.RowsAffected() == 0 {
			var savedHash, savedPath string
			var respCode *int
			var respBody []byte
			var lockedUntil *time.Time
			err = tx.QueryRow(c.Request.Context(), `
				SELECT request_hash, request_path, response_code, response_body, locked_until
				FROM idempotency_keys
				WHERE organization_id = $1 AND key = $2
				FOR UPDATE
			`, orgID, idempotencyKey).Scan(&savedHash, &savedPath, &respCode, &respBody, &lockedUntil)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "failed to read idempotency record"})
				return
			}

			if savedHash != reqHash || savedPath != c.Request.URL.Path {
				_ = tx.Commit(c.Request.Context())
				c.AbortWithStatusJSON(http.StatusUnprocessableEntity, gin.H{
					"error": "Idempotency key reuse with a different request is prohibited",
				})
				return
			}

			if respCode != nil {
				var jsonResp any
				if err := json.Unmarshal(respBody, &jsonResp); err != nil {
					c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "stored idempotency response is invalid"})
					return
				}
				if err := tx.Commit(c.Request.Context()); err != nil {
					c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "failed to complete idempotency lookup"})
					return
				}
				c.AbortWithStatusJSON(*respCode, jsonResp)
				return
			}

			if lockedUntil != nil && lockedUntil.After(time.Now()) {
				_ = tx.Commit(c.Request.Context())
				c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": "concurrent request with same idempotency key is already in progress"})
				return
			}

			_, err = tx.Exec(c.Request.Context(), `
				UPDATE idempotency_keys
				SET request_path = $1, request_hash = $2, created_at = NOW(),
				    locked_until = NOW() + INTERVAL '1 minute'
				WHERE organization_id = $3 AND key = $4 AND response_code IS NULL
			`, c.Request.URL.Path, reqHash, orgID, idempotencyKey)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "failed to reclaim expired idempotency record"})
				return
			}
		}
		if err := tx.Commit(c.Request.Context()); err != nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "failed to reserve idempotency key"})
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
	FullName         string `json:"full_name" binding:"max=255"`
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
	req.FullName = strings.TrimSpace(req.FullName)
	if req.FullName == "" {
		req.FullName = strings.SplitN(req.Email, "@", 2)[0]
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
		INSERT INTO users (organization_id, full_name, email, password_hash, role)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`, orgID, req.FullName, req.Email, pwHash, assignedRole).Scan(&userID)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "email already registered"})
		return
	}

	var walletID uuid.UUID
	if err := tx.QueryRow(c.Request.Context(), `
		INSERT INTO wallets (organization_id, owner_user_id, currency, account_type, balance, hold_balance)
		VALUES ($1, $2, 'USD', 'customer', 0, 0)
		RETURNING id
	`, orgID, userID).Scan(&walletID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create default wallet"})
		return
	}

	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, "user", userID,
		"register", "users", userID.String(), c.ClientIP(), nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record registration audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"organization_id": orgID,
		"user_id":         userID,
		"wallet_id":       walletID,
		"wallet_currency": "USD",
		"email":           req.Email,
		"full_name":       req.FullName,
		"role":            assignedRole,
	})
	return
}

func (s *Server) handleGetOrganizationProfile(c *gin.Context) {
	requestedOrgID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid organization id"})
		return
	}
	orgID := c.MustGet("organization_id").(uuid.UUID)
	if requestedOrgID != orgID {
		c.JSON(http.StatusNotFound, gin.H{"error": "organization profile not found"})
		return
	}

	var organization struct {
		ID        uuid.UUID `json:"id"`
		Name      string    `json:"name"`
		CreatedAt time.Time `json:"created_at"`
	}
	if err := s.db.QueryRow(c.Request.Context(), `
		SELECT id, name, created_at FROM organizations WHERE id = $1
	`, orgID).Scan(&organization.ID, &organization.Name, &organization.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "organization profile not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying organization profile"})
		}
		return
	}

	limit := parsePageLimit(c, 50)
	if limit < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
		return
	}
	cursor, cursorID, err := parsePageCursor(c.Query("cursor"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid member cursor"})
		return
	}

	query := `
		SELECT id, COALESCE(NULLIF(full_name, ''), split_part(email, '@', 1)), email, role, created_at
		FROM users
		WHERE organization_id = $1
	`
	args := []any{orgID}
	if cursor != nil {
		query += ` AND (created_at, id) < ($2, $3)`
		args = append(args, *cursor, cursorID)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)

	rows, err := s.db.Query(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying organization members"})
		return
	}
	defer rows.Close()

	type organizationMember struct {
		ID        uuid.UUID `json:"id"`
		Name      string    `json:"name"`
		Email     string    `json:"email"`
		Role      string    `json:"role"`
		CreatedAt time.Time `json:"created_at"`
	}
	members := make([]organizationMember, 0, limit)
	for rows.Next() {
		var member organizationMember
		if err := rows.Scan(&member.ID, &member.Name, &member.Email, &member.Role, &member.CreatedAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed reading organization members"})
			return
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed iterating organization members"})
		return
	}

	var memberCount int64
	if err := s.db.QueryRow(c.Request.Context(), `
		SELECT COUNT(*) FROM users WHERE organization_id = $1
	`, orgID).Scan(&memberCount); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed counting organization members"})
		return
	}

	nextCursor := ""
	if len(members) == limit {
		last := members[len(members)-1]
		nextCursor = last.CreatedAt.Format(time.RFC3339Nano) + "|" + last.ID.String()
	}
	c.JSON(http.StatusOK, gin.H{
		"organization": organization,
		"members":      members,
		"member_count": memberCount,
		"next_cursor":  nextCursor,
	})
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
	var isActive bool
	err := s.db.QueryRow(c.Request.Context(), `
		SELECT id, organization_id, password_hash, role, is_active FROM users WHERE email = $1
	`, req.Email).Scan(&userID, &orgID, &pwHash, &role, &isActive)
	if err != nil || !isActive {
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

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "session store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	refHash := auth.HashAPIKey(refresh)
	if _, err := tx.Exec(c.Request.Context(), `
		INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
		VALUES ($1, $2, $3, NOW() + INTERVAL '7 days')
	`, userID, familyID, refHash); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist refresh token"})
		return
	}

	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, "user", userID, "login", "users", userID.String(), c.ClientIP(), nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record login audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist login session"})
		return
	}

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
	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "session store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	var isRevoked, isActive bool
	var familyID uuid.UUID
	var tokenUserID, organizationID uuid.UUID
	var role string
	var expiresAt time.Time
	err = tx.QueryRow(c.Request.Context(), `
		SELECT rt.revoked, rt.family_id, rt.user_id, u.organization_id, u.role, rt.expires_at, u.is_active
		FROM refresh_tokens rt
		JOIN users u ON u.id = rt.user_id
		WHERE rt.token_hash = $1
		FOR UPDATE OF rt
	`, tokenHash).Scan(&isRevoked, &familyID, &tokenUserID, &organizationID, &role, &expiresAt, &isActive)
	if err != nil || familyID != claims.FamilyID ||
		tokenUserID != claims.UserID || organizationID != claims.OrganizationID {
		if _, revokeErr := tx.Exec(c.Request.Context(), `
			UPDATE refresh_tokens SET revoked = true WHERE family_id = $1
		`, claims.FamilyID); revokeErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to revoke invalid refresh session"})
			return
		}
		if commitErr := tx.Commit(c.Request.Context()); commitErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist refresh session revocation"})
			return
		}
		c.JSON(http.StatusUnauthorized, gin.H{"error": "refresh token reuse detected: session revoked"})
		return
	}
	if !isActive {
		if _, err := tx.Exec(c.Request.Context(), `
			UPDATE refresh_tokens SET revoked = true WHERE user_id = $1
		`, tokenUserID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to revoke deactivated user sessions"})
			return
		}
		if err := tx.Commit(c.Request.Context()); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist deactivated user session revocation"})
			return
		}
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user account is deactivated"})
		return
	}
	if isRevoked || !expiresAt.After(time.Now()) {
		if _, err := tx.Exec(c.Request.Context(), `
			UPDATE refresh_tokens SET revoked = true WHERE family_id = $1
		`, familyID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to revoke reused refresh session"})
			return
		}
		if err := tx.Commit(c.Request.Context()); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist refresh session revocation"})
			return
		}
		c.JSON(http.StatusUnauthorized, gin.H{"error": "refresh token reuse detected: session revoked"})
		return
	}

	rotated, err := tx.Exec(c.Request.Context(), `
		UPDATE refresh_tokens SET revoked = true
		WHERE token_hash = $1 AND family_id = $2 AND revoked = false
	`, tokenHash, familyID)
	if err != nil || rotated.RowsAffected() != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "refresh token was already rotated"})
		return
	}

	access, refresh, _, err := auth.GenerateTokenPair(s.jwtSecret, tokenUserID, organizationID, role, &familyID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "token rotation failed"})
		return
	}

	newHash := auth.HashAPIKey(refresh)
	if _, err := tx.Exec(c.Request.Context(), `
		INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
		VALUES ($1, $2, $3, NOW() + INTERVAL '7 days')
	`, tokenUserID, familyID, newHash); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist rotated refresh token"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, organizationID, "user", tokenUserID, "refresh_token", "users", tokenUserID.String(), c.ClientIP(), nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record refresh audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit refresh rotation"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"access_token":  access,
		"refresh_token": refresh,
		"token_type":    "Bearer",
		"expires_in":    900,
	})
}

func (s *Server) handleLogout(c *gin.Context) {
	userIDValue, exists := c.Get("user_id")
	if !exists {
		c.JSON(http.StatusForbidden, gin.H{"error": "logout requires a user session"})
		return
	}
	userID, ok := userIDValue.(uuid.UUID)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user session"})
		return
	}
	orgID := c.MustGet("organization_id").(uuid.UUID)
	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "session store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	if _, err := tx.Exec(c.Request.Context(), `
		UPDATE refresh_tokens SET revoked = true WHERE user_id = $1
	`, userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to revoke sessions"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, "user", userID, "logout", "users", userID.String(), c.ClientIP(), nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record logout audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist logout"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "successfully logged out"})
}

func (s *Server) handleListOrganizationUsers(c *gin.Context) {
	orgID := c.MustGet("organization_id").(uuid.UUID)
	limit := parsePageLimit(c, 50)
	if limit < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
		return
	}
	cursor, cursorID, err := parsePageCursor(c.Query("cursor"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user cursor"})
		return
	}

	query := `
		SELECT u.id, u.email, u.role, u.is_active, u.created_at,
		       COUNT(w.id)::BIGINT AS wallet_count
		FROM users u
		LEFT JOIN wallets w ON w.owner_user_id = u.id AND w.organization_id = u.organization_id
		WHERE u.organization_id = $1
	`
	args := []any{orgID}
	if cursor != nil {
		query += ` AND (u.created_at, u.id) < ($2, $3)`
		args = append(args, *cursor, cursorID)
	}
	query += ` GROUP BY u.id ORDER BY u.created_at DESC, u.id DESC LIMIT $` +
		strconv.Itoa(len(args)+1)
	args = append(args, limit)

	rows, err := s.db.Query(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying organization users"})
		return
	}
	defer rows.Close()

	type organizationUser struct {
		ID          uuid.UUID `json:"id"`
		Email       string    `json:"email"`
		Role        string    `json:"role"`
		IsActive    bool      `json:"is_active"`
		WalletCount int64     `json:"wallet_count"`
		CreatedAt   time.Time `json:"created_at"`
	}
	users := make([]organizationUser, 0, limit)
	for rows.Next() {
		var user organizationUser
		if err := rows.Scan(&user.ID, &user.Email, &user.Role, &user.IsActive, &user.CreatedAt, &user.WalletCount); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed reading organization users"})
			return
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed iterating organization users"})
		return
	}

	nextCursor := ""
	if len(users) == limit {
		last := users[len(users)-1]
		nextCursor = last.CreatedAt.Format(time.RFC3339Nano) + "|" + last.ID.String()
	}
	c.JSON(http.StatusOK, gin.H{"users": users, "next_cursor": nextCursor})
}

type updateUserStatusRequest struct {
	Active *bool `json:"active" binding:"required"`
}

func (s *Server) handleUpdateUserStatus(c *gin.Context) {
	targetID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user id"})
		return
	}
	var req updateUserStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "active must be provided"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	actorID := c.MustGet("user_id").(uuid.UUID)
	if targetID == actorID {
		c.JSON(http.StatusConflict, gin.H{"error": "admins cannot deactivate or reactivate their own account"})
		return
	}

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "user store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	var lockedOrg uuid.UUID
	if err := tx.QueryRow(c.Request.Context(), `
		SELECT id FROM organizations WHERE id = $1 FOR UPDATE
	`, orgID).Scan(&lockedOrg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to lock organization user records"})
		return
	}

	var currentActive bool
	var targetRole string
	err = tx.QueryRow(c.Request.Context(), `
		SELECT is_active, role FROM users
		WHERE id = $1 AND organization_id = $2
		FOR UPDATE
	`, targetID, orgID).Scan(&currentActive, &targetRole)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load organization user"})
		return
	}

	if currentActive == *req.Active {
		if err := tx.Commit(c.Request.Context()); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to confirm account status"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"user_id": targetID, "is_active": currentActive})
		return
	}

	if !*req.Active && targetRole == "admin" {
		var activeAdmins int
		if err := tx.QueryRow(c.Request.Context(), `
			SELECT COUNT(*) FROM users
			WHERE organization_id = $1 AND role = 'admin' AND is_active = TRUE
		`, orgID).Scan(&activeAdmins); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to verify active administrators"})
			return
		}
		if activeAdmins <= 1 {
			c.JSON(http.StatusConflict, gin.H{"error": "the organization must retain at least one active admin"})
			return
		}
	}

	if _, err := tx.Exec(c.Request.Context(), `
		UPDATE users SET is_active = $1, updated_at = NOW()
		WHERE id = $2 AND organization_id = $3
	`, *req.Active, targetID, orgID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update user status"})
		return
	}
	if !*req.Active {
		if _, err := tx.Exec(c.Request.Context(), `
			UPDATE refresh_tokens SET revoked = TRUE WHERE user_id = $1
		`, targetID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to revoke deactivated user sessions"})
			return
		}
	}
	action := "deactivate_user"
	if *req.Active {
		action = "reactivate_user"
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, "user", actorID, action,
		"users", targetID.String(), c.ClientIP(), map[string]any{"is_active": *req.Active}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record user status audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit user status change"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": targetID, "is_active": *req.Active})
}

// -------------------------------------------------------------
// HANDLERS: API KEYS
// -------------------------------------------------------------
type CreateAPIKeyRequest struct {
	Name      string     `json:"name" binding:"required,max=100"`
	Scopes    []string   `json:"scopes" binding:"required,min=1"`
	ExpiresAt *time.Time `json:"expires_at"`
}

func (s *Server) handleCreateAPIKey(c *gin.Context) {
	var req CreateAPIKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	allowedScopes := map[string]struct{}{
		"wallets:write": {}, "wallets:read": {}, "wallets:deposit": {},
		"ledger:read": {}, "transfers:write": {}, "transfers:read": {},
		"transfers:reverse": {}, "api_keys:manage": {}, "webhooks:manage": {},
		"reports:read": {},
	}
	seenScopes := make(map[string]struct{}, len(req.Scopes))
	for _, scope := range req.Scopes {
		if _, ok := allowedScopes[scope]; !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported API key scope"})
			return
		}
		if _, exists := seenScopes[scope]; exists {
			c.JSON(http.StatusBadRequest, gin.H{"error": "duplicate API key scope"})
			return
		}
		seenScopes[scope] = struct{}{}
	}
	if req.ExpiresAt != nil && !req.ExpiresAt.After(time.Now()) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "API key expiration must be in the future"})
		return
	}

	rawKey, prefix, hash, err := auth.GenerateAPIKey()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate key"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	actorID := c.MustGet("actor_id").(uuid.UUID)

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "API key store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	var keyID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		INSERT INTO api_keys (organization_id, name, key_prefix, key_hash, scopes, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id
	`, orgID, req.Name, prefix, hash, req.Scopes, req.ExpiresAt).Scan(&keyID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save key"})
		return
	}
	actorType := c.GetString("actor_type")
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, actorType, actorID,
		"create_api_key", "api_keys", keyID.String(), c.ClientIP(),
		map[string]any{"name": req.Name, "scopes": req.Scopes, "expires_at": req.ExpiresAt}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record API key audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit API key creation"})
		return
	}

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

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "API key store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	result, err := tx.Exec(c.Request.Context(), `
		UPDATE api_keys SET revoked = true WHERE id = $1 AND organization_id = $2
	`, keyID, orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to revoke key"})
		return
	}
	if result.RowsAffected() != 1 {
		c.JSON(http.StatusNotFound, gin.H{"error": "API key not found"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, c.GetString("actor_type"), actorID,
		"revoke_api_key", "api_keys", keyID.String(), c.ClientIP(), nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record API key audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit API key revocation"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "api key revoked"})
}

// -------------------------------------------------------------
// HANDLERS: WALLETS
// -------------------------------------------------------------
type CreateWalletRequest struct {
	Currency    string             `json:"currency" binding:"required,oneof=USD EUR"`
	AccountType domain.AccountType `json:"account_type"`
}

func parsePageLimit(c *gin.Context, defaultLimit int) int {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(defaultLimit)))
	if err != nil || limit < 1 {
		return -1
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func parsePageCursor(value string) (*time.Time, uuid.UUID, error) {
	if value == "" {
		return nil, uuid.Nil, nil
	}
	parts := strings.SplitN(value, "|", 2)
	if len(parts) != 2 {
		return nil, uuid.Nil, errors.New("cursor must contain timestamp and ID")
	}
	cursorTime, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return nil, uuid.Nil, err
	}
	cursorID, err := uuid.Parse(parts[1])
	if err != nil {
		return nil, uuid.Nil, err
	}
	return &cursorTime, cursorID, nil
}

func (s *Server) handleListWallets(c *gin.Context) {
	orgID := c.MustGet("organization_id").(uuid.UUID)
	limit := parsePageLimit(c, 50)
	if limit < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
		return
	}
	cursor, cursorID, err := parsePageCursor(c.Query("cursor"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet cursor"})
		return
	}

	actorType := c.GetString("actor_type")
	userID := uuid.Nil
	if actorType == "user" {
		userID = c.MustGet("user_id").(uuid.UUID)
	}
	var rows pgx.Rows
	if cursor == nil {
		rows, err = s.db.Query(c.Request.Context(), `
			SELECT id, organization_id, currency, account_type, balance, hold_balance, status,
			       version, created_at, updated_at
			FROM wallets
			WHERE organization_id = $1 AND account_type = 'customer'
			  AND ($2 <> 'user' OR owner_user_id = $3)
			ORDER BY created_at DESC, id DESC
			LIMIT $4
		`, orgID, actorType, userID, limit)
	} else {
		rows, err = s.db.Query(c.Request.Context(), `
			SELECT id, organization_id, currency, account_type, balance, hold_balance, status,
			       version, created_at, updated_at
			FROM wallets
			WHERE organization_id = $1 AND account_type = 'customer'
			  AND ($2 <> 'user' OR owner_user_id = $3)
			  AND (created_at, id) < ($4, $5)
			ORDER BY created_at DESC, id DESC
			LIMIT $6
		`, orgID, actorType, userID, *cursor, cursorID, limit)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying wallets"})
		return
	}
	defer rows.Close()

	wallets := make([]domain.Wallet, 0, limit)
	for rows.Next() {
		var wallet domain.Wallet
		if err := rows.Scan(
			&wallet.ID, &wallet.OrganizationID, &wallet.Currency, &wallet.AccountType,
			&wallet.Balance, &wallet.HoldBalance, &wallet.Status, &wallet.Version,
			&wallet.CreatedAt, &wallet.UpdatedAt,
		); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed reading wallets"})
			return
		}
		wallets = append(wallets, wallet)
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed iterating wallets"})
		return
	}

	nextCursor := ""
	walletResults := make([]gin.H, 0, len(wallets))
	for _, wallet := range wallets {
		walletResults = append(walletResults, gin.H{
			"id":                wallet.ID,
			"currency":          wallet.Currency,
			"account_type":      wallet.AccountType,
			"balance":           wallet.Balance,
			"hold_balance":      wallet.HoldBalance,
			"available_balance": wallet.AvailableBalance(),
			"status":            wallet.Status,
			"created_at":        wallet.CreatedAt,
			"updated_at":        wallet.UpdatedAt,
		})
	}
	if len(wallets) == limit {
		last := wallets[len(wallets)-1]
		nextCursor = last.CreatedAt.Format(time.RFC3339Nano) + "|" + last.ID.String()
	}
	c.JSON(http.StatusOK, gin.H{"wallets": walletResults, "next_cursor": nextCursor})
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
	if req.AccountType != domain.AccountCustomer {
		c.JSON(http.StatusBadRequest, gin.H{"error": "system account types are managed internally"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	var ownerUserID any
	if c.GetString("actor_type") == "user" {
		ownerUserID = c.MustGet("user_id").(uuid.UUID)
	}
	var walletID uuid.UUID
	err := s.db.QueryRow(c.Request.Context(), `
		INSERT INTO wallets (organization_id, owner_user_id, currency, account_type, balance, hold_balance)
		VALUES ($1, $2, $3, $4, 0, 0)
		RETURNING id
	`, orgID, ownerUserID, req.Currency, req.AccountType).Scan(&walletID)
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

func (s *Server) handleDeleteUnusedWallet(c *gin.Context) {
	walletID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet id"})
		return
	}
	orgID := c.MustGet("organization_id").(uuid.UUID)
	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "wallet store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	var balance, holdBalance int64
	var accountType string
	var ownerUserID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		SELECT balance, hold_balance, account_type,
		       COALESCE(owner_user_id, '00000000-0000-0000-0000-000000000000'::uuid)
		FROM wallets
		WHERE id = $1 AND organization_id = $2
		FOR UPDATE
	`, walletID, orgID).Scan(&balance, &holdBalance, &accountType, &ownerUserID)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load wallet"})
		return
	}
	if c.GetString("actor_type") == "user" && ownerUserID != c.MustGet("user_id").(uuid.UUID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}
	if accountType != string(domain.AccountCustomer) {
		c.JSON(http.StatusConflict, gin.H{"error": "system wallets cannot be deleted"})
		return
	}
	if balance != 0 || holdBalance != 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "only an empty wallet with no held funds can be deleted"})
		return
	}

	var hasLedger, hasTransfers bool
	if err := tx.QueryRow(c.Request.Context(), `
		SELECT EXISTS (SELECT 1 FROM ledger_entries WHERE wallet_id = $1)
	`, walletID).Scan(&hasLedger); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check wallet ledger history"})
		return
	}
	if err := tx.QueryRow(c.Request.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM transfers
			WHERE source_wallet_id = $1 OR destination_wallet_id = $1
		)
	`, walletID).Scan(&hasTransfers); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check wallet transfer history"})
		return
	}
	if hasLedger || hasTransfers {
		c.JSON(http.StatusConflict, gin.H{
			"error": "wallet has financial history and cannot be deleted; close it instead after all funds and pending transfers are cleared",
		})
		return
	}

	if _, err := tx.Exec(c.Request.Context(), `
		DELETE FROM wallets WHERE id = $1 AND organization_id = $2
	`, walletID, orgID); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "wallet could not be deleted"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, c.GetString("actor_type"),
		c.MustGet("actor_id").(uuid.UUID), "delete_wallet", "wallets", walletID.String(), c.ClientIP(), nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record wallet deletion audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit wallet deletion"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"wallet_id": walletID, "status": "DELETED"})
}

func (s *Server) handleCloseWallet(c *gin.Context) {
	walletID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet id"})
		return
	}
	orgID := c.MustGet("organization_id").(uuid.UUID)
	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "wallet store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	var balance, holdBalance int64
	var status, accountType string
	var ownerUserID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		SELECT balance, hold_balance, status, account_type,
		       COALESCE(owner_user_id, '00000000-0000-0000-0000-000000000000'::uuid)
		FROM wallets
		WHERE id = $1 AND organization_id = $2
		FOR UPDATE
	`, walletID, orgID).Scan(&balance, &holdBalance, &status, &accountType, &ownerUserID)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load wallet"})
		return
	}
	if c.GetString("actor_type") == "user" && ownerUserID != c.MustGet("user_id").(uuid.UUID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}
	if accountType != string(domain.AccountCustomer) {
		c.JSON(http.StatusConflict, gin.H{"error": "system wallets cannot be closed"})
		return
	}
	if status == string(domain.WalletStatusClosed) {
		c.JSON(http.StatusConflict, gin.H{"error": "wallet is already closed"})
		return
	}
	if balance != 0 || holdBalance != 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "wallet must have zero balance and no held funds before it can be closed"})
		return
	}

	var hasOpenTransfers bool
	if err := tx.QueryRow(c.Request.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM transfers
			WHERE (source_wallet_id = $1 OR destination_wallet_id = $1)
			  AND status IN ('PENDING', 'PROCESSING', 'COMPLETED')
		)
	`, walletID).Scan(&hasOpenTransfers); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check active transfer history"})
		return
	}
	if hasOpenTransfers {
		c.JSON(http.StatusConflict, gin.H{"error": "wallet has pending or completed transfers and cannot be closed yet"})
		return
	}

	result, err := tx.Exec(c.Request.Context(), `
		UPDATE wallets
		SET status = 'closed', version = version + 1, updated_at = NOW()
		WHERE id = $1 AND organization_id = $2 AND status <> 'closed'
	`, walletID, orgID)
	if err != nil || result.RowsAffected() != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "wallet changed before it could be closed"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, c.GetString("actor_type"),
		c.MustGet("actor_id").(uuid.UUID), "close_wallet", "wallets", walletID.String(), c.ClientIP(), nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record wallet closure audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit wallet closure"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"wallet_id": walletID, "status": "closed"})
}

func (s *Server) handleReopenWallet(c *gin.Context) {
	walletID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wallet id"})
		return
	}
	orgID := c.MustGet("organization_id").(uuid.UUID)
	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "wallet store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	var balance, holdBalance int64
	var status, accountType string
	var ownerUserID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		SELECT balance, hold_balance, status, account_type,
		       COALESCE(owner_user_id, '00000000-0000-0000-0000-000000000000'::uuid)
		FROM wallets
		WHERE id = $1 AND organization_id = $2
		FOR UPDATE
	`, walletID, orgID).Scan(&balance, &holdBalance, &status, &accountType, &ownerUserID)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load wallet"})
		return
	}
	if c.GetString("actor_type") == "user" && ownerUserID != c.MustGet("user_id").(uuid.UUID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}
	if accountType != string(domain.AccountCustomer) {
		c.JSON(http.StatusConflict, gin.H{"error": "system wallets cannot be reopened"})
		return
	}
	if status != string(domain.WalletStatusClosed) {
		c.JSON(http.StatusConflict, gin.H{"error": "only closed wallets can be reopened"})
		return
	}
	if balance != 0 || holdBalance != 0 {
		c.JSON(http.StatusConflict, gin.H{"error": "wallet must have zero balance and no held funds before it can be reopened"})
		return
	}

	result, err := tx.Exec(c.Request.Context(), `
		UPDATE wallets
		SET status = 'active', version = version + 1, updated_at = NOW()
		WHERE id = $1 AND organization_id = $2 AND status = 'closed'
	`, walletID, orgID)
	if err != nil || result.RowsAffected() != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "wallet changed before it could be reopened"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, c.GetString("actor_type"),
		c.MustGet("actor_id").(uuid.UUID), "reopen_wallet", "wallets", walletID.String(), c.ClientIP(), nil); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record wallet reopening audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit wallet reopening"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"wallet_id": walletID, "status": "active"})
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
	actorType := c.GetString("actor_type")
	userID := uuid.Nil
	if actorType == "user" {
		userID = c.MustGet("user_id").(uuid.UUID)
	}
	err = s.db.QueryRow(c.Request.Context(), `
		SELECT id, organization_id, currency, account_type, balance, hold_balance, status, created_at, updated_at
		FROM wallets
		WHERE id = $1 AND organization_id = $2
		  AND ($3 <> 'user' OR owner_user_id = $4)
	`, walletID, orgID, actorType, userID).Scan(
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
	if err := c.ShouldBindJSON(&req); err != nil || req.Amount <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "amount must be greater than 0"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	actorType := c.GetString("actor_type")
	userID := uuid.Nil
	if actorType == "user" {
		userID = c.MustGet("user_id").(uuid.UUID)
	}

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "transaction start failed"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	// Fetch wallet
	var currency, walletStatus, accountType string
	var ownerUserID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		SELECT currency, status, account_type,
		       COALESCE(owner_user_id, '00000000-0000-0000-0000-000000000000'::uuid)
		FROM wallets
		WHERE id = $1 AND organization_id = $2 FOR UPDATE
	`, walletID, orgID).Scan(&currency, &walletStatus, &accountType, &ownerUserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}
	if actorType == "user" && ownerUserID != userID {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}
	if accountType != string(domain.AccountCustomer) || walletStatus != string(domain.WalletStatusActive) {
		c.JSON(http.StatusConflict, gin.H{"error": "only active customer wallets can receive deposits"})
		return
	}

	// Ensure external_funding system account
	_, err = tx.Exec(c.Request.Context(), `
		INSERT INTO wallets (organization_id, currency, account_type, balance, hold_balance)
		VALUES ($1, $2, 'external_funding', 0, 0)
		ON CONFLICT (organization_id, currency, account_type)
		WHERE account_type IN ('external_funding', 'fees', 'suspense', 'inter_org_clearing')
		DO NOTHING
	`, orgID, currency)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to initialize funding account"})
		return
	}
	var fundingWalletID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		SELECT id FROM wallets
		WHERE organization_id = $1 AND currency = $2 AND account_type = 'external_funding'
		FOR UPDATE
	`, orgID, currency).Scan(&fundingWalletID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load funding account"})
		return
	}

	// Balanced Double-Entry:
	// DR: external_funding
	// CR: customer wallet
	journalID := uuid.New()
	_, err = tx.Exec(c.Request.Context(), `
		INSERT INTO journal_entries (id, organization_id, reference_id, reference_type, currency, description)
		VALUES ($1, $2, $1, 'deposit', $3, 'External wallet deposit')
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
	res, err := tx.Exec(c.Request.Context(), `
		UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE id = $2
	`, req.Amount, walletID)
	if err != nil || res.RowsAffected() != 1 {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed updating wallet balance"})
		return
	}

	res, err = tx.Exec(c.Request.Context(), `
		UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE id = $2
	`, req.Amount, fundingWalletID)
	if err != nil || res.RowsAffected() != 1 {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed updating funding account balance"})
		return
	}

	// Save Idempotency response
	respPayload := gin.H{
		"journal_entry_id": journalID,
		"wallet_id":        walletID,
		"credited_amount":  req.Amount,
		"currency":         currency,
		"status":           "SUCCESS",
	}
	if err := s.saveIdempotencyResponse(c.Request.Context(), tx, orgID, c.GetString("idempotency_key"), http.StatusOK, respPayload); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save idempotency response"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, c.GetString("actor_type"),
		c.MustGet("actor_id").(uuid.UUID), "deposit", "wallets", walletID.String(), c.ClientIP(),
		map[string]any{"amount": req.Amount, "currency": currency, "journal_entry_id": journalID}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record deposit audit"})
		return
	}

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
	DestinationWalletID uuid.UUID `json:"destination_wallet_id"`
	RecipientEmail      string    `json:"recipient_email" binding:"omitempty,email"`
	Currency            string    `json:"currency" binding:"required,oneof=USD EUR"`
	Amount              int64     `json:"amount" binding:"required,gt=0"`
}

func (s *Server) handleCreateTransfer(c *gin.Context) {
	var req CreateTransferRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transfer request payload"})
		return
	}

	if req.DestinationWalletID == uuid.Nil && strings.TrimSpace(req.RecipientEmail) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "destination_wallet_id or recipient_email is required"})
		return
	}
	if req.DestinationWalletID != uuid.Nil && strings.TrimSpace(req.RecipientEmail) != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "provide either destination_wallet_id or recipient_email, not both"})
		return
	}
	if req.Amount > math.MaxInt64-s.transferFee {
		c.JSON(http.StatusBadRequest, gin.H{"error": "amount exceeds the supported range"})
		return
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	idempotencyKey := c.GetString("idempotency_key")
	recipientEmailRequested := strings.TrimSpace(req.RecipientEmail) != ""

	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "tx failed"})
		return
	}
	defer tx.Rollback(c.Request.Context())

	if recipientEmail := strings.TrimSpace(req.RecipientEmail); recipientEmail != "" {
		rows, err := tx.Query(c.Request.Context(), `
			SELECT w.id
			FROM users u
			JOIN wallets w ON w.owner_user_id = u.id AND w.organization_id = u.organization_id
			WHERE lower(u.email) = lower($1)
			  AND u.is_active = TRUE
			  AND w.currency = $2 AND w.account_type = 'customer' AND w.status = 'active'
			ORDER BY w.created_at, w.id
			LIMIT 2
		`, recipientEmail, req.Currency)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resolve recipient wallet"})
			return
		}
		var matchingWallets []uuid.UUID
		for rows.Next() {
			var walletID uuid.UUID
			if err := rows.Scan(&walletID); err != nil {
				rows.Close()
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read recipient wallet"})
				return
			}
			matchingWallets = append(matchingWallets, walletID)
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read recipient wallets"})
			return
		}
		switch len(matchingWallets) {
		case 0:
			c.JSON(http.StatusNotFound, gin.H{"error": "recipient wallet is unavailable for this currency"})
			return
		case 1:
			req.DestinationWalletID = matchingWallets[0]
		default:
			c.JSON(http.StatusConflict, gin.H{"error": "recipient has multiple active wallets in this currency; provide destination_wallet_id"})
			return
		}
	}
	if req.SourceWalletID == req.DestinationWalletID {
		c.JSON(http.StatusBadRequest, gin.H{"error": domain.ErrSelfTransfer.Error()})
		return
	}

	// Lock both wallets in UUID order so opposite-direction transfers cannot deadlock.
	walletRows, err := tx.Query(c.Request.Context(), `
		SELECT w.id, w.organization_id, w.balance, w.hold_balance, w.currency, w.status, w.account_type,
		       COALESCE(w.owner_user_id, '00000000-0000-0000-0000-000000000000'::uuid),
		       COALESCE(owner.is_active, TRUE)
		FROM wallets w
		LEFT JOIN users owner ON owner.id = w.owner_user_id AND owner.organization_id = w.organization_id
		WHERE w.id = ANY($1)
		ORDER BY w.id
		FOR UPDATE OF w
	`, []uuid.UUID{req.SourceWalletID, req.DestinationWalletID})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to lock transfer wallets"})
		return
	}
	type lockedWallet struct {
		organizationID uuid.UUID
		balance, hold  int64
		currency       string
		status         string
		accountType    string
		ownerUserID    uuid.UUID
		ownerActive    bool
	}
	locked := make(map[uuid.UUID]lockedWallet, 2)
	for walletRows.Next() {
		var id uuid.UUID
		var wallet lockedWallet
		if err := walletRows.Scan(
			&id, &wallet.organizationID, &wallet.balance, &wallet.hold, &wallet.currency, &wallet.status,
			&wallet.accountType, &wallet.ownerUserID, &wallet.ownerActive,
		); err != nil {
			walletRows.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read transfer wallets"})
			return
		}
		locked[id] = wallet
	}
	rowsErr := walletRows.Err()
	walletRows.Close()
	if rowsErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read transfer wallets"})
		return
	}
	source, sourceExists := locked[req.SourceWalletID]
	destination, destinationExists := locked[req.DestinationWalletID]
	if !sourceExists || !destinationExists {
		c.JSON(http.StatusNotFound, gin.H{"error": "source or destination wallet not found"})
		return
	}
	if source.organizationID != orgID {
		c.JSON(http.StatusNotFound, gin.H{"error": "source wallet not found"})
		return
	}
	if c.GetString("actor_type") == "user" {
		userID := c.MustGet("user_id").(uuid.UUID)
		if source.ownerUserID != userID {
			c.JSON(http.StatusNotFound, gin.H{"error": "source wallet not found"})
			return
		}
		if destination.ownerUserID == uuid.Nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "destination wallet not found"})
			return
		}
		if recipientEmailRequested && destination.ownerUserID == userID {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user-to-user transfers must target another user's wallet"})
			return
		}
	}

	if source.currency != req.Currency {
		c.JSON(http.StatusBadRequest, gin.H{"error": "currency mismatch with source wallet"})
		return
	}
	if destination.currency != req.Currency {
		c.JSON(http.StatusBadRequest, gin.H{"error": "destination wallet currency does not match transfer currency"})
		return
	}
	if destination.ownerUserID != uuid.Nil && !destination.ownerActive {
		c.JSON(http.StatusConflict, gin.H{"error": "recipient account is deactivated"})
		return
	}
	if source.accountType != string(domain.AccountCustomer) ||
		destination.accountType != string(domain.AccountCustomer) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "transfers are only allowed between customer wallets"})
		return
	}
	if source.status != string(domain.WalletStatusActive) ||
		destination.status != string(domain.WalletStatusActive) {
		c.JSON(http.StatusConflict, gin.H{"error": "both wallets must be active"})
		return
	}

	totalNeeded := req.Amount + s.transferFee
	if (source.balance - source.hold) < totalNeeded {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": domain.ErrInsufficientBalance.Error()})
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
			id, organization_id, source_wallet_id, destination_wallet_id, destination_organization_id,
			currency, amount, fee_amount,
			status, saga_step, fencing_token, idempotency_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING', 'RESERVE_FUNDS', 1, $9)
	`, transferID, orgID, req.SourceWalletID, req.DestinationWalletID, destination.organizationID,
		req.Currency, req.Amount, s.transferFee, idempotencyKey)
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
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, c.GetString("actor_type"),
		c.MustGet("actor_id").(uuid.UUID), "create_transfer", "transfers", transferID.String(),
		c.ClientIP(), map[string]any{"amount": req.Amount, "currency": req.Currency}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record transfer audit"})
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
	if err := s.saveIdempotencyResponse(c.Request.Context(), tx, orgID, idempotencyKey, http.StatusAccepted, respPayload); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save idempotency response"})
		return
	}

	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}
	s.metrics.TransfersTotal.WithLabelValues(req.Currency, "accepted").Inc()

	// Return 202 Accepted
	c.JSON(http.StatusAccepted, respPayload)
}

func (s *Server) handleListTransfers(c *gin.Context) {
	orgID := c.MustGet("organization_id").(uuid.UUID)
	limit := parsePageLimit(c, 50)
	if limit < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
		return
	}
	cursor, cursorID, err := parsePageCursor(c.Query("cursor"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transfer cursor"})
		return
	}
	status := strings.ToUpper(c.Query("status"))
	switch domain.TransferStatus(status) {
	case "", domain.TransferStatusPending, domain.TransferStatusProcessing,
		domain.TransferStatusCompleted, domain.TransferStatusFailed, domain.TransferStatusReversed:
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid transfer status"})
		return
	}

	var rows pgx.Rows
	actorType := c.GetString("actor_type")
	userID := uuid.Nil
	if actorType == "user" {
		userID = c.MustGet("user_id").(uuid.UUID)
	}
	const transferVisibility = `($3 <> 'user' OR EXISTS (
		SELECT 1
		FROM wallets source_wallet
		JOIN wallets destination_wallet ON destination_wallet.id = transfers.destination_wallet_id
		WHERE source_wallet.id = transfers.source_wallet_id
		  AND ((source_wallet.organization_id = $1 AND source_wallet.owner_user_id = $4)
		       OR (destination_wallet.organization_id = $1 AND destination_wallet.owner_user_id = $4))
	))`
	if cursor == nil {
		rows, err = s.db.Query(c.Request.Context(), `
			SELECT id, organization_id, destination_organization_id, source_wallet_id, destination_wallet_id, currency,
			       amount, fee_amount, status, saga_step, COALESCE(failure_reason, ''),
			       fencing_token, idempotency_key, created_at, updated_at
			FROM transfers
			WHERE (organization_id = $1 OR destination_organization_id = $1) AND ($2 = '' OR status = $2)
			  AND `+transferVisibility+`
			ORDER BY created_at DESC, id DESC
			LIMIT $5
		`, orgID, status, actorType, userID, limit)
	} else {
		rows, err = s.db.Query(c.Request.Context(), `
			SELECT id, organization_id, destination_organization_id, source_wallet_id, destination_wallet_id, currency,
			       amount, fee_amount, status, saga_step, COALESCE(failure_reason, ''),
			       fencing_token, idempotency_key, created_at, updated_at
			FROM transfers
			WHERE (organization_id = $1 OR destination_organization_id = $1) AND ($2 = '' OR status = $2)
			  AND `+transferVisibility+`
			  AND (created_at, id) < ($5, $6)
			ORDER BY created_at DESC, id DESC
			LIMIT $7
		`, orgID, status, actorType, userID, *cursor, cursorID, limit)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying transfers"})
		return
	}
	defer rows.Close()

	transfers := make([]domain.Transfer, 0, limit)
	for rows.Next() {
		var transfer domain.Transfer
		if err := rows.Scan(
			&transfer.ID, &transfer.OrganizationID, &transfer.DestinationOrganizationID, &transfer.SourceWalletID,
			&transfer.DestinationWalletID, &transfer.Currency, &transfer.Amount,
			&transfer.FeeAmount, &transfer.Status, &transfer.SagaStep,
			&transfer.FailureReason, &transfer.FencingToken, &transfer.IdempotencyKey,
			&transfer.CreatedAt, &transfer.UpdatedAt,
		); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed reading transfers"})
			return
		}
		transfers = append(transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed iterating transfers"})
		return
	}

	nextCursor := ""
	transferResults := make([]gin.H, 0, len(transfers))
	for _, transfer := range transfers {
		transferResults = append(transferResults, gin.H{
			"id":                          transfer.ID,
			"organization_id":             transfer.OrganizationID,
			"destination_organization_id": transfer.DestinationOrganizationID,
			"source_wallet_id":            transfer.SourceWalletID,
			"destination_wallet_id":       transfer.DestinationWalletID,
			"currency":                    transfer.Currency,
			"amount":                      transfer.Amount,
			"fee_amount":                  transfer.FeeAmount,
			"status":                      transfer.Status,
			"saga_step":                   transfer.SagaStep,
			"failure_reason":              transfer.FailureReason,
			"created_at":                  transfer.CreatedAt,
			"updated_at":                  transfer.UpdatedAt,
		})
	}
	if len(transfers) == limit {
		last := transfers[len(transfers)-1]
		nextCursor = last.CreatedAt.Format(time.RFC3339Nano) + "|" + last.ID.String()
	}
	c.JSON(http.StatusOK, gin.H{"transfers": transferResults, "next_cursor": nextCursor})
}

type transferAccountSummary struct {
	WalletID    uuid.UUID `json:"wallet_id"`
	Currency    string    `json:"currency"`
	AccountType string    `json:"account_type"`
}

type transferLedgerEntry struct {
	ID             uuid.UUID        `json:"id"`
	JournalEntryID uuid.UUID        `json:"journal_entry_id"`
	WalletID       uuid.UUID        `json:"wallet_id"`
	Direction      domain.Direction `json:"direction"`
	Amount         int64            `json:"amount"`
	Currency       string           `json:"currency"`
	AccountType    string           `json:"account_type"`
	ReferenceType  string           `json:"reference_type"`
	Description    string           `json:"description"`
	CreatedAt      time.Time        `json:"created_at"`
}

type transferDetailResponse struct {
	domain.Transfer
	SenderAccount   transferAccountSummary `json:"sender_account"`
	ReceiverAccount transferAccountSummary `json:"receiver_account"`
	LedgerEntries   []transferLedgerEntry  `json:"ledger_entries"`
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
	actorType := c.GetString("actor_type")
	userID := uuid.Nil
	if actorType == "user" {
		userID = c.MustGet("user_id").(uuid.UUID)
	}
	var senderAccount, receiverAccount transferAccountSummary
	err = s.db.QueryRow(c.Request.Context(), `
		SELECT t.id, t.organization_id, t.destination_organization_id, t.source_wallet_id, t.destination_wallet_id, t.currency, t.amount, t.fee_amount,
		       t.status, t.saga_step, COALESCE(t.failure_reason, ''), t.fencing_token, t.created_at, t.updated_at,
		       source_wallet.currency, source_wallet.account_type,
		       destination_wallet.currency, destination_wallet.account_type
		FROM transfers t
		JOIN wallets source_wallet ON source_wallet.id = t.source_wallet_id
		JOIN wallets destination_wallet ON destination_wallet.id = t.destination_wallet_id
		WHERE t.id = $1 AND (t.organization_id = $2 OR t.destination_organization_id = $2)
		  AND ($3 <> 'user' OR EXISTS (
			SELECT 1
			FROM wallets source_wallet
			JOIN wallets destination_wallet ON destination_wallet.id = t.destination_wallet_id
			WHERE source_wallet.id = t.source_wallet_id
			  AND ((source_wallet.organization_id = $2 AND source_wallet.owner_user_id = $4)
			       OR (destination_wallet.organization_id = $2 AND destination_wallet.owner_user_id = $4))
		  ))
	`, transferID, orgID, actorType, userID).Scan(
		&t.ID, &t.OrganizationID, &t.DestinationOrganizationID, &t.SourceWalletID, &t.DestinationWalletID, &t.Currency, &t.Amount, &t.FeeAmount,
		&t.Status, &t.SagaStep, &t.FailureReason, &t.FencingToken, &t.CreatedAt, &t.UpdatedAt,
		&senderAccount.Currency, &senderAccount.AccountType, &receiverAccount.Currency, &receiverAccount.AccountType,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "transfer not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying transfer"})
		return
	}

	senderAccount.WalletID = t.SourceWalletID
	receiverAccount.WalletID = t.DestinationWalletID
	ledgerRows, err := s.db.Query(c.Request.Context(), `
		SELECT le.id, le.journal_entry_id, le.wallet_id, le.direction, le.amount,
		       w.currency, w.account_type, j.reference_type, COALESCE(j.description, ''), le.created_at
		FROM journal_entries j
		JOIN ledger_entries le ON le.journal_entry_id = j.id
		JOIN wallets w ON w.id = le.wallet_id
		WHERE j.reference_id = $1
		  AND j.reference_type IN ('transfer', 'reversal')
		  AND j.organization_id = $2
		ORDER BY j.created_at, le.created_at, le.id
	`, transferID, orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying transfer ledger"})
		return
	}
	defer ledgerRows.Close()

	ledgerEntries := make([]transferLedgerEntry, 0)
	for ledgerRows.Next() {
		var entry transferLedgerEntry
		if err := ledgerRows.Scan(
			&entry.ID, &entry.JournalEntryID, &entry.WalletID, &entry.Direction, &entry.Amount,
			&entry.Currency, &entry.AccountType, &entry.ReferenceType, &entry.Description, &entry.CreatedAt,
		); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed reading transfer ledger"})
			return
		}
		ledgerEntries = append(ledgerEntries, entry)
	}
	if err := ledgerRows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed iterating transfer ledger"})
		return
	}

	c.JSON(http.StatusOK, transferDetailResponse{
		Transfer:        t,
		SenderAccount:   senderAccount,
		ReceiverAccount: receiverAccount,
		LedgerEntries:   ledgerEntries,
	})
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
	var fencingToken int64
	err = tx.QueryRow(c.Request.Context(), `
		SELECT id, organization_id, destination_organization_id, source_wallet_id, destination_wallet_id,
		       currency, amount, fee_amount, status, fencing_token
		FROM transfers WHERE id = $1 AND organization_id = $2 FOR UPDATE
	`, transferID, orgID).Scan(
		&t.ID, &t.OrganizationID, &t.DestinationOrganizationID, &t.SourceWalletID, &t.DestinationWalletID, &t.Currency,
		&t.Amount, &t.FeeAmount, &t.Status, &fencingToken,
	)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "transfer not found"})
		return
	}

	if t.Status != domain.TransferStatusCompleted {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only COMPLETED transfers can be reversed"})
		return
	}
	if t.Amount <= 0 || t.FeeAmount < 0 || t.Amount > math.MaxInt64-t.FeeAmount {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "transfer amount is outside the supported range"})
		return
	}

	type originalEntry struct {
		organizationID uuid.UUID
		walletID       uuid.UUID
		direction      domain.Direction
		amount         int64
	}
	originalRows, err := tx.Query(c.Request.Context(), `
		SELECT j.organization_id, le.wallet_id, le.direction, le.amount
		FROM journal_entries j
		JOIN ledger_entries le ON le.journal_entry_id = j.id
		WHERE j.reference_id = $1 AND j.reference_type = 'transfer'
		ORDER BY j.organization_id, le.wallet_id
	`, t.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load original transfer ledger"})
		return
	}
	var originalEntries []originalEntry
	walletIDSet := make(map[uuid.UUID]struct{})
	organizationsWithJournals := make(map[uuid.UUID]struct{})
	for originalRows.Next() {
		var entry originalEntry
		if err := originalRows.Scan(&entry.organizationID, &entry.walletID, &entry.direction, &entry.amount); err != nil {
			originalRows.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read original transfer ledger"})
			return
		}
		originalEntries = append(originalEntries, entry)
		walletIDSet[entry.walletID] = struct{}{}
		organizationsWithJournals[entry.organizationID] = struct{}{}
	}
	rowsErr := originalRows.Err()
	originalRows.Close()
	if rowsErr != nil || len(originalEntries) < 2 {
		c.JSON(http.StatusConflict, gin.H{"error": "original transfer ledger is missing or incomplete"})
		return
	}
	if _, ok := organizationsWithJournals[t.OrganizationID]; !ok {
		c.JSON(http.StatusConflict, gin.H{"error": "sender transfer journal is missing"})
		return
	}
	if _, ok := organizationsWithJournals[t.DestinationOrganizationID]; !ok {
		c.JSON(http.StatusConflict, gin.H{"error": "recipient transfer journal is missing"})
		return
	}
	walletIDs := make([]uuid.UUID, 0, len(walletIDSet))
	for walletID := range walletIDSet {
		walletIDs = append(walletIDs, walletID)
	}

	type reversalWallet struct {
		organizationID        uuid.UUID
		balance, holdBalance  int64
		currency, accountType string
	}
	locked := make(map[uuid.UUID]reversalWallet, len(walletIDs))
	walletRows, err := tx.Query(c.Request.Context(), `
		SELECT id, organization_id, balance, hold_balance, currency, account_type FROM wallets
		WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE
	`, walletIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to lock reversal wallets"})
		return
	}
	for walletRows.Next() {
		var id uuid.UUID
		var wallet reversalWallet
		if err := walletRows.Scan(&id, &wallet.organizationID, &wallet.balance, &wallet.holdBalance,
			&wallet.currency, &wallet.accountType); err != nil {
			walletRows.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read reversal wallets"})
			return
		}
		locked[id] = wallet
	}
	rowsErr = walletRows.Err()
	walletRows.Close()
	if rowsErr != nil || len(locked) != len(walletIDs) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "one or more reversal wallets are missing"})
		return
	}
	deltas := make(map[uuid.UUID]int64)
	groupedEntries := make(map[uuid.UUID][]originalEntry)
	for _, entry := range originalEntries {
		wallet := locked[entry.walletID]
		if wallet.organizationID != entry.organizationID || wallet.currency != t.Currency {
			c.JSON(http.StatusConflict, gin.H{"error": "original transfer ledger contains a mismatched wallet"})
			return
		}
		if entry.organizationID != t.OrganizationID && entry.organizationID != t.DestinationOrganizationID {
			c.JSON(http.StatusConflict, gin.H{"error": "original transfer ledger contains an unexpected organization"})
			return
		}
		var delta int64
		switch entry.direction {
		case domain.DirectionDebit:
			delta = entry.amount
		case domain.DirectionCredit:
			delta = -entry.amount
		default:
			c.JSON(http.StatusConflict, gin.H{"error": "original transfer ledger contains an invalid direction"})
			return
		}
		currentDelta := deltas[entry.walletID]
		if (delta > 0 && currentDelta > math.MaxInt64-delta) ||
			(delta < 0 && currentDelta < math.MinInt64-delta) {
			c.JSON(http.StatusConflict, gin.H{"error": "original transfer ledger exceeds the supported balance range"})
			return
		}
		deltas[entry.walletID] = currentDelta + delta
		groupedEntries[entry.organizationID] = append(groupedEntries[entry.organizationID], entry)
	}
	for walletID, delta := range deltas {
		wallet := locked[walletID]
		if (delta > 0 && wallet.balance > math.MaxInt64-delta) ||
			(delta < 0 && wallet.balance < math.MinInt64-delta) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "reversal would exceed the supported wallet balance"})
			return
		}
		newBalance := wallet.balance + delta
		if wallet.accountType == string(domain.AccountCustomer) &&
			(newBalance < 0 || newBalance < wallet.holdBalance) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "recipient wallet has insufficient available funds to reverse transfer"})
			return
		}
		if wallet.accountType == string(domain.AccountFees) && newBalance < 0 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "fee account has insufficient funds to reverse transfer"})
			return
		}
	}

	reversalJournalIDs := make([]uuid.UUID, 0, len(groupedEntries))
	orderedOrganizations := []uuid.UUID{t.OrganizationID}
	if t.DestinationOrganizationID != t.OrganizationID {
		orderedOrganizations = append(orderedOrganizations, t.DestinationOrganizationID)
	}
	for _, journalOrganizationID := range orderedOrganizations {
		entries, ok := groupedEntries[journalOrganizationID]
		if !ok {
			c.JSON(http.StatusConflict, gin.H{"error": "original transfer journal is incomplete"})
			return
		}
		journalID := uuid.New()
		if _, err := tx.Exec(c.Request.Context(), `
			INSERT INTO journal_entries (id, organization_id, reference_id, reference_type, currency, description)
			VALUES ($1, $2, $3, 'reversal', $4, $5)
		`, journalID, journalOrganizationID, t.ID, t.Currency, fmt.Sprintf("Reversal of Transfer %s", t.ID)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create reversal journal"})
			return
		}
		for _, entry := range entries {
			direction := domain.DirectionDebit
			if entry.direction == domain.DirectionDebit {
				direction = domain.DirectionCredit
			}
			if _, err := tx.Exec(c.Request.Context(), `
				INSERT INTO ledger_entries (journal_entry_id, wallet_id, direction, amount)
				VALUES ($1, $2, $3, $4)
			`, journalID, entry.walletID, direction, entry.amount); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to post reversal ledger entries"})
				return
			}
		}
		reversalJournalIDs = append(reversalJournalIDs, journalID)
	}

	sort.Slice(walletIDs, func(i, j int) bool {
		return walletIDs[i].String() < walletIDs[j].String()
	})
	for _, walletID := range walletIDs {
		if _, err := tx.Exec(c.Request.Context(), `
			UPDATE wallets SET balance = balance + $1, updated_at = NOW()
			WHERE id = $2 AND organization_id = $3
		`, deltas[walletID], walletID, locked[walletID].organizationID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update reversal wallet balances"})
			return
		}
	}

	result, err := tx.Exec(c.Request.Context(), `
		UPDATE transfers
		SET status = 'REVERSED', fencing_token = fencing_token + 1, updated_at = NOW()
		WHERE id = $1 AND organization_id = $2 AND status = 'COMPLETED' AND fencing_token = $3
	`, t.ID, orgID, fencingToken)
	if err != nil || result.RowsAffected() != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "transfer changed while reversal was being processed"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, c.GetString("actor_type"),
		c.MustGet("actor_id").(uuid.UUID), "reverse_transfer", "transfers", t.ID.String(),
		c.ClientIP(), map[string]any{"reversal_journal_ids": reversalJournalIDs}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record reversal audit"})
		return
	}

	respPayload := gin.H{
		"reversal_journal_id":  reversalJournalIDs[0],
		"reversal_journal_ids": reversalJournalIDs,
		"transfer_id":          t.ID,
		"status":               "REVERSED",
	}
	if err := s.saveIdempotencyResponse(c.Request.Context(), tx, orgID, idempotencyKey, http.StatusOK, respPayload); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save idempotency response"})
		return
	}

	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}
	s.metrics.TransfersTotal.WithLabelValues(t.Currency, "reversed").Inc()

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
	actorType := c.GetString("actor_type")
	userID := uuid.Nil
	if actorType == "user" {
		userID = c.MustGet("user_id").(uuid.UUID)
	}
	var ownsWallet bool
	if err := s.db.QueryRow(c.Request.Context(), `
		SELECT EXISTS (
			SELECT 1 FROM wallets
			WHERE id = $1 AND organization_id = $2
			  AND ($3 <> 'user' OR owner_user_id = $4)
		)
	`, walletID, orgID, actorType, userID).Scan(&ownsWallet); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to authorize wallet ledger"})
		return
	}
	if !ownsWallet {
		c.JSON(http.StatusNotFound, gin.H{"error": "wallet not found"})
		return
	}

	cursorStr := c.Query("cursor")
	limitStr := c.DefaultQuery("limit", "20")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
		return
	}
	if limit > 100 {
		limit = 100
	}

	var query string
	var args []any
	if cursorStr != "" {
		parts := strings.SplitN(cursorStr, "|", 2)
		cursorTime, parseErr := time.Parse(time.RFC3339Nano, parts[0])
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ledger cursor"})
			return
		}
		if len(parts) == 2 {
			cursorID, parseErr := uuid.Parse(parts[1])
			if parseErr != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ledger cursor"})
				return
			}
			query = `
				SELECT le.id, le.journal_entry_id, le.wallet_id, le.direction, le.amount, le.created_at
				FROM ledger_entries le
				JOIN wallets w ON w.id = le.wallet_id
				WHERE le.wallet_id = $1 AND w.organization_id = $2
				  AND (le.created_at, le.id) < ($3, $4)
				ORDER BY le.created_at DESC, le.id DESC
				LIMIT $5
			`
			args = []any{walletID, orgID, cursorTime, cursorID, limit}
		} else {
			query = `
				SELECT le.id, le.journal_entry_id, le.wallet_id, le.direction, le.amount, le.created_at
				FROM ledger_entries le
				JOIN wallets w ON w.id = le.wallet_id
				WHERE le.wallet_id = $1 AND w.organization_id = $2 AND le.created_at < $3
				ORDER BY le.created_at DESC, le.id DESC
				LIMIT $4
			`
			args = []any{walletID, orgID, cursorTime, limit}
		}
	}

	if query == "" {
		query = `
			SELECT le.id, le.journal_entry_id, le.wallet_id, le.direction, le.amount, le.created_at
			FROM ledger_entries le
			JOIN wallets w ON w.id = le.wallet_id
			WHERE le.wallet_id = $1 AND w.organization_id = $2
			ORDER BY le.created_at DESC, le.id DESC
			LIMIT $3
		`
		args = []any{walletID, orgID, limit}
	}

	rows, err := s.db.Query(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed querying ledger"})
		return
	}
	defer rows.Close()

	var entries []domain.LedgerEntry
	var lastEntry *domain.LedgerEntry
	for rows.Next() {
		var e domain.LedgerEntry
		if err := rows.Scan(&e.ID, &e.JournalEntryID, &e.WalletID, &e.Direction, &e.Amount, &e.CreatedAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read ledger entries"})
			return
		}
		entries = append(entries, e)
		lastEntry = &e
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to iterate ledger entries"})
		return
	}
	nextCursor := ""
	if lastEntry != nil && len(entries) == limit {
		nextCursor = lastEntry.CreatedAt.Format(time.RFC3339Nano) + "|" + lastEntry.ID.String()
	}

	c.JSON(http.StatusOK, gin.H{
		"entries":     entries,
		"next_cursor": nextCursor,
	})
}

func (s *Server) handleReconciliationReport(c *gin.Context) {
	orgID := c.MustGet("organization_id").(uuid.UUID)
	discrepancies, err := s.auditor.RunAuditForOrganization(c.Request.Context(), orgID)
	if err != nil {
		s.metrics.ReconciliationErrors.Inc()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "audit failed: " + err.Error()})
		return
	}
	if len(discrepancies) > 0 {
		s.metrics.ReconciliationErrors.Add(float64(len(discrepancies)))
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
	Secret string   `json:"secret" binding:"required,min=32"`
	Events []string `json:"events"`
}

func (s *Server) handleRegisterWebhook(c *gin.Context) {
	var req RegisterWebhookRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := webhooks.ValidateWebhookURL(req.URL); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.Events) == 0 {
		req.Events = []string{"transfer.completed", "transfer.failed"}
	}
	allowedEvents := map[string]struct{}{"transfer.completed": {}, "transfer.failed": {}}
	seenEvents := make(map[string]struct{}, len(req.Events))
	for _, event := range req.Events {
		if _, ok := allowedEvents[event]; !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported webhook event"})
			return
		}
		if _, exists := seenEvents[event]; exists {
			c.JSON(http.StatusBadRequest, gin.H{"error": "duplicate webhook event"})
			return
		}
		seenEvents[event] = struct{}{}
	}

	orgID := c.MustGet("organization_id").(uuid.UUID)
	encryptedSecret, err := webhooks.EncryptSecret(s.webhookEncryptionKey, req.Secret)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to protect webhook secret"})
		return
	}
	tx, err := s.db.Begin(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "webhook store unavailable"})
		return
	}
	defer tx.Rollback(c.Request.Context())
	var webhookID uuid.UUID
	err = tx.QueryRow(c.Request.Context(), `
		INSERT INTO webhooks (organization_id, url, secret, events)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, orgID, req.URL, encryptedSecret, req.Events).Scan(&webhookID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to register webhook"})
		return
	}
	if err := s.recordAuditTx(c.Request.Context(), tx, orgID, c.GetString("actor_type"),
		c.MustGet("actor_id").(uuid.UUID), "register_webhook", "webhooks", webhookID.String(),
		c.ClientIP(), map[string]any{"url": req.URL, "events": req.Events}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to record webhook audit"})
		return
	}
	if err := tx.Commit(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit webhook registration"})
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
func (s *Server) saveIdempotencyResponse(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, key string, code int, body any) error {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal idempotency response: %w", err)
	}
	result, err := tx.Exec(ctx, `
		UPDATE idempotency_keys
		SET response_code = $1, response_body = $2
		WHERE organization_id = $3 AND key = $4 AND response_code IS NULL
	`, code, bodyBytes, orgID, key)
	if err != nil {
		return fmt.Errorf("store idempotency response: %w", err)
	}
	if result.RowsAffected() != 1 {
		return fmt.Errorf("idempotency reservation is missing or already completed")
	}
	return nil
}

func (s *Server) recordAudit(ctx context.Context, orgID uuid.UUID, actorType string, actorID uuid.UUID, action, resType, resID, ip string, details any) error {
	detBytes, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("marshal audit details: %w", err)
	}
	_, err = s.db.Exec(ctx, `
		INSERT INTO audit_logs (organization_id, actor_type, actor_id, action, resource_type, resource_id, ip_address, details)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, orgID, actorType, actorID, action, resType, resID, ip, detBytes)
	if err != nil {
		return fmt.Errorf("write audit log: %w", err)
	}
	return nil
}

func (s *Server) recordAuditTx(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, actorType string, actorID uuid.UUID, action, resType, resID, ip string, details any) error {
	detBytes, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("marshal audit details: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_logs (organization_id, actor_type, actor_id, action, resource_type, resource_id, ip_address, details)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, orgID, actorType, actorID, action, resType, resID, ip, detBytes); err != nil {
		return fmt.Errorf("write audit log: %w", err)
	}
	return nil
}
