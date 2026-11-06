package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/distributed-wallet/platform/pkg/auth"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// setupMockAuthRouter creates a lightweight Gin router with role and scope authorization checks
func setupMockAuthRouter(jwtSecret string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()

	authMiddleware := func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-Key")
		if apiKey != "" {
			// Mock API key scopes based on header value for testing
			switch apiKey {
			case "key_wallets_read_only":
				c.Set("actor_type", "api_key")
				c.Set("organization_id", uuid.MustParse("00000000-0000-0000-0000-000000000001"))
				c.Set("scopes", []string{"wallets:read"})
				c.Set("role", "api_client")
			case "key_full_admin":
				c.Set("actor_type", "api_key")
				c.Set("organization_id", uuid.MustParse("00000000-0000-0000-0000-000000000001"))
				c.Set("scopes", []string{"*"})
				c.Set("role", "api_client")
			default:
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid api key"})
			}
			c.Next()
			return
		}

		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authorization required"})
			return
		}

		tokenStr := authHeader[len("Bearer "):]
		claims, err := auth.ValidateToken(jwtSecret, tokenStr)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		c.Set("actor_type", "user")
		c.Set("user_id", claims.UserID)
		c.Set("organization_id", claims.OrganizationID)
		c.Set("role", claims.Role)
		c.Next()
	}

	requirePermission := func(requiredScope string, allowedRoles ...string) gin.HandlerFunc {
		return func(c *gin.Context) {
			actorType, _ := c.Get("actor_type")
			if actorType == "api_key" {
				scopesVal, exists := c.Get("scopes")
				if !exists {
					c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "missing scopes"})
					return
				}
				scopes := scopesVal.([]string)
				for _, s := range scopes {
					if s == requiredScope || s == "*" || s == "admin" {
						c.Next()
						return
					}
				}
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": fmt.Sprintf("api key lacks scope: %s", requiredScope)})
				return
			}

			userRole, _ := c.Get("role")
			roleStr := userRole.(string)
			for _, r := range allowedRoles {
				if r == roleStr {
					c.Next()
					return
				}
			}
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "insufficient role"})
		}
	}

	// Routes
	v1 := r.Group("/api/v1")
	v1.Use(authMiddleware)
	{
		v1.POST("/wallets", requirePermission("wallets:write", "admin", "finance"), func(c *gin.Context) {
			c.JSON(http.StatusCreated, gin.H{"status": "wallet created"})
		})
		v1.POST("/transfers", requirePermission("transfers:write", "admin", "finance"), func(c *gin.Context) {
			c.JSON(http.StatusAccepted, gin.H{"status": "transfer accepted"})
		})
		v1.POST("/transfers/:id/reverse", requirePermission("transfers:reverse", "admin"), func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"status": "transfer reversed"})
		})
		v1.GET("/wallets/:id", requirePermission("wallets:read", "admin", "finance", "viewer"), func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"status": "wallet retrieved"})
		})
	}

	return r
}

func TestNegativeAuthorization_ViewerRoleCannotCreateTransfer(t *testing.T) {
	jwtSecret := "test-secret-key-for-authz-tests"
	r := setupMockAuthRouter(jwtSecret)

	orgID := uuid.New()
	userID := uuid.New()

	// Generate token with "viewer" role
	viewerToken, _, _, err := auth.GenerateTokenPair(jwtSecret, userID, orgID, "viewer", nil)
	if err != nil {
		t.Fatalf("failed generating token: %v", err)
	}

	// 1. Viewer tries to create a transfer (requires admin or finance)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/transfers", nil)
	req.Header.Set("Authorization", "Bearer "+viewerToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for viewer creating transfer, got: %d", w.Code)
	}

	// 2. Viewer tries to reverse a transfer (requires admin)
	reqRev := httptest.NewRequest(http.MethodPost, "/api/v1/transfers/123/reverse", nil)
	reqRev.Header.Set("Authorization", "Bearer "+viewerToken)
	wRev := httptest.NewRecorder()
	r.ServeHTTP(wRev, reqRev)

	if wRev.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for viewer reversing transfer, got: %d", wRev.Code)
	}
}

func TestNegativeAuthorization_FinanceRoleCannotReverseTransfer(t *testing.T) {
	jwtSecret := "test-secret-key-for-authz-tests"
	r := setupMockAuthRouter(jwtSecret)

	orgID := uuid.New()
	userID := uuid.New()

	// Generate token with "finance" role
	financeToken, _, _, err := auth.GenerateTokenPair(jwtSecret, userID, orgID, "finance", nil)
	if err != nil {
		t.Fatalf("failed generating token: %v", err)
	}

	// Finance can create transfer: 202 Accepted
	reqTransfer := httptest.NewRequest(http.MethodPost, "/api/v1/transfers", nil)
	reqTransfer.Header.Set("Authorization", "Bearer "+financeToken)
	wTransfer := httptest.NewRecorder()
	r.ServeHTTP(wTransfer, reqTransfer)
	if wTransfer.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for finance creating transfer, got: %d", wTransfer.Code)
	}

	// Finance CANNOT reverse transfer (only admin can reverse): 403 Forbidden
	reqRev := httptest.NewRequest(http.MethodPost, "/api/v1/transfers/123/reverse", nil)
	reqRev.Header.Set("Authorization", "Bearer "+financeToken)
	wRev := httptest.NewRecorder()
	r.ServeHTTP(wRev, reqRev)

	if wRev.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for finance reversing transfer, got: %d", wRev.Code)
	}
}

func TestNegativeAuthorization_APIKeyScopeEnforcement(t *testing.T) {
	jwtSecret := "test-secret-key-for-authz-tests"
	r := setupMockAuthRouter(jwtSecret)

	// API key with only 'wallets:read' scope tries to create a transfer ('transfers:write')
	req := httptest.NewRequest(http.MethodPost, "/api/v1/transfers", nil)
	req.Header.Set("X-API-Key", "key_wallets_read_only")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for API key without transfers:write scope, got: %d", w.Code)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["error"] != "api key lacks scope: transfers:write" {
		t.Fatalf("unexpected error response message: %v", resp["error"])
	}

	// API key CAN read wallet ('wallets:read')
	reqRead := httptest.NewRequest(http.MethodGet, "/api/v1/wallets/w1", nil)
	reqRead.Header.Set("X-API-Key", "key_wallets_read_only")
	wRead := httptest.NewRecorder()
	r.ServeHTTP(wRead, reqRead)

	if wRead.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for API key with wallets:read scope, got: %d", wRead.Code)
	}
}
