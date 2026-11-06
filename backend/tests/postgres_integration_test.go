package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/distributed-wallet/platform/pkg/api"
	"github.com/distributed-wallet/platform/pkg/auth"
	"github.com/distributed-wallet/platform/pkg/database"
	"github.com/distributed-wallet/platform/pkg/inbox"
	"github.com/distributed-wallet/platform/pkg/ratelimit"
	"github.com/distributed-wallet/platform/pkg/reconciliation"
	riskservice "github.com/distributed-wallet/platform/pkg/risk"
	"github.com/distributed-wallet/platform/pkg/saga"
	"github.com/distributed-wallet/platform/pkg/telemetry"
	"github.com/distributed-wallet/platform/pkg/webhooks"
	riskv1 "github.com/distributed-wallet/platform/proto/risk/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const integrationJWTSecret = "integration-test-secret-with-at-least-32-bytes"

var integrationMetricsOnce sync.Once
var integrationMetrics *telemetry.Metrics

type postgresService struct {
	pool              *pgxpool.Pool
	router            http.Handler
	orgID             uuid.UUID
	recipientOrgID    uuid.UUID
	userID            uuid.UUID
	sourceID          uuid.UUID
	destID            uuid.UUID
	recipientID       uuid.UUID
	recipientWalletID uuid.UUID
	recipientEmail    string
	token             string
}

type approvingRiskClient struct{}

func (approvingRiskClient) EvaluateTransferRisk(context.Context, *riskv1.EvaluateRiskRequest, ...grpc.CallOption) (*riskv1.EvaluateRiskResponse, error) {
	return &riskv1.EvaluateRiskResponse{Decision: riskv1.RiskDecision_RISK_DECISION_APPROVED, Reason: "integration test approval"}, nil
}

type failOnceRiskClient struct {
	calls atomic.Int32
}

func (client *failOnceRiskClient) EvaluateTransferRisk(context.Context, *riskv1.EvaluateRiskRequest, ...grpc.CallOption) (*riskv1.EvaluateRiskResponse, error) {
	if client.calls.Add(1) == 1 {
		return nil, fmt.Errorf("simulated transient risk outage")
	}
	return &riskv1.EvaluateRiskResponse{
		Decision: riskv1.RiskDecision_RISK_DECISION_APPROVED,
		Reason:   "recovered risk service",
	}, nil
}

type rejectingRiskClient struct{}

func (rejectingRiskClient) EvaluateTransferRisk(context.Context, *riskv1.EvaluateRiskRequest, ...grpc.CallOption) (*riskv1.EvaluateRiskResponse, error) {
	return &riskv1.EvaluateRiskResponse{Decision: riskv1.RiskDecision_RISK_DECISION_REJECTED, Reason: "integration test rejection"}, nil
}

func startIntegrationRiskService(t *testing.T) (*riskservice.Server, riskv1.RiskServiceClient) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for integration risk service: %v", err)
	}
	server := riskservice.NewRiskServer(zap.NewNop())
	grpcServer := grpc.NewServer()
	riskv1.RegisterRiskServiceServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("integration risk service stopped: %v", err)
		}
	}()
	conn, err := grpc.DialContext(
		context.Background(),
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		t.Fatalf("connect to integration risk service: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		grpcServer.Stop()
	})
	return server, riskv1.NewRiskServiceClient(conn)
}

func newPostgresService(t *testing.T) *postgresService {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL-backed service integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adminPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to integration PostgreSQL: %v", err)
	}
	schema := "wallet_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		adminPool.Close()
		t.Fatalf("create isolated integration schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		adminPool.Close()
		t.Fatalf("parse integration database configuration: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	config.MaxConns = 50
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		adminPool.Close()
		t.Fatalf("connect using isolated integration schema: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		adminPool.Close()
	})

	migrationPath := filepath.Join("..", "migrations", "000001_init_schema.sql")
	if err := database.RunMigrations(ctx, pool, migrationPath); err != nil {
		t.Fatalf("apply schema to integration database: %v", err)
	}

	service := &postgresService{
		pool:              pool,
		orgID:             uuid.New(),
		recipientOrgID:    uuid.New(),
		userID:            uuid.New(),
		sourceID:          uuid.New(),
		destID:            uuid.New(),
		recipientID:       uuid.New(),
		recipientWalletID: uuid.New(),
		recipientEmail:    uuid.NewString() + "@example.test",
	}
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, name) VALUES ($1, $2)`,
		service.orgID, "integration-"+service.orgID.String()); err != nil {
		t.Fatalf("create integration organization: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, name) VALUES ($1, $2)`,
		service.recipientOrgID, "integration-recipient-"+service.recipientOrgID.String()); err != nil {
		t.Fatalf("create integration recipient organization: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO users (id, organization_id, email, password_hash, role)
		VALUES ($1, $2, $3, 'test-hash', 'admin')
	`, service.userID, service.orgID, service.userID.String()+"@example.test"); err != nil {
		t.Fatalf("create integration user: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO users (id, organization_id, email, password_hash, role)
		VALUES ($1, $2, $3, 'test-hash', 'viewer')
	`, service.recipientID, service.recipientOrgID, service.recipientEmail); err != nil {
		t.Fatalf("create integration recipient: %v", err)
	}
	if err := seedWallets(ctx, pool, service.orgID, service.userID, service.sourceID, service.destID, 2_000_000); err != nil {
		t.Fatalf("seed integration wallets: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO wallets (id, organization_id, owner_user_id, currency, account_type)
		VALUES ($1, $2, $3, 'USD', 'customer')
	`, service.recipientWalletID, service.recipientOrgID, service.recipientID); err != nil {
		t.Fatalf("seed integration recipient wallet: %v", err)
	}
	service.token, _, _, err = auth.GenerateTokenPair(
		integrationJWTSecret, service.userID, service.orgID, "admin", nil,
	)
	if err != nil {
		t.Fatalf("generate integration access token: %v", err)
	}

	integrationMetricsOnce.Do(func() {
		integrationMetrics = telemetry.InitMetrics("wallet_integration_test")
	})
	service.router = api.NewServer(
		pool,
		ratelimit.NewLimiter(nil, false),
		integrationMetrics,
		zap.NewNop(),
		integrationJWTSecret,
		bytes.Repeat([]byte{7}, 32),
	).Router()
	return service
}

func seedWallets(ctx context.Context, pool *pgxpool.Pool, orgID, ownerID, sourceID, destID uuid.UUID, initialBalance int64) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, id := range []uuid.UUID{sourceID, destID} {
		if _, err := tx.Exec(ctx, `
			INSERT INTO wallets (id, organization_id, owner_user_id, currency, account_type)
			VALUES ($1, $2, $3, 'USD', 'customer')
		`, id, orgID, ownerID); err != nil {
			return err
		}
	}
	fundingID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO wallets (id, organization_id, currency, account_type, balance)
		VALUES ($1, $2, 'USD', 'external_funding', 0)
	`, fundingID, orgID); err != nil {
		return err
	}
	journalID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (id, organization_id, reference_type, currency, description)
		VALUES ($1, $2, 'deposit', 'USD', 'integration opening balance')
	`, journalID, orgID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (journal_entry_id, wallet_id, direction, amount)
		VALUES ($1, $2, 'DR', $3), ($1, $4, 'CR', $3)
	`, journalID, fundingID, initialBalance, sourceID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = balance - $1 WHERE id = $2`, initialBalance, fundingID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = balance + $1 WHERE id = $2`, initialBalance, sourceID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func transferRequest(t *testing.T, service *postgresService, key string, amount int64) *httptest.ResponseRecorder {
	t.Helper()
	return transferRequestWithPayload(t, service, key, map[string]any{
		"source_wallet_id":      service.sourceID,
		"destination_wallet_id": service.destID,
		"currency":              "USD",
		"amount":                amount,
	})
}

func transferRequestWithPayload(t *testing.T, service *postgresService, key string, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/transfers", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+service.token)
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	service.router.ServeHTTP(response, req)
	return response
}

func TestPostgresService_UserToUserTransferByEmail(t *testing.T) {
	service := newPostgresService(t)
	response := transferRequestWithPayload(t, service, "user-transfer-email", map[string]any{
		"source_wallet_id": service.sourceID,
		"recipient_email":  service.recipientEmail,
		"currency":         "USD",
		"amount":           500,
	})
	if response.Code != http.StatusAccepted {
		t.Fatalf("email recipient transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}

	var transferID, destinationID uuid.UUID
	if err := service.pool.QueryRow(context.Background(), `
		SELECT id, destination_wallet_id FROM transfers WHERE idempotency_key = $1
	`, "user-transfer-email").Scan(&transferID, &destinationID); err != nil {
		t.Fatal(err)
	}
	if destinationID != service.recipientWalletID {
		t.Fatalf("transfer destination = %s, want recipient wallet %s", destinationID, service.recipientWalletID)
	}

	recipientToken, _, _, err := auth.GenerateTokenPair(
		integrationJWTSecret, service.recipientID, service.recipientOrgID, "viewer", nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	unrelatedResponse := transferRequest(t, service, "unrelated-transfer-visibility", 700)
	if unrelatedResponse.Code != http.StatusAccepted {
		t.Fatalf("unrelated transfer returned HTTP %d: %s", unrelatedResponse.Code, unrelatedResponse.Body.String())
	}
	var unrelatedTransferID uuid.UUID
	if err := service.pool.QueryRow(context.Background(), `
		SELECT id FROM transfers WHERE idempotency_key = $1
	`, "unrelated-transfer-visibility").Scan(&unrelatedTransferID); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/transfers", nil)
	req.Header.Set("Authorization", "Bearer "+recipientToken)
	history := httptest.NewRecorder()
	service.router.ServeHTTP(history, req)
	if history.Code != http.StatusOK {
		t.Fatalf("recipient transfer history returned HTTP %d: %s", history.Code, history.Body.String())
	}
	var result struct {
		Transfers []struct {
			ID uuid.UUID `json:"id"`
		} `json:"transfers"`
	}
	if err := json.Unmarshal(history.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Transfers) != 1 || result.Transfers[0].ID != transferID {
		t.Fatalf("recipient history = %+v, want transfer %s", result.Transfers, transferID)
	}

	detailRequest := httptest.NewRequest(http.MethodGet,
		"/api/v1/transfers/"+transferID.String(), nil)
	detailRequest.Header.Set("Authorization", "Bearer "+recipientToken)
	detailResponse := httptest.NewRecorder()
	service.router.ServeHTTP(detailResponse, detailRequest)
	if detailResponse.Code != http.StatusOK {
		t.Fatalf("recipient transfer detail returned HTTP %d: %s", detailResponse.Code, detailResponse.Body.String())
	}
	var detail struct {
		SenderAccount struct {
			WalletID uuid.UUID `json:"wallet_id"`
		} `json:"sender_account"`
		ReceiverAccount struct {
			WalletID uuid.UUID `json:"wallet_id"`
		} `json:"receiver_account"`
		LedgerEntries []json.RawMessage `json:"ledger_entries"`
	}
	if err := json.Unmarshal(detailResponse.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	if detail.SenderAccount.WalletID != service.sourceID ||
		detail.ReceiverAccount.WalletID != service.recipientWalletID {
		t.Fatalf("recipient detail returned incorrect account identities: %+v", detail)
	}

	unrelatedRequest := httptest.NewRequest(http.MethodGet,
		"/api/v1/transfers/"+unrelatedTransferID.String(), nil)
	unrelatedRequest.Header.Set("Authorization", "Bearer "+recipientToken)
	unrelatedDetail := httptest.NewRecorder()
	service.router.ServeHTTP(unrelatedDetail, unrelatedRequest)
	if unrelatedDetail.Code != http.StatusNotFound {
		t.Fatalf("recipient access to an unrelated transfer returned HTTP %d, want 404",
			unrelatedDetail.Code)
	}

	var held int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT hold_balance FROM wallets WHERE id = $1
	`, service.sourceID).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 1_220 {
		t.Fatalf("source hold = %d, want 1,220 across the authorized and unrelated transfers", held)
	}
}

func TestPostgresService_RegistrationCreatesOwnedWalletForViewer(t *testing.T) {
	service := newPostgresService(t)
	email := uuid.NewString() + "@example.test"
	body, err := json.Marshal(map[string]string{
		"organization_name": "integration-" + service.orgID.String(),
		"full_name":         "Viewer Person",
		"email":             email,
		"password":          "correct-horse",
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	service.router.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("viewer registration returned HTTP %d: %s", response.Code, response.Body.String())
	}

	var registration struct {
		UserID   uuid.UUID `json:"user_id"`
		WalletID uuid.UUID `json:"wallet_id"`
		FullName string    `json:"full_name"`
		Role     string    `json:"role"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &registration); err != nil {
		t.Fatal(err)
	}
	if registration.Role != "viewer" {
		t.Fatalf("registered role is %q, want viewer", registration.Role)
	}
	if registration.UserID == uuid.Nil || registration.WalletID == uuid.Nil {
		t.Fatalf("registration omitted user or default wallet ID: %+v", registration)
	}
	if registration.FullName != "Viewer Person" {
		t.Fatalf("registration returned name %q, want Viewer Person", registration.FullName)
	}
	var storedName string
	if err := service.pool.QueryRow(context.Background(), `SELECT full_name FROM users WHERE id = $1`, registration.UserID).Scan(&storedName); err != nil {
		t.Fatal(err)
	}
	if storedName != "Viewer Person" {
		t.Fatalf("stored full name is %q, want Viewer Person", storedName)
	}

	var walletOwner, walletOrg uuid.UUID
	var currency, accountType, status string
	var balance, holdBalance int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT owner_user_id, organization_id, currency, account_type, status, balance, hold_balance
		FROM wallets WHERE id = $1
	`, registration.WalletID).Scan(
		&walletOwner, &walletOrg, &currency, &accountType, &status, &balance, &holdBalance,
	); err != nil {
		t.Fatal(err)
	}
	if walletOwner != registration.UserID || walletOrg != service.orgID ||
		currency != "USD" || accountType != "customer" || status != "active" ||
		balance != 0 || holdBalance != 0 {
		t.Fatalf("registered default wallet has unexpected values: owner=%s org=%s currency=%s type=%s status=%s balance=%d hold=%d",
			walletOwner, walletOrg, currency, accountType, status, balance, holdBalance)
	}

	viewerToken, _, _, err := auth.GenerateTokenPair(
		integrationJWTSecret, registration.UserID, walletOrg, registration.Role, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	walletRequest := httptest.NewRequest(http.MethodGet, "/api/v1/wallets", nil)
	walletRequest.Header.Set("Authorization", "Bearer "+viewerToken)
	walletResponse := httptest.NewRecorder()
	service.router.ServeHTTP(walletResponse, walletRequest)
	if walletResponse.Code != http.StatusOK {
		t.Fatalf("new viewer wallet list returned HTTP %d: %s", walletResponse.Code, walletResponse.Body.String())
	}
	var walletList struct {
		Wallets []struct {
			ID uuid.UUID `json:"id"`
		} `json:"wallets"`
	}
	if err := json.Unmarshal(walletResponse.Body.Bytes(), &walletList); err != nil {
		t.Fatal(err)
	}
	if len(walletList.Wallets) != 1 || walletList.Wallets[0].ID != registration.WalletID {
		t.Fatalf("new viewer sees wallets %+v, want only their default wallet %s",
			walletList.Wallets, registration.WalletID)
	}
}

func TestPostgresService_AdminCanDeactivateAndReactivateOrganizationUsers(t *testing.T) {
	service := newPostgresService(t)
	email := uuid.NewString() + "@example.test"
	password := "correct-horse"
	registerBody, err := json.Marshal(map[string]string{
		"organization_name": "integration-" + service.orgID.String(),
		"email":             email,
		"password":          password,
	})
	if err != nil {
		t.Fatal(err)
	}

	registerRequest := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", bytes.NewReader(registerBody))
	registerRequest.Header.Set("Content-Type", "application/json")
	registerResponse := httptest.NewRecorder()
	service.router.ServeHTTP(registerResponse, registerRequest)
	if registerResponse.Code != http.StatusCreated {
		t.Fatalf("register viewer returned HTTP %d: %s", registerResponse.Code, registerResponse.Body.String())
	}
	var registered struct {
		UserID   uuid.UUID `json:"user_id"`
		WalletID uuid.UUID `json:"wallet_id"`
	}
	if err := json.Unmarshal(registerResponse.Body.Bytes(), &registered); err != nil {
		t.Fatal(err)
	}

	login := func() *httptest.ResponseRecorder {
		body, err := json.Marshal(map[string]string{"email": email, "password": password})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		service.router.ServeHTTP(response, request)
		return response
	}
	loginResponse := login()
	if loginResponse.Code != http.StatusOK {
		t.Fatalf("viewer login returned HTTP %d: %s", loginResponse.Code, loginResponse.Body.String())
	}
	var session struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(loginResponse.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}

	adminRequest := func(method, path string, body []byte) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, path, bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+service.token)
		if len(body) > 0 {
			request.Header.Set("Content-Type", "application/json")
		}
		response := httptest.NewRecorder()
		service.router.ServeHTTP(response, request)
		return response
	}

	usersResponse := adminRequest(http.MethodGet, "/api/v1/users", nil)
	if usersResponse.Code != http.StatusOK {
		t.Fatalf("admin user list returned HTTP %d: %s", usersResponse.Code, usersResponse.Body.String())
	}
	var usersPayload struct {
		Users []struct {
			ID       uuid.UUID `json:"id"`
			IsActive bool      `json:"is_active"`
		} `json:"users"`
	}
	if err := json.Unmarshal(usersResponse.Body.Bytes(), &usersPayload); err != nil {
		t.Fatal(err)
	}
	if len(usersPayload.Users) != 2 {
		t.Fatalf("admin user list returned %d accounts, want exactly the two organization users", len(usersPayload.Users))
	}

	viewerRequest := httptest.NewRequest(http.MethodGet, "/api/v1/users", nil)
	viewerRequest.Header.Set("Authorization", "Bearer "+session.AccessToken)
	viewerResponse := httptest.NewRecorder()
	service.router.ServeHTTP(viewerResponse, viewerRequest)
	if viewerResponse.Code != http.StatusForbidden {
		t.Fatalf("viewer user list returned HTTP %d, want 403", viewerResponse.Code)
	}

	deactivateBody, _ := json.Marshal(map[string]bool{"active": false})
	deactivated := adminRequest(http.MethodPatch,
		"/api/v1/users/"+registered.UserID.String()+"/status", deactivateBody)
	if deactivated.Code != http.StatusOK {
		t.Fatalf("admin deactivate returned HTTP %d: %s", deactivated.Code, deactivated.Body.String())
	}

	existingAccess := httptest.NewRequest(http.MethodGet, "/api/v1/wallets", nil)
	existingAccess.Header.Set("Authorization", "Bearer "+session.AccessToken)
	blockedResponse := httptest.NewRecorder()
	service.router.ServeHTTP(blockedResponse, existingAccess)
	if blockedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("deactivated user's existing access token returned HTTP %d, want 401", blockedResponse.Code)
	}

	refreshBody, _ := json.Marshal(map[string]string{"refresh_token": session.RefreshToken})
	refreshResponse := adminRequest(http.MethodPost, "/api/v1/auth/refresh", refreshBody)
	if refreshResponse.Code != http.StatusUnauthorized {
		t.Fatalf("deactivated user's refresh token returned HTTP %d, want 401", refreshResponse.Code)
	}
	if blockedLogin := login(); blockedLogin.Code != http.StatusUnauthorized {
		t.Fatalf("deactivated user login returned HTTP %d, want 401", blockedLogin.Code)
	}

	reactivated := adminRequest(http.MethodPatch,
		"/api/v1/users/"+registered.UserID.String()+"/status", []byte(`{"active":true}`))
	if reactivated.Code != http.StatusOK {
		t.Fatalf("admin reactivate returned HTTP %d: %s", reactivated.Code, reactivated.Body.String())
	}
	if restoredLogin := login(); restoredLogin.Code != http.StatusOK {
		t.Fatalf("reactivated user login returned HTTP %d: %s", restoredLogin.Code, restoredLogin.Body.String())
	}
	var walletExists bool
	if err := service.pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM wallets
			WHERE id = $1 AND owner_user_id = $2 AND status = 'active'
		)
	`, registered.WalletID, registered.UserID).Scan(&walletExists); err != nil {
		t.Fatal(err)
	}
	if !walletExists {
		t.Fatal("account management did not preserve the viewer's wallet")
	}
}

func TestPostgresService_CompanyProfileIsOrganizationScoped(t *testing.T) {
	service := newPostgresService(t)
	ctx := context.Background()
	memberID := uuid.New()
	if _, err := service.pool.Exec(ctx, `
		UPDATE users SET full_name = 'Company Admin' WHERE id = $1 AND organization_id = $2
	`, service.userID, service.orgID); err != nil {
		t.Fatalf("set integration admin name: %v", err)
	}
	if _, err := service.pool.Exec(ctx, `
		INSERT INTO users (id, organization_id, full_name, email, password_hash, role)
		VALUES ($1, $2, 'Company Member', $3, 'test-hash', 'viewer')
	`, memberID, service.orgID, memberID.String()+"@example.test"); err != nil {
		t.Fatalf("create organization member: %v", err)
	}

	requestProfile := func(token, orgID string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, "/api/v1/organizations/"+orgID+"/profile?limit=50", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		response := httptest.NewRecorder()
		service.router.ServeHTTP(response, request)
		return response
	}

	response := requestProfile(service.token, service.orgID.String())
	if response.Code != http.StatusOK {
		t.Fatalf("admin profile returned HTTP %d: %s", response.Code, response.Body.String())
	}
	var profile struct {
		Organization struct {
			ID   uuid.UUID `json:"id"`
			Name string    `json:"name"`
		} `json:"organization"`
		Members []struct {
			ID    uuid.UUID `json:"id"`
			Name  string    `json:"name"`
			Email string    `json:"email"`
			Role  string    `json:"role"`
		} `json:"members"`
		MemberCount int64 `json:"member_count"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &profile); err != nil {
		t.Fatalf("decode company profile: %v", err)
	}
	if profile.Organization.ID != service.orgID || profile.Organization.Name == "" {
		t.Fatalf("profile returned wrong organization: %+v", profile.Organization)
	}
	if profile.MemberCount != 2 || len(profile.Members) != 2 {
		t.Fatalf("profile returned %d members (count %d), want only two same-organization users", len(profile.Members), profile.MemberCount)
	}
	seen := map[uuid.UUID]bool{}
	for _, member := range profile.Members {
		seen[member.ID] = true
		if member.Name == "" || member.Email == "" || member.Role == "" {
			t.Fatalf("member details are incomplete: %+v", member)
		}
	}
	if !seen[service.userID] || !seen[memberID] || seen[service.recipientID] {
		t.Fatalf("profile member IDs leaked or omitted organization accounts: %v", seen)
	}

	viewerToken, _, _, err := auth.GenerateTokenPair(
		integrationJWTSecret, memberID, service.orgID, "viewer", nil,
	)
	if err != nil {
		t.Fatalf("generate viewer profile token: %v", err)
	}
	viewerResponse := requestProfile(viewerToken, service.orgID.String())
	if viewerResponse.Code != http.StatusOK {
		t.Fatalf("viewer profile returned HTTP %d, want 200: %s", viewerResponse.Code, viewerResponse.Body.String())
	}

	crossOrganizationResponse := requestProfile(service.token, service.recipientOrgID.String())
	if crossOrganizationResponse.Code != http.StatusNotFound {
		t.Fatalf("cross-organization profile returned HTTP %d, want 404", crossOrganizationResponse.Code)
	}
}

func TestPostgresService_InternalAndUserTransfersCompleteThroughSaga(t *testing.T) {
	service := newPostgresService(t)
	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		approvingRiskClient{},
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	ctx := context.Background()

	cases := []struct {
		name     string
		key      string
		payload  map[string]any
		target   uuid.UUID
		journals int
		entries  int
	}{
		{
			name: "internal wallet transfer",
			key:  "saga-internal-transfer",
			payload: map[string]any{
				"source_wallet_id":      service.sourceID,
				"destination_wallet_id": service.destID,
				"currency":              "USD",
				"amount":                1_000,
			},
			target: service.destID, journals: 1, entries: 3,
		},
		{
			name: "user-to-user transfer",
			key:  "saga-user-transfer",
			payload: map[string]any{
				"source_wallet_id": service.sourceID,
				"recipient_email":  service.recipientEmail,
				"currency":         "USD",
				"amount":           2_000,
			},
			target: service.recipientWalletID, journals: 2, entries: 5,
		},
		{
			name: "user-to-user transfer by wallet ID",
			key:  "saga-user-wallet-id-transfer",
			payload: map[string]any{
				"source_wallet_id":      service.sourceID,
				"destination_wallet_id": service.recipientWalletID,
				"currency":              "USD",
				"amount":                1_500,
			},
			target: service.recipientWalletID, journals: 2, entries: 5,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			response := transferRequestWithPayload(t, service, testCase.key, testCase.payload)
			if response.Code != http.StatusAccepted {
				t.Fatalf("transfer returned HTTP %d: %s", response.Code, response.Body.String())
			}
			var accepted struct {
				TransferID uuid.UUID `json:"transfer_id"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
				t.Fatal(err)
			}

			if err := orchestrator.ExecuteSaga(ctx, accepted.TransferID); err != nil {
				t.Fatalf("execute transfer saga: %v", err)
			}
			var status string
			var destinationID uuid.UUID
			if err := service.pool.QueryRow(ctx, `
				SELECT status, destination_wallet_id FROM transfers WHERE id = $1
			`, accepted.TransferID).Scan(&status, &destinationID); err != nil {
				t.Fatal(err)
			}
			if status != "COMPLETED" || destinationID != testCase.target {
				t.Fatalf("transfer result status=%s destination=%s, want COMPLETED to %s",
					status, destinationID, testCase.target)
			}
			detailRequest := httptest.NewRequest(http.MethodGet,
				"/api/v1/transfers/"+accepted.TransferID.String(), nil)
			detailRequest.Header.Set("Authorization", "Bearer "+service.token)
			detailResponse := httptest.NewRecorder()
			service.router.ServeHTTP(detailResponse, detailRequest)
			if detailResponse.Code != http.StatusOK {
				t.Fatalf("transfer detail returned HTTP %d: %s", detailResponse.Code, detailResponse.Body.String())
			}
			var detail struct {
				SenderAccount struct {
					WalletID uuid.UUID `json:"wallet_id"`
				} `json:"sender_account"`
				ReceiverAccount struct {
					WalletID uuid.UUID `json:"wallet_id"`
				} `json:"receiver_account"`
				LedgerEntries []struct {
					WalletID  uuid.UUID `json:"wallet_id"`
					Direction string    `json:"direction"`
					Amount    int64     `json:"amount"`
				} `json:"ledger_entries"`
			}
			if err := json.Unmarshal(detailResponse.Body.Bytes(), &detail); err != nil {
				t.Fatal(err)
			}
			if detail.SenderAccount.WalletID != service.sourceID ||
				detail.ReceiverAccount.WalletID != testCase.target {
				t.Fatalf("transaction accounts sender=%s receiver=%s, want %s and %s",
					detail.SenderAccount.WalletID, detail.ReceiverAccount.WalletID,
					service.sourceID, testCase.target)
			}
			foundDebit, foundCredit, foundReceiverCredit := false, false, false
			for _, entry := range detail.LedgerEntries {
				foundDebit = foundDebit || entry.WalletID == service.sourceID && entry.Direction == "DR"
				foundCredit = foundCredit || entry.Direction == "CR"
				foundReceiverCredit = foundReceiverCredit || entry.WalletID == testCase.target && entry.Direction == "CR"
			}
			if !foundDebit || !foundCredit || (testCase.target == service.destID && !foundReceiverCredit) {
				t.Fatalf("transaction detail ledger is missing expected debit/credit postings: %+v", detail.LedgerEntries)
			}
			var journalCount, ledgerCount int
			if err := service.pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM journal_entries WHERE reference_id = $1 AND reference_type = 'transfer'
			`, accepted.TransferID).Scan(&journalCount); err != nil {
				t.Fatal(err)
			}
			if err := service.pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM ledger_entries le
				JOIN journal_entries je ON je.id = le.journal_entry_id
				WHERE je.reference_id = $1 AND je.reference_type = 'transfer'
			`, accepted.TransferID).Scan(&ledgerCount); err != nil {
				t.Fatal(err)
			}
			if journalCount != testCase.journals || ledgerCount != testCase.entries {
				t.Fatalf("posted ledger journal count=%d entries=%d; want %d journals and %d entries",
					journalCount, ledgerCount, testCase.journals, testCase.entries)
			}
			var unbalancedJournals int
			if err := service.pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM (
					SELECT j.id
					FROM journal_entries j
					JOIN ledger_entries le ON le.journal_entry_id = j.id
					WHERE j.reference_id = $1 AND j.reference_type = 'transfer'
					GROUP BY j.id
					HAVING SUM(le.amount) FILTER (WHERE le.direction = 'DR')
					    <> SUM(le.amount) FILTER (WHERE le.direction = 'CR')
				) AS invalid
			`, accepted.TransferID).Scan(&unbalancedJournals); err != nil {
				t.Fatal(err)
			}
			if unbalancedJournals != 0 {
				t.Fatalf("transfer has %d unbalanced organization journals", unbalancedJournals)
			}
		})
	}
}

func TestPostgresService_CrossOrganizationTransferCanBeReversedAtomically(t *testing.T) {
	service := newPostgresService(t)
	response := transferRequestWithPayload(t, service, "cross-org-reversal", map[string]any{
		"source_wallet_id":      service.sourceID,
		"destination_wallet_id": service.recipientWalletID,
		"currency":              "USD",
		"amount":                2_500,
	})
	if response.Code != http.StatusAccepted {
		t.Fatalf("cross-organization transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}
	var accepted struct {
		TransferID uuid.UUID `json:"transfer_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		approvingRiskClient{},
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	if err := orchestrator.ExecuteSaga(context.Background(), accepted.TransferID); err != nil {
		t.Fatalf("complete cross-organization transfer: %v", err)
	}

	reverseRequest := httptest.NewRequest(http.MethodPost,
		"/api/v1/transfers/"+accepted.TransferID.String()+"/reverse", nil)
	reverseRequest.Header.Set("Authorization", "Bearer "+service.token)
	reverseRequest.Header.Set("Idempotency-Key", "cross-org-reversal-key")
	reverseResponse := httptest.NewRecorder()
	service.router.ServeHTTP(reverseResponse, reverseRequest)
	if reverseResponse.Code != http.StatusOK {
		t.Fatalf("cross-organization reversal returned HTTP %d: %s", reverseResponse.Code, reverseResponse.Body.String())
	}

	var transferStatus string
	if err := service.pool.QueryRow(context.Background(), `
		SELECT status FROM transfers WHERE id = $1
	`, accepted.TransferID).Scan(&transferStatus); err != nil {
		t.Fatal(err)
	}
	if transferStatus != "REVERSED" {
		t.Fatalf("transfer status = %s, want REVERSED", transferStatus)
	}
	var reversalJournalCount int
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM journal_entries WHERE reference_id = $1 AND reference_type = 'reversal'
	`, accepted.TransferID).Scan(&reversalJournalCount); err != nil {
		t.Fatal(err)
	}
	if reversalJournalCount != 2 {
		t.Fatalf("reversal posted %d journals, want one for each organization", reversalJournalCount)
	}
	var recipientBalance, sourceBalance int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT balance FROM wallets WHERE id = $1
	`, service.recipientWalletID).Scan(&recipientBalance); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(context.Background(), `
		SELECT balance FROM wallets WHERE id = $1
	`, service.sourceID).Scan(&sourceBalance); err != nil {
		t.Fatal(err)
	}
	if recipientBalance != 0 || sourceBalance != 2_000_000 {
		t.Fatalf("reversed balances source=%d recipient=%d, want source=2000000 recipient=0",
			sourceBalance, recipientBalance)
	}
	var clearingBalance int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COALESCE(SUM(balance), 0) FROM wallets WHERE account_type = 'inter_org_clearing'
	`).Scan(&clearingBalance); err != nil {
		t.Fatal(err)
	}
	if clearingBalance != 0 {
		t.Fatalf("reversal left clearing balance %d, want zero", clearingBalance)
	}
	discrepancies, err := reconciliation.NewAuditor(service.pool, zap.NewNop()).RunAudit(context.Background())
	if err != nil {
		t.Fatalf("reconcile cross-organization reversal: %v", err)
	}
	if len(discrepancies) != 0 {
		t.Fatalf("reconciliation found discrepancies after reversal: %+v", discrepancies)
	}
}

func TestPostgresService_TransientRiskFailureCanRetryWithoutDoublePosting(t *testing.T) {
	service := newPostgresService(t)
	riskServer, riskClient := startIntegrationRiskService(t)
	riskServer.SetSimulatedFailure(true)
	response := transferRequest(t, service, "saga-risk-retry", 700)
	if response.Code != http.StatusAccepted {
		t.Fatalf("transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}
	var accepted struct {
		TransferID uuid.UUID `json:"transfer_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}

	newOrchestrator := func(risk riskv1.RiskServiceClient) *saga.Orchestrator {
		return saga.NewOrchestrator(
			service.pool,
			nil,
			risk,
			webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
			zap.NewNop(),
			integrationMetrics,
		)
	}
	if err := newOrchestrator(riskClient).ExecuteSaga(context.Background(), accepted.TransferID); err == nil {
		t.Fatal("transient risk-service failure unexpectedly succeeded")
	}
	var status string
	var sourceHold int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT t.status, w.hold_balance
		FROM transfers t JOIN wallets w ON w.id = t.source_wallet_id
		WHERE t.id = $1
	`, accepted.TransferID).Scan(&status, &sourceHold); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || sourceHold != 710 {
		t.Fatalf("after transient failure status=%s source hold=%d, want PENDING and 710", status, sourceHold)
	}

	riskServer.SetSimulatedFailure(false)
	if err := newOrchestrator(riskClient).ExecuteSaga(context.Background(), accepted.TransferID); err != nil {
		t.Fatalf("retry saga after transient risk failure: %v", err)
	}
	var journalCount int
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM journal_entries WHERE reference_id = $1 AND reference_type = 'transfer'
	`, accepted.TransferID).Scan(&journalCount); err != nil {
		t.Fatal(err)
	}
	if journalCount != 1 {
		t.Fatalf("retry created %d transfer journals, want exactly one", journalCount)
	}
	if err := service.pool.QueryRow(context.Background(), `
		SELECT status FROM transfers WHERE id = $1
	`, accepted.TransferID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "COMPLETED" {
		t.Fatalf("retry left transfer in %s, want COMPLETED", status)
	}
}

func TestPostgresService_SlowRiskCheckTimesOutWithoutPosting(t *testing.T) {
	service := newPostgresService(t)
	riskServer, riskClient := startIntegrationRiskService(t)
	riskServer.SetSimulatedDelay(5 * time.Second)
	response := transferRequest(t, service, "saga-slow-risk-timeout", 700)
	if response.Code != http.StatusAccepted {
		t.Fatalf("transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}
	var accepted struct {
		TransferID uuid.UUID `json:"transfer_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}

	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		riskClient,
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	started := time.Now()
	if err := orchestrator.ExecuteSaga(context.Background(), accepted.TransferID); err == nil {
		t.Fatal("slow risk check unexpectedly approved before its deadline")
	}
	if elapsed := time.Since(started); elapsed < 3*time.Second || elapsed >= 8*time.Second {
		t.Fatalf("risk timeout took %s, want approximately 3 seconds", elapsed)
	}

	var status string
	var sourceBalance, sourceHold, transferJournalCount int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT t.status, source.balance, source.hold_balance,
		       (SELECT COUNT(*) FROM journal_entries j
		        WHERE j.reference_id = t.id AND j.reference_type = 'transfer')
		FROM transfers t
		JOIN wallets source ON source.id = t.source_wallet_id
		WHERE t.id = $1
	`, accepted.TransferID).Scan(&status, &sourceBalance, &sourceHold, &transferJournalCount); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || sourceBalance != 2_000_000 ||
		sourceHold != 710 || transferJournalCount != 0 {
		t.Fatalf("timed-out risk check changed financial state: status=%s balance=%d hold=%d journals=%d",
			status, sourceBalance, sourceHold, transferJournalCount)
	}
}

func TestPostgresService_RiskRejectionCompensatesTransferAtomically(t *testing.T) {
	service := newPostgresService(t)
	response := transferRequest(t, service, "saga-risk-rejection", 700)
	if response.Code != http.StatusAccepted {
		t.Fatalf("transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}
	var accepted struct {
		TransferID uuid.UUID `json:"transfer_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}

	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		rejectingRiskClient{},
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	if err := orchestrator.ExecuteSaga(context.Background(), accepted.TransferID); err != nil {
		t.Fatalf("execute rejecting saga: %v", err)
	}
	if err := orchestrator.ExecuteSaga(context.Background(), accepted.TransferID); err != nil {
		t.Fatalf("replay terminal saga: %v", err)
	}

	var status string
	var sourceBalance, sourceHold, destinationBalance int64
	var journalCount int
	if err := service.pool.QueryRow(context.Background(), `
		SELECT t.status, source.balance, source.hold_balance, destination.balance,
		       (SELECT COUNT(*) FROM journal_entries je
		        WHERE je.reference_id = t.id AND je.reference_type = 'transfer')
		FROM transfers t
		JOIN wallets source ON source.id = t.source_wallet_id
		JOIN wallets destination ON destination.id = t.destination_wallet_id
		WHERE t.id = $1
	`, accepted.TransferID).Scan(&status, &sourceBalance, &sourceHold, &destinationBalance, &journalCount); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" || sourceBalance != 2_000_000 || sourceHold != 0 ||
		destinationBalance != 0 || journalCount != 0 {
		t.Fatalf("compensation result status=%s source=%d hold=%d destination=%d journals=%d",
			status, sourceBalance, sourceHold, destinationBalance, journalCount)
	}
}

func TestPostgresMigration_AssignsLegacyWalletsOnlyForSingleUserOrganizations(t *testing.T) {
	ctx := context.Background()
	t.Run("single-user organization", func(t *testing.T) {
		service := newPostgresService(t)
		if _, err := service.pool.Exec(ctx, `DELETE FROM wallets WHERE id = $1`, service.recipientWalletID); err != nil {
			t.Fatal(err)
		}
		if _, err := service.pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, service.recipientID); err != nil {
			t.Fatal(err)
		}
		legacyWalletID := uuid.New()
		if _, err := service.pool.Exec(ctx, `
			INSERT INTO wallets (id, organization_id, currency, account_type)
			VALUES ($1, $2, 'USD', 'customer')
		`, legacyWalletID, service.orgID); err != nil {
			t.Fatal(err)
		}
		if err := database.RunMigrations(ctx, service.pool, filepath.Join("..", "migrations", "000001_init_schema.sql")); err != nil {
			t.Fatalf("rerun migration for legacy wallet: %v", err)
		}
		var ownerID uuid.UUID
		if err := service.pool.QueryRow(ctx, `SELECT owner_user_id FROM wallets WHERE id = $1`, legacyWalletID).Scan(&ownerID); err != nil {
			t.Fatal(err)
		}
		if ownerID != service.userID {
			t.Fatalf("legacy wallet owner = %s, want sole organization user %s", ownerID, service.userID)
		}
	})

	t.Run("multi-user organization", func(t *testing.T) {
		service := newPostgresService(t)
		if _, err := service.pool.Exec(ctx, `
			INSERT INTO users (id, organization_id, email, password_hash, role)
			VALUES ($1, $2, $3, 'test-hash', 'viewer')
		`, uuid.New(), service.orgID, uuid.NewString()+"@example.test"); err != nil {
			t.Fatal(err)
		}
		legacyWalletID := uuid.New()
		if _, err := service.pool.Exec(ctx, `
			INSERT INTO wallets (id, organization_id, currency, account_type)
			VALUES ($1, $2, 'USD', 'customer')
		`, legacyWalletID, service.orgID); err != nil {
			t.Fatal(err)
		}
		if err := database.RunMigrations(ctx, service.pool, filepath.Join("..", "migrations", "000001_init_schema.sql")); err != nil {
			t.Fatalf("rerun migration for multi-user legacy wallet: %v", err)
		}
		var remainsUnowned bool
		if err := service.pool.QueryRow(ctx, `
			SELECT owner_user_id IS NULL FROM wallets WHERE id = $1
		`, legacyWalletID).Scan(&remainsUnowned); err != nil {
			t.Fatal(err)
		}
		if !remainsUnowned {
			t.Fatal("migration assigned a legacy wallet when ownership was ambiguous")
		}
	})
}

func TestPostgresService_RejectsUnauthorizedAndUnavailableTransferWallets(t *testing.T) {
	t.Run("source owned by another user", func(t *testing.T) {
		service := newPostgresService(t)
		otherUserID := uuid.New()
		otherWalletID := uuid.New()
		if _, err := service.pool.Exec(context.Background(), `
			INSERT INTO users (id, organization_id, email, password_hash, role)
			VALUES ($1, $2, $3, 'test-hash', 'viewer')
		`, otherUserID, service.orgID, uuid.NewString()+"@example.test"); err != nil {
			t.Fatal(err)
		}
		if _, err := service.pool.Exec(context.Background(), `
			INSERT INTO wallets (id, organization_id, owner_user_id, currency, account_type)
			VALUES ($1, $2, $3, 'USD', 'customer')
		`, otherWalletID, service.orgID, otherUserID); err != nil {
			t.Fatal(err)
		}
		response := transferRequestWithPayload(t, service, "foreign-source", map[string]any{
			"source_wallet_id":      otherWalletID,
			"destination_wallet_id": service.destID,
			"currency":              "USD",
			"amount":                100,
		})
		if response.Code != http.StatusNotFound {
			t.Fatalf("foreign source returned HTTP %d, want 404: %s", response.Code, response.Body.String())
		}
	})

	t.Run("recipient wallet is suspended or closed", func(t *testing.T) {
		service := newPostgresService(t)
		for _, status := range []string{"suspended", "closed"} {
			if _, err := service.pool.Exec(context.Background(), `
				UPDATE wallets SET status = $1 WHERE id = $2
			`, status, service.recipientWalletID); err != nil {
				t.Fatal(err)
			}
			response := transferRequestWithPayload(t, service, "unavailable-"+status, map[string]any{
				"source_wallet_id":      service.sourceID,
				"destination_wallet_id": service.recipientWalletID,
				"currency":              "USD",
				"amount":                100,
			})
			if response.Code != http.StatusConflict {
				t.Fatalf("%s recipient returned HTTP %d, want 409: %s", status, response.Code, response.Body.String())
			}
		}
	})

	t.Run("insufficient available balance includes transfer fee", func(t *testing.T) {
		service := newPostgresService(t)
		response := transferRequest(t, service, "insufficient-transfer-funds", 2_000_000)
		if response.Code != http.StatusUnprocessableEntity {
			t.Fatalf("insufficient transfer returned HTTP %d, want 422: %s", response.Code, response.Body.String())
		}
		var held int64
		var transfers int
		if err := service.pool.QueryRow(context.Background(), `
			SELECT hold_balance FROM wallets WHERE id = $1
		`, service.sourceID).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if err := service.pool.QueryRow(context.Background(), `
			SELECT COUNT(*) FROM transfers WHERE organization_id = $1
		`, service.orgID).Scan(&transfers); err != nil {
			t.Fatal(err)
		}
		if held != 0 || transfers != 0 {
			t.Fatalf("rejected transfer left hold=%d transfers=%d; want zero", held, transfers)
		}
	})

	t.Run("recipient email with multiple matching wallets is rejected", func(t *testing.T) {
		service := newPostgresService(t)
		if _, err := service.pool.Exec(context.Background(), `
			INSERT INTO wallets (organization_id, owner_user_id, currency, account_type)
			VALUES ($1, $2, 'USD', 'customer')
		`, service.recipientOrgID, service.recipientID); err != nil {
			t.Fatal(err)
		}
		response := transferRequestWithPayload(t, service, "ambiguous-recipient", map[string]any{
			"source_wallet_id": service.sourceID,
			"recipient_email":  service.recipientEmail,
			"currency":         "USD",
			"amount":           100,
		})
		if response.Code != http.StatusConflict {
			t.Fatalf("ambiguous email recipient returned HTTP %d, want 409: %s", response.Code, response.Body.String())
		}
	})

	t.Run("cross-organization destination", func(t *testing.T) {
		service := newPostgresService(t)
		otherOrgID := uuid.New()
		otherUserID := uuid.New()
		otherWalletID := uuid.New()
		if _, err := service.pool.Exec(context.Background(), `INSERT INTO organizations (id, name) VALUES ($1, $2)`,
			otherOrgID, "other-"+otherOrgID.String()); err != nil {
			t.Fatal(err)
		}
		if _, err := service.pool.Exec(context.Background(), `
			INSERT INTO users (id, organization_id, email, password_hash, role)
			VALUES ($1, $2, $3, 'test-hash', 'viewer')
		`, otherUserID, otherOrgID, uuid.NewString()+"@example.test"); err != nil {
			t.Fatal(err)
		}
		if _, err := service.pool.Exec(context.Background(), `
			INSERT INTO wallets (id, organization_id, owner_user_id, currency, account_type)
			VALUES ($1, $2, $3, 'USD', 'customer')
		`, otherWalletID, otherOrgID, otherUserID); err != nil {
			t.Fatal(err)
		}
		response := transferRequestWithPayload(t, service, "cross-org-destination", map[string]any{
			"source_wallet_id":      service.sourceID,
			"destination_wallet_id": otherWalletID,
			"currency":              "USD",
			"amount":                100,
		})
		if response.Code != http.StatusAccepted {
			t.Fatalf("cross-organization destination returned HTTP %d, want 202: %s", response.Code, response.Body.String())
		}
		var destinationOrganizationID uuid.UUID
		if err := service.pool.QueryRow(context.Background(), `
			SELECT destination_organization_id FROM transfers WHERE idempotency_key = $1
		`, "cross-org-destination").Scan(&destinationOrganizationID); err != nil {
			t.Fatal(err)
		}
		if destinationOrganizationID != otherOrgID {
			t.Fatalf("transfer destination organization = %s, want %s", destinationOrganizationID, otherOrgID)
		}
	})
}

func TestPostgresService_1000ConcurrentTransfers(t *testing.T) {
	service := newPostgresService(t)
	const concurrentRequests = 1000
	statuses := make(chan int, concurrentRequests)
	var wait sync.WaitGroup
	wait.Add(concurrentRequests)
	for i := 0; i < concurrentRequests; i++ {
		go func(index int) {
			defer wait.Done()
			response := transferRequest(t, service, fmt.Sprintf("stress-%d", index), 100)
			statuses <- response.Code
		}(i)
	}
	wait.Wait()
	close(statuses)
	for status := range statuses {
		if status != http.StatusAccepted {
			t.Errorf("transfer request returned HTTP %d, want %d", status, http.StatusAccepted)
		}
	}

	var transferCount int
	var heldAmount int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM transfers WHERE organization_id = $1
	`, service.orgID).Scan(&transferCount); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(context.Background(), `
		SELECT hold_balance FROM wallets WHERE id = $1
	`, service.sourceID).Scan(&heldAmount); err != nil {
		t.Fatal(err)
	}
	if transferCount != concurrentRequests {
		t.Fatalf("database contains %d transfers, want %d", transferCount, concurrentRequests)
	}
	if heldAmount != concurrentRequests*110 {
		t.Fatalf("source wallet holds %d, want %d", heldAmount, concurrentRequests*110)
	}

	rows, err := service.pool.Query(context.Background(), `
		SELECT id FROM transfers WHERE organization_id = $1
	`, service.orgID)
	if err != nil {
		t.Fatal(err)
	}
	transferIDs := make([]uuid.UUID, 0, concurrentRequests)
	for rows.Next() {
		var transferID uuid.UUID
		if err := rows.Scan(&transferID); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		transferIDs = append(transferIDs, transferID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	if len(transferIDs) != concurrentRequests {
		t.Fatalf("loaded %d transfers for saga completion, want %d", len(transferIDs), concurrentRequests)
	}

	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		approvingRiskClient{},
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	sagaErrors := make(chan error, len(transferIDs))
	var sagaWait sync.WaitGroup
	sagaWait.Add(len(transferIDs))
	for _, transferID := range transferIDs {
		go func(id uuid.UUID) {
			defer sagaWait.Done()
			sagaErrors <- orchestrator.ExecuteSaga(context.Background(), id)
		}(transferID)
	}
	sagaWait.Wait()
	close(sagaErrors)
	for err := range sagaErrors {
		if err != nil {
			t.Errorf("complete concurrent transfer saga: %v", err)
		}
	}

	var completed, incomplete int
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FILTER (WHERE status = 'COMPLETED'),
		       COUNT(*) FILTER (WHERE status <> 'COMPLETED')
		FROM transfers WHERE organization_id = $1
	`, service.orgID).Scan(&completed, &incomplete); err != nil {
		t.Fatal(err)
	}
	if completed != concurrentRequests || incomplete != 0 {
		t.Fatalf("transfer states completed=%d incomplete=%d; want %d completed and none incomplete",
			completed, incomplete, concurrentRequests)
	}

	var sourceBalance, sourceHold, destinationBalance, feeBalance int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT balance, hold_balance FROM wallets WHERE id = $1
	`, service.sourceID).Scan(&sourceBalance, &sourceHold); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(context.Background(), `
		SELECT balance FROM wallets WHERE id = $1
	`, service.destID).Scan(&destinationBalance); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(context.Background(), `
		SELECT balance FROM wallets
		WHERE organization_id = $1 AND account_type = 'fees' AND currency = 'USD'
	`, service.orgID).Scan(&feeBalance); err != nil {
		t.Fatal(err)
	}
	if sourceBalance != 1_890_000 || sourceHold != 0 ||
		destinationBalance != 100_000 || feeBalance != 10_000 {
		t.Fatalf("final balances source=%d hold=%d destination=%d fees=%d; want 1890000, 0, 100000, 10000",
			sourceBalance, sourceHold, destinationBalance, feeBalance)
	}

	var ledgerEntries, transferJournals int
	var debits, credits, totalWalletBalance int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(le.id), COUNT(DISTINCT j.id),
		       COALESCE(SUM(le.amount) FILTER (WHERE le.direction = 'DR'), 0),
		       COALESCE(SUM(le.amount) FILTER (WHERE le.direction = 'CR'), 0)
		FROM journal_entries j
		JOIN ledger_entries le ON le.journal_entry_id = j.id
		WHERE j.organization_id = $1 AND j.reference_type = 'transfer'
	`, service.orgID).Scan(&ledgerEntries, &transferJournals, &debits, &credits); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COALESCE(SUM(balance), 0) FROM wallets WHERE organization_id = $1
	`, service.orgID).Scan(&totalWalletBalance); err != nil {
		t.Fatal(err)
	}
	if ledgerEntries != concurrentRequests*3 || transferJournals != concurrentRequests ||
		debits != 110_000 || credits != 110_000 || totalWalletBalance != 0 {
		t.Fatalf("ledger entries=%d journals=%d debits=%d credits=%d wallet total=%d; want 3000, 1000, 110000, 110000, 0",
			ledgerEntries, transferJournals, debits, credits, totalWalletBalance)
	}
}

func TestPostgresService_ConcurrentIdempotencyAndPayloadScope(t *testing.T) {
	service := newPostgresService(t)
	const concurrentRequests = 50
	responses := make(chan *httptest.ResponseRecorder, concurrentRequests)
	var wait sync.WaitGroup
	wait.Add(concurrentRequests)
	for i := 0; i < concurrentRequests; i++ {
		go func() {
			defer wait.Done()
			responses <- transferRequest(t, service, "same-idempotency-key", 100)
		}()
	}
	wait.Wait()
	close(responses)

	var accepted *httptest.ResponseRecorder
	for response := range responses {
		if response.Code == http.StatusAccepted {
			accepted = response
			continue
		}
		if response.Code != http.StatusConflict {
			t.Errorf("concurrent duplicate returned HTTP %d, want 202 or 409", response.Code)
		}
	}
	if accepted == nil {
		t.Fatal("no concurrent request was accepted")
	}

	replay := transferRequest(t, service, "same-idempotency-key", 100)
	if replay.Code != http.StatusAccepted {
		t.Fatalf("completed duplicate returned HTTP %d, want 202", replay.Code)
	}
	var acceptedBody, replayBody map[string]any
	if err := json.Unmarshal(accepted.Body.Bytes(), &acceptedBody); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(replay.Body.Bytes(), &replayBody); err != nil {
		t.Fatal(err)
	}
	if acceptedBody["transfer_id"] != replayBody["transfer_id"] {
		t.Fatalf("duplicate did not replay the original transfer response")
	}
	var acceptedTransfer struct {
		TransferID uuid.UUID `json:"transfer_id"`
	}
	if err := json.Unmarshal(accepted.Body.Bytes(), &acceptedTransfer); err != nil {
		t.Fatal(err)
	}
	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		approvingRiskClient{},
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	if err := orchestrator.ExecuteSaga(context.Background(), acceptedTransfer.TransferID); err != nil {
		t.Fatalf("complete idempotent transfer: %v", err)
	}

	mismatch := transferRequest(t, service, "same-idempotency-key", 101)
	if mismatch.Code != http.StatusUnprocessableEntity {
		t.Fatalf("different payload with the same key returned HTTP %d, want 422", mismatch.Code)
	}
	var count int
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM transfers WHERE organization_id = $1
	`, service.orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("idempotent requests created %d transfers, want 1", count)
	}
	var ledgerCount int
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM ledger_entries le
		JOIN journal_entries j ON j.id = le.journal_entry_id
		WHERE j.reference_type = 'transfer'
		  AND j.reference_id IN (SELECT id FROM transfers WHERE organization_id = $1)
	`, service.orgID).Scan(&ledgerCount); err != nil {
		t.Fatal(err)
	}
	if ledgerCount != 3 {
		t.Fatalf("idempotent requests created %d transfer ledger entries, want exactly 3", ledgerCount)
	}
}

func TestPostgresService_NATSRedeliveryRetriesWithoutDuplicatePosting(t *testing.T) {
	natsURL := os.Getenv("TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("set TEST_NATS_URL to run NATS saga redelivery integration test")
	}
	service := newPostgresService(t)
	response := transferRequest(t, service, "nats-saga-redelivery", 700)
	if response.Code != http.StatusAccepted {
		t.Fatalf("transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}
	var accepted struct {
		TransferID uuid.UUID `json:"transfer_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(natsURL, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect to integration NATS: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("create integration JetStream context: %v", err)
	}
	streamName := "SAGA_REDELIVERY_" + strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", ""))
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{"transfer.*.*"},
		Storage:  nats.MemoryStorage,
	}); err != nil {
		t.Fatalf("create saga redelivery stream: %v", err)
	}
	defer func() { _ = js.DeleteStream(streamName) }()

	riskClient := &failOnceRiskClient{}
	orchestrator := saga.NewOrchestrator(
		service.pool,
		js,
		riskClient,
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := orchestrator.StartConsumer(ctx); err != nil {
		t.Fatalf("start saga consumer: %v", err)
	}

	messageID := uuid.NewString()
	message := &nats.Msg{
		Subject: "transfer.created." + service.sourceID.String(),
		Data:    []byte(strconv.Quote(accepted.TransferID.String())),
		Header:  nats.Header{},
	}
	message.Header.Set("Nats-Msg-Id", messageID)
	if _, err := js.PublishMsg(message); err != nil {
		t.Fatalf("publish saga event: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		if err := service.pool.QueryRow(context.Background(), `
			SELECT status FROM transfers WHERE id = $1
		`, accepted.TransferID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == "COMPLETED" {
			var inboxCount, journalCount, ledgerCount int
			if err := service.pool.QueryRow(context.Background(), `
				SELECT COUNT(*) FROM inbox_records
				WHERE message_id IN ($1, $2) AND consumer_group = 'saga_orchestrator'
			`, messageID, string(message.Data)).Scan(&inboxCount); err != nil {
				t.Fatal(err)
			}
			if err := service.pool.QueryRow(context.Background(), `
				SELECT COUNT(*) FROM journal_entries
				WHERE reference_id = $1 AND reference_type = 'transfer'
			`, accepted.TransferID).Scan(&journalCount); err != nil {
				t.Fatal(err)
			}
			if err := service.pool.QueryRow(context.Background(), `
				SELECT COUNT(*) FROM ledger_entries le
				JOIN journal_entries j ON j.id = le.journal_entry_id
				WHERE j.reference_id = $1 AND j.reference_type = 'transfer'
			`, accepted.TransferID).Scan(&ledgerCount); err != nil {
				t.Fatal(err)
			}
			if riskClient.calls.Load() != 2 || inboxCount != 1 ||
				journalCount != 1 || ledgerCount != 3 {
				t.Fatalf("redelivery calls=%d inbox=%d journals=%d ledger entries=%d; want 2, 1, 1, 3",
					riskClient.calls.Load(), inboxCount, journalCount, ledgerCount)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("NATS redelivery did not complete the saga within 15 seconds")
}

func TestPostgresService_SagaRecoveryResumesPersistedStep(t *testing.T) {
	service := newPostgresService(t)
	response := transferRequest(t, service, "saga-crash-recovery", 700)
	if response.Code != http.StatusAccepted {
		t.Fatalf("transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}
	var accepted struct {
		TransferID uuid.UUID `json:"transfer_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}

	if _, err := service.pool.Exec(context.Background(), `
		UPDATE transfers
		SET status = 'PROCESSING', saga_step = 'POST_LEDGER',
		    fencing_token = fencing_token + 1,
		    updated_at = NOW() - INTERVAL '31 seconds'
		WHERE id = $1
	`, accepted.TransferID); err != nil {
		t.Fatal(err)
	}
	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		approvingRiskClient{},
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	orchestrator.ResumeIncompleteSagas(context.Background())

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		if err := service.pool.QueryRow(context.Background(), `
			SELECT status FROM transfers WHERE id = $1
		`, accepted.TransferID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == "COMPLETED" {
			var sourceBalance, sourceHold, destinationBalance, journals int64
			if err := service.pool.QueryRow(context.Background(), `
				SELECT source.balance, source.hold_balance, destination.balance,
				       (SELECT COUNT(*) FROM journal_entries j
				        WHERE j.reference_id = t.id AND j.reference_type = 'transfer')
				FROM transfers t
				JOIN wallets source ON source.id = t.source_wallet_id
				JOIN wallets destination ON destination.id = t.destination_wallet_id
				WHERE t.id = $1
			`, accepted.TransferID).Scan(&sourceBalance, &sourceHold, &destinationBalance, &journals); err != nil {
				t.Fatal(err)
			}
			if sourceBalance != 1_999_290 || sourceHold != 0 ||
				destinationBalance != 700 || journals != 1 {
				t.Fatalf("recovered transfer balances source=%d hold=%d destination=%d journals=%d",
					sourceBalance, sourceHold, destinationBalance, journals)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("persisted saga step was not resumed after orchestrator recovery")
}

func TestPostgresService_InboxDeduplicatesConcurrentSagaDeliveries(t *testing.T) {
	service := newPostgresService(t)
	response := transferRequest(t, service, "saga-duplicate-delivery", 700)
	if response.Code != http.StatusAccepted {
		t.Fatalf("transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}
	var accepted struct {
		TransferID uuid.UUID `json:"transfer_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}

	const deliveries = 20
	start := make(chan struct{})
	results := make(chan error, deliveries)
	var wait sync.WaitGroup
	wait.Add(deliveries)
	manager := inbox.NewInboxManager(service.pool)
	for range deliveries {
		go func() {
			defer wait.Done()
			<-start
			tx, err := service.pool.Begin(context.Background())
			if err != nil {
				results <- err
				return
			}
			err = manager.CheckAndRecordInTx(
				context.Background(), tx, accepted.TransferID.String(), "saga_orchestrator",
			)
			if err != nil {
				_ = tx.Rollback(context.Background())
				results <- err
				return
			}
			results <- tx.Commit(context.Background())
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	inserted := 0
	duplicates := 0
	for err := range results {
		if err == nil {
			inserted++
		} else if err == inbox.ErrDuplicateMessage {
			duplicates++
		} else {
			t.Fatalf("record duplicate inbox delivery: %v", err)
		}
	}
	if inserted != 1 || duplicates != deliveries-1 {
		t.Fatalf("inbox results inserted=%d duplicates=%d; want 1 and %d",
			inserted, duplicates, deliveries-1)
	}

	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		approvingRiskClient{},
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	for range deliveries {
		if err := orchestrator.ExecuteSaga(context.Background(), accepted.TransferID); err != nil {
			t.Fatalf("replay completed saga: %v", err)
		}
	}

	var status string
	var journals, entries int
	var sourceBalance, sourceHold, destinationBalance int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT t.status, source.balance, source.hold_balance, destination.balance,
		       (SELECT COUNT(*) FROM journal_entries j
		        WHERE j.reference_id = t.id AND j.reference_type = 'transfer'),
		       (SELECT COUNT(*) FROM ledger_entries le
		        JOIN journal_entries j ON j.id = le.journal_entry_id
		        WHERE j.reference_id = t.id AND j.reference_type = 'transfer')
		FROM transfers t
		JOIN wallets source ON source.id = t.source_wallet_id
		JOIN wallets destination ON destination.id = t.destination_wallet_id
		WHERE t.id = $1
	`, accepted.TransferID).Scan(
		&status, &sourceBalance, &sourceHold, &destinationBalance, &journals, &entries,
	); err != nil {
		t.Fatal(err)
	}
	if status != "COMPLETED" || sourceBalance != 1_999_290 ||
		sourceHold != 0 || destinationBalance != 700 || journals != 1 || entries != 3 {
		t.Fatalf("redelivered saga result status=%s source=%d hold=%d destination=%d journals=%d entries=%d",
			status, sourceBalance, sourceHold, destinationBalance, journals, entries)
	}
}

func TestPostgresService_RedisFailureFailsOpenWithoutChangingLedgerCorrectness(t *testing.T) {
	service := newPostgresService(t)
	redisClient := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		MaxRetries:  0,
		DialTimeout: 100 * time.Millisecond,
	})
	defer redisClient.Close()
	service.router = api.NewServer(
		service.pool,
		ratelimit.NewLimiter(redisClient, false),
		integrationMetrics,
		zap.NewNop(),
		integrationJWTSecret,
		bytes.Repeat([]byte{7}, 32),
	).Router()

	response := transferRequest(t, service, "redis-outage-transfer", 700)
	if response.Code != http.StatusAccepted {
		t.Fatalf("Redis outage caused transfer HTTP %d, want 202: %s", response.Code, response.Body.String())
	}
	var transferCount int
	var sourceHold int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM transfers WHERE organization_id = $1
	`, service.orgID).Scan(&transferCount); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(context.Background(), `
		SELECT hold_balance FROM wallets WHERE id = $1
	`, service.sourceID).Scan(&sourceHold); err != nil {
		t.Fatal(err)
	}
	if transferCount != 1 || sourceHold != 710 {
		t.Fatalf("Redis outage resulted in transfers=%d hold=%d; want 1 and 710",
			transferCount, sourceHold)
	}
}

func TestPostgresService_OppositeDirectionTransfersStayBalanced(t *testing.T) {
	service := newPostgresService(t)
	ctx := context.Background()

	fundingWalletID := uuid.New()
	if err := service.pool.QueryRow(ctx, `
		SELECT id FROM wallets
		WHERE organization_id = $1 AND currency = 'USD' AND account_type = 'external_funding'
	`, service.orgID).Scan(&fundingWalletID); err != nil {
		t.Fatal(err)
	}
	openingJournalID := uuid.New()
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (id, organization_id, reference_type, currency, description)
		VALUES ($1, $2, 'deposit', 'USD', 'fund opposing transfer test')
	`, openingJournalID, service.orgID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (journal_entry_id, wallet_id, direction, amount)
		VALUES ($1, $2, 'DR', 2000000), ($1, $3, 'CR', 2000000)
	`, openingJournalID, fundingWalletID, service.destID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = balance - 2000000 WHERE id = $1`, fundingWalletID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = balance + 2000000 WHERE id = $1`, service.destID); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	const transfersPerDirection = 100
	statuses := make(chan int, transfersPerDirection*2)
	var wait sync.WaitGroup
	wait.Add(transfersPerDirection * 2)
	for i := 0; i < transfersPerDirection; i++ {
		forwardKey := fmt.Sprintf("opposite-forward-%d", i)
		reverseKey := fmt.Sprintf("opposite-reverse-%d", i)
		go func(key string) {
			defer wait.Done()
			response := transferRequestWithPayload(t, service, key, map[string]any{
				"source_wallet_id": service.sourceID, "destination_wallet_id": service.destID,
				"currency": "USD", "amount": 100,
			})
			statuses <- response.Code
		}(forwardKey)
		go func(key string) {
			defer wait.Done()
			response := transferRequestWithPayload(t, service, key, map[string]any{
				"source_wallet_id": service.destID, "destination_wallet_id": service.sourceID,
				"currency": "USD", "amount": 100,
			})
			statuses <- response.Code
		}(reverseKey)
	}
	wait.Wait()
	close(statuses)
	for status := range statuses {
		if status != http.StatusAccepted {
			t.Errorf("opposite-direction transfer returned HTTP %d, want 202", status)
		}
	}

	rows, err := service.pool.Query(ctx, `SELECT id FROM transfers WHERE organization_id = $1`, service.orgID)
	if err != nil {
		t.Fatal(err)
	}
	transferIDs := make([]uuid.UUID, 0, transfersPerDirection*2)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		transferIDs = append(transferIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()

	orchestrator := saga.NewOrchestrator(
		service.pool,
		nil,
		approvingRiskClient{},
		webhooks.NewDispatcher(service.pool, zap.NewNop(), bytes.Repeat([]byte{9}, 32)),
		zap.NewNop(),
		integrationMetrics,
	)
	sagaErrors := make(chan error, len(transferIDs))
	var sagaWait sync.WaitGroup
	sagaWait.Add(len(transferIDs))
	for _, id := range transferIDs {
		go func(transferID uuid.UUID) {
			defer sagaWait.Done()
			sagaErrors <- orchestrator.ExecuteSaga(ctx, transferID)
		}(id)
	}
	sagaWait.Wait()
	close(sagaErrors)
	for err := range sagaErrors {
		if err != nil {
			t.Errorf("complete opposite-direction transfer saga: %v", err)
		}
	}

	var transferCount, completedCount int
	var debitTotal, creditTotal, sumBalance int64
	if err := service.pool.QueryRow(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE status = 'COMPLETED')
		FROM transfers WHERE organization_id = $1
	`, service.orgID).Scan(&transferCount, &completedCount); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(le.amount) FILTER (WHERE le.direction = 'DR'), 0),
		       COALESCE(SUM(le.amount) FILTER (WHERE le.direction = 'CR'), 0)
		FROM journal_entries j JOIN ledger_entries le ON le.journal_entry_id = j.id
		WHERE j.organization_id = $1 AND j.reference_type = 'transfer'
	`, service.orgID).Scan(&debitTotal, &creditTotal); err != nil {
		t.Fatal(err)
	}
	if err := service.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(balance), 0) FROM wallets WHERE organization_id = $1
	`, service.orgID).Scan(&sumBalance); err != nil {
		t.Fatal(err)
	}
	if transferCount != transfersPerDirection*2 || completedCount != transferCount ||
		debitTotal != creditTotal || sumBalance != 0 {
		t.Fatalf("opposing transfers count=%d completed=%d DR=%d CR=%d wallet total=%d",
			transferCount, completedCount, debitTotal, creditTotal, sumBalance)
	}
}

func TestPostgresService_ReconciliationReportsCorruptionWithoutRepair(t *testing.T) {
	service := newPostgresService(t)
	const corruptBalance = int64(123)
	tx, err := service.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
		UPDATE wallets SET balance = balance + $1 WHERE id = $2
	`, corruptBalance, service.sourceID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	discrepancies, err := reconciliation.NewAuditor(service.pool, zap.NewNop()).
		RunAuditForOrganization(context.Background(), service.orgID)
	if err != nil {
		t.Fatalf("run reconciliation: %v", err)
	}
	found := false
	for _, discrepancy := range discrepancies {
		if discrepancy.Type == "BALANCE_MISMATCH" && discrepancy.ID == service.sourceID.String() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("reconciliation did not report corrupted wallet %s: %+v", service.sourceID, discrepancies)
	}

	var storedBalance int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT balance FROM wallets WHERE id = $1
	`, service.sourceID).Scan(&storedBalance); err != nil {
		t.Fatal(err)
	}
	if storedBalance != 2_000_000+corruptBalance {
		t.Fatalf("reconciliation changed corrupted balance to %d; expected it to remain %d",
			storedBalance, 2_000_000+corruptBalance)
	}
	var runStatus string
	if err := service.pool.QueryRow(context.Background(), `
		SELECT status FROM reconciliation_runs
		WHERE organization_id = $1 ORDER BY started_at DESC LIMIT 1
	`, service.orgID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "DISCREPANCIES_FOUND" {
		t.Fatalf("reconciliation status is %q, want DISCREPANCIES_FOUND", runStatus)
	}
}

func TestPostgresService_CrossOrganizationAuthorizationAndLedgerGuard(t *testing.T) {
	service := newPostgresService(t)
	otherOrgID, otherWalletID := uuid.New(), uuid.New()
	if _, err := service.pool.Exec(context.Background(), `
		INSERT INTO organizations (id, name) VALUES ($1, $2)
	`, otherOrgID, "other-"+otherOrgID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.pool.Exec(context.Background(), `
		INSERT INTO wallets (id, organization_id, currency, account_type)
		VALUES ($1, $2, 'USD', 'customer')
	`, otherWalletID, otherOrgID); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/wallets/"+otherWalletID.String(), nil)
	req.Header.Set("Authorization", "Bearer "+service.token)
	response := httptest.NewRecorder()
	service.router.ServeHTTP(response, req)
	if response.Code != http.StatusNotFound {
		t.Fatalf("cross-organization wallet read returned HTTP %d, want 404", response.Code)
	}

	tx, err := service.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
		UPDATE wallets SET balance = balance + 1 WHERE id = $1
	`, service.sourceID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatalf("balance update should fail at deferred ledger invariant: %v", err)
	}
	if err := tx.Commit(context.Background()); err == nil {
		t.Fatal("unbalanced wallet balance update unexpectedly committed")
	}
}

func TestPostgresService_TenantScopedWalletAndTransferCollections(t *testing.T) {
	service := newPostgresService(t)
	response := transferRequest(t, service, "collection-list-test", 100)
	if response.Code != http.StatusAccepted {
		t.Fatalf("create transfer returned HTTP %d: %s", response.Code, response.Body.String())
	}

	walletRequest := httptest.NewRequest(http.MethodGet, "/api/v1/wallets", nil)
	walletRequest.Header.Set("Authorization", "Bearer "+service.token)
	walletResponse := httptest.NewRecorder()
	service.router.ServeHTTP(walletResponse, walletRequest)
	if walletResponse.Code != http.StatusOK {
		t.Fatalf("wallet list returned HTTP %d: %s", walletResponse.Code, walletResponse.Body.String())
	}
	var walletPayload struct {
		Wallets []map[string]any `json:"wallets"`
	}
	if err := json.Unmarshal(walletResponse.Body.Bytes(), &walletPayload); err != nil {
		t.Fatal(err)
	}
	if len(walletPayload.Wallets) != 2 {
		t.Fatalf("wallet list returned %d rows, want 2", len(walletPayload.Wallets))
	}
	if _, exists := walletPayload.Wallets[0]["available_balance"]; !exists {
		t.Fatal("wallet collection omitted available_balance")
	}

	transferRequest := httptest.NewRequest(http.MethodGet, "/api/v1/transfers?status=PENDING", nil)
	transferRequest.Header.Set("Authorization", "Bearer "+service.token)
	transferResponse := httptest.NewRecorder()
	service.router.ServeHTTP(transferResponse, transferRequest)
	if transferResponse.Code != http.StatusOK {
		t.Fatalf("transfer list returned HTTP %d: %s", transferResponse.Code, transferResponse.Body.String())
	}
	var transferPayload struct {
		Transfers []map[string]any `json:"transfers"`
	}
	if err := json.Unmarshal(transferResponse.Body.Bytes(), &transferPayload); err != nil {
		t.Fatal(err)
	}
	if len(transferPayload.Transfers) != 1 || transferPayload.Transfers[0]["status"] != "PENDING" {
		t.Fatalf("status-filtered transfer collection was unexpected: %#v", transferPayload.Transfers)
	}
	if _, exists := transferPayload.Transfers[0]["idempotency_key"]; exists {
		t.Fatal("transfer collection unexpectedly exposed an idempotency key")
	}
}

func TestPostgresService_WalletDeleteAndCloseRules(t *testing.T) {
	service := newPostgresService(t)
	makeWallet := func() uuid.UUID {
		id := uuid.New()
		if _, err := service.pool.Exec(context.Background(), `
			INSERT INTO wallets (id, organization_id, owner_user_id, currency, account_type)
			VALUES ($1, $2, $3, 'USD', 'customer')
		`, id, service.orgID, service.userID); err != nil {
			t.Fatal(err)
		}
		return id
	}
	request := func(method, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		req.Header.Set("Authorization", "Bearer "+service.token)
		recorder := httptest.NewRecorder()
		service.router.ServeHTTP(recorder, req)
		return recorder
	}

	unusedWallet := makeWallet()
	deleted := request(http.MethodDelete, "/api/v1/wallets/"+unusedWallet.String())
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete unused wallet returned HTTP %d: %s", deleted.Code, deleted.Body.String())
	}

	usedWallet := makeWallet()
	journalID := uuid.New()
	tx, err := service.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
		INSERT INTO journal_entries (id, organization_id, reference_type, currency)
		VALUES ($1, $2, 'deposit', 'USD')
	`, journalID, service.orgID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	var fundingWalletID uuid.UUID
	if err := tx.QueryRow(context.Background(), `
		SELECT id FROM wallets
		WHERE organization_id = $1 AND currency = 'USD' AND account_type = 'external_funding'
	`, service.orgID).Scan(&fundingWalletID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
		INSERT INTO ledger_entries (journal_entry_id, wallet_id, direction, amount)
		VALUES ($1, $2, 'DR', 100), ($1, $3, 'CR', 100)
	`, journalID, fundingWalletID, usedWallet); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
		UPDATE wallets SET balance = 100 WHERE id = $1
	`, usedWallet); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `
		UPDATE wallets SET balance = balance - 100 WHERE id = $1
	`, fundingWalletID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	rejectedDelete := request(http.MethodDelete, "/api/v1/wallets/"+usedWallet.String())
	if rejectedDelete.Code != http.StatusConflict {
		t.Fatalf("delete wallet with ledger history returned HTTP %d, want 409", rejectedDelete.Code)
	}
	rejectedClose := request(http.MethodPost, "/api/v1/wallets/"+usedWallet.String()+"/close")
	if rejectedClose.Code != http.StatusConflict {
		t.Fatalf("close wallet with funds returned HTTP %d, want 409", rejectedClose.Code)
	}

	closableWallet := makeWallet()
	closed := request(http.MethodPost, "/api/v1/wallets/"+closableWallet.String()+"/close")
	if closed.Code != http.StatusOK {
		t.Fatalf("close empty wallet returned HTTP %d: %s", closed.Code, closed.Body.String())
	}
	var status string
	if err := service.pool.QueryRow(context.Background(), `SELECT status FROM wallets WHERE id = $1`, closableWallet).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "closed" {
		t.Fatalf("wallet status is %q, want closed", status)
	}
	reopened := request(http.MethodPost, "/api/v1/wallets/"+closableWallet.String()+"/reopen")
	if reopened.Code != http.StatusOK {
		t.Fatalf("reopen closed wallet returned HTTP %d: %s", reopened.Code, reopened.Body.String())
	}
	if err := service.pool.QueryRow(context.Background(), `SELECT status FROM wallets WHERE id = $1`, closableWallet).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "active" {
		t.Fatalf("wallet status is %q after reopening, want active", status)
	}
}

func TestPostgresService_ExternalFundingDepositIsIdempotent(t *testing.T) {
	service := newPostgresService(t)
	makeRequest := func() *httptest.ResponseRecorder {
		body, err := json.Marshal(map[string]int64{"amount": 2500})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost,
			"/api/v1/wallets/"+service.destID.String()+"/deposit", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+service.token)
		req.Header.Set("Idempotency-Key", "deposit-test-key")
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		service.router.ServeHTTP(response, req)
		return response
	}

	first := makeRequest()
	if first.Code != http.StatusOK {
		t.Fatalf("deposit returned HTTP %d: %s", first.Code, first.Body.String())
	}
	replay := makeRequest()
	if replay.Code != http.StatusOK {
		t.Fatalf("idempotent deposit replay returned HTTP %d: %s", replay.Code, replay.Body.String())
	}
	var firstBody, replayBody map[string]any
	if err := json.Unmarshal(first.Body.Bytes(), &firstBody); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(replay.Body.Bytes(), &replayBody); err != nil {
		t.Fatal(err)
	}
	if firstBody["journal_entry_id"] != replayBody["journal_entry_id"] {
		t.Fatal("deposit replay did not return the original journal entry")
	}

	var balance int64
	if err := service.pool.QueryRow(context.Background(), `
		SELECT balance FROM wallets WHERE id = $1
	`, service.destID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 2500 {
		t.Fatalf("wallet balance is %d minor units, want 2500", balance)
	}
	var entries int
	if err := service.pool.QueryRow(context.Background(), `
		SELECT COUNT(*) FROM ledger_entries WHERE wallet_id = $1
	`, service.destID).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != 1 {
		t.Fatalf("idempotent deposit created %d wallet ledger entries, want 1", entries)
	}
}

func TestPostgresService_RefreshTokenReplayRevokesFamily(t *testing.T) {
	service := newPostgresService(t)
	_, refreshToken, familyID, err := auth.GenerateTokenPair(
		integrationJWTSecret, service.userID, service.orgID, "admin", nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.pool.Exec(context.Background(), `
		INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
		VALUES ($1, $2, $3, NOW() + INTERVAL '7 days')
	`, service.userID, familyID, auth.HashAPIKey(refreshToken)); err != nil {
		t.Fatal(err)
	}
	refreshRequest := func(token string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"refresh_token": token})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		service.router.ServeHTTP(response, req)
		return response
	}
	firstRefresh := refreshRequest(refreshToken)
	if firstRefresh.Code != http.StatusOK {
		t.Fatalf("first refresh returned HTTP %d, want 200: %s", firstRefresh.Code, firstRefresh.Body.String())
	}
	var rotated struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(firstRefresh.Body.Bytes(), &rotated); err != nil {
		t.Fatal(err)
	}
	if rotated.RefreshToken == "" || rotated.RefreshToken == refreshToken {
		t.Fatal("first refresh did not return a newly rotated refresh token")
	}
	if response := refreshRequest(refreshToken); response.Code != http.StatusUnauthorized {
		t.Fatalf("replayed refresh returned HTTP %d, want 401", response.Code)
	}
	if response := refreshRequest(rotated.RefreshToken); response.Code != http.StatusUnauthorized {
		t.Fatalf("rotated token from revoked family returned HTTP %d, want 401: %s",
			response.Code, response.Body.String())
	}
	var allRevoked bool
	if err := service.pool.QueryRow(context.Background(), `
		SELECT bool_and(revoked) FROM refresh_tokens WHERE family_id = $1
	`, familyID).Scan(&allRevoked); err != nil {
		t.Fatal(err)
	}
	if !allRevoked {
		t.Fatal("refresh-token replay did not revoke the full token family")
	}
}
