import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import App from "./App.jsx";

const walletA = {
  id: "11111111-1111-4111-8111-111111111111",
  currency: "USD",
  account_type: "customer",
  balance: 10000,
  hold_balance: 0,
  available_balance: 10000,
  status: "active",
};
const walletB = {
  ...walletA,
  id: "22222222-2222-4222-8222-222222222222",
  balance: 0,
  available_balance: 0,
};
const acceptedTransfer = {
  transfer_id: "33333333-3333-4333-8333-333333333333",
  status: "ACCEPTED",
};
const organizationID = "55555555-5555-4555-8555-555555555555";
const companyProfile = {
  organization: { id: organizationID, name: "Acme Payments", created_at: "2025-01-01T00:00:00Z" },
  members: [
    { id: "admin-user-id", name: "Amina Admin", email: "admin@example.test", role: "admin" },
    { id: "viewer-user-id", name: "Vikram Viewer", email: "viewer@example.test", role: "viewer" },
  ],
  member_count: 2,
  next_cursor: "",
};

function response(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function accessToken(role = "admin") {
  const claims = btoa(JSON.stringify({
    role,
    user_id: role === "admin" ? "admin-user-id" : "viewer-user-id",
    organization_id: organizationID,
  }));
  return `header.${claims}.signature`;
}

function installBackendMock(role = "admin", {
  transferHistory = [],
  transferDetail,
  wallets = [walletA, walletB],
  organizationUsers = [],
} = {}) {
  const token = accessToken(role);
  const fetchMock = vi.fn(async (url, options = {}) => {
    const path = new URL(url, window.location.origin).pathname;
    if (path.endsWith("/auth/login")) {
      return response({ access_token: token, refresh_token: "refresh-token" });
    }
    if (path.endsWith("/auth/register")) {
      return response({
        organization_id: "55555555-5555-4555-8555-555555555555",
        user_id: "66666666-6666-4666-8666-666666666666",
        wallet_id: "77777777-7777-4777-8777-777777777777",
        wallet_currency: "USD",
        role: "viewer",
      }, 201);
    }
    if (/\/organizations\/[^/]+\/profile$/.test(path)) {
      return response(companyProfile);
    }
    if (path.endsWith("/users") && options.method !== "PATCH") {
      return response({ users: organizationUsers, next_cursor: "" });
    }
    if (/\/users\/[^/]+\/status$/.test(path) && options.method === "PATCH") {
      return response({ is_active: JSON.parse(options.body).active });
    }
    if (path.endsWith("/auth/logout")) return response({ message: "successfully logged out" });
    if (path.endsWith("/wallets") && options.method === "POST") {
      return response({ id: "44444444-4444-4444-8444-444444444444" }, 201);
    }
    if (path.endsWith("/deposit")) {
      return response({ credited_amount: 250, currency: "USD", status: "SUCCESS" });
    }
    if (path.endsWith("/close")) return response({ status: "closed" });
    if (path.endsWith("/reopen")) return response({ status: "active" });
    if (/\/wallets\/[^/]+$/.test(path) && options.method === "DELETE") {
      return response({ status: "DELETED" });
    }
    if (/\/transfers\/[^/]+$/.test(path) && options.method !== "POST") {
      return response(transferDetail?.() || {
        id: path.split("/").pop(),
        source_wallet_id: walletA.id,
        destination_wallet_id: walletB.id,
        currency: "USD",
        amount: 1234,
        fee_amount: 10,
        status: "PROCESSING",
        saga_step: "RISK_CHECK",
        created_at: "2026-11-04T12:00:00Z",
        updated_at: "2026-11-04T12:00:00Z",
      });
    }
    if (path.endsWith("/wallets")) {
      return response({ wallets: typeof wallets === "function" ? wallets() : wallets, next_cursor: "" });
    }
    if (path.endsWith("/transfers") && options.method === "POST") return response(acceptedTransfer, 202);
    if (path.endsWith("/transfers")) return response({ transfers: transferHistory, next_cursor: "" });
    if (path.endsWith("/ledger")) return response({ entries: [], next_cursor: "" });
    return response({ error: `Unexpected endpoint ${path}` }, 404);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

beforeEach(() => {
  sessionStorage.clear();
  vi.stubGlobal("crypto", { randomUUID: () => "idempotency-test-key" });
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("wallet dashboard", () => {
  it("validates the password before attempting login", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock();
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "person@example.test");
    await user.type(screen.getByLabelText("Password"), "short");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("at least 8 characters");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("confirms signup includes a ready-to-receive wallet", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock();
    render(<App />);
    await user.click(screen.getByRole("button", { name: "Create an account" }));
    await user.type(screen.getByLabelText("Full name"), "Viewer Person");
    await user.type(screen.getByLabelText("Company name"), "New workspace");
    await user.type(screen.getByLabelText("Work email"), "viewer@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Create account" }));

    expect(await screen.findByRole("status")).toHaveTextContent("own USD wallet");
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes("/api/v1/auth/register"))).toBe(true);
    const registration = fetchMock.mock.calls.find(([url]) => String(url).includes("/api/v1/auth/register"));
    expect(JSON.parse(registration[1].body).full_name).toBe("Viewer Person");
  });

  it("loads the JWT organization profile and members for every signed-in role", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock("viewer");
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "viewer@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findAllByText("Acme Payments")).not.toHaveLength(0);
    const profileRequest = fetchMock.mock.calls.find(([url]) => String(url).includes("/organizations/"));
    expect(profileRequest[0]).toContain(`/api/v1/organizations/${organizationID}/profile`);
    await user.click(screen.getByRole("button", { name: /Company profile/ }));
    expect(await screen.findByRole("heading", { name: "Company profile" })).toBeInTheDocument();
    expect(screen.getByText("Amina Admin")).toBeInTheDocument();
    expect(screen.getByText("admin@example.test")).toBeInTheDocument();
    expect(screen.getByText("Admin")).toBeInTheDocument();
    expect(screen.getByText("Vikram Viewer")).toBeInTheDocument();
    expect(screen.getByText("Member")).toBeInTheDocument();
  });

  it("signs in and loads live workspace data through the API client", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock("viewer");
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "finance@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByRole("heading", { name: "Good to see you." })).toBeInTheDocument();
    expect(await screen.findByText("Your wallets")).toBeInTheDocument();
    expect(await screen.findAllByText("$100.00")).not.toHaveLength(0);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/v1/auth/login"),
      expect.objectContaining({ method: "POST" }),
    );
    expect(sessionStorage.getItem("wallet.accessToken")).toContain("header.");
    expect(screen.queryByRole("button", { name: "Reconciliation" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "＋ New transfer" })).not.toBeInTheDocument();
  });

  it("opens the selected wallet ledger from the wallets page", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock();
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: /Wallets/ }));
    await screen.findByRole("heading", { name: "Wallets" });
    await user.click(screen.getAllByRole("button", { name: "View ledger" })[0]);

    expect(await screen.findByRole("heading", { name: "Ledger activity" })).toBeInTheDocument();
    expect(screen.getByText("Account entries")).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes(`/wallets/${walletA.id}/ledger`))).toBe(true);
  });

  it("offers reopening for a closed wallet and calls the reopen endpoint", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock("admin", { wallets: [{ ...walletA, status: "closed" }] });
    vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: /Wallets/ }));
    await user.click(await screen.findByRole("button", { name: "Reopen" }));

    await waitFor(() => {
      expect(fetchMock.mock.calls.some(([url, options]) =>
        String(url).endsWith(`/api/v1/wallets/${walletA.id}/reopen`) && options?.method === "POST",
      )).toBe(true);
    });
  });

  it("allows only admins to manage organization accounts and deactivate other users", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock("admin", {
      organizationUsers: [
        { id: "admin-user-id", email: "admin@example.test", role: "admin", is_active: true, wallet_count: 1, created_at: "2026-01-01T00:00:00Z" },
        { id: "viewer-user-id", email: "viewer@example.test", role: "viewer", is_active: true, wallet_count: 1, created_at: "2026-01-02T00:00:00Z" },
      ],
    });
    vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: /Company accounts/ }));
    expect(await screen.findByText("viewer@example.test")).toBeInTheDocument();
    expect(screen.getAllByText("admin@example.test")).not.toHaveLength(0);
    expect(screen.getByText("Current admin")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Deactivate" }));

    await waitFor(() => {
      const request = fetchMock.mock.calls.find(([url, options]) =>
        String(url).endsWith("/api/v1/users/viewer-user-id/status") && options?.method === "PATCH");
      expect(request).toBeDefined();
      expect(JSON.parse(request[1].body)).toEqual({ active: false });
    });

    cleanup();
    sessionStorage.clear();
    const viewerUser = userEvent.setup();
    installBackendMock("viewer");
    render(<App />);
    await viewerUser.type(screen.getByLabelText("Work email"), "viewer@example.test");
    await viewerUser.type(screen.getByLabelText("Password"), "correct-horse");
    await viewerUser.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    expect(screen.queryByRole("button", { name: /Company accounts/ })).not.toBeInTheDocument();
  });

  it("submits a single transfer with an idempotency key and shows the returned state", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock();
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: "＋ New transfer" }));
    await screen.findByRole("heading", { name: "New transfer" });
    await user.selectOptions(screen.getByLabelText("From wallet"), walletA.id);
    await user.selectOptions(screen.getByLabelText("To wallet"), walletB.id);
    await user.type(screen.getByLabelText("Amount"), "12.34");
    await user.click(screen.getByRole("button", { name: /Review and submit transfer/ }));

    await waitFor(() => {
      const posts = fetchMock.mock.calls.filter(([url, options]) =>
        String(url).includes("/api/v1/transfers") && options?.method === "POST");
      expect(posts).toHaveLength(1);
      expect(new Headers(posts[0][1].headers).get("Idempotency-Key")).toBe("idempotency-test-key");
      expect(JSON.parse(posts[0][1].body).amount).toBe(1234);
    });
    expect(await screen.findByText(/Transfer accepted\. Status: ACCEPTED/)).toBeInTheDocument();
  });

  it("automatically updates a pending transfer and notifies when it completes", async () => {
    const user = userEvent.setup();
    let status = "PROCESSING";
    const history = [{
      id: acceptedTransfer.transfer_id,
      source_wallet_id: walletA.id,
      destination_wallet_id: walletB.id,
      currency: "USD",
      amount: 1234,
      fee_amount: 10,
      status: "PENDING",
      saga_step: "RESERVE_FUNDS",
      created_at: "2026-11-04T12:00:00Z",
      updated_at: "2026-11-04T12:00:00Z",
    }];
    const fetchMock = installBackendMock("admin", {
      transferHistory: history,
      wallets: () => [{
        ...walletA,
        balance: status === "COMPLETED" ? 8756 : walletA.balance,
        available_balance: status === "COMPLETED" ? 8756 : walletA.available_balance,
      }, walletB],
      transferDetail: () => ({
        ...history[0],
        status,
        saga_step: status === "COMPLETED" ? "COMPLETED" : "RISK_CHECK",
      }),
    });
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: "＋ New transfer" }));
    await user.selectOptions(screen.getByLabelText("From wallet"), walletA.id);
    await user.selectOptions(screen.getByLabelText("To wallet"), walletB.id);
    await user.type(screen.getByLabelText("Amount"), "12.34");
    await user.click(screen.getByRole("button", { name: /Review and submit transfer/ }));
    expect(await screen.findByText(/Transfer accepted/)).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "Transaction history" })).toBeInTheDocument();

    status = "COMPLETED";
    expect(await screen.findByText(/Transfer completed: \$12\.34/, {}, { timeout: 6000 })).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes(`/transfers/${acceptedTransfer.transfer_id}`))).toBe(true);
    await user.click(screen.getByRole("button", { name: /Wallets/ }));
    expect(await screen.findAllByText("$87.56")).not.toHaveLength(0);
  });

  it("shows identified sender and receiver accounts with authoritative ledger postings", async () => {
    const user = userEvent.setup();
    const history = [{
      id: acceptedTransfer.transfer_id,
      source_wallet_id: walletA.id,
      destination_wallet_id: walletB.id,
      currency: "USD",
      amount: 1234,
      fee_amount: 0,
      status: "COMPLETED",
      created_at: "2026-11-04T12:00:00Z",
      updated_at: "2026-11-04T12:01:00Z",
    }];
    installBackendMock("admin", {
      transferHistory: history,
      transferDetail: () => ({
        ...history[0],
        sender_account: { wallet_id: walletA.id, currency: "USD", account_type: "customer" },
        receiver_account: { wallet_id: walletB.id, currency: "USD", account_type: "customer" },
        ledger_entries: [
          {
            id: "ledger-debit",
            journal_entry_id: "journal-entry",
            wallet_id: walletA.id,
            direction: "DR",
            amount: 1234,
            currency: "USD",
            account_type: "customer",
            reference_type: "transfer",
            created_at: "2026-11-04T12:00:00Z",
          },
          {
            id: "ledger-credit",
            journal_entry_id: "journal-entry",
            wallet_id: walletB.id,
            direction: "CR",
            amount: 1234,
            currency: "USD",
            account_type: "customer",
            reference_type: "transfer",
            created_at: "2026-11-04T12:00:00Z",
          },
        ],
      }),
    });
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: /Transfers/ }));
    await screen.findByRole("heading", { name: "Transaction history" });
    await user.click(screen.getByRole("button", { name: /33333333/ }));

    expect(await screen.findByText("Sender · debited")).toBeInTheDocument();
    expect(screen.getByText("Receiver · credited")).toBeInTheDocument();
    expect(screen.getAllByText(walletA.id).length).toBeGreaterThan(0);
    expect(screen.getAllByText(walletB.id).length).toBeGreaterThan(0);
    expect(screen.getByRole("heading", { name: "Debit and credit ledger entries" })).toBeInTheDocument();
    expect(screen.getByText("Debit")).toBeInTheDocument();
    expect(screen.getByText("Credit")).toBeInTheDocument();
  });

  it("submits a user-to-user transfer by recipient email", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock();
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: "＋ New transfer" }));
    await screen.findByRole("heading", { name: "New transfer" });
    await user.selectOptions(screen.getByLabelText("From wallet"), walletA.id);
    await user.selectOptions(screen.getByLabelText("Transfer to"), "email");
    await user.type(screen.getByLabelText("Recipient email"), "receiver@example.test");
    await user.type(screen.getByLabelText("Amount"), "4.00");
    await user.click(screen.getByRole("button", { name: /Review and submit transfer/ }));

    await waitFor(() => {
      const post = fetchMock.mock.calls.find(([url, options]) =>
        String(url).includes("/api/v1/transfers") && options?.method === "POST");
      expect(post).toBeDefined();
      expect(JSON.parse(post[1].body)).toMatchObject({
        source_wallet_id: walletA.id,
        recipient_email: "receiver@example.test",
        currency: "USD",
        amount: 400,
      });
      expect(JSON.parse(post[1].body)).not.toHaveProperty("destination_wallet_id");
    });
  });

  it("reuses the idempotency key when retrying an unknown transfer outcome", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock();
    vi.stubGlobal("crypto", {
      randomUUID: vi.fn().mockReturnValueOnce("first-attempt-key").mockReturnValueOnce("new-key"),
    });
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: "＋ New transfer" }));
    await screen.findByRole("heading", { name: "New transfer" });
    await user.selectOptions(screen.getByLabelText("From wallet"), walletA.id);
    await user.selectOptions(screen.getByLabelText("To wallet"), walletB.id);
    await user.type(screen.getByLabelText("Amount"), "2.00");

    fetchMock.mockImplementationOnce(async (url, options) => {
      if (String(url).includes("/api/v1/transfers") && options?.method === "POST") {
        throw new TypeError("connection reset");
      }
      return response({ error: `Unexpected endpoint ${url}` }, 404);
    });
    await user.click(screen.getByRole("button", { name: /Review and submit transfer/ }));
    expect(await screen.findByText(/outcome is unknown/)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Review and submit transfer/ }));
    await waitFor(() => {
      const posts = fetchMock.mock.calls.filter(([url, options]) =>
        String(url).includes("/api/v1/transfers") && options?.method === "POST");
      expect(posts).toHaveLength(2);
      expect(posts.map(([, options]) => new Headers(options.headers).get("Idempotency-Key")))
        .toEqual(["first-attempt-key", "first-attempt-key"]);
    });
  });

  it("supports sign out and clears session tokens", async () => {
    const user = userEvent.setup();
    installBackendMock();
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: /Sign out/ }));
    expect(await screen.findByRole("heading", { name: "Sign in to your account" })).toBeInTheDocument();
    expect(sessionStorage.getItem("wallet.accessToken")).toBeNull();
  });

  it("explains external-funding deposits and requires confirmation before recording one", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock();
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: /Wallets/ }));
    await screen.findByRole("heading", { name: "Record an external deposit" });
    expect(screen.getByText(/does not connect to a bank or card processor/i)).toBeInTheDocument();

    await user.type(screen.getByLabelText("Deposit amount"), "2.50");
    await user.click(screen.getByRole("button", { name: "Record deposit" }));
    expect(await screen.findByRole("status")).toHaveTextContent("Confirm that this deposit");
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes("/deposit"))).toBe(false);

    await user.click(screen.getByLabelText(/I confirm this is an authorized external-funding/));
    await user.click(screen.getByRole("button", { name: "Record deposit" }));
    await waitFor(() => {
      const call = fetchMock.mock.calls.find(([url]) => String(url).includes("/deposit"));
      expect(call).toBeDefined();
      expect(JSON.parse(call[1].body).amount).toBe(250);
      expect(new Headers(call[1].headers).get("Idempotency-Key")).toBeTruthy();
    });
  });

  it("shows wallet lifecycle actions and deletes only after confirmation", async () => {
    const user = userEvent.setup();
    const fetchMock = installBackendMock();
    vi.stubGlobal("confirm", vi.fn(() => true));
    render(<App />);
    await user.type(screen.getByLabelText("Work email"), "admin@example.test");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    await screen.findByRole("heading", { name: "Good to see you." });
    await user.click(screen.getByRole("button", { name: /Wallets/ }));
    expect(await screen.findAllByRole("button", { name: "Delete" })).toHaveLength(2);
    expect(screen.getAllByRole("button", { name: "Close" })).toHaveLength(2);
    await user.click(screen.getAllByRole("button", { name: "Delete" })[0]);
    await waitFor(() => {
      expect(fetchMock.mock.calls.some(([url, options]) =>
        /\/api\/v1\/wallets\/[^/]+$/.test(String(url)) && options?.method === "DELETE")).toBe(true);
    });
  });
});
