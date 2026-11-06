const API_BASE = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const ACCESS_KEY = "wallet.accessToken";
const REFRESH_KEY = "wallet.refreshToken";

export const authStorage = {
  get accessToken() {
    return sessionStorage.getItem(ACCESS_KEY);
  },
  get refreshToken() {
    return sessionStorage.getItem(REFRESH_KEY);
  },
  save(tokens) {
    sessionStorage.setItem(ACCESS_KEY, tokens.access_token);
    sessionStorage.setItem(REFRESH_KEY, tokens.refresh_token);
  },
  clear() {
    sessionStorage.removeItem(ACCESS_KEY);
    sessionStorage.removeItem(REFRESH_KEY);
  },
};

async function decodeResponse(response) {
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const message =
      typeof payload === "object" && payload?.error
        ? payload.error
        : `Request failed (${response.status})`;
    const error = new Error(message);
    error.status = response.status;
    throw error;
  }
  return payload;
}

async function refreshAccessToken() {
  const refreshToken = authStorage.refreshToken;
  if (!refreshToken) return false;

  try {
    const response = await fetch(`${API_BASE}/api/v1/auth/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });
    const tokens = await decodeResponse(response);
    authStorage.save(tokens);
    return true;
  } catch {
    authStorage.clear();
    return false;
  }
}

export async function request(path, options = {}, canRefresh = true) {
  const headers = new Headers(options.headers || {});
  const token = authStorage.accessToken;
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (options.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  let response = await fetch(`${API_BASE}${path}`, { ...options, headers });
  if (response.status === 401 && canRefresh && authStorage.refreshToken) {
    if (await refreshAccessToken()) {
      return request(path, options, false);
    }
    window.dispatchEvent(new Event("wallet-session-expired"));
  }
  return decodeResponse(response);
}

export const api = {
  login: (email, password) =>
    request(
      "/api/v1/auth/login",
      { method: "POST", body: JSON.stringify({ email, password }) },
      false,
    ),
  register: (organizationName, fullName, email, password) =>
    request(
      "/api/v1/auth/register",
      {
        method: "POST",
        body: JSON.stringify({
          organization_name: organizationName,
          full_name: fullName,
          email,
          password,
        }),
      },
      false,
    ),
  logout: () => request("/api/v1/auth/logout", { method: "POST" }, false),
  wallets: (cursor = "") =>
    request(`/api/v1/wallets?limit=100${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`),
  wallet: (id) => request(`/api/v1/wallets/${encodeURIComponent(id)}`),
  createWallet: (currency) =>
    request("/api/v1/wallets", {
      method: "POST",
      body: JSON.stringify({ currency, account_type: "customer" }),
    }),
  deleteWallet: (walletId) =>
    request(`/api/v1/wallets/${encodeURIComponent(walletId)}`, { method: "DELETE" }),
  closeWallet: (walletId) =>
    request(`/api/v1/wallets/${encodeURIComponent(walletId)}/close`, {
      method: "POST",
      body: JSON.stringify({}),
    }),
  reopenWallet: (walletId) =>
    request(`/api/v1/wallets/${encodeURIComponent(walletId)}/reopen`, {
      method: "POST",
      body: JSON.stringify({}),
    }),
  deposit: (walletId, amount, idempotencyKey) =>
    request(`/api/v1/wallets/${encodeURIComponent(walletId)}/deposit`, {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify({ amount }),
    }),
  ledger: (walletId, cursor = "") =>
    request(
      `/api/v1/wallets/${encodeURIComponent(walletId)}/ledger?limit=20${
        cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""
      }`,
    ),
  transfers: (status = "", cursor = "") => {
    const params = new URLSearchParams({ limit: "50" });
    if (status) params.set("status", status);
    if (cursor) params.set("cursor", cursor);
    return request(`/api/v1/transfers?${params}`);
  },
  organizationUsers: (cursor = "") =>
    request(`/api/v1/users?limit=50${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`),
  organizationProfile: (organizationId, cursor = "") =>
    request(`/api/v1/organizations/${encodeURIComponent(organizationId)}/profile?limit=50${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`),
  setUserActive: (userId, active) =>
    request(`/api/v1/users/${encodeURIComponent(userId)}/status`, {
      method: "PATCH",
      body: JSON.stringify({ active }),
    }),
  transfer: (id) => request(`/api/v1/transfers/${encodeURIComponent(id)}`),
  createTransfer: (payload, idempotencyKey) =>
    request("/api/v1/transfers", {
      method: "POST",
      headers: { "Idempotency-Key": idempotencyKey },
      body: JSON.stringify(payload),
    }),
  reconciliation: () => request("/api/v1/reports/reconciliation"),
};

export function formatMoney(minorUnits, currency = "USD") {
  const minor = toMinorBigInt(minorUnits);
  if (minor === null) return "—";
  const negative = minor < 0n;
  const absolute = negative ? -minor : minor;
  const whole = absolute / 100n;
  const fraction = String(absolute % 100n).padStart(2, "0");
  const formatter = new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
  const parts = formatter.formatToParts(whole);
  const decimal = new Intl.NumberFormat(undefined).formatToParts(1.1)
    .find((part) => part.type === "decimal")?.value || ".";
  const lastInteger = parts.reduce((last, part, index) => part.type === "integer" ? index : last, -1);
  if (lastInteger >= 0) {
    parts.splice(lastInteger + 1, 0, { type: "decimal", value: decimal }, { type: "fraction", value: fraction });
  }
  if (negative) parts.unshift({ type: "minusSign", value: "-" });
  return parts.map((part) => part.value).join("");
}

export function toMinorBigInt(value) {
  if (typeof value === "bigint") return value;
  if (typeof value === "string" && /^-?\d+$/.test(value)) {
    try { return BigInt(value); } catch { return null; }
  }
  if (typeof value === "number" && Number.isSafeInteger(value)) return BigInt(value);
  return null;
}

export function formatDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "—"
    : new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(date);
}
