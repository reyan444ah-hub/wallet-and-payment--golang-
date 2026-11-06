import { useEffect, useMemo, useRef, useState } from "react";
import { api, authStorage, formatDate, formatMoney, toMinorBigInt } from "./api.js";

const navigation = [
  { id: "overview", label: "Overview", icon: "◫" },
  { id: "wallets", label: "Wallets", icon: "▣" },
  { id: "activity", label: "Ledger activity", icon: "≋" },
  { id: "transfers", label: "Transfers", icon: "↗" },
];

function getRole(token) {
  try {
    const encoded = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    const claims = JSON.parse(atob(encoded));
    return claims.role || claims.Role || "viewer";
  } catch {
    return "viewer";
  }
}

function getUserID(token) {
  try {
    const encoded = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    const claims = JSON.parse(atob(encoded));
    return claims.user_id || claims.UserID || "";
  } catch {
    return "";
  }
}

function getOrganizationID(token) {
  try {
    const encoded = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    const claims = JSON.parse(atob(encoded));
    return claims.organization_id || claims.OrganizationID || "";
  } catch {
    return "";
  }
}

function AuthScreen({ onAuthenticated }) {
  const [mode, setMode] = useState("login");
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [organization, setOrganization] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");

  async function submit(event) {
    event.preventDefault();
    setError("");
    setNotice("");
    if (password.length < 8) {
      setError("Password must contain at least 8 characters.");
      return;
    }
    if (mode === "register" && !organization.trim()) {
      setError("Organization name is required.");
      return;
    }
    setBusy(true);
    try {
      if (mode === "register") {
        await api.register(organization.trim(), fullName.trim(), email.trim(), password);
        setMode("login");
        setNotice("Account created with your own USD wallet. Sign in to receive transfers.");
      } else {
        const tokens = await api.login(email.trim(), password);
        authStorage.save(tokens);
        sessionStorage.setItem("wallet.email", email.trim());
        onAuthenticated(tokens.access_token);
      }
    } catch (requestError) {
      setError(requestError.message || "Unable to complete sign in.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="auth-page">
      <section className="auth-brand">
        <div className="brand-mark">H</div>
        <span className="eyebrow">HALAAL FINANCIAL</span>
        <h1>Clarity for every movement of money.</h1>
        <p>One secure workspace for wallets, ledger activity and payment operations.</p>
        <div className="auth-proof">
          <span><b>●</b> Double-entry ledger</span>
          <span><b>●</b> Organization-scoped access</span>
          <span><b>●</b> Saga-tracked transfers</span>
        </div>
      </section>
      <section className="auth-panel">
        <div className="auth-card">
          <div className="mobile-brand"><span className="brand-mark">H</span> HALAAL</div>
          <span className="eyebrow">{mode === "login" ? "WELCOME BACK" : "GET STARTED"}</span>
          <h2>{mode === "login" ? "Sign in to your account" : "Create your workspace"}</h2>
          <p className="muted">Use your organization account to securely continue.</p>
          <form onSubmit={submit} className="form-stack">
            {mode === "register" && (
              <>
                <label>
                  Full name
                  <input value={fullName} onChange={(event) => setFullName(event.target.value)} required autoComplete="name" />
                </label>
                <label>
                  Company name
                  <input value={organization} onChange={(event) => setOrganization(event.target.value)} required autoComplete="organization" />
                </label>
              </>
            )}
            <label>
              Work email
              <input type="email" value={email} onChange={(event) => setEmail(event.target.value)} required autoComplete="email" />
            </label>
            <label>
              Password
              <input type="password" value={password} onChange={(event) => setPassword(event.target.value)} required autoComplete={mode === "login" ? "current-password" : "new-password"} />
            </label>
            {error && <div className="alert error" role="alert">{error}</div>}
            {notice && <div className="alert success" role="status">{notice}</div>}
            <button className="button primary full" disabled={busy}>
              {busy ? "Please wait…" : mode === "login" ? "Sign in" : "Create account"}
            </button>
          </form>
          <p className="auth-switch">
            {mode === "login" ? "New to HALAAL?" : "Already have an account?"}{" "}
            <button type="button" className="text-button" onClick={() => { setMode(mode === "login" ? "register" : "login"); setError(""); setNotice(""); }}>
              {mode === "login" ? "Create an account" : "Sign in"}
            </button>
          </p>
          <p className="legal-note">Your session tokens stay in this browser session and are cleared when you sign out.</p>
        </div>
      </section>
    </main>
  );
}

function App() {
  const [accessToken, setAccessToken] = useState(authStorage.accessToken);
  const [role, setRole] = useState(() => getRole(authStorage.accessToken || ""));
  const [page, setPage] = useState("overview");
  const [wallets, setWallets] = useState([]);
  const [organizationUsers, setOrganizationUsers] = useState([]);
  const [organizationUserCursor, setOrganizationUserCursor] = useState("");
  const [organizationUsersLoading, setOrganizationUsersLoading] = useState(false);
  const [organizationUsersError, setOrganizationUsersError] = useState("");
  const [organizationProfile, setOrganizationProfile] = useState(null);
  const [organizationMembers, setOrganizationMembers] = useState([]);
  const [organizationMemberCursor, setOrganizationMemberCursor] = useState("");
  const [organizationProfileLoading, setOrganizationProfileLoading] = useState(false);
  const [organizationProfileError, setOrganizationProfileError] = useState("");
  const [transfers, setTransfers] = useState([]);
  const [ledger, setLedger] = useState([]);
  const [selectedWallet, setSelectedWallet] = useState("");
  const [walletCursor, setWalletCursor] = useState("");
  const [transferCursor, setTransferCursor] = useState("");
  const [selectedTransfer, setSelectedTransfer] = useState(null);
  const [transferDetailBusy, setTransferDetailBusy] = useState(false);
  const [toast, setToast] = useState("");
  const [loadError, setLoadError] = useState("");
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [transferBusy, setTransferBusy] = useState(false);
  const [transferAttempt, setTransferAttempt] = useState(null);
  const [watchedTransferIDs, setWatchedTransferIDs] = useState([]);
  const transferStatuses = useRef(new Map());
  const pollError = useRef("");
  const [walletBusy, setWalletBusy] = useState(false);
  const [depositBusy, setDepositBusy] = useState(false);
  const [reconciliation, setReconciliation] = useState(null);
  const [transferForm, setTransferForm] = useState({
    source: "",
    destination: "",
    recipientType: "internal",
    recipientWalletId: "",
    recipientEmail: "",
    amount: "",
  });
  const [walletCurrency, setWalletCurrency] = useState("USD");
  const [depositWallet, setDepositWallet] = useState("");
  const [depositAmount, setDepositAmount] = useState("");
  const [depositConfirmed, setDepositConfirmed] = useState(false);
  const [transferStatus, setTransferStatus] = useState("");
  const email = sessionStorage.getItem("wallet.email") || "Account";
  const currentUserID = getUserID(accessToken || "");
  const currentOrganizationID = getOrganizationID(accessToken || "");
  const canWrite = role === "admin" || role === "finance";
  const canAudit = role === "admin" || role === "finance";
  const canManageUsers = role === "admin";

  useEffect(() => {
    const expired = () => signOut(false);
    window.addEventListener("wallet-session-expired", expired);
    return () => window.removeEventListener("wallet-session-expired", expired);
  }, []);

  useEffect(() => {
    if (accessToken) {
      loadWorkspace();
      loadOrganizationProfile();
    }
  }, [accessToken]);

  useEffect(() => {
    if (!toast) return undefined;
    const timeout = window.setTimeout(() => setToast(""), 5000);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  useEffect(() => {
    if (accessToken && canManageUsers && page === "users") {
      loadOrganizationUsers();
    }
  }, [accessToken, canManageUsers, page]);

  async function loadWorkspace(isRefresh = false) {
    if (isRefresh) setRefreshing(true);
    else setLoading(true);
    setLoadError("");
    try {
      const walletResult = await api.wallets();
      setWallets(walletResult.wallets || []);
      setWalletCursor(walletResult.next_cursor || "");
      setDepositWallet((current) => current || walletResult.wallets?.[0]?.id || "");
      setTransferForm((current) => ({
        ...current,
        source: current.source || walletResult.wallets?.[0]?.id || "",
        destination: current.destination || walletResult.wallets?.[1]?.id || "",
      }));
      setSelectedWallet((current) => current || walletResult.wallets?.[0]?.id || "");
    } catch (error) {
      setLoadError(error.message || "Could not load your account data.");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }

  async function loadTransfers(status = transferStatus) {
    try {
      const result = await api.transfers(status);
      for (const transfer of result.transfers || []) {
        if (!transferStatuses.current.has(transfer.id)) {
          transferStatuses.current.set(transfer.id, transfer.status);
        }
      }
      setTransfers(result.transfers || []);
      setTransferCursor(result.next_cursor || "");
    } catch (error) {
      setToast(error.message || "Could not load transfers.");
    }
  }

  async function loadOrganizationUsers(cursor = "", append = false) {
    setOrganizationUsersLoading(true);
    setOrganizationUsersError("");
    try {
      const result = await api.organizationUsers(cursor);
      setOrganizationUsers((current) => append
        ? [...current, ...(result.users || [])]
        : result.users || []);
      setOrganizationUserCursor(result.next_cursor || "");
    } catch (error) {
      setOrganizationUsersError(error.message || "Could not load company accounts.");
    } finally {
      setOrganizationUsersLoading(false);
    }
  }

  async function loadOrganizationProfile(cursor = "", append = false) {
    if (!currentOrganizationID) {
      setOrganizationProfileError("Your session does not include a company organization.");
      return;
    }
    setOrganizationProfileLoading(true);
    setOrganizationProfileError("");
    try {
      const result = await api.organizationProfile(currentOrganizationID, cursor);
      setOrganizationProfile(result);
      setOrganizationMembers((current) => append
        ? [...current, ...(result.members || [])]
        : result.members || []);
      setOrganizationMemberCursor(result.next_cursor || "");
    } catch (error) {
      setOrganizationProfileError(error.message || "Could not load your company profile.");
    } finally {
      setOrganizationProfileLoading(false);
    }
  }

  async function loadMoreOrganizationMembers() {
    if (!organizationMemberCursor || organizationProfileLoading) return;
    await loadOrganizationProfile(organizationMemberCursor, true);
  }

  async function updateOrganizationUser(user) {
    const active = !user.is_active;
    const action = active ? "reactivate" : "deactivate";
    if (!window.confirm(`${active ? "Reactivate" : "Deactivate"} ${user.email}? Financial and wallet history will be preserved.`)) return;
    try {
      await api.setUserActive(user.id, active);
      setToast(`Account ${active ? "reactivated" : "deactivated"}. Financial history is preserved.`);
      await loadOrganizationUsers();
    } catch (error) {
      setToast(error.message || `Could not ${action} account.`);
    }
  }

  async function loadMoreOrganizationUsers() {
    if (!organizationUserCursor || organizationUsersLoading) return;
    await loadOrganizationUsers(organizationUserCursor, true);
  }

  async function loadMoreWallets() {
    if (!walletCursor) return;
    try {
      const result = await api.wallets(walletCursor);
      setWallets((current) => [...current, ...(result.wallets || [])]);
      setWalletCursor(result.next_cursor || "");
    } catch (error) {
      setToast(error.message || "Could not load more wallets.");
    }
  }

  async function loadMoreTransfers() {
    if (!transferCursor) return;
    try {
      const result = await api.transfers(transferStatus, transferCursor);
      setTransfers((current) => [...current, ...(result.transfers || [])]);
      setTransferCursor(result.next_cursor || "");
    } catch (error) {
      setToast(error.message || "Could not load more transfers.");
    }
  }

  async function showTransferDetails(transferId) {
    setTransferDetailBusy(true);
    try {
      setSelectedTransfer(await api.transfer(transferId));
    } catch (error) {
      setToast(error.message || "Could not load transfer details.");
    } finally {
      setTransferDetailBusy(false);
    }
  }

  useEffect(() => {
    if (accessToken) loadTransfers(transferStatus);
  }, [accessToken, transferStatus]);

  const activeTransferIDs = [
    ...new Set([
      ...transfers
        .filter((transfer) => ["PENDING", "PROCESSING"].includes(transfer.status))
        .map((transfer) => transfer.id),
      ...watchedTransferIDs,
    ]),
  ].join(",");

  useEffect(() => {
    if (!accessToken || !activeTransferIDs) return undefined;
    let active = true;
    let inFlight = false;

    async function refreshActiveTransfers() {
      if (inFlight) return;
      inFlight = true;
      try {
        const ids = activeTransferIDs.split(",");
        const results = await Promise.all(ids.map(async (id) => {
          try {
            return { transfer: await api.transfer(id) };
          } catch (error) {
            return { id, error };
          }
        }));
        if (!active) return;

        const updates = [];
        const failures = [];
        for (const result of results) {
          if (result.error) {
            failures.push(result.error);
            continue;
          }
          const transfer = result.transfer;
          const previousStatus = transferStatuses.current.get(transfer.id);
          if (previousStatus && previousStatus !== transfer.status) {
            if (transfer.status === "COMPLETED") {
              setToast(`Transfer completed: ${formatMoney(transfer.amount, transfer.currency)}.`);
            } else if (transfer.status === "FAILED") {
              setToast(`Transfer failed: ${transfer.failure_reason || "The transfer was rejected."}`);
            }
          }
          transferStatuses.current.set(transfer.id, transfer.status);
          updates.push(transfer);
        }

        if (failures.length > 0) {
          const message = failures[0].message || "Could not refresh transfer status.";
          if (pollError.current !== message) {
            setToast(`Transfer status could not be refreshed; retrying. ${message}`);
            pollError.current = message;
          }
        } else {
          pollError.current = "";
        }

        if (updates.length > 0) {
          const updatesByID = new Map(updates.map((transfer) => [transfer.id, transfer]));
          setTransfers((current) => current
            .map((transfer) => updatesByID.get(transfer.id) || transfer)
            .filter((transfer) => !transferStatus || transfer.status === transferStatus));
          setSelectedTransfer((current) => current && updatesByID.has(current.id)
            ? updatesByID.get(current.id)
            : current);
          const terminalIDs = new Set(
            updates
              .filter((transfer) => ["COMPLETED", "FAILED", "REVERSED"].includes(transfer.status))
              .map((transfer) => transfer.id),
          );
          if (terminalIDs.size > 0) {
            setWatchedTransferIDs((current) => current.filter((id) => !terminalIDs.has(id)));
            try {
              const latest = await api.transfers(transferStatus);
              if (active) {
                setTransfers(latest.transfers || []);
                setTransferCursor(latest.next_cursor || "");
              }
            } catch (error) {
              if (active && pollError.current !== error.message) {
                setToast(`Transfer finished, but history could not be refreshed. ${error.message}`);
                pollError.current = error.message;
              }
            }
            await loadWorkspace(true);
            if (selectedWallet) {
              try {
                const ledgerResult = await api.ledger(selectedWallet);
                if (active) setLedger(ledgerResult.entries || []);
              } catch (error) {
                if (active) setToast(error.message || "Transaction finished, but wallet ledger could not be refreshed.");
              }
            }
          }
        }
      } finally {
        inFlight = false;
      }
    }

    const interval = window.setInterval(refreshActiveTransfers, 2000);
    return () => {
      active = false;
      window.clearInterval(interval);
    };
  }, [accessToken, activeTransferIDs, transferStatus, selectedWallet]);

  useEffect(() => {
    if (!accessToken || !selectedWallet) {
      setLedger([]);
      return;
    }
    let active = true;
    api.ledger(selectedWallet)
      .then((result) => { if (active) setLedger(result.entries || []); })
      .catch((error) => { if (active) setToast(error.message || "Could not load ledger activity."); });
    return () => { active = false; };
  }, [accessToken, selectedWallet]);

  async function signOut(callBackend = true) {
    if (callBackend && authStorage.accessToken) {
      try { await api.logout(); } catch { /* local sign-out still clears this session */ }
    }
    authStorage.clear();
    sessionStorage.removeItem("wallet.email");
    setAccessToken("");
    setRole("viewer");
    setWallets([]);
    setTransfers([]);
    setWatchedTransferIDs([]);
    transferStatuses.current.clear();
    setLedger([]);
    setTransferAttempt(null);
    setPage("overview");
  }

  async function createWallet(event) {
    event.preventDefault();
    setWalletBusy(true);
    try {
      await api.createWallet(walletCurrency);
      setToast(`${walletCurrency} wallet created.`);
      await loadWorkspace(true);
    } catch (error) {
      setToast(error.message || "Wallet could not be created.");
    } finally {
      setWalletBusy(false);
    }
  }

  async function deleteWallet(wallet) {
    if (!window.confirm(`Permanently delete this unused ${wallet.currency} wallet? Wallets with financial history cannot be deleted.`)) return;
    try {
      await api.deleteWallet(wallet.id);
      setToast("Empty wallet deleted.");
      if (selectedWallet === wallet.id) setSelectedWallet("");
      if (depositWallet === wallet.id) setDepositWallet("");
      setWallets((current) => current.filter((item) => item.id !== wallet.id));
      await loadWorkspace(true);
    } catch (error) {
      setToast(error.message || "Wallet could not be deleted.");
    }
  }

  async function closeWallet(wallet) {
    if (!window.confirm(`Close this ${wallet.currency} wallet? It must have no funds, holds, or open transfers. Its ledger history will be retained.`)) return;
    try {
      await api.closeWallet(wallet.id);
      setToast("Wallet closed. Its ledger history has been retained.");
      await loadWorkspace(true);
    } catch (error) {
      setToast(error.message || "Wallet could not be closed.");
    }
  }

  async function reopenWallet(wallet) {
    if (!window.confirm(`Reopen this ${wallet.currency} wallet? Its existing ledger history will be retained.`)) return;
    try {
      await api.reopenWallet(wallet.id);
      setToast("Wallet reopened.");
      await loadWorkspace(true);
    } catch (error) {
      setToast(error.message || "Wallet could not be reopened.");
    }
  }

  async function submitDeposit(event) {
    event.preventDefault();
    if (!depositWallet) {
      setToast("Select a wallet to receive the deposit.");
      return;
    }
    const [whole, fraction = ""] = depositAmount.trim().split(".");
    if (!/^\d+(\.\d{1,2})?$/.test(depositAmount.trim())) {
      setToast("Enter an amount with up to two decimal places.");
      return;
    }
    const amountMinor = BigInt(whole) * 100n + BigInt(fraction.padEnd(2, "0"));
    if (amountMinor <= 0n || amountMinor > BigInt(Number.MAX_SAFE_INTEGER)) {
      setToast("Deposit amount must be positive and within the safe amount range.");
      return;
    }
    if (!depositConfirmed) {
      setToast("Confirm that this deposit represents external funding.");
      return;
    }
    setDepositBusy(true);
    try {
      const result = await api.deposit(depositWallet, Number(amountMinor), crypto.randomUUID());
      setToast(`Deposit recorded: ${formatMoney(result.credited_amount, result.currency)}.`);
      setDepositAmount("");
      setDepositConfirmed(false);
      await loadWorkspace(true);
    } catch (error) {
      setToast(error.status ? error.message : "The deposit outcome is unknown. Check the wallet ledger before trying again.");
      await loadWorkspace(true);
    } finally {
      setDepositBusy(false);
    }
  }

  async function submitTransfer(event) {
    event.preventDefault();
    if (transferBusy) return;
    const amountText = transferForm.amount.trim();
    if (!/^\d+(\.\d{1,2})?$/.test(amountText)) {
      setToast("Enter an amount with up to two decimal places.");
      return;
    }
    const [whole, fraction = ""] = amountText.split(".");
    const amountMinor = BigInt(whole) * 100n + BigInt(fraction.padEnd(2, "0"));
    if (amountMinor <= 0n || amountMinor > BigInt(Number.MAX_SAFE_INTEGER)) {
      setToast("Transfer amount must be greater than zero.");
      return;
    }
    const amount = Number(amountMinor);
    if (!transferForm.source) {
      setToast("Choose a source wallet.");
      return;
    }
    const source = wallets.find((wallet) => wallet.id === transferForm.source);
    if (!source) {
      setToast("Choose a valid source wallet.");
      return;
    }
    let destination;
    const payload = {
      source_wallet_id: source.id,
      currency: source.currency,
      amount,
    };
    if (transferForm.recipientType === "internal") {
      destination = wallets.find((wallet) => wallet.id === transferForm.destination);
      if (!destination || destination.id === source.id) {
        setToast("Choose a different wallet that you own.");
        return;
      }
      if (destination.currency !== source.currency) {
        setToast("Choose wallets with the same currency.");
        return;
      }
      payload.destination_wallet_id = destination.id;
    } else if (transferForm.recipientType === "wallet_id") {
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(transferForm.recipientWalletId.trim())) {
        setToast("Enter a valid recipient wallet ID.");
        return;
      }
      payload.destination_wallet_id = transferForm.recipientWalletId.trim();
    } else {
      const recipientEmail = transferForm.recipientEmail.trim();
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(recipientEmail)) {
        setToast("Enter a valid recipient email address.");
        return;
      }
      payload.recipient_email = recipientEmail;
    }
    const availableBalance = toMinorBigInt(source.available_balance);
    if (availableBalance === null) {
      setToast("The source balance is outside the safe transfer range. Contact your administrator.");
      return;
    }
    if (amountMinor > availableBalance) {
      setToast("Amount exceeds the source wallet's available balance.");
      return;
    }
    setTransferBusy(true);
    const payloadFingerprint = JSON.stringify(payload);
    const idempotencyKey = transferAttempt?.payloadFingerprint === payloadFingerprint
      ? transferAttempt.key
      : crypto.randomUUID();
    setTransferAttempt({ payloadFingerprint, key: idempotencyKey });
    try {
      const accepted = await api.createTransfer(payload, idempotencyKey);
      transferStatuses.current.set(accepted.transfer_id, accepted.status);
      setWatchedTransferIDs((current) => [...new Set([...current, accepted.transfer_id])]);
      setTransferAttempt(null);
      setToast(`Transfer accepted. Status: ${accepted.status}. Use the transfer list to follow processing.`);
      setTransferForm((current) => ({ ...current, amount: "" }));
      setPage("transfers");
      await loadWorkspace(true);
      await loadTransfers();
    } catch (error) {
      if (error.status && error.status < 500) setTransferAttempt(null);
      setToast(error.status ? error.message : "The request outcome is unknown. Check Transfers before submitting again.");
      await loadWorkspace(true);
      await loadTransfers();
    } finally {
      setTransferBusy(false);
    }
  }

  async function runReconciliation() {
    setRefreshing(true);
    try {
      setReconciliation(await api.reconciliation());
    } catch (error) {
      setToast(error.message || "Reconciliation request failed.");
    } finally {
      setRefreshing(false);
    }
  }

  const totalByCurrency = useMemo(() => wallets.reduce((totals, wallet) => {
    const balance = toMinorBigInt(wallet.available_balance);
    if (totals[wallet.currency] === null || balance === null) {
      totals[wallet.currency] = null;
    } else {
      totals[wallet.currency] = (totals[wallet.currency] || 0n) + balance;
    }
    return totals;
  }, {}), [wallets]);

  if (!accessToken) {
    return <AuthScreen onAuthenticated={(token) => { setRole(getRole(token)); setAccessToken(token); }} />;
  }

  const filteredTransfers = transfers;
  const currentWallet = wallets.find((wallet) => wallet.id === selectedWallet);
  const shownNav = canWrite ? [...navigation, { id: "new-transfer", label: "New transfer", icon: "+" }] : navigation;
  const activeLabel = shownNav.find((item) => item.id === page)?.label
    || (page === "admin" ? "Reconciliation" : page === "users" ? "Company accounts" : page === "company" ? "Company profile" : "Overview");

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-lockup"><span className="brand-mark">H</span><span>HALAAL<small>WALLET & LEDGER</small></span></div>
        <div className="workspace-label">WORKSPACE</div>
        <div className="workspace-card"><span className="workspace-avatar">{(organizationProfile?.organization?.name || email).slice(0, 1).toUpperCase()}</span><span><b>{organizationProfile?.organization?.name || "Your company"}</b><small>{email}</small></span><span className="chevron">⌄</span></div>
        <nav className="side-nav" aria-label="Main navigation">
          <div className="nav-caption">FINANCE</div>
          {shownNav.map((item) => (
            <button key={item.id} className={`nav-item ${page === item.id ? "active" : ""}`} onClick={() => setPage(item.id)}>
              <span className="nav-icon">{item.icon}</span>{item.label}
              {item.id === "transfers" && transfers.some((transfer) => ["PENDING", "PROCESSING"].includes(transfer.status)) && <span className="nav-dot" />}
            </button>
          ))}
          {canAudit && <><div className="nav-caption spaced">OPERATIONS</div><button className={`nav-item ${page === "admin" ? "active" : ""}`} onClick={() => setPage("admin")}><span className="nav-icon">⌁</span>Reconciliation</button></>}
          {canManageUsers && <button className={`nav-item ${page === "users" ? "active" : ""}`} onClick={() => setPage("users")}><span className="nav-icon">♙</span>Company accounts</button>}
          <button className={`nav-item ${page === "company" ? "active" : ""}`} onClick={() => setPage("company")}><span className="nav-icon">⌂</span>Company profile</button>
        </nav>
        <div className="sidebar-bottom">
          <div className="secure-note"><span>◈</span><div><b>Secure ledger</b><small>Protected with double-entry controls</small></div></div>
          <button className="nav-item signout" onClick={() => signOut()}><span className="nav-icon">↪</span>Sign out</button>
          <div className="version">HALAAL FINANCIAL <span>•</span> LOCAL</div>
        </div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div className="breadcrumbs"><span>Workspace</span><span>/</span><strong>{activeLabel}</strong></div>
          <div className="topbar-actions"><span className="live-status"><i /> Services connected</span><button className="icon-button" aria-label="Refresh data" onClick={() => loadWorkspace(true)} disabled={refreshing}>↻</button><span className="top-avatar">{email.slice(0, 1).toUpperCase()}</span></div>
        </header>
        <div className="page-content">
          {loadError && <div className="alert error page-alert" role="alert">{loadError} <button className="text-button" onClick={() => loadWorkspace(true)}>Retry</button></div>}
          {loading ? <LoadingState /> : (
            <>
              {page === "overview" && <Overview wallets={wallets} totals={totalByCurrency} transfers={transfers} ledger={ledger} onGo={setPage} selectedWallet={selectedWallet} onSelectWallet={setSelectedWallet} canWrite={canWrite} organization={organizationProfile?.organization} memberCount={organizationProfile?.member_count} />}
              {page === "wallets" && <WalletsPage wallets={wallets} selectedWallet={selectedWallet} onViewLedger={(walletId) => { setSelectedWallet(walletId); setPage("activity"); }} onCreate={createWallet} onDelete={deleteWallet} onClose={closeWallet} onReopen={reopenWallet} currency={walletCurrency} setCurrency={setWalletCurrency} busy={walletBusy} cursor={walletCursor} onLoadMore={loadMoreWallets} canWrite={canWrite} onDeposit={submitDeposit} depositWallet={depositWallet} setDepositWallet={setDepositWallet} depositAmount={depositAmount} setDepositAmount={setDepositAmount} depositConfirmed={depositConfirmed} setDepositConfirmed={setDepositConfirmed} depositBusy={depositBusy} />}
              {page === "activity" && <ActivityPage wallets={wallets} selectedWallet={selectedWallet} onSelect={setSelectedWallet} ledger={ledger} />}
              {page === "transfers" && <TransfersPage transfers={filteredTransfers} status={transferStatus} onStatus={setTransferStatus} onRefresh={async () => { await loadWorkspace(true); await loadTransfers(); }} onLoadMore={loadMoreTransfers} cursor={transferCursor} onNew={() => setPage("new-transfer")} onSelectTransfer={showTransferDetails} detail={selectedTransfer} detailBusy={transferDetailBusy} canWrite={canWrite} />}
              {page === "new-transfer" && canWrite && <TransferPage wallets={wallets} form={transferForm} setForm={setTransferForm} onSubmit={submitTransfer} busy={transferBusy} />}
              {page === "admin" && canAudit && <ReconciliationPage report={reconciliation} onRun={runReconciliation} busy={refreshing} />}
              {page === "users" && canManageUsers && <CompanyUsersPage users={organizationUsers} currentUserID={currentUserID} cursor={organizationUserCursor} loading={organizationUsersLoading} error={organizationUsersError} onRefresh={() => loadOrganizationUsers()} onLoadMore={loadMoreOrganizationUsers} onToggle={updateOrganizationUser} />}
              {page === "company" && <CompanyProfilePage profile={organizationProfile} members={organizationMembers} cursor={organizationMemberCursor} loading={organizationProfileLoading} error={organizationProfileError} onRefresh={() => loadOrganizationProfile()} onLoadMore={loadMoreOrganizationMembers} />}
            </>
          )}
        </div>
        <footer className="page-footer"><span>© {new Date().getFullYear()} HALAAL Financial</span><span>Balances shown in minor units converted for display · Backend is authoritative</span></footer>
      </main>
      {toast && <div className="toast" role="status">{toast}<button aria-label="Dismiss notification" onClick={() => setToast("")}>×</button></div>}
    </div>
  );
}

function LoadingState() {
  return <div className="loading-state" aria-label="Loading account data"><span /><span /><span /><p>Loading your workspace…</p></div>;
}

function PageHeading({ eyebrow, title, description, action }) {
  return <div className="page-heading"><div><div className="eyebrow">{eyebrow}</div><h1>{title}</h1><p>{description}</p></div>{action}</div>;
}

function Overview({ wallets, totals, transfers, ledger, onGo, selectedWallet, onSelectWallet, canWrite, organization, memberCount }) {
  const inFlight = transfers.filter((item) => ["PENDING", "PROCESSING"].includes(item.status)).length;
  const complete = transfers.filter((item) => item.status === "COMPLETED").length;
  return (
    <>
      <PageHeading eyebrow="YOUR FINANCIAL OVERVIEW" title="Good to see you." description="Here’s what’s happening across your wallet workspace." action={canWrite && <button className="button primary" onClick={() => onGo("new-transfer")}>＋ New transfer</button>} />
      {organization && <section className="company-summary"><div className="company-summary-icon">⌂</div><div><span className="eyebrow">YOUR COMPANY</span><h2>{organization.name}</h2><p>{memberCount} member{memberCount === 1 ? "" : "s"} · Workspace created {formatDate(organization.created_at)}</p></div><button className="button secondary" onClick={() => onGo("company")}>View profile & members</button></section>}
      <div className="overview-banner"><div className="banner-copy"><span className="banner-label"><i /> LIVE ACCOUNT SUMMARY</span><h2>Your money, <em>in view.</em></h2><p>Balances and activity are retrieved directly from your organization ledger.</p></div><div className="banner-art"><div className="art-ring ring-one" /><div className="art-ring ring-two" /><div className="art-card"><span>AVAILABLE BALANCE</span><b>{Object.keys(totals).length ? Object.entries(totals).map(([currency, value]) => formatMoney(value, currency)).join("  ·  ") : formatMoney(0)}</b><small>Across {wallets.length} loaded wallet{wallets.length === 1 ? "" : "s"}</small></div><span className="art-orb">✳</span></div></div>
      <div className="stats-grid">
        {Object.entries(totals).length ? Object.entries(totals).map(([currency, amount]) => <StatCard key={currency} label={`${currency} AVAILABLE`} value={formatMoney(amount, currency)} detail="Sum of wallet available balances" icon="◈" />) : <StatCard label="AVAILABLE BALANCE" value={formatMoney(0)} detail="No customer wallets available" icon="◈" />}
        <StatCard label="WALLETS" value={wallets.length} detail="Customer wallets loaded" icon="▣" />
        <StatCard label="IN PROGRESS" value={inFlight} detail="Pending or processing" icon="◷" accent />
        <StatCard label="COMPLETED" value={complete} detail="In the latest 50 transfers" icon="✓" />
      </div>
      <div className="content-grid">
        <section className="panel recent-panel"><div className="panel-heading"><div><h3>Recent transfers</h3><p>Latest activity reported by the backend</p></div><button className="link-button" onClick={() => onGo("transfers")}>View all <span>→</span></button></div><TransferTable transfers={transfers.slice(0, 5)} compact /></section>
        <section className="panel wallet-panel"><div className="panel-heading"><div><h3>Your wallets</h3><p>Live organization balances</p></div><button className="link-button" onClick={() => onGo("wallets")}>View all <span>→</span></button></div>{wallets.length ? wallets.slice(0, 4).map((wallet) => <WalletRow key={wallet.id} wallet={wallet} onClick={() => { onSelectWallet(wallet.id); onGo("activity"); }} />) : <EmptyState title="No wallets yet" text="Create a wallet to get started." />}</section>
      </div>
      <div className="panel ledger-preview"><div className="panel-heading"><div><h3>Latest ledger entries</h3><p>{wallets.find((wallet) => wallet.id === selectedWallet)?.currency || "Select a wallet"} · Immutable account activity</p></div><button className="link-button" onClick={() => onGo("activity")}>Open ledger <span>→</span></button></div><LedgerTable entries={ledger.slice(0, 5)} wallets={wallets} /></div>
    </>
  );
}

function CompanyProfilePage({ profile, members, cursor, loading, error, onRefresh, onLoadMore }) {
  const organization = profile?.organization;
  return (
    <>
      <PageHeading
        eyebrow="YOUR WORKSPACE"
        title="Company profile"
        description="Company details and members who share your organization."
        action={<button className="button secondary" onClick={onRefresh} disabled={loading}>↻ Refresh</button>}
      />
      {error && <div className="alert error page-alert" role="alert">{error} <button className="text-button" onClick={onRefresh}>Retry</button></div>}
      {organization && (
        <section className="company-profile-card">
          <div className="company-summary-icon">⌂</div>
          <div className="company-profile-details">
            <span className="eyebrow">COMPANY</span>
            <h2>{organization.name}</h2>
            <p>Organization ID <span className="transaction-id">{organization.id}</span></p>
            <p>Workspace created {formatDate(organization.created_at)}</p>
          </div>
          <div className="company-member-count"><strong>{profile.member_count}</strong><span>members</span></div>
        </section>
      )}
      <section className="panel company-members-panel">
        <div className="panel-heading"><div><h3>Company members</h3><p>Only accounts in your company are shown.</p></div></div>
        {loading && !members.length ? <div className="detail-loading">Loading company members…</div> : members.length ? (
          <div className="table-scroll">
            <table>
              <thead><tr><th>NAME</th><th>EMAIL</th><th>ROLE</th></tr></thead>
              <tbody>{members.map((member) => (
                <tr key={member.id}>
                  <td>{member.name}</td>
                  <td><span className="transaction-id">{member.email}</span></td>
                  <td><span className={`role-label ${member.role === "admin" ? "role-admin" : ""}`}>{member.role === "admin" ? "Admin" : "Member"}</span></td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        ) : !error ? <EmptyState title="No company members" text="No members are currently associated with this company." /> : null}
        {cursor && <div className="load-more"><button className="button secondary" onClick={onLoadMore} disabled={loading}>{loading ? "Loading…" : "Load more members"}</button></div>}
      </section>
    </>
  );
}

function StatCard({ label, value, detail, icon, accent }) {
  return <div className={`stat-card ${accent ? "stat-accent" : ""}`}><div className="stat-top"><span>{label}</span><b>{icon}</b></div><strong>{value}</strong><small>{detail}</small></div>;
}

function WalletRow({ wallet, onClick }) {
  return <button className="wallet-row" onClick={onClick}><span className={`currency-icon ${wallet.currency.toLowerCase()}`}>{wallet.currency === "EUR" ? "€" : "$"}</span><span className="wallet-row-info"><b>{wallet.currency} wallet</b><small>{wallet.id.slice(0, 8)}…{wallet.id.slice(-4)} · {wallet.status}</small></span><span className="wallet-row-balance">{formatMoney(wallet.available_balance, wallet.currency)}<small>available</small></span><span className="row-arrow">→</span></button>;
}

function WalletsPage({
  wallets, selectedWallet, onViewLedger, onCreate, onDelete, onClose, onReopen, currency, setCurrency,
  busy, cursor, onLoadMore, canWrite, onDeposit, depositWallet, setDepositWallet,
  depositAmount, setDepositAmount, depositConfirmed, setDepositConfirmed, depositBusy,
}) {
  const activeWallets = wallets.filter((wallet) => wallet.status === "active");
  return (
    <>
      <PageHeading eyebrow="ACCOUNTS" title="Wallets" description="Review balances or open a wallet to inspect its ledger." action={canWrite && <button className="button primary" onClick={() => document.getElementById("create-wallet")?.scrollIntoView({ behavior: "smooth" })}>＋ Create wallet</button>} />
      <div className="wallet-cards">
        {wallets.map((wallet) => (
          <article key={wallet.id} className={`wallet-card ${selectedWallet === wallet.id ? "selected" : ""}`}>
            <div className="wallet-card-top">
              <span className={`currency-icon ${wallet.currency.toLowerCase()}`}>{wallet.currency === "EUR" ? "€" : "$"}</span>
              <span className={`status-pill status-${wallet.status}`}>{wallet.status}</span>
            </div>
            <span className="eyebrow">{wallet.currency} CUSTOMER WALLET</span>
            <strong>{formatMoney(wallet.available_balance, wallet.currency)}</strong>
            <div className="wallet-breakdown">
              <span>Current balance <b>{formatMoney(wallet.balance, wallet.currency)}</b></span>
              <span>On hold <b>{formatMoney(wallet.hold_balance, wallet.currency)}</b></span>
            </div>
            <span className="wallet-id">{wallet.id}</span>
            <div className="wallet-actions">
              <button className="button secondary small" onClick={() => onViewLedger(wallet.id)}>View ledger</button>
              {canWrite && wallet.status === "closed" && <button className="button secondary small" onClick={() => onReopen(wallet)}>Reopen</button>}
              {canWrite && wallet.status !== "closed" && <button className="button secondary small" onClick={() => onClose(wallet)}>Close</button>}
              {canWrite && <button className="button danger small" onClick={() => onDelete(wallet)}>Delete</button>}
            </div>
          </article>
        ))}
      </div>
      {!wallets.length && <div className="panel"><EmptyState title="No wallets found" text="Create a customer wallet below." /></div>}
      {cursor && <div className="load-more"><button className="button secondary" onClick={onLoadMore}>Load more wallets</button></div>}

      {canWrite && (
        <section className="wallet-tools">
          <div className="panel create-wallet-panel" id="create-wallet">
            <div className="panel-heading"><div><h3>Create a wallet</h3><p>Customer wallet for a supported currency. New wallets start at zero.</p></div></div>
            <form className="inline-form" onSubmit={onCreate}>
              <label>Currency<select value={currency} onChange={(event) => setCurrency(event.target.value)}><option value="USD">US Dollar (USD)</option><option value="EUR">Euro (EUR)</option></select></label>
              <button className="button primary" disabled={busy}>{busy ? "Creating…" : "Create wallet"}</button>
            </form>
          </div>
          <div className="panel deposit-panel">
            <div className="panel-heading"><div><h3>Record an external deposit</h3><p>For local testing or authorized manual ledger entries.</p></div><span className="demo-badge">ADMIN / FINANCE</span></div>
            <div className="alert warning">This project does not connect to a bank or card processor. Submitting here records an external-funding ledger credit; it does not collect or verify real money. Use only in a controlled test environment unless a verified funding provider is integrated.</div>
            {activeWallets.length ? (
              <form className="deposit-form" onSubmit={onDeposit}>
                <div className="inline-form">
                  <label>Wallet<select value={depositWallet} onChange={(event) => setDepositWallet(event.target.value)} required>{activeWallets.map((wallet) => <option key={wallet.id} value={wallet.id}>{wallet.currency} · {wallet.id.slice(0, 8)}…</option>)}</select></label>
                  <label>Amount<input aria-label="Deposit amount" inputMode="decimal" placeholder="0.00" value={depositAmount} onChange={(event) => setDepositAmount(event.target.value)} required /></label>
                  <button className="button primary" disabled={depositBusy}>{depositBusy ? "Recording…" : "Record deposit"}</button>
                </div>
                <label className="confirm-label"><input type="checkbox" checked={depositConfirmed} onChange={(event) => setDepositConfirmed(event.target.checked)} />I confirm this is an authorized external-funding test/manual entry, not a payment being collected by this app.</label>
              </form>
            ) : <EmptyState title="No active wallet" text="Create an active wallet before recording a test deposit." />}
          </div>
        </section>
      )}
      <p className="wallet-retention-note">Financial history is append-only. Deleting permanently removes an unused wallet only; wallets with financial history cannot be deleted. Close an empty wallet to retain its ledger, then reopen it when needed.</p>
    </>
  );
}

function ActivityPage({ wallets, selectedWallet, onSelect, ledger }) {
  return <><PageHeading eyebrow="DOUBLE-ENTRY RECORD" title="Ledger activity" description="Append-only entries for a wallet, retrieved from the authoritative ledger." /><section className="panel"><div className="panel-heading"><div><h3>Account entries</h3><p>Choose the account whose journal entries you want to inspect.</p></div><label className="select-inline"><span className="sr-only">Select wallet</span><select value={selectedWallet} onChange={(event) => onSelect(event.target.value)}><option value="">Select wallet</option>{wallets.map((wallet) => <option key={wallet.id} value={wallet.id}>{wallet.currency} · {wallet.id.slice(0, 8)}…</option>)}</select></label></div>{selectedWallet ? <LedgerTable entries={ledger} wallets={wallets} /> : <EmptyState title="Choose a wallet" text="Select a wallet to view its ledger entries." />}</section></>;
}

function LedgerTable({ entries, wallets }) {
  if (!entries.length) return <EmptyState title="No ledger entries" text="Entries will appear here when this wallet has recorded activity." />;
  return <div className="table-scroll"><table><thead><tr><th>JOURNAL ENTRY</th><th>DIRECTION</th><th>AMOUNT</th><th>RECORDED</th></tr></thead><tbody>{entries.map((entry) => { const wallet = wallets.find((item) => item.id === entry.wallet_id); return <tr key={entry.id}><td><span className="transaction-id">{entry.journal_entry_id.slice(0, 8)}…{entry.journal_entry_id.slice(-4)}</span><small className="table-sub">{entry.id}</small></td><td><span className={`direction ${entry.direction === "CR" ? "credit" : "debit"}`}>{entry.direction === "CR" ? "Credit" : "Debit"}</span></td><td className="amount-cell">{formatMoney(entry.amount, wallet?.currency || "USD")}</td><td>{formatDate(entry.created_at)}</td></tr>; })}</tbody></table></div>;
}

function TransfersPage({ transfers, status, onStatus, onRefresh, onLoadMore, cursor, onNew, onSelectTransfer, detail, detailBusy, canWrite }) {
  return <><PageHeading eyebrow="PAYMENT OPERATIONS" title="Transaction history" description="Review sender and receiver accounts, authoritative status, and ledger postings." action={canWrite && <button className="button primary" onClick={onNew}>＋ New transfer</button>} /><section className="panel"><div className="panel-heading"><div><h3>Transactions</h3><p>Paginated results · status and amounts are authoritative from PostgreSQL</p></div><div className="table-actions"><select aria-label="Filter by transfer status" value={status} onChange={(event) => onStatus(event.target.value)}><option value="">All statuses</option><option value="PENDING">Pending</option><option value="PROCESSING">Processing</option><option value="COMPLETED">Completed</option><option value="FAILED">Failed</option><option value="REVERSED">Reversed</option></select><button className="button secondary small" onClick={onRefresh}>↻ Refresh</button></div></div><TransferTable transfers={transfers} onSelect={onSelectTransfer} />{cursor && <div className="load-more"><button className="button secondary" onClick={onLoadMore}>Load more transactions</button></div>}</section>{detailBusy && <div className="panel detail-loading">Loading transaction detail…</div>}{detail && <TransferDetail transfer={detail} />}</>;
}

function TransferTable({ transfers, compact = false, onSelect }) {
  if (!transfers.length) return <EmptyState title="No transfers to show" text="When a transfer is submitted, its actual processing status will appear here." />;
  return <div className="table-scroll"><table><thead><tr><th>TRANSACTION</th>{compact ? <th>WALLETS</th> : <><th>SENDER</th><th>RECEIVER</th></>}<th>AMOUNT</th><th>STATUS</th><th>{compact ? "UPDATED" : "DATE & TIME"}</th></tr></thead><tbody>{transfers.map((transfer) => <tr key={transfer.id}><td>{onSelect ? <button className="transaction-link" onClick={() => onSelect(transfer.id)}>{transfer.id.slice(0, 8)}…{transfer.id.slice(-4)}</button> : <span className="transaction-id">{transfer.id.slice(0, 8)}…{transfer.id.slice(-4)}</span>}{!compact && transfer.failure_reason && <small className="failure-detail">{transfer.failure_reason}</small>}</td>{compact ? <td><span className="account-id">{transfer.source_wallet_id.slice(0, 8)} → {transfer.destination_wallet_id.slice(0, 8)}</span></td> : <><td><span className="account-id">{transfer.source_wallet_id}</span><small className="table-sub">Debited wallet</small></td><td><span className="account-id">{transfer.destination_wallet_id}</span><small className="table-sub">Credited wallet</small></td></>}<td className="amount-cell">{formatMoney(transfer.amount, transfer.currency)}{Number(transfer.fee_amount) > 0 && <small className="table-sub">Fee {formatMoney(transfer.fee_amount, transfer.currency)}</small>}</td><td><StatusPill status={transfer.status} /></td><td>{formatDate(compact ? transfer.updated_at : transfer.created_at)}</td></tr>)}</tbody></table></div>;
}

function TransferDetail({ transfer }) {
  const sender = transfer.sender_account || { wallet_id: transfer.source_wallet_id, currency: transfer.currency };
  const receiver = transfer.receiver_account || { wallet_id: transfer.destination_wallet_id, currency: transfer.currency };
  const entries = transfer.ledger_entries || [];
  return <section className="panel transfer-detail">
    <div className="panel-heading"><div><h3>Transaction details</h3><p className="detail-transaction-id">{transfer.id}</p></div><StatusPill status={transfer.status} /></div>
    <div className="detail-grid"><span>Amount<b>{formatMoney(transfer.amount, transfer.currency)}</b></span><span>Fee<b>{formatMoney(transfer.fee_amount, transfer.currency)}</b></span><span>Status<b>{transfer.status || "UNKNOWN"}</b></span><span>Created<b>{formatDate(transfer.created_at)}</b></span></div>
    <div className="transfer-accounts">
      <AccountSummary title="Sender · debited" account={sender} />
      <AccountSummary title="Receiver · credited" account={receiver} />
    </div>
    <div className="ledger-postings">
      <div className="panel-heading"><div><h3>Debit and credit ledger entries</h3><p>Immutable postings returned from the transaction’s organization-scoped journals.</p></div></div>
      {entries.length ? <div className="table-scroll"><table><thead><tr><th>ACCOUNT</th><th>JOURNAL ENTRY</th><th>DIRECTION</th><th>AMOUNT</th><th>RECORDED</th></tr></thead><tbody>{entries.map((entry) => <tr key={entry.id}><td><span className="account-id">{entry.wallet_id}</span><small className="table-sub">{entry.account_type} account</small></td><td><span className="transaction-id">{entry.journal_entry_id}</span><small className="table-sub">{entry.reference_type}{entry.description ? ` · ${entry.description}` : ""}</small></td><td><span className={`direction ${entry.direction === "CR" ? "credit" : "debit"}`}>{entry.direction === "CR" ? "Credit" : "Debit"}</span></td><td className="amount-cell">{formatMoney(entry.amount, entry.currency || transfer.currency)}</td><td>{formatDate(entry.created_at)}</td></tr>)}</tbody></table></div> : <EmptyState title="No ledger postings yet" text="Entries appear once the transaction is posted by the ledger." />}
    </div>
    {transfer.failure_reason && <div className="alert error">{transfer.failure_reason}</div>}
  </section>;
}

function AccountSummary({ title, account }) {
  return <div className="account-summary"><span>{title}</span><strong>{account.wallet_id}</strong><small>{account.account_type || "Customer"} account · {account.currency}</small></div>;
}

function StatusPill({ status }) {
  return <span className={`status-pill status-${String(status || "").toLowerCase()}`}><i />{status || "UNKNOWN"}</span>;
}

function TransferPage({ wallets, form, setForm, onSubmit, busy }) {
  const source = wallets.find((wallet) => wallet.id === form.source);
  const validDestinations = wallets.filter((wallet) => wallet.id !== form.source && wallet.currency === source?.currency);
  const internal = form.recipientType === "internal";
  const recipientWallet = form.recipientType === "wallet_id";
  return (
    <>
      <PageHeading eyebrow="MOVE FUNDS" title="New transfer" description="Transfers are submitted to a saga and may remain pending while processing." />
      <div className="transfer-layout">
        <section className="panel transfer-form-panel">
          <div className="panel-heading"><div><h3>Transfer details</h3><p>Send between your wallets or to another registered user, including users in other organizations.</p></div></div>
          <form className="form-stack" onSubmit={onSubmit}>
            <label>
              From wallet
              <select required value={form.source} onChange={(event) => setForm((value) => ({ ...value, source: event.target.value, destination: "" }))}>
                <option value="">Choose source wallet</option>
                {wallets.map((wallet) => <option key={wallet.id} value={wallet.id} disabled={wallet.status !== "active"}>{wallet.currency} · {wallet.id.slice(0, 8)}… · Available {formatMoney(wallet.available_balance, wallet.currency)}</option>)}
              </select>
            </label>
            <label>
              Transfer to
              <select value={form.recipientType || "internal"} onChange={(event) => setForm((value) => ({ ...value, recipientType: event.target.value, destination: "" }))}>
                <option value="internal">My other wallet</option>
                <option value="wallet_id">Another user's wallet ID</option>
                <option value="email">Another registered user by email</option>
              </select>
            </label>
            {internal && (
              <label>
                To wallet
                <select required value={form.destination} onChange={(event) => setForm((value) => ({ ...value, destination: event.target.value }))}>
                  <option value="">Choose destination wallet</option>
                  {validDestinations.map((wallet) => <option key={wallet.id} value={wallet.id} disabled={wallet.status !== "active"}>{wallet.currency} · {wallet.id.slice(0, 8)}…</option>)}
                </select>
              </label>
            )}
            {recipientWallet && (
              <label>
                Recipient wallet ID
                <input value={form.recipientWalletId || ""} onChange={(event) => setForm((value) => ({ ...value, recipientWalletId: event.target.value }))} required placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" />
              </label>
            )}
            {form.recipientType === "email" && (
              <label>
                Recipient email
                <input type="email" value={form.recipientEmail || ""} onChange={(event) => setForm((value) => ({ ...value, recipientEmail: event.target.value }))} required placeholder="recipient@example.com" />
              </label>
            )}
            <label>
              Amount
              <input aria-label="Amount" inputMode="decimal" placeholder="0.00" value={form.amount} onChange={(event) => setForm((value) => ({ ...value, amount: event.target.value }))} required aria-describedby="amount-help" />
              <small id="amount-help">Enter major currency units, up to 2 decimal places. The backend checks available funds including the applicable fee.</small>
            </label>
            <div className="transfer-confirm"><span>Transfer currency<b>{source?.currency || "—"}</b></span><span>Available balance<b>{source ? formatMoney(source.available_balance, source.currency) : "—"}</b></span></div>
            <button className="button primary full" disabled={busy || !wallets.length}>{busy ? "Submitting transfer…" : "Review and submit transfer →"}</button>
            <p className="legal-note">A unique idempotency key is sent with this request. If the network times out, check transfer history before trying again.</p>
          </form>
        </section>
        <aside className="transfer-aside">
          <div className="aside-icon">↗</div><h3>How transfers work</h3>
          <p>Every transfer is queued for asynchronous risk checks and saga processing.</p>
          <ul><li>Funds are reserved at submission.</li><li>The backend determines the final status.</li><li>Pending is not the same as completed.</li></ul>
          <div className="aside-note">Cross-organization transfers require the same currency and are recorded in balanced journals for both organizations.</div>
        </aside>
      </div>
    </>
  );
}

function ReconciliationPage({ report, onRun, busy }) {
  return <><PageHeading eyebrow="LEDGER INTEGRITY" title="Reconciliation" description="Run an organization-scoped audit against wallet and journal invariants." action={<button className="button primary" onClick={onRun} disabled={busy}>{busy ? "Running audit…" : "Run reconciliation"}</button>} /><section className="panel reconciliation-panel">{report ? <><div className="reconciliation-result"><span className={report.status === "BALANCED" ? "result-check" : "result-warning"}>{report.status === "BALANCED" ? "✓" : "!"}</span><div><span className="eyebrow">LATEST AUDIT RESULT</span><h2>{report.status === "BALANCED" ? "Ledger is balanced" : "Discrepancies detected"}</h2><p>{report.discrepancies_count} discrepancy(ies) · {formatDate(report.timestamp)}</p></div></div>{report.discrepancies?.length > 0 && <pre className="discrepancies">{JSON.stringify(report.discrepancies, null, 2)}</pre>}</> : <EmptyState title="No audit run in this session" text="Run reconciliation to request an organization-scoped integrity report." />}</section><p className="muted">Reconciliation results are live backend data. This page does not alter financial records.</p></>;
}

function CompanyUsersPage({ users, currentUserID, cursor, loading, error, onRefresh, onLoadMore, onToggle }) {
  const activeCount = users.filter((user) => user.is_active).length;
  return (
    <>
      <PageHeading
        eyebrow="ADMINISTRATION"
        title="Company accounts"
        description="Manage access for users in your organization. Deactivation preserves wallets and all financial history."
        action={<button className="button secondary" onClick={onRefresh} disabled={loading}>↻ Refresh</button>}
      />
      {error && <div className="alert error page-alert" role="alert">{error} <button className="text-button" onClick={onRefresh}>Retry</button></div>}
      <section className="panel">
        <div className="panel-heading">
          <div><h3>Organization users</h3><p>{activeCount} active account(s) in this page</p></div>
        </div>
        {loading && !users.length ? <div className="detail-loading">Loading company accounts…</div> : users.length ? (
          <div className="table-scroll">
            <table>
              <thead><tr><th>ACCOUNT</th><th>ROLE</th><th>WALLETS</th><th>STATUS</th><th>JOINED</th><th>ACTION</th></tr></thead>
              <tbody>
                {users.map((user) => {
                  const isSelf = user.id === currentUserID;
                  return (
                    <tr key={user.id}>
                      <td><span className="transaction-id">{user.email}</span>{isSelf && <small className="table-sub">You</small>}</td>
                      <td><span className="role-label">{user.role}</span></td>
                      <td>{user.wallet_count}</td>
                      <td><span className={`status-pill status-${user.is_active ? "active" : "suspended"}`}>{user.is_active ? "Active" : "Deactivated"}</span></td>
                      <td>{formatDate(user.created_at)}</td>
                      <td>{isSelf
                        ? <span className="muted">Current admin</span>
                        : <button className={`button small ${user.is_active ? "danger" : "secondary"}`} onClick={() => onToggle(user)} disabled={loading}>{user.is_active ? "Deactivate" : "Reactivate"}</button>}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : !error ? <EmptyState title="No company accounts" text="There are no users in this organization." /> : null}
        {cursor && <div className="load-more"><button className="button secondary" onClick={onLoadMore} disabled={loading}>{loading ? "Loading…" : "Load more accounts"}</button></div>}
      </section>
      <p className="wallet-retention-note">Deactivated users cannot sign in or receive new transfers. Their wallets and transaction records are retained for audit. The organization must always have at least one active admin.</p>
    </>
  );
}

function EmptyState({ title, text }) {
  return <div className="empty-state"><span>◇</span><h4>{title}</h4><p>{text}</p></div>;
}

export default App;
