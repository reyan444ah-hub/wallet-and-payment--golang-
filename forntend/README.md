# Wallet dashboard frontend

The existing directory name is `forntend`. The app uses React and Vite and
proxies `/api` requests to the local Go API at `http://localhost:8080`.
No API or authentication secrets belong in Vite environment variables.

## Run locally

Start the backend services from `backend\` first:

```powershell
docker compose --env-file .env -f deployments\docker-compose.yml up -d
```

In another PowerShell window:

```powershell
Set-Location 'C:\Users\Hashim Traders\Documents\Task (Go lang)\wallet and payment\forntend'
npm.cmd ci
npm.cmd run dev
```

Open the Vite URL printed in the console, normally `http://localhost:5173`.
To proxy to a different API host, set `VITE_API_PROXY_TARGET` for the Vite
process. For a deployed frontend, set `VITE_API_BASE_URL` to the browser-
reachable API origin and configure that API deployment's CORS policy.

## Checks

```powershell
npm.cmd test
npm.cmd run build
npm.cmd audit
```

The interface uses the real API for authentication, wallet and transfer
collections, wallet ledger entries, transfer creation and reconciliation.
Amounts use backend integer minor units. The dashboard presents only the
wallet and transfer pages returned by the API; it does not invent totals or
transaction states.

The transfer form supports moves between the signed-in user's own wallets and
payments to another registered user by wallet ID or email. Email resolution
selects a single active wallet in the source currency; users with multiple
matching wallets must provide a wallet ID. User-to-user transfers can cross
organization boundaries; the recipient wallet must be active and use the same
currency. The transfer is recorded in balanced journals in both organizations.

Pending and processing transfers are checked automatically every two seconds.
The transfer list and open transfer details update when the backend reports a
terminal state, and the dashboard displays a completion or failure notification.

Admins and finance users can create wallets and record a manual external-
funding ledger credit. That deposit endpoint does not process a bank/card
payment; the UI warns and requires explicit confirmation. For real deposits,
integrate a payment provider and verified settlement/webhook flow first.

Wallets with no balance, holds, ledger entries, or transfers can be permanently
deleted. A used wallet is retained for audit and can only be closed when its
balance and holds are zero and it has no pending, processing, or completed
transfers. Closing is not physical deletion.
