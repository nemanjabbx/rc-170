# RingCentral Availability API

A lightweight Node.js server that acts as a middleware between **Ringba** and **RingCentral**. Ringba pings this API to check whether agents are available to take calls for a given state before routing traffic.

## How It Works

1. Ringba sends a GET request to `/availability?state=TX`
2. The server authenticates with RingCentral using JWT
3. It finds the call queue matching the state name (e.g. `Texas`)
4. It checks presence status of all queue members in parallel
5. Returns `{ "available": true }` if at least one agent is: **Available + TakeAllCalls + NoCall**

---

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check — returns `{"status":"ok"}` |
| `GET /availability?state=TX` | State-based availability check (2-letter state code) |
| `GET /agent?id=x7k2m` | Agent-specific availability check by agent ID |

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RC_CLIENT_ID` | RingCentral App Client ID |
| `RC_CLIENT_SECRET` | RingCentral App Client Secret |
| `RC_JWT` | RingCentral JWT token (linked to the app above) |

---

## Setup Guide

### 1. RingCentral — Create App & Get Credentials

1. Go to **https://developers.ringcentral.com** → Login
2. **Console → Apps → Create App**
   - App Type: **REST API App**
   - Auth: **JWT auth flow** ← required
   - Issue refresh tokens: **Off**
   - Who can access: **Private**
   - Scopes: `Read Accounts`, `Read Presence`, `Call Queues`
3. After creation → **Credentials tab** → copy `Client ID` and `Client Secret`
4. In the same tab → click **"Create JWT"** → copy the token (starts with `eyJ...`)
   - If no JWT button: go to **https://developers.ringcentral.com/console/jwt** → Create JWT → select your app under Authorized Apps

> **Important:** The app must be created on the same RingCentral account where call queues are defined (Michigan, Florida, etc.). Otherwise all checks will return `available: false`.

---

### 2. Render — Deploy the Server

1. Go to **https://render.com** → Sign up (GitHub recommended)
2. **New → Web Service** → connect your GitHub repo
3. Configure:
   - **Runtime:** Node
   - **Build Command:** `npm install`
   - **Start Command:** `node index.js`
   - **Instance Type:** Starter ($7/mo) — required for always-on, no cold starts
4. **Environment tab → Add variables:**

   | KEY | VALUE |
   |-----|-------|
   | `RC_CLIENT_ID` | from step 1 |
   | `RC_CLIENT_SECRET` | from step 1 |
   | `RC_JWT` | from step 1 |

5. Click **Save Changes** → auto redeploy

> **Do not use Free plan in production.** Free tier sleeps after 15 min idle → 30–60s cold start → Ringba timeout.

---

### 3. Ringba — Configure Availability Check

In Ringba → Target → **Availability Check:**
- **URL:** `https://your-service.onrender.com/availability?state={state}`
- **Method:** GET
- **Available condition:** response body contains `"available":true`

---

### 4. Test

```bash
# Health check
curl https://your-service.onrender.com/health
# → {"status":"ok","message":"Availability API is running"}

# State check
curl https://your-service.onrender.com/availability?state=MI
# → {"available":true,"agents":5,"state":"MI","queue":"Michigan","total_members":7}

# Agent check
curl https://your-service.onrender.com/agent?id=x7k2m
# → {"available":true,"agents":1,"total_members":1}
```

---

## Token Expiry & Renewal

JWT tokens can expire or be invalidated. If you see this error:
```
{"available":false,"error":"No access token: {\"invalid_grant\"..."}
```

**Fix:**
1. Go to RC Developer Portal → your app → Credentials → Create new JWT
2. Update `RC_JWT` in Render → Environment → Save
3. Render will auto-redeploy

---

## File Structure

```
index.js      — Main server (state + agent availability, port 3000)
agents.js     — Agent-only server variant (port 3001)
package.json  — Node dependencies
```

