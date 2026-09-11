# SOP: Agent Availability Ping Setup
**Last updated:** June 2026  
**Applies to:** All offices using RingCentral + Ringba

---

## What Is This?

The Agent Availability API checks whether agents are currently available to take calls in a specific RingCentral queue. Ringba uses these URLs to decide whether to route a call to that queue.

**API Base URL:** `https://ringcentral-availability-api.onrender.com`

---

## How to Build a Ping URL

### Main Office (Orlando)
```
https://ringcentral-availability-api.onrender.com/availability?state=XX
```
Replace `XX` with the 2-letter state code.

**Examples:**
- Florida: `?state=FL`
- Texas: `?state=TX`
- New York: `?state=NY`

### Tampa Office
```
https://ringcentral-availability-api.onrender.com/availability?state=XX&office=Tampa
```

**Examples:**
- Florida Tampa: `?state=FL&office=Tampa`
- Texas Tampa: `?state=TX&office=Tampa`

### Future Offices (PA, NJ, etc.)
Same formula — just change the `office=` value to match the suffix used in RingCentral queue names.

**Rule:** If the queue in RingCentral is named `"Florida - Philadelphia"`, the URL is `?state=FL&office=Philadelphia`

---

## State Code Reference

| State | Code | State | Code |
|-------|------|-------|------|
| Alabama | AL | Montana | MT |
| Alaska | AK | Nebraska | NE |
| Arizona | AZ | Nevada | NV |
| Arkansas | AR | New Hampshire | NH |
| California | CA | New Jersey | NJ |
| Colorado | CO | New Mexico | NM |
| Connecticut | CT | New York | NY |
| Delaware | DE | North Carolina | NC |
| Florida | FL | North Dakota | ND |
| Georgia | GA | Ohio | OH |
| Hawaii | HI | Oklahoma | OK |
| Idaho | ID | Oregon | OR |
| Illinois | IL | Pennsylvania | PA |
| Indiana | IN | Rhode Island | RI |
| Iowa | IA | South Carolina | SC |
| Kansas | KS | South Dakota | SD |
| Kentucky | KY | Tennessee | TN |
| Louisiana | LA | Texas | TX |
| Maine | ME | Utah | UT |
| Maryland | MD | Vermont | VT |
| Massachusetts | MA | Virginia | VA |
| Michigan | MI | Washington | WA |
| Minnesota | MN | Washington DC | DC |
| Mississippi | MS | West Virginia | WV |
| Missouri | MO | Wisconsin | WI |
| | | Wyoming | WY |

---

## Tampa Office — All Ping URLs

```
https://ringcentral-availability-api.onrender.com/availability?state=AL&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=AK&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=AZ&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=AR&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=CA&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=CO&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=CT&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=DE&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=FL&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=GA&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=HI&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=ID&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=IL&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=IN&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=IA&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=KS&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=KY&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=LA&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=ME&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=MD&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=MA&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=MI&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=MN&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=MS&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=MO&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=MT&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=NE&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=NV&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=NH&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=NJ&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=NM&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=NY&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=NC&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=ND&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=OH&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=OK&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=OR&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=PA&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=RI&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=SC&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=SD&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=TN&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=TX&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=UT&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=VT&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=VA&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=WA&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=DC&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=WV&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=WI&office=Tampa
https://ringcentral-availability-api.onrender.com/availability?state=WY&office=Tampa
```

**General Tampa queue:**
```
https://ringcentral-availability-api.onrender.com/queue?name=1%20-%20General%20-%20Tampa%20Office
```

---

## How to Test a Ping

Open any URL in your browser. You should see:

**Agents available:**
```json
{ "available": true, "agents": 2, "queue": "Florida - Tampa", "total_members": 6 }
```

**No agents available:**
```json
{ "available": false, "agents": 0, "queue": "Florida - Tampa", "total_members": 6 }
```

---

## How to Add a New Office (PA, NJ, etc.)

**Step 1 — RingCentral:** Create queues named exactly as:
```
[State Name] - [Office Name]
```
Example: `Florida - Philadelphia`, `Texas - Philadelphia`

**Step 2 — Ringba:** Use this URL format:
```
https://ringcentral-availability-api.onrender.com/availability?state=FL&office=Philadelphia
```

**No code changes needed.** The API automatically detects all queues from RingCentral.

---

## Check a Specific Queue

```
https://ringcentral-availability-api.onrender.com/queue?name=Florida
https://ringcentral-availability-api.onrender.com/queue?name=Florida%20-%20Tampa
```
Returns `available`, how many agents are free, and full member list with presence status.

---

## Check a Specific Agent

Find the agent's RC extension number (visible in RingCentral Admin → Users):

```
https://ringcentral-availability-api.onrender.com/agent?ext=183
```

Response:
```json
{
  "available": true,
  "ext": "183",
  "name": "ORL - Jennifer Owaesh - 168 - Ext. 183",
  "presenceStatus": "Available",
  "dndStatus": "TakeAllCalls",
  "telephonyStatus": "NoCall"
}
```

**Agent is available when all three are true:**
- `presenceStatus` = `Available`
- `dndStatus` = `TakeAllCalls` (DND is off)
- `telephonyStatus` = `NoCall` (not on a call)

**Common reasons for `available: false`:**
| reason | meaning |
|--------|---------|
| `OnCall` | Agent is currently on a call |
| `DND` | Agent has Do Not Disturb turned on |
| `Busy` / `Offline` | Agent is away or logged out of RC |

---

## When You Add a New Queue in RingCentral

1. Open `/queues` — confirm the queue appears and check the exact name
2. Test it: `/queue?name=EXACT_NAME`
3. Build the Ringba ping URL using that state/office
4. Add to Ringba target with 30–60s polling interval

---

## Useful Debug Endpoints

| URL | What it shows |
|-----|---------------|
| `/queues` | All queues in RingCentral |
| `/queue?name=Florida` | Specific queue status + agent presence |
| `/agent?ext=183` | Single agent availability |
| `/webhook-status` | Webhook active + cache stats |
| `/health` | API status check |

---

## How to Configure in Ringba

1. Go to the **Target** or **Campaign** in Ringba
2. Find **Agent Availability** or **Ping URL** setting
3. Paste the URL for that state/office
4. Set method to **GET**
5. Set **Available condition:** `available` = `true`
6. Set polling interval to **30–60 seconds** (important — lower = risk of rate limiting)
