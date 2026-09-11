const http = require('http');
const https = require('https');
const crypto = require('crypto');

const WEBHOOK_URL = process.env.WEBHOOK_URL || null;
const WEBHOOK_VERIFICATION_TOKEN = process.env.WEBHOOK_VERIFICATION_TOKEN || 'rc-webhook-verify-token';
let webhookSubscriptionId = null;
let webhookRenewalTimer = null;

const RC_CLIENT_ID = process.env.RC_CLIENT_ID;
const RC_CLIENT_SECRET = process.env.RC_CLIENT_SECRET;
const RC_JWT = process.env.RC_JWT;

// === RingCX (Contact Center) ===
const RINGCX_ACCOUNT_ID = process.env.RINGCX_ACCOUNT_ID || '50560001';
const RINGCX_GATE_GROUP_ID = '1973';

const STATE_GATE_MAP = {
  'AK': 12530, 'AL': 12475, 'AR': 12476, 'AZ': 12477, 'CA': 12478,
  'CO': 12479, 'CT': 12480, 'DC': 12481, 'DE': 12482, 'FL': 12483,
  'GA': 12484, 'HI': 12485, 'IA': 12486, 'ID': 12487, 'IL': 12488,
  'IN': 12489, 'KS': 12490, 'KY': 12491, 'LA': 12492, 'MA': 12493,
  'MD': 12494, 'ME': 12495, 'MI': 12496, 'MN': 12497, 'MO': 12499,
  'MS': 12500, 'MT': 12501, 'NC': 12502, 'ND': 12503, 'NE': 12504,
  'NH': 12505, 'NJ': 12506, 'NM': 12507, 'NV': 12508, 'NY': 12509,
  'OH': 12510, 'OK': 12511, 'OR': 12512, 'PA': 12513, 'RI': 12514,
  'SC': 12515, 'SD': 12516, 'TN': 12517, 'TX': 12518, 'UT': 12519,
  'VA': 12520, 'VT': 12521, 'WA': 12522, 'WI': 12523, 'WV': 12524,
  'WY': 12525
};

let ringcxTokenCache = null;
let ringcxTokenExpiry = 0;

let tokenCache = null;
let tokenExpiry = 0;

// --- RC API Rate Limiter (max 2 concurrent requests) ---
let rcActiveRequests = 0;
const RC_MAX_CONCURRENT = 1; // Reduced to minimize rate limit hits
const rcQueue = [];
let rcRateLimitedUntil = 0;

function rcThrottle(fn) {
  return new Promise((resolve, reject) => {
    const now = Date.now();
    if (now < rcRateLimitedUntil) {
      return reject(new Error('RC error CMN-301: Request rate exceeded (cooldown)'));
    }
    const run = () => {
      rcActiveRequests++;
      fn().then(result => {
        resolve(result);
      }).catch(err => {
        if (err.message && err.message.includes('CMN-301')) {
          rcRateLimitedUntil = Date.now() + 120000;
          console.log('[RATE LIMIT] CMN-301 hit, cooling down 2 minutes');
        }
        reject(err);
      }).finally(() => {
        rcActiveRequests--;
        if (rcQueue.length > 0) rcQueue.shift()();
      });
    };
    if (rcActiveRequests < RC_MAX_CONCURRENT) {
      run();
    } else {
      rcQueue.push(run);
    }
  });
}

// --- Caching ---
const presenceCache = new Map();
const PRESENCE_TTL = 60 * 1000; // 1 minute - on-demand fetches (webhook handles real-time updates)
const PRESENCE_WEBHOOK_TTL = 5 * 60 * 1000; // 5 minutes - webhook-backed entries; webhook updates instantly on any change

const presenceInFlight = new Map(); // dedup simultaneous fetches for same extension
const queueMembersCache = new Map();
const QUEUE_MEMBERS_TTL = 30 * 60 * 1000; // 30 minutes

const queuePresenceCache = new Map();
const QUEUE_PRESENCE_TTL = 2 * 60 * 1000; // 2 minutes

let queuesCache = null;
let queuesCacheExpiry = 0;
const QUEUES_TTL = 5 * 60 * 1000; // 5 minutes

let extensionsCache = null;
let extensionsCacheExpiry = 0;
const EXTENSIONS_TTL = 10 * 60 * 1000; // 10 minutes

async function getPresenceCached(token, extensionId) {
  const now = Date.now();
  const cached = presenceCache.get(extensionId);
  if (cached) {
    // Webhook-backed entries are trusted for their full TTL only while the
    // webhook subscription is active. If the webhook is down, fall back to the
    // short on-demand TTL so we don't serve stale data for up to 5 minutes.
    if (cached.source === 'webhook' && !webhookSubscriptionId) {
      const effectiveExpiry = (cached.webhookAt || 0) + PRESENCE_TTL;
      if (now < effectiveExpiry) return cached.data;
    } else if (now < cached.expiry) {
      return cached.data;
    }
    // During RC rate-limit cooldown, always serve stale cached data — never call RC
    if (now < rcRateLimitedUntil) {
      return cached.data;
    }
  } else if (now < rcRateLimitedUntil) {
    // No cached data + in cooldown: return null (agent treated as unavailable)
    return null;
  }
  // In-flight deduplication: if another request is already fetching this extension, share the result
  if (presenceInFlight.has(extensionId)) {
    try { return await presenceInFlight.get(extensionId); } catch(e) { if (cached) return cached.data; throw e; }
  }
  const fetchPromise = rcThrottle(() => getPresence(token, extensionId)).then(data => {
    presenceInFlight.delete(extensionId);
    if (data) presenceCache.set(extensionId, { data, expiry: Date.now() + PRESENCE_TTL, source: 'ondemand' });
    return data;
  }).catch(err => {
    presenceInFlight.delete(extensionId);
    if (err.message && err.message.includes('CMN-301') && cached) {
      presenceCache.set(extensionId, { data: cached.data, expiry: Date.now() + 3 * 60 * 1000 }); // extend through cooldown
      return cached.data;
    }
    if (cached) return cached.data;
    throw err;
  });
  presenceInFlight.set(extensionId, fetchPromise);
  return fetchPromise;
}

async function getQueueMembersCached(token, queueId) {
  const now = Date.now();
  const cached = queueMembersCache.get(queueId);
  if (cached && now < cached.expiry) return cached.data;
  try {
    const data = await rcThrottle(() => getQueueMembers(token, queueId));
    queueMembersCache.set(queueId, { data, expiry: now + QUEUE_MEMBERS_TTL });
    return data;
  } catch (err) {
    if (cached) return cached.data;
    throw err;
  }
}

async function getQueuesCached(token) {
  const now = Date.now();
  if (queuesCache && now < queuesCacheExpiry) return queuesCache;
  try {
    const data = await rcThrottle(() => getQueues(token));
    queuesCache = data;
    queuesCacheExpiry = now + QUEUES_TTL;
    return data;
  } catch (err) {
    if (queuesCache) return queuesCache;
    throw err;
  }
}

async function getExtensionsCached(token) {
  const now = Date.now();
  if (extensionsCache && now < extensionsCacheExpiry) return extensionsCache;
  try {
    const data = await getExtensions(token);
    extensionsCache = data;
    extensionsCacheExpiry = now + EXTENSIONS_TTL;
    return data;
  } catch (err) {
    if (extensionsCache) return extensionsCache;
    throw err;
  }
}

async function getCallQueuePresence(token, extensionId) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: `/restapi/v1.0/account/~/extension/${extensionId}/call-queue-presence`,
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.errorCode && json.errorCode.includes('CMN-301')) {
            reject(new Error('RC error CMN-301: Request rate exceeded'));
          } else {
            resolve(json);
          }
        } catch(e) { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.end();
  });
}

async function getCallQueuePresenceCached(token, extensionId) {
  const now = Date.now();
  const cached = queuePresenceCache.get(String(extensionId));
  if (cached && now < cached.expiry) return cached.data;
  try {
    const data = await rcThrottle(() => getCallQueuePresence(token, extensionId));
    if (data) queuePresenceCache.set(String(extensionId), { data, expiry: now + QUEUE_PRESENCE_TTL });
    return data;
  } catch (err) {
    if (cached) return cached.data;
    return null;
  }
}

// --- Call log storage (in-memory, last 500 calls) ---
const callLog = [];
const CALL_LOG_MAX = 500;

// --- State map ---
const STATE_NAME_MAP = {
  'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
  'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
  'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
  'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
  'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
  'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
  'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
  'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
  'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
  'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
  'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
  'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'DC': 'Washington, DC',
  'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
};

// --- Agent map (legacy) ---
const AGENT_MAP = {
  '0000as': 'MI - April Simpson',
  '0000by': 'MI - Bebeto Yewah',
  '0000dt': 'MI - Dylan Trout',
  '0000lt': 'MI - Lee Trawick',
};

// --- Tampa queue shortcuts ---
// Add Tampa queue names here once confirmed by Mark/Edmar
const TAMPA_QUEUE_SHORTCUTS = {
  // '/queue/tampa-vip': 'Tampa VIP Response',
  // '/queue/tampa-general': 'Tampa General',
};

// --- RC API functions ---
async function getAccessToken() {
  const now = Date.now();
  if (tokenCache && now < tokenExpiry) return tokenCache;
  return new Promise((resolve, reject) => {
    const credentials = Buffer.from(`${RC_CLIENT_ID}:${RC_CLIENT_SECRET}`).toString('base64');
    const body = `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${RC_JWT}`;
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/oauth/token',
      method: 'POST',
      headers: {
        'Authorization': `Basic ${credentials}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.access_token) {
            tokenCache = json.access_token;
            tokenExpiry = now + (55 * 60 * 1000);
            resolve(tokenCache);
          } else {
            reject(new Error('No access token: ' + data));
          }
        } catch(e) {
          reject(new Error('Failed to parse token response: ' + data));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function getRingCXToken() {
  const now = Date.now();
  if (ringcxTokenCache && now < ringcxTokenExpiry) return ringcxTokenCache;
  const rcToken = await getAccessToken();
  return new Promise((resolve, reject) => {
    const body = `rcAccessToken=${encodeURIComponent(rcToken)}`;
    const options = {
      hostname: 'engage.ringcentral.com',
      path: '/api/auth/login/rc/accesstoken',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body)
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.accessToken) {
            ringcxTokenCache = json.accessToken;
            ringcxTokenExpiry = now + (50 * 60 * 1000);
            console.log('[RINGCX] Token obtained');
            resolve(ringcxTokenCache);
          } else {
            reject(new Error('No RingCX token: ' + data.slice(0, 300)));
          }
        } catch(e) {
          reject(new Error('Failed to parse RingCX token: ' + data.slice(0, 300)));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function ringcxGet(token, path) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'engage.ringcentral.com',
      path,
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch(e) { resolve({ status: res.statusCode, raw: data.slice(0, 2000) }); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getQueues(token) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/v1.0/account/~/call-queues?perPage=1000',
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.errorCode && json.errorCode.includes('CMN-301')) reject(new Error('RC error CMN-301: Request rate exceeded'));
          else resolve(json);
        }
        catch(e) { reject(new Error('Failed to parse queues: ' + data)); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getQueueMembers(token, queueId) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: `/restapi/v1.0/account/~/call-queues/${queueId}/members?perPage=200`,
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.errorCode) reject(new Error(`RC error ${json.errorCode}: ${json.message}`));
          else resolve(json);
        }
        catch(e) { reject(new Error('Failed to parse members: ' + data)); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function getPresence(token, extensionId) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: `/restapi/v1.0/account/~/extension/${extensionId}/presence`,
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.errorCode && json.errorCode.includes('CMN-301')) {
            reject(new Error('RC error CMN-301: Request rate exceeded'));
          } else {
            resolve(json);
          }
        }
        catch(e) { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.end();
  });
}

async function getExtensions(token) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/v1.0/account/~/extension?perPage=1000&type=User&status=Enabled',
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('Failed to parse extensions: ' + data)); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// --- Availability check functions ---
async function checkQueueAvailability(queueName) {
  const token = await getAccessToken();
  const queuesData = await getQueuesCached(token);
  const queues = queuesData.records || [];

  const matchedQueue = queues.find(q =>
    q.name.toLowerCase() === queueName.toLowerCase()
  );

  if (!matchedQueue) {
    return { available: false, agents: 0, reason: `No queue found for: ${queueName}` };
  }

  const membersData = await getQueueMembersCached(token, matchedQueue.id);
  const members = membersData.records || [];

  const presenceResults = await Promise.all(
    members.map(async (m) => getPresenceCached(token, m.id).catch(() => null))
  );

  const availableAgents = presenceResults.filter(p => {
    if (!p) return false;
    return (
      p.presenceStatus === 'Available' &&
      p.dndStatus === 'TakeAllCalls' &&
      p.telephonyStatus === 'NoCall'
    );
  });
  // [RESULT] log removed;
  presenceResults.forEach((p, i) => {
    if (!p) return;
    const status = `${p.presenceStatus}/${p.telephonyStatus}/${p.dndStatus}`;
    // [AGENT] log removed to reduce log noise;
  });

  const activeCalls = presenceResults.filter(p => {
    if (!p) return false;
    return p.telephonyStatus === 'CallConnected' || p.telephonyStatus === 'OnHold' || p.telephonyStatus === 'Ringing';
  }).length;

  return {
    available: availableAgents.length > 0,
    agents: availableAgents.length,
    active_calls: activeCalls,
    total_members: members.length,
    queue: matchedQueue.name
  };
}

async function checkAvailabilityWithMinAgents(stateUpper, office, minAgents) {
  const result = await checkAvailability(stateUpper, office);
  if (minAgents && result.agents < minAgents) {
    return { ...result, available: false, reason: `Not enough agents: ${result.agents} available, ${minAgents} required` };
  }
  return result;
}

async function checkAvailability(stateUpper, office) {
  const stateName = STATE_NAME_MAP[stateUpper] || stateUpper;
  const queueName = office ? `${stateName} - ${office}` : stateName;
  const result = await checkQueueAvailability(queueName);
  return { ...result, state: stateUpper, state_name: stateName, office: office || 'main' };
}

// --- RC Webhook Subscription ---
async function deleteOldSubscriptions(token) {
  return new Promise((resolve) => {
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/v1.0/subscription',
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}` }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', async () => {
        try {
          const json = JSON.parse(data);
          const subs = json.records || [];
          console.log(`[WEBHOOK] Found ${subs.length} existing subscriptions, deleting...`);
          for (const sub of subs) {
            await new Promise((done) => {
              const delReq = https.request({
                hostname: 'platform.ringcentral.com',
                path: `/restapi/v1.0/subscription/${sub.id}`,
                method: 'DELETE',
                headers: { 'Authorization': `Bearer ${token}` }
              }, (r) => { r.resume(); r.on('end', done); });
              delReq.on('error', done);
              delReq.end();
            });
            console.log(`[WEBHOOK] Deleted subscription: ${sub.id}`);
          }
        } catch(e) {
          console.error('[WEBHOOK] Failed to delete old subscriptions:', e.message);
        }
        resolve();
      });
    });
    req.on('error', () => resolve());
    req.end();
  });
}

async function createWebhookSubscription(token) {
  if (!WEBHOOK_URL) {
    console.log('WEBHOOK_URL not set, skipping webhook subscription');
    return;
  }
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      eventFilters: [
        '/restapi/v1.0/account/~/extension/~/presence?detailedTelephonyState=true'
      ],
      deliveryMode: {
        transportType: 'WebHook',
        address: `${WEBHOOK_URL}/webhook/presence`,
        verificationToken: WEBHOOK_VERIFICATION_TOKEN
      },
      expiresIn: 86400
    });
    const options = {
      hostname: 'platform.ringcentral.com',
      path: '/restapi/v1.0/subscription',
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.id) {
            webhookSubscriptionId = json.id;
            console.log(`RC webhook subscription created: ${json.id}`);
            scheduleWebhookRenewal();
            resolve(json);
          } else {
            console.error('Webhook subscription failed:', data);
            resolve(null);
          }
        } catch(e) { resolve(null); }
      });
    });
    req.on('error', (err) => { console.error('Webhook subscription error:', err.message); resolve(null); });
    req.write(body);
    req.end();
  });
}

function scheduleWebhookRenewal() {
  if (webhookRenewalTimer) clearTimeout(webhookRenewalTimer);
  webhookRenewalTimer = setTimeout(async () => {
    try {
      console.log('Renewing RC webhook subscription...');
      const token = await getAccessToken();
      await createWebhookSubscription(token);
    } catch(err) {
      console.error('Webhook renewal failed:', err.message);
    }
  }, 23 * 60 * 60 * 1000);
}

function handleWebhookPresence(body) {
  try {
    const data = JSON.parse(body);
    const presence = data.body || data;
    const extensionId = presence.extensionId ||
      (presence.extension && presence.extension.id) ||
      (data.body && data.body.extension && data.body.extension.id);
    if (!extensionId) {
      console.log('[WEBHOOK] No extensionId found in payload:', JSON.stringify(data).slice(0, 300));
      return;
    }
    const prev = presenceCache.get(String(extensionId));
    const prevStatus = prev ? `${prev.data.presenceStatus}/${prev.data.telephonyStatus}` : 'none';
    presenceCache.set(String(extensionId), {
      data: presence,
      expiry: Date.now() + PRESENCE_WEBHOOK_TTL,
      source: 'webhook',
      webhookAt: Date.now()
    });
    console.log(`[WEBHOOK] ext ${extensionId}: ${prevStatus} → ${presence.presenceStatus}/${presence.telephonyStatus} dnd=${presence.dndStatus}`);
  } catch(e) {
    console.error('[WEBHOOK] Parse error:', e.message, body.slice(0, 200));
  }
}

// --- Cache warmup ---
async function warmupCache() {
  try {
    const token = await getAccessToken();
    const queuesData = await getQueuesCached(token);
    const queues = queuesData.records || [];
    await Promise.all(
      queues.map(q => getQueueMembersCached(token, q.id).catch(() => null))
    );
    console.log(`Cache warmed: ${queues.length} queues (presence fetched on-demand)`);
  } catch (err) {
    console.error('Cache warmup failed:', err.message);
  }
}

// --- HTTP Server ---
const server = http.createServer(async (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');

  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;

  if (pathname === '/' || pathname === '/health') {
    res.writeHead(200);
    return res.end(JSON.stringify({ status: 'ok', message: 'Availability API is running' }));
  }

  // Manual cooldown control: /cooldown?minutes=10
  if (pathname === '/cooldown') {
    const minutes = parseInt(url.searchParams.get('minutes') || '10', 10);
    rcRateLimitedUntil = Date.now() + minutes * 60 * 1000;
    const until = new Date(rcRateLimitedUntil).toISOString();
    console.log(`[RATE LIMIT] Manual cooldown activated for ${minutes} minutes (until ${until})`);
    res.writeHead(200);
    return res.end(JSON.stringify({ status: 'cooldown_active', minutes, until }));
  }


  // Webhook status: /webhook-status
  if (pathname === '/webhook-status') {
    const cacheEntries = [...presenceCache.entries()].map(([k, v]) => ({
      extId: k, source: v.source || 'ondemand', age_sec: Math.round((Date.now() - (v.webhookAt || 0)) / 1000)
    }));
    const webhookBacked = cacheEntries.filter(e => e.source === 'webhook').length;
    res.writeHead(200);
    return res.end(JSON.stringify({
      webhook_subscription_id: webhookSubscriptionId || null,
      webhook_active: !!webhookSubscriptionId,
      webhook_url: WEBHOOK_URL || null,
      cache_total: cacheEntries.length,
      cache_webhook_backed: webhookBacked,
      cache_ondemand: cacheEntries.length - webhookBacked
    }));
  }

  // Cooldown status: /cooldown/status
  if (pathname === '/cooldown/status') {
    const now = Date.now();
    const active = now < rcRateLimitedUntil;
    const remainingMs = active ? rcRateLimitedUntil - now : 0;
    res.writeHead(200);
    return res.end(JSON.stringify({
      active,
      remaining_seconds: Math.round(remainingMs / 1000),
      until: active ? new Date(rcRateLimitedUntil).toISOString() : null
    }));
  }

  // RingCX state-based availability /availability?state=FL
  if (pathname === '/availability') {
    const state = url.searchParams.get('state');
    if (!state) {
      res.writeHead(400);
      return res.end(JSON.stringify({ available: false, error: 'Missing state parameter. Use ?state=TX' }));
    }
    const stateUpper = state.toUpperCase().trim();
    const gateId = STATE_GATE_MAP[stateUpper];
    if (!gateId) {
      res.writeHead(400);
      return res.end(JSON.stringify({ available: false, error: `Unknown state: ${stateUpper}` }));
    }
    const minAgentsParam = url.searchParams.get('min_agents');
    const minAgents = minAgentsParam ? parseInt(minAgentsParam, 10) : null;
    try {
      const token = await getRingCXToken();
      const result = await ringcxGet(token, `/voice/api/v1/admin/accounts/${RINGCX_ACCOUNT_ID}/agentSessions`);
      const sessions = Array.isArray(result.body) ? result.body :
        (result.body && (result.body.agentSessions || result.body.records)) || [];
      // Filter agents logged into this gate
      const inQueue = sessions.filter(s => {
        const queues = s.loginQueues || s.agentGateAssignments || s.queues || [];
        return queues.some(q => String(q.gateId || q.id || q.gate && q.gate.id) === String(gateId));
      });
      const availableAgents = inQueue.filter(s =>
        (s.loginState || s.currentState || s.agentState || '').toUpperCase() === 'AVAILABLE'
      );
      const count = availableAgents.length;
      const isAvailable = minAgents ? count >= minAgents : count > 0;
      res.writeHead(200);
      return res.end(JSON.stringify({
        available: isAvailable,
        agents: count,
        total_in_queue: inQueue.length,
        state: stateUpper,
        gate_id: gateId,
        ...(minAgents && { min_agents: minAgents })
      }));
    } catch (err) {
      console.error('[RINGCX] availability error:', err.message);
      res.writeHead(500);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Agent availability: /agent?ext=183 (by RC extension number) or /agent?id=xxx (legacy)
  if (pathname === '/agent') {
    const ext = url.searchParams.get('ext');
    const id = url.searchParams.get('id');

    // New: per-agent availability by RC extension number
    if (ext) {
      try {
        const token = await getAccessToken();
        // Use cached extensions list instead of individual RC lookup (saves 1 API call per ping)
        const extData = await getExtensionsCached(token);
        const allExts = (extData && extData.records) || [];
        const matched = allExts.find(e => String(e.extensionNumber) === String(ext));
        if (!matched) {
          res.writeHead(404);
          return res.end(JSON.stringify({ available: false, ext, reason: 'Extension not found' }));
        }
        const extId = matched.id;
        const extName = matched.name;
        // Only presence check — device check removed (unreliable for WebRTC, not used for availability)
        const presence = await getPresenceCached(token, extId);
        if (!presence || presence.errorCode) {
          res.writeHead(200);
          return res.end(JSON.stringify({ available: false, ext, reason: (presence && presence.message) || 'No presence data' }));
        }
        const isAvailable = (
          presence.presenceStatus === 'Available' &&
          presence.dndStatus === 'TakeAllCalls' &&
          presence.telephonyStatus === 'NoCall'
        );
        let reason = null;
        if (!isAvailable) {
          if (presence.dndStatus !== 'TakeAllCalls') reason = 'DND';
          else if (presence.telephonyStatus !== 'NoCall') reason = 'OnCall';
          else reason = presence.presenceStatus;
        }
        res.writeHead(200);
        return res.end(JSON.stringify({
          available: isAvailable,
          ext,
          name: extName,
          presenceStatus: presence.presenceStatus,
          dndStatus: presence.dndStatus,
          telephonyStatus: presence.telephonyStatus,
          ...(reason && { reason })
        }));
      } catch (err) {
        res.writeHead(500);
        return res.end(JSON.stringify({ error: err.message }));
      }
    }

    // Legacy: /agent?id=xxx (maps to queue name)
    if (!id) {
      res.writeHead(400);
      return res.end(JSON.stringify({ available: false, error: 'Missing ext or id parameter' }));
    }
    const queueName = AGENT_MAP[id.toLowerCase().trim()];
    if (!queueName) {
      res.writeHead(404);
      return res.end(JSON.stringify({ available: false, error: 'Unknown agent id' }));
    }
    try {
      const result = await checkQueueAvailability(queueName);
      res.writeHead(200);
      return res.end(JSON.stringify(result));
    } catch (err) {
      console.error('Error:', err.message);
      const status = err.message && err.message.includes('CMN-301') ? 200 : 500;
      res.writeHead(status);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Queue by name: /queue?name=Tampa+VIP
  if (pathname === '/queue') {
    const name = url.searchParams.get('name');
    if (!name) {
      res.writeHead(400);
      return res.end(JSON.stringify({ available: false, error: 'Missing name parameter. Use ?name=QueueName' }));
    }
    try {
      const result = await checkQueueAvailability(name.trim());
      res.writeHead(200);
      return res.end(JSON.stringify(result));
    } catch (err) {
      console.error('Error:', err.message);
      const status = err.message && err.message.includes('CMN-301') ? 200 : 500;
      res.writeHead(status);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }

  // Tampa queue shortcuts
  if (TAMPA_QUEUE_SHORTCUTS[pathname]) {
    try {
      const result = await checkQueueAvailability(TAMPA_QUEUE_SHORTCUTS[pathname]);
      res.writeHead(200);
      return res.end(JSON.stringify(result));
    } catch (err) {
      console.error('Error:', err.message);
      const status = err.message && err.message.includes('CMN-301') ? 200 : 500;
      res.writeHead(status);
      return res.end(JSON.stringify({ available: false, error: err.message }));
    }
  }



  // Debug: RingCX agent sessions
  if (pathname === '/debug/ringcx') {
    try {
      const token = await getRingCXToken();
      const [sessions, gates] = await Promise.all([
        ringcxGet(token, `/voice/api/v1/admin/accounts/${RINGCX_ACCOUNT_ID}/agentSessions`),
        ringcxGet(token, `/voice/api/v1/admin/accounts/${RINGCX_ACCOUNT_ID}/gateGroups/${RINGCX_GATE_GROUP_ID}/gates?perPage=10`)
      ]);
      res.writeHead(200);
      return res.end(JSON.stringify({ sessions, gates }, null, 2));
    } catch(err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // Debug: all extensions/groups in account
  if (pathname === '/debug/all') {
    try {
      const token = await getAccessToken();
      const types = ['User','Department','Announcement','Voicemail','DigitalUser','VirtualUser','FaxUser','PagingOnly','SharedLinesGroup','IvrMenu','ApplicationExtension','ParkedLocation'];
      function rcGet(path) {
        return new Promise((resolve) => {
          const req = https.request({
            hostname: 'platform.ringcentral.com',
            path,
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
          }, (r) => { let d=''; r.on('data',c=>d+=c); r.on('end',()=>{ try{resolve(JSON.parse(d))}catch(e){resolve({raw:d.slice(0,500)})} }); });
          req.on('error', () => resolve(null));
          req.end();
        });
      }
      const [allExt, callQueues, ringGroups, ivrMenus] = await Promise.all([
        rcGet('/restapi/v1.0/account/~/extension?perPage=1000&status=Enabled'),
        rcGet('/restapi/v1.0/account/~/call-queues?perPage=1000'),
        rcGet('/restapi/v1.0/account/~/ring-groups?perPage=1000'),
        rcGet('/restapi/v1.0/account/~/ivr-menus?perPage=100'),
      ]);
      const extRecords = (allExt && allExt.records) || [];
      const byType = {};
      for (const e of extRecords) {
        const t = e.type || 'Unknown';
        if (!byType[t]) byType[t] = [];
        byType[t].push({ id: e.id, ext: e.extensionNumber, name: e.name, status: e.status });
      }
      res.writeHead(200);
      return res.end(JSON.stringify({
        total_extensions: extRecords.length,
        by_type: byType,
        call_queues: callQueues,
        ring_groups: ringGroups,
        ivr_menus: ivrMenus
      }, null, 2));
    } catch (err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // Debug: account info
  if (pathname === '/debug/account') {
    try {
      const token = await getAccessToken();
      const [accountInfo, extensionInfo, queuesRaw, extensionTypes] = await Promise.all([
        new Promise((resolve) => {
          const req = https.request({
            hostname: 'platform.ringcentral.com',
            path: '/restapi/v1.0/account/~',
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
          }, (r) => { let d=''; r.on('data',c=>d+=c); r.on('end',()=>{ try{resolve(JSON.parse(d))}catch(e){resolve({raw:d})} }); });
          req.on('error', () => resolve(null));
          req.end();
        }),
        new Promise((resolve) => {
          const req = https.request({
            hostname: 'platform.ringcentral.com',
            path: '/restapi/v1.0/account/~/extension/~',
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
          }, (r) => { let d=''; r.on('data',c=>d+=c); r.on('end',()=>{ try{resolve(JSON.parse(d))}catch(e){resolve({raw:d})} }); });
          req.on('error', () => resolve(null));
          req.end();
        }),
        new Promise((resolve) => {
          const req = https.request({
            hostname: 'platform.ringcentral.com',
            path: '/restapi/v1.0/account/~/call-queues?perPage=10',
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
          }, (r) => { let d=''; r.on('data',c=>d+=c); r.on('end',()=>{ try{resolve(JSON.parse(d))}catch(e){resolve({raw:d})} }); });
          req.on('error', () => resolve(null));
          req.end();
        }),
        new Promise((resolve) => {
          const req = https.request({
            hostname: 'platform.ringcentral.com',
            path: '/restapi/v1.0/account/~/extension?perPage=10&type=Department',
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` }
          }, (r) => { let d=''; r.on('data',c=>d+=c); r.on('end',()=>{ try{resolve(JSON.parse(d))}catch(e){resolve({raw:d})} }); });
          req.on('error', () => resolve(null));
          req.end();
        })
      ]);
      res.writeHead(200);
      return res.end(JSON.stringify({
        account: {
          id: accountInfo && accountInfo.id,
          mainNumber: accountInfo && accountInfo.mainNumber,
          name: accountInfo && accountInfo.name,
          status: accountInfo && accountInfo.status,
          serviceInfo: accountInfo && accountInfo.serviceInfo && accountInfo.serviceInfo.brand && accountInfo.serviceInfo.brand.name
        },
        currentExtension: {
          id: extensionInfo && extensionInfo.id,
          extensionNumber: extensionInfo && extensionInfo.extensionNumber,
          name: extensionInfo && extensionInfo.name,
          type: extensionInfo && extensionInfo.type,
          permissions: extensionInfo && extensionInfo.permissions
        },
        callQueues: queuesRaw,
        departmentExtensions: extensionTypes
      }, null, 2));
    } catch (err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // List all queues
  // Debug: raw RC response for queues
  if (pathname === '/queues/raw') {
    try {
      const token = await getAccessToken();
      const queuesData = await getQueues(token);
      res.writeHead(200);
      return res.end(JSON.stringify(queuesData));
    } catch (err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  if (pathname === '/queues') {
    const gates = Object.entries(STATE_GATE_MAP).map(([state, gateId]) => ({
      state,
      gate_id: gateId,
      name: `${state} - Standard vs Preferred`
    }));
    res.writeHead(200);
    return res.end(JSON.stringify({
      total: gates.length,
      gate_group_id: RINGCX_GATE_GROUP_ID,
      account_id: RINGCX_ACCOUNT_ID,
      queues: gates
    }));
  }

  // Debug: all agents presence
  if (pathname === '/agents/debug') {
    try {
      const token = await getAccessToken();
      const extData = await getExtensionsCached(token);
      const extensions = (extData.records || []).map(e => e.extensionNumber);
      const results = await Promise.all(extensions.map(async (ext) => {
        try {
          const json = await new Promise((resolve, reject) => {
            const options = {
              hostname: 'platform.ringcentral.com',
              path: `/restapi/v1.0/account/~/extension?extensionNumber=${ext}`,
              method: 'GET',
              headers: { 'Authorization': `Bearer ${token}` }
            };
            const req = https.request(options, (res) => {
              let data = '';
              res.on('data', chunk => data += chunk);
              res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { resolve(null); } });
            });
            req.on('error', reject);
            req.end();
          });
          const records = json && json.records || [];
          if (!records.length) return { extension: ext, error: 'not found' };
          const extId = records[0].id;
          const extName = records[0].name;
          const presence = await getPresenceCached(token, extId);
          return {
            extension: ext,
            name: extName,
            presenceStatus: presence ? presence.presenceStatus : null,
            dndStatus: presence ? presence.dndStatus : null,
            telephonyStatus: presence ? presence.telephonyStatus : null,
            userStatus: presence ? presence.userStatus : null,
            raw: presence
          };
        } catch(e) {
          return { extension: ext, error: e.message };
        }
      }));
      res.writeHead(200);
      return res.end(JSON.stringify({ agents: results }));
    } catch (err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }


  // RC Webhook receiver
  if (pathname === '/webhook/presence') {
    const validationToken = req.headers['validation-token'];
    if (validationToken) {
      console.log(`[WEBHOOK] Validation request received`);
      res.writeHead(200, { 'Validation-Token': validationToken });
      return res.end();
    }
    if (req.method === 'POST') {
      let body = '';
      req.on('data', chunk => body += chunk);
      req.on('end', () => {
        console.log(`[WEBHOOK] Incoming payload (${body.length} bytes):`, body.slice(0, 300));
        handleWebhookPresence(body);
        res.writeHead(200);
        res.end();
      });
      return;
    }
  }

  if (pathname === '/ringba-postback') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      try {
        const params = new URLSearchParams(body);
        const data = {};
        for (const [k, v] of params.entries()) data[k] = v;
        const ts = new Date().toISOString();
        const caller = data.caller || data.inboundCallId || data.callId || 'unknown';
        const target = data.targetName || data.targetId || data.target || 'unknown';
        const duration = data.duration || data.callDuration || '?';
        const disposition = data.disposition || data.callStatus || data.connectedCallDuration || 'unknown';
        const state = data.state || data.callerState || '';
        console.log(`[RINGBA] ${ts} | caller=${caller} | target=${target} | state=${state} | duration=${duration}s | disposition=${disposition}`);
        console.log(`[RINGBA] raw=${JSON.stringify(data)}`);
        callLog.unshift({ ts, caller, target, state, duration, disposition, raw: data });
        if (callLog.length > CALL_LOG_MAX) callLog.pop();
      } catch(e) {
        console.log(`[RINGBA] postback parse error: ${e.message} | raw=${body.slice(0,300)}`);
      }
      res.writeHead(200);
      res.end('OK');
    });
    return;
  }

  if (pathname === '/ext') {
    const extId = url.searchParams.get('id') || '';
    if (!extId) {
      res.writeHead(400);
      return res.end(JSON.stringify({ error: 'Missing id parameter. Use ?id=770314052' }));
    }
    try {
      const token = await getAccessToken();
      const data = await new Promise((resolve, reject) => {
        const options = {
          hostname: 'platform.ringcentral.com',
          path: `/restapi/v1.0/account/~/extension/${extId}`,
          method: 'GET',
          headers: { 'Authorization': `Bearer ${token}` }
        };
        const req2 = https.request(options, (r) => {
          let d = '';
          r.on('data', c => d += c);
          r.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(e); } });
        });
        req2.on('error', reject);
        req2.end();
      });
      res.writeHead(200);
      return res.end(JSON.stringify({
        id: extId,
        name: data.name,
        extensionNumber: data.extensionNumber,
        type: data.type,
        status: data.status,
        contact: data.contact && data.contact.firstName ? `${data.contact.firstName} ${data.contact.lastName || ''}`.trim() : null,
        email: data.contact && data.contact.email
      }));
    } catch(err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  if (pathname === '/rc-call') {
    const number = url.searchParams.get('number') || '';
    if (!number) {
      res.writeHead(400);
      return res.end(JSON.stringify({ error: 'Missing number parameter. Use ?number=2708942933' }));
    }
    try {
      const token = await getAccessToken();
      const encoded = encodeURIComponent(number);
      const dateFrom = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      const data = await new Promise((resolve, reject) => {
        const options = {
          hostname: 'platform.ringcentral.com',
          path: `/restapi/v1.0/account/~/call-log?phoneNumber=${encoded}&dateFrom=${dateFrom}&perPage=20&view=Detailed`,
          method: 'GET',
          headers: { 'Authorization': `Bearer ${token}` }
        };
        const req2 = https.request(options, (r) => {
          let d = '';
          r.on('data', c => d += c);
          r.on('end', () => {
            console.log(`[RC-CALL] status=${r.statusCode} phoneNumber=${number} raw=${d.slice(0,500)}`);
            try { resolve(JSON.parse(d)); } catch(e) { reject(e); }
          });
        });
        req2.on('error', reject);
        req2.end();
      });
      const records = (data.records || []).map(r => {
        const legs = (r.legs || []).map(l => ({
          action: l.action,
          result: l.result,
          duration: l.duration,
          extension: l.extension && l.extension.extensionNumber,
          agent: l.extension && l.extension.name,
          type: l.type
        }));
        const queueLeg = legs.find(l => l.type === 'Accept' || (l.agent && l.agent.toLowerCase().includes('queue')) || l.action === 'QueueCall');
        const agentLegs = legs.filter(l => l.type === 'Accept' || l.action === 'HoldOff' || l.action === 'CallAccepted' || l.action === 'Missed');
        return {
          start: r.startTime,
          duration: r.duration,
          result: r.result,
          caller: r.from && r.from.phoneNumber,
          callee: r.to && r.to.phoneNumber,
          queue: queueLeg ? (queueLeg.agent || queueLeg.extension) : (r.to && r.to.name),
          agents_offered: agentLegs.length > 0 ? agentLegs : legs,
          all_legs: legs
        };
      });
      res.writeHead(200);
      return res.end(JSON.stringify({ number, total: records.length, calls: records }));
    } catch(err) {
      res.writeHead(500);
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  if (pathname === '/call') {
    const number = url.searchParams.get('number') || '';
    const clean = number.replace(/\D/g, '');
    const matches = callLog.filter(c => c.caller && c.caller.replace(/\D/g, '').endsWith(clean));
    res.writeHead(200);
    return res.end(JSON.stringify({ number, matches_found: matches.length, calls: matches }));
  }

  if (pathname === '/calls') {
    const limit = parseInt(url.searchParams.get('limit') || '50', 10);
    res.writeHead(200);
    return res.end(JSON.stringify({ total: callLog.length, calls: callLog.slice(0, limit) }));
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found. Available: /availability?state=TX, /agent?id=xxx, /queue?name=QueueName, /queues, /calls, /call?number=5551234567' }));
});

async function setupWebhookWithRetry(attempt = 1) {
  try {
    const token = await getAccessToken();
    if (attempt === 1) await deleteOldSubscriptions(token);
    await createWebhookSubscription(token);
    if (!webhookSubscriptionId) {
      const delay = attempt * 3 * 60 * 1000; // 3min, 6min, 9min...
      const maxAttempts = 5;
      if (attempt < maxAttempts) {
        console.log(`[WEBHOOK] Subscription failed (attempt ${attempt}/${maxAttempts}), retrying in ${attempt * 3} min...`);
        setTimeout(() => setupWebhookWithRetry(attempt + 1), delay);
      } else {
        console.error('[WEBHOOK] All retry attempts failed — running in on-demand mode.');
      }
    }
  } catch(err) {
    const delay = attempt * 3 * 60 * 1000;
    console.error(`[WEBHOOK] Setup error (attempt ${attempt}): ${err.message}, retrying in ${attempt * 3} min...`);
    if (attempt < 5) setTimeout(() => setupWebhookWithRetry(attempt + 1), delay);
  }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, async () => {
  console.log(`Availability API running on port ${PORT}`);
  console.log(`WEBHOOK_URL: ${WEBHOOK_URL || 'NOT SET'}`);
  console.log('Skipping warmup - cache will populate on demand');
  // Delay initial webhook setup by 10s to let rate limit cooldown after busy deploys
  setTimeout(() => setupWebhookWithRetry(1), 10000);
});
