# Phase 5A: APM & Latency Analytics — Walkthrough

## What Was Implemented

Extended the platform to accept latency telemetry (responseTime, dbQueryTime, externalApiTime) and compute real-time P50/P95/P99 percentiles using Redis Sorted Sets. Existing error alerting, EWMA, and sliding windows are untouched.

---

## Architecture — Data Flow for a Log With Latency

```
POST /api/v1/logs {service, message, level, traceId, responseTime:142, dbQueryTime:45}
  │
  ├── Zod validates (5 optional APM fields)
  └── redisProducer.pushLog() → XADD (conditionally adds APM fields)
        │
        └── logConsumer processes:
              1. normaliseLog()    ← parses APM fields (parseFloat/parseInt)
              2. insertLog(mongo)  ← full doc including latency fields
              3. updateBucket()    ← unchanged
              4. recordEvent()    ← unchanged (error counting)
              5. recordLatency()  ← NEW: ZADD to lat:resp/lat:db/lat:ext ZSETs
              6. XACK

Alert Evaluator (30s cycle) for each service:
  Path 1: Threshold alerting       ← unchanged
  Path 2: EWMA adaptive alerting   ← unchanged
  Path 3: Latency percentiles      ← NEW: P50/P95/P99 computation + logging
```

---

## Changes Made

| File | Action | Description |
|------|--------|-------------|
| [latencyAggregator.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/services/latencyAggregator.js) | **NEW** | ZSET recording + percentile computation |
| [logSchema.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/schemas/logSchema.js) | **MODIFY** | +5 optional APM fields |
| [redisProducer.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/services/redisProducer.js) | **MODIFY** | Conditional APM fields in XADD |
| [logNormaliser.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/workers/logNormaliser.js) | **MODIFY** | Parse APM fields from stream |
| [index.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/config/index.js) | **MODIFY** | +latency config section |
| [logConsumer.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/workers/logConsumer.js) | **MODIFY** | +recordLatency() call |
| [alertEvaluator.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/services/alertEvaluator.js) | **MODIFY** | +Path 3 percentile logging |

**Untouched:** `slidingWindowAggregator.js`, `bucketAggregator.js`, `ewmaBaseline.js`, `worker.js`, `mongo.js`

---

## Redis Latency ZSET Structure

```
lat:resp:auth-service    ← ZSET: response time samples
  Score:  1716000060001                    (epoch_ms — for cleanup)
  Member: "1716000060001-0:142.5"          (streamId:latencyMs)

lat:db:auth-service      ← ZSET: DB query time samples
  Score:  1716000060001
  Member: "1716000060001-0:45.2"

lat:ext:auth-service     ← ZSET: external API time samples
  Score:  1716000060001
  Member: "1716000060001-0:78.1"
```

**Why encode latency in the member?**
- Score = timestamp → enables ZREMRANGEBYSCORE for time-based cleanup
- Latency in member → extracted via `member.split(':').pop()` → sorted in-memory for percentiles
- StreamId in member → guarantees global uniqueness

---

## Percentile Calculation

```
1. ZRANGEBYSCORE lat:resp:auth-service (now-60000) +inf
   → ["1716000060001-0:50", "1716000060002-0:85", ..., "1716000060500-0:420"]

2. Extract latency: [50, 85, ..., 420]  (18,000 values at 300 req/s)

3. Sort ascending: [12, 15, 18, ..., 420, 485, 510]

4. Nearest-rank percentile:
   P50 = values[ceil(0.50 × 18000) - 1]  → median response time
   P95 = values[ceil(0.95 × 18000) - 1]  → 95th percentile (tail latency)
   P99 = values[ceil(0.99 × 18000) - 1]  → 99th percentile (worst case)
```

**Performance:** Sorting 18,000 numbers = ~1ms. Runs every 30s. Trivial.

---

## Conditional Write Behavior

| Log Payload | ZSET Writes | Redis Cost |
|-------------|------------|------------|
| `{service, message, level, traceId}` | **0** | None |
| `{..., responseTime: 142}` | **1** ZADD + 1 ZREMRANGEBYSCORE | 1 pipeline |
| `{..., responseTime: 142, dbQueryTime: 45}` | **2** ZADDs + 2 ZREMRANGEBYSCOREs | 1 pipeline |
| `{..., responseTime, dbQueryTime, externalApiTime}` | **3** ZADDs + 3 ZREMRANGEBYSCOREs | 1 pipeline |

---

## Rolling Cleanup

Cleanup runs in two places:
1. **On every insert** — `recordLatency()` runs `ZREMRANGEBYSCORE` for each active key
2. **In the evaluator** — `cleanupLatencyWindows()` runs defensively before percentile computation

This ensures bounded memory even when a service stops sending logs.

---

## Testing Workflow

### Step 0: Start System
```bash
# Terminal 1: Server
cd backend && npm run dev

# Terminal 2: Worker
cd backend && npm run worker:dev
```

### Test 1: Backward Compatibility (no latency)
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{service="old-svc"; message="no latency"; level="info"; traceId=[guid]::NewGuid().ToString()} | ConvertTo-Json)
```
**Expected:** No `⏱️ Latency recorded` log. No latency ZSETs created. Everything works as before.

### Test 2: Single Latency Field
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{service="api-svc"; message="login"; level="info"; traceId=[guid]::NewGuid().ToString(); responseTime=142.5; endpoint="/login"; statusCode=200} | ConvertTo-Json)
```
**Expected:** `⏱️ Latency recorded [api-svc] responseTime=142.5ms`

### Test 3: All Three Latency Fields
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{service="api-svc"; message="checkout"; level="info"; traceId=[guid]::NewGuid().ToString(); responseTime=250; dbQueryTime=45; externalApiTime=120; endpoint="/checkout"; statusCode=200} | ConvertTo-Json)
```
**Expected:** `⏱️ Latency recorded [api-svc] responseTime=250ms dbQueryTime=45ms externalApiTime=120ms`

### Test 4: Generate Varied Latency (for percentiles)
```powershell
1..200 | ForEach-Object {
  $rt = Get-Random -Minimum 50 -Maximum 500
  $db = Get-Random -Minimum 10 -Maximum 100
  $ext = Get-Random -Minimum 20 -Maximum 300
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="perf-svc"; message="request $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=$rt; dbQueryTime=$db; externalApiTime=$ext; statusCode=200
    } | ConvertTo-Json)
}
```

**Expected evaluator output (after next 30s cycle):**
```
📐 [perf-svc] responseTime: P50=275ms P95=475ms P99=495ms (min=51ms max=499ms samples=200)
📐 [perf-svc] dbQueryTime: P50=55ms P95=95ms P99=99ms (min=10ms max=100ms samples=200)
📐 [perf-svc] externalApiTime: P50=160ms P95=285ms P99=297ms (min=21ms max=299ms samples=200)
```

### Test 5: Validation Errors
```powershell
# Negative latency → 400
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{service="test"; message="bad"; level="info"; traceId=[guid]::NewGuid().ToString(); responseTime=-5} | ConvertTo-Json)

# Invalid statusCode → 400
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{service="test"; message="bad"; level="info"; traceId=[guid]::NewGuid().ToString(); statusCode=999} | ConvertTo-Json)
```

### Artillery Stress Test

**`load-test-latency.yml`:**
```yaml
config:
  target: "http://localhost:3001"
  phases:
    - duration: 5
      arrivalRate: 50
      name: "Warm-up"
    - duration: 15
      arrivalRate: 200
      name: "Sustained load"

scenarios:
  - flow:
      - post:
          url: "/api/v1/logs"
          json:
            service: "load-svc"
            message: "stress test"
            level: "info"
            traceId: "550e8400-e29b-41d4-a716-446655440000"
            responseTime: "{{ $randomNumber(50, 800) }}"
            dbQueryTime: "{{ $randomNumber(5, 150) }}"
            statusCode: 200
```

```bash
npx artillery run load-test-latency.yml
```

---

## Redis Inspection Commands

```bash
# Latency ZSET sizes
redis-cli ZCARD "lat:resp:perf-svc"
redis-cli ZCARD "lat:db:perf-svc"
redis-cli ZCARD "lat:ext:perf-svc"

# View recent samples (with timestamps as scores)
redis-cli ZREVRANGE "lat:resp:perf-svc" 0 4 WITHSCORES

# Check all latency keys
redis-cli SCAN 0 MATCH "lat:*"

# Memory usage
redis-cli MEMORY USAGE "lat:resp:perf-svc"

# Verify stale cleanup (should be 0 old entries)
redis-cli ZCOUNT "lat:resp:perf-svc" -inf 1716000000000
```

---

## Memory & Scalability

| Metric | Value |
|--------|-------|
| Members per ZSET (300 req/s) | ~18,000 |
| Bytes per member | ~33 bytes (25 member + 8 score) |
| Memory per ZSET | ~594 KB |
| 3 types × 10 services | ~17.8 MB |
| After 60s idle | 0 (all cleaned up) |

Total Redis memory for latency analytics: **well under 20 MB** even at sustained high throughput.

---

## Project Structure After Phase 5A

```
backend/src/
├── config/index.js                 ← modified (+latency config)
├── schemas/logSchema.js            ← modified (+5 APM fields)
├── services/
│   ├── alertEvaluator.js           ← modified (+Path 3 percentiles)
│   ├── bucketAggregator.js          (untouched)
│   ├── ewmaBaseline.js              (untouched)
│   ├── latencyAggregator.js        ← NEW
│   ├── redisProducer.js            ← modified (+conditional APM fields)
│   └── slidingWindowAggregator.js   (untouched)
├── workers/
│   ├── logConsumer.js              ← modified (+recordLatency call)
│   └── logNormaliser.js            ← modified (+APM field parsing)
├── server.js
└── worker.js                        (untouched)
```
