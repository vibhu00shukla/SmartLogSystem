# Phase 4: EWMA Baselines & Adaptive Anomaly Detection — Walkthrough

## What Was Implemented

Added an EWMA baseline system that learns "normal" error rates per service, then detects abnormal deviations — **augmenting** existing threshold alerts without replacing them.

---

## Architecture — Dual Alert Paths

```
evaluateService(redis, service)
│
├── 1. cleanupWindow()
├── 2. getRollingMetrics()        ← ZCOUNT on sw:* ZSETs
├── 3. min request check
├── 4. compute errorRate
├── 5. updateBaseline()           ← EWMA always learns ★
│
├── 6. handleThresholdAlert()     ← Path 1 (Phase 2A logic, unchanged)
│      │  computeSeverity()
│      │  cooldown: alert:cd:{service}
│      │  alertType: error_rate_threshold
│      └─ escalation: WARNING→CRITICAL→SEVERE
│
└── 7. handleAdaptiveAlert()      ← Path 2 (Phase 4, NEW) ★
       │  detectAnomaly() — 3 safety guards
       │  cooldown: alert:cd:adp:{service}  (SEPARATE namespace)
       └─ alertType: adaptive_deviation
```

**Key:** Both paths run every cycle. Separate cooldowns = independent operation.

---

## Changes Made

| File | Action | Description |
|------|--------|-------------|
| [ewmaBaseline.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/services/ewmaBaseline.js) | **NEW** | EWMA calc, Redis Hash read/write, deviation detection, 3 safety guards |
| [index.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/config/index.js) | **MODIFY** | +15 lines: `ewma` config section |
| [alertEvaluator.js](file:///c:/Users/vibhu/OneDrive/Desktop/log%20intelligence%20plat/backend/src/services/alertEvaluator.js) | **MODIFY** | Restructured to dual-path: threshold + adaptive |

**NOT modified:** `logConsumer.js`, `slidingWindowAggregator.js`, `bucketAggregator.js`, `worker.js`, `mongo.js`

---

## EWMA Lifecycle

```
newBaseline = (currentRate × α) + (previousBaseline × (1 - α))     α = 0.5
```

| Cycle | Error Rate | EWMA Calculation | Baseline |
|-------|-----------|------------------|----------|
| 1 | 1.0% | Seed (first value) | 1.000% |
| 2 | 1.0% | (1.0×0.5)+(1.0×0.5) | 1.000% |
| 3 | 1.0% | (1.0×0.5)+(1.0×0.5) | 1.000% |
| 4 | 1.0% | (1.0×0.5)+(1.0×0.5) | 1.000% |
| 5 | **8.0%** | (8.0×0.5)+(1.0×0.5) | 4.500% |
| | | **↑ SPIKE: 8% vs baseline 1% = 8× deviation → ANOMALY** | |

---

## Redis Structure

```
ewma:auth-service           ← Hash (NO TTL — persists forever)
  baseline:   "1.000000"     ← current EWMA value (%)
  samples:    "15"           ← evaluation cycles completed
  updatedAt:  "2026-05-17T12:30:00Z"

alert:cd:adp:auth-service   ← String (300s TTL)
  value: "ANOMALY"           ← adaptive cooldown
```

**Full key inventory:**
```
tb:*:*             Hash    (time buckets, 180s TTL)
sw:*               ZSET    (sliding windows, self-cleaning)
sw:*:err           ZSET    (error windows, self-cleaning)
sw:services        SET     (service registry)
ewma:*             Hash    (EWMA baselines, persistent)       ★ NEW
alert:cd:*         String  (threshold cooldowns, 300s TTL)
alert:cd:adp:*     String  (adaptive cooldowns, 300s TTL)     ★ NEW
```

---

## Cold-Start Behavior

EWMA requires **≥5 evaluation cycles** (2.5 min) before arming adaptive alerts:

```
Cycle 1: 📊 EWMA [svc] rate=1.0% baseline=0.00→1.00% samples=1
         🔬 [svc] adaptive: training (samples=1/5) — no anomaly

Cycle 4: 📊 EWMA [svc] rate=1.0% baseline=1.00→1.00% samples=4
         🔬 [svc] adaptive: training (samples=4/5) — no anomaly

Cycle 5: 📊 EWMA [svc] rate=8.0% baseline=1.00→4.50% samples=5
         🔬 ANOMALY [svc] rate=8.00% baseline=1.00% deviation=8.00×  ← ARMED!
```

**During cold-start, threshold alerts STILL fire normally** — only adaptive detection is delayed.

---

## 3 Safety Guards (detectAnomaly)

| Guard | Purpose | Default |
|-------|---------|---------|
| `samples >= minSamples` | Cold-start: baseline must be trained | 5 cycles |
| `currentRate >= minAbsoluteRate` | Filters noise from tiny rates | 1% |
| `deviation >= deviationThreshold` | Rate must significantly exceed baseline | 3× |

All three must pass for an anomaly alert to fire.

---

## Testing Workflow

### Step 0: Clean State
```bash
redis-cli KEYS "ewma:*" | xargs -r redis-cli DEL
redis-cli KEYS "alert:cd:adp:*" | xargs -r redis-cli DEL
redis-cli KEYS "alert:cd:*" | xargs -r redis-cli DEL
redis-cli KEYS "sw:*" | xargs -r redis-cli DEL
```
```bash
mongosh "mongodb://localhost:27017/log-intelligence" --eval "db.alerts.deleteMany({})"
```

### Test 1: Baseline Learning (steady 1% errors)

Send 5 rounds of 100 info + 1 error, 30s apart:
```powershell
1..5 | ForEach-Object {
  $r = $_
  1..100 | ForEach-Object {
    Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
      -ContentType "application/json" `
      -Body (@{service="learn-svc"; message="ok r$r-$_"; level="info"; traceId=[guid]::NewGuid().ToString()} | ConvertTo-Json)
  }
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{service="learn-svc"; message="fail r$r"; level="error"; traceId=[guid]::NewGuid().ToString()} | ConvertTo-Json)
  if ($_ -lt 5) { Start-Sleep -Seconds 30 }
}
```

**Expected:** EWMA converges to ~1%. No threshold alerts (1% < 2%). No anomaly (training).

### Test 2: Anomaly Spike (after training)

After 5 cycles, spike to 8%:
```powershell
1..92 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{service="learn-svc"; message="ok spike $_"; level="info"; traceId=[guid]::NewGuid().ToString()} | ConvertTo-Json)
}
1..8 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{service="learn-svc"; message="fail spike $_"; level="error"; traceId=[guid]::NewGuid().ToString()} | ConvertTo-Json)
}
```

**Expected:** `🔬 ANOMALY [learn-svc] rate=8.00% baseline=1.00% deviation=8.00×`
AND `🚨 ALERT [CRITICAL]` — both fire independently!

### Test 3: Cold-Start Suppression

Brand new service with immediate high errors:
```powershell
1..90 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{service="new-svc"; message="ok $_"; level="info"; traceId=[guid]::NewGuid().ToString()} | ConvertTo-Json)
}
1..12 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{service="new-svc"; message="fail $_"; level="error"; traceId=[guid]::NewGuid().ToString()} | ConvertTo-Json)
}
```

**Expected:**
- Threshold SEVERE fires ✅ (11.76% ≥ 10%)
- Adaptive: `training (samples=1/5) — no anomaly` ✅ (suppressed)

### Test 4: Adaptive Cooldown

After Test 2 fires an anomaly, send same spike again:
```powershell
# Same 8% error pattern
1..92 | ForEach-Object { ... service="learn-svc" level="info" ... }
1..8 | ForEach-Object { ... service="learn-svc" level="error" ... }
```

**Expected:** `🔇 [learn-svc] ANOMALY alert suppressed — adaptive cooldown active (TTL=275s)`

### Test 5: Below Min Absolute Rate

Service with baseline 0.5%, current rate 0.8% (1.6× deviation, below 3×, but also below 1% min):
```powershell
1..200 | ForEach-Object {
  Invoke-RestMethod ... service="low-err-svc" level="info" ...
}
1..1 | ForEach-Object {
  Invoke-RestMethod ... service="low-err-svc" level="error" ...
}
```

**Expected:** `rate=0.50% below min absolute 1% — no anomaly`

---

## Redis Inspection Commands

```bash
# EWMA baselines
redis-cli HGETALL "ewma:learn-svc"
redis-cli SCAN 0 MATCH "ewma:*"

# Adaptive cooldowns (separate from threshold)
redis-cli SCAN 0 MATCH "alert:cd:adp:*"
redis-cli GET "alert:cd:adp:learn-svc"
redis-cli TTL "alert:cd:adp:learn-svc"

# Threshold cooldowns (unchanged)
redis-cli SCAN 0 MATCH "alert:cd:*" 
# Note: this also matches alert:cd:adp:* — filter visually

# Memory (EWMA hashes are ~100 bytes each)
redis-cli MEMORY USAGE "ewma:learn-svc"
```

---

## MongoDB Verification

```js
// Adaptive anomaly alerts
db.alerts.find({ alertType: "adaptive_deviation" }).sort({ timestamp: -1 }).pretty()

// Threshold alerts (unchanged)
db.alerts.find({ alertType: "error_rate_threshold" }).sort({ timestamp: -1 }).pretty()

// Both for same service
db.alerts.find({ service: "learn-svc" }).sort({ timestamp: -1 }).pretty()

// Count by type
db.alerts.aggregate([
  { $group: { _id: "$alertType", count: { $sum: 1 } } }
])
```

**Adaptive alert document shape:**
```json
{
  "service": "learn-svc",
  "severity": "ANOMALY",
  "errorRate": 8.0,
  "totalLogs": 100,
  "errorLogs": 8,
  "alertType": "adaptive_deviation",
  "dataSource": "sliding_window",
  "windowSeconds": 60,
  "ewmaBaseline": 1.0,
  "deviationMultiplier": 8.0,
  "evaluatedAt": "2026-05-17T..."
}
```

---

## Project Structure After Phase 4

```
backend/src/services/
├── alertEvaluator.js           ← modified (dual-path: threshold + adaptive)
├── bucketAggregator.js          (untouched)
├── ewmaBaseline.js             ← NEW (EWMA calc + Redis + anomaly detection)
├── redisProducer.js
└── slidingWindowAggregator.js   (untouched)
```
