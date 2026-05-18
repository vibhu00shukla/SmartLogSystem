# Phase 5A: Latency Analytics — Testing & Validation Guide

## Part 1: Understanding Real-World Latency

### Why Latency Distributions Are NOT Normal

Most developers assume latency follows a bell curve. It doesn't. Real-world latency follows a **right-skewed distribution** — a long tail of slow requests:

```
Frequency
│
│ ██
│ ████
│ ██████
│ ████████
│ ██████████
│ ████████████
│ █████████████░░░░░░░░░░░░░░░░░░░░
│ █████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
└─────────────────────────────────────────────────── Latency (ms)
  50ms  100ms  200ms  500ms        1000ms     2000ms+
  ← most requests here →          ← tail latency →
```

**Key insight:** The average is misleading. A service with avg=120ms might have P99=1800ms — meaning 1% of users wait 15× longer than average.

### Why P50, P95, P99 Matter

| Percentile | What It Tells You | Operational Impact |
|------------|-------------------|-------------------|
| **P50** (median) | "Normal" user experience | If P50 is high, the service is fundamentally slow |
| **P95** | The worst 5% of users | SLA-relevant — most SLAs target P95 |
| **P99** | The worst 1% of users | Reveals hidden problems: GC pauses, connection pool exhaustion, cold caches |
| **Max** | Absolute worst case | Often an outlier — useful for debugging, not alerting |

### Patterns That Should Trigger Concern

| Pattern | What It Means | Example |
|---------|---------------|---------|
| **P50 rising gradually** | Service is degrading for everyone | DB table growing, no indexing |
| **P99 >> P95** | Tail latency problem | GC pauses, lock contention, cache misses |
| **P95 sudden spike** | Something broke for many users | Downstream service failing, connection pool full |
| **P99 oscillates** | Periodic interference | Cron jobs, batch processing, autoscaler lag |
| **All percentiles converge** | Service is saturated | All requests equally slow = resource exhaustion |

### How Tail Latency Evolves Under Load

```
Load (req/s)    P50     P95     P99
─────────────────────────────────────
50              80ms    120ms   150ms    ← healthy headroom
100             85ms    130ms   180ms    ← slight P99 growth
200             90ms    180ms   400ms    ← P99 diverging (queue buildup)
300             95ms    350ms   1200ms   ← P99 explosion (saturation)
400             250ms   800ms   3000ms   ← P50 rising = system overloaded
```

**Key:** P99 is the canary. It degrades FIRST, long before P50 shows trouble.

---

## Part 2: Testing Scenarios

### Prerequisites

```bash
# Clean state
redis-cli KEYS "lat:*" | xargs -r redis-cli DEL
redis-cli KEYS "sw:*" | xargs -r redis-cli DEL
redis-cli KEYS "tb:*" | xargs -r redis-cli DEL
redis-cli KEYS "ewma:*" | xargs -r redis-cli DEL
redis-cli KEYS "alert:cd:*" | xargs -r redis-cli DEL
```

Start server + worker in two terminals.

---

### Scenario 1: Backward Compatibility

**Goal:** Verify logs WITHOUT latency fields work exactly as before.

```powershell
# Send 10 plain logs — no APM fields
1..10 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="plain-svc"; message="no latency $_"; level="info";
      traceId=[guid]::NewGuid().ToString()
    } | ConvertTo-Json)
}
```

**Verify:**
- Worker logs: NO `⏱️ Latency recorded` lines
- Redis: `redis-cli ZCARD "lat:resp:plain-svc"` → `0` (or key doesn't exist)
- MongoDB: documents have `responseTime: null`, `dbQueryTime: null`
- Sliding windows and buckets still work normally

---

### Scenario 2: Single Latency Field

**Goal:** Verify only the present field creates a ZSET entry.

```powershell
1..50 | ForEach-Object {
  $rt = Get-Random -Minimum 80 -Maximum 200
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="single-lat"; message="request $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=$rt
    } | ConvertTo-Json)
}
```

**Verify:**
```bash
redis-cli ZCARD "lat:resp:single-lat"    # should be ~50
redis-cli ZCARD "lat:db:single-lat"      # should be 0
redis-cli ZCARD "lat:ext:single-lat"     # should be 0
```

---

### Scenario 3: Uniform Distribution (baseline)

**Goal:** Establish known percentile behavior with predictable data.

```powershell
# 200 requests with responseTime uniformly distributed 100-500ms
1..200 | ForEach-Object {
  $rt = 100 + ($_ * 2)  # 102, 104, 106, ..., 500 — perfectly spread
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="uniform-svc"; message="req $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=$rt; statusCode=200
    } | ConvertTo-Json)
}
```

**Expected percentiles** (uniform 102-500):
```
P50 ≈ 300ms    (middle of range)
P95 ≈ 480ms    (95% of 102-500)
P99 ≈ 496ms    (99% of 102-500)
min = 102ms
max = 500ms
```

**Verify in evaluator output:**
```
📐 [uniform-svc] responseTime: P50=~300ms P95=~480ms P99=~496ms
```

---

### Scenario 4: Bimodal Distribution (fast + slow)

**Goal:** Simulate a service where most requests are fast but some hit a slow path.

```powershell
# 180 fast requests (50-100ms) + 20 slow requests (800-1500ms)
1..180 | ForEach-Object {
  $rt = Get-Random -Minimum 50 -Maximum 100
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="bimodal-svc"; message="fast $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=$rt; statusCode=200
    } | ConvertTo-Json)
}
1..20 | ForEach-Object {
  $rt = Get-Random -Minimum 800 -Maximum 1500
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="bimodal-svc"; message="slow $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=$rt; statusCode=200
    } | ConvertTo-Json)
}
```

**Expected:**
```
P50 ≈ 75ms     (median is in the fast cluster)
P95 ≈ 1100ms   (95th percentile hits the slow cluster!)
P99 ≈ 1400ms   (deep in the slow cluster)
```

**Why this matters:** Average = ~180ms looks fine. But P95=1100ms reveals 5% of users are waiting 15× longer. This is a classic tail latency problem.

---

### Scenario 5: Tail Latency Spike

**Goal:** Simulate a healthy service that suddenly gets outliers.

```powershell
# 190 normal requests (60-120ms)
1..190 | ForEach-Object {
  $rt = Get-Random -Minimum 60 -Maximum 120
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="tail-svc"; message="normal $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=$rt; dbQueryTime=(Get-Random -Min 5 -Max 30); statusCode=200
    } | ConvertTo-Json)
}

# 10 extreme outliers (3000-8000ms) — simulating GC pauses or timeouts
1..10 | ForEach-Object {
  $rt = Get-Random -Minimum 3000 -Maximum 8000
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="tail-svc"; message="outlier $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=$rt; dbQueryTime=(Get-Random -Min 500 -Max 2000); statusCode=200
    } | ConvertTo-Json)
}
```

**Expected:**
```
responseTime: P50≈90ms  P95≈120ms  P99≈5000ms   ← P99 explodes while P50/P95 look fine
dbQueryTime:  P50≈17ms  P95≈30ms   P99≈1500ms   ← DB also shows tail latency
```

**Key learning:** P99 catches the outliers. P50 and even P95 can look normal while P99 screams.

---

### Scenario 6: Multiple Services (concurrent)

**Goal:** Verify per-service isolation and evaluator handles multiple services.

```powershell
# Service A: fast (30-80ms)
1..100 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="fast-api"; message="req $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=(Get-Random -Min 30 -Max 80); statusCode=200
    } | ConvertTo-Json)
}

# Service B: slow (200-600ms)
1..100 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="slow-api"; message="req $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=(Get-Random -Min 200 -Max 600); statusCode=200
    } | ConvertTo-Json)
}

# Service C: DB-heavy (response fast, DB slow)
1..100 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="db-heavy"; message="query $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=(Get-Random -Min 100 -Max 200);
      dbQueryTime=(Get-Random -Min 80 -Max 180); statusCode=200
    } | ConvertTo-Json)
}
```

**Expected evaluator output:**
```
📐 [fast-api]  responseTime: P50≈55ms   P95≈76ms   P99≈79ms
📐 [slow-api]  responseTime: P50≈400ms  P95≈580ms  P99≈595ms
📐 [db-heavy]  responseTime: P50≈150ms  P95≈195ms  P99≈199ms
📐 [db-heavy]  dbQueryTime:  P50≈130ms  P95≈175ms  P99≈179ms
```

**Verify isolation:**
```bash
redis-cli ZCARD "lat:resp:fast-api"   # ~100
redis-cli ZCARD "lat:resp:slow-api"   # ~100
redis-cli ZCARD "lat:resp:db-heavy"   # ~100
redis-cli ZCARD "lat:db:db-heavy"     # ~100
redis-cli ZCARD "lat:db:fast-api"     # 0 (fast-api didn't send dbQueryTime)
```

---

### Scenario 7: Rolling Cleanup Verification

**Goal:** Prove entries expire from the window after 60 seconds.

```powershell
# Send 50 requests with responseTime=999ms
1..50 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="cleanup-svc"; message="wave1 $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=999; statusCode=200
    } | ConvertTo-Json)
}

Write-Host "Wave 1 sent. Checking ZCARD..."
redis-cli ZCARD "lat:resp:cleanup-svc"
# Should show ~50

Write-Host "Waiting 65 seconds for window to expire..."
Start-Sleep -Seconds 65

# Send 30 requests with responseTime=100ms
1..30 | ForEach-Object {
  Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
    -ContentType "application/json" `
    -Body (@{
      service="cleanup-svc"; message="wave2 $_"; level="info";
      traceId=[guid]::NewGuid().ToString();
      responseTime=100; statusCode=200
    } | ConvertTo-Json)
}

Write-Host "Wave 2 sent. Checking ZCARD..."
redis-cli ZCARD "lat:resp:cleanup-svc"
# Should show ~30 (wave 1 expired!)
```

**Key verification:** ZCARD drops from ~50 to ~30. The 999ms entries are gone. Evaluator now shows:
```
📐 [cleanup-svc] responseTime: P50=100ms P95=100ms P99=100ms (samples=30)
```

No trace of the old 999ms entries — the window is clean.

---

### Scenario 8: Edge Cases

#### 8a: Zero Latency
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{
    service="zero-svc"; message="instant"; level="info";
    traceId=[guid]::NewGuid().ToString();
    responseTime=0; statusCode=200
  } | ConvertTo-Json)
```
**Expected:** Accepted (0 is non-negative). ZSET member: `"...:0"`.

#### 8b: Very Large Latency
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{
    service="timeout-svc"; message="timeout"; level="error";
    traceId=[guid]::NewGuid().ToString();
    responseTime=30000; statusCode=504
  } | ConvertTo-Json)
```
**Expected:** Accepted. 30-second response time is valid (gateway timeout).

#### 8c: Decimal Latency
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{
    service="precise-svc"; message="fast"; level="info";
    traceId=[guid]::NewGuid().ToString();
    responseTime=0.5; dbQueryTime=0.1; statusCode=200
  } | ConvertTo-Json)
```
**Expected:** Accepted. Sub-millisecond precision preserved.

#### 8d: Negative Latency (should reject)
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{
    service="bad-svc"; message="bad"; level="info";
    traceId=[guid]::NewGuid().ToString();
    responseTime=-5
  } | ConvertTo-Json)
```
**Expected:** `400 Validation failed` — `responseTime must be non-negative`

#### 8e: String Instead of Number (should reject)
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3001/api/v1/logs" `
  -ContentType "application/json" `
  -Body (@{
    service="bad-svc"; message="bad"; level="info";
    traceId=[guid]::NewGuid().ToString();
    responseTime="fast"
  } | ConvertTo-Json)
```
**Expected:** `400 Validation failed` — `responseTime must be a number`

---

### Scenario 9: Artillery Stress Test

**Create `load-test-latency.yml`:**
```yaml
config:
  target: "http://localhost:3001"
  phases:
    - duration: 10
      arrivalRate: 50
      name: "Warm-up"
    - duration: 20
      arrivalRate: 200
      name: "Sustained load"
    - duration: 10
      arrivalRate: 500
      name: "Peak burst"

scenarios:
  - name: "Normal latency"
    weight: 80
    flow:
      - post:
          url: "/api/v1/logs"
          json:
            service: "stress-svc"
            message: "load test normal"
            level: "info"
            traceId: "550e8400-e29b-41d4-a716-446655440000"
            responseTime: "{{ $randomNumber(50, 200) }}"
            dbQueryTime: "{{ $randomNumber(5, 50) }}"
            statusCode: 200

  - name: "Slow requests"
    weight: 15
    flow:
      - post:
          url: "/api/v1/logs"
          json:
            service: "stress-svc"
            message: "load test slow"
            level: "warn"
            traceId: "550e8400-e29b-41d4-a716-446655440001"
            responseTime: "{{ $randomNumber(500, 2000) }}"
            dbQueryTime: "{{ $randomNumber(200, 800) }}"
            statusCode: 200

  - name: "Timeout errors"
    weight: 5
    flow:
      - post:
          url: "/api/v1/logs"
          json:
            service: "stress-svc"
            message: "load test timeout"
            level: "error"
            traceId: "550e8400-e29b-41d4-a716-446655440002"
            responseTime: "{{ $randomNumber(5000, 15000) }}"
            statusCode: 504
```

```bash
npx artillery run load-test-latency.yml
```

**What to observe during the test:**
```bash
# In a separate terminal, poll every 10 seconds:
while true; do
  echo "---"
  echo "ZCARD lat:resp:stress-svc: $(redis-cli ZCARD 'lat:resp:stress-svc')"
  echo "ZCARD lat:db:stress-svc: $(redis-cli ZCARD 'lat:db:stress-svc')"
  echo "Memory: $(redis-cli MEMORY USAGE 'lat:resp:stress-svc')"
  sleep 10
done
```

**Expected behavior:**
- ZCARD grows during load, caps at ~window × arrival_rate (e.g., 60s × 500 = 30,000)
- Memory stays bounded (< 1MB per ZSET)
- After test ends + 60s, ZCARD drops to 0
- Evaluator shows percentiles reflecting the 80/15/5 distribution

---

## Part 3: Redis Inspection Workflows

### Comprehensive Inspection After Tests

```bash
echo "=== Service Registry ==="
redis-cli SMEMBERS "sw:services"

echo "=== Latency ZSET Sizes ==="
for svc in $(redis-cli SMEMBERS "sw:services"); do
  echo "  $svc:"
  echo "    lat:resp  = $(redis-cli ZCARD "lat:resp:$svc")"
  echo "    lat:db    = $(redis-cli ZCARD "lat:db:$svc")"
  echo "    lat:ext   = $(redis-cli ZCARD "lat:ext:$svc")"
done

echo "=== Memory Usage ==="
for svc in $(redis-cli SMEMBERS "sw:services"); do
  echo "  lat:resp:$svc = $(redis-cli MEMORY USAGE "lat:resp:$svc") bytes"
done

echo "=== Sample Members (last 5) ==="
redis-cli ZREVRANGE "lat:resp:stress-svc" 0 4 WITHSCORES

echo "=== Total Redis Memory ==="
redis-cli INFO memory | grep used_memory_human

echo "=== Total Keys ==="
redis-cli DBSIZE
```

### Manual Percentile Verification

To independently verify the evaluator's calculations:

```bash
# Get all members in the window
redis-cli ZRANGEBYSCORE "lat:resp:uniform-svc" "-inf" "+inf" > /tmp/members.txt

# Count samples
wc -l /tmp/members.txt

# In Node.js REPL or script:
# const members = fs.readFileSync('/tmp/members.txt','utf8').trim().split('\n');
# const values = members.map(m => parseFloat(m.split(':').pop())).sort((a,b) => a-b);
# console.log('P50:', values[Math.ceil(0.50 * values.length) - 1]);
# console.log('P95:', values[Math.ceil(0.95 * values.length) - 1]);
# console.log('P99:', values[Math.ceil(0.99 * values.length) - 1]);
```

---

## Part 4: Memory Growth Observation

### Expected Behavior Table

| Scenario | req/s | Window | ZSET Size | Memory/ZSET | Total (3 types × 1 svc) |
|----------|-------|--------|-----------|-------------|--------------------------|
| Low traffic | 10 | 60s | 600 | ~20 KB | ~60 KB |
| Medium | 100 | 60s | 6,000 | ~200 KB | ~600 KB |
| High | 300 | 60s | 18,000 | ~594 KB | ~1.8 MB |
| Burst (500) | 500 | 60s | 30,000 | ~990 KB | ~3 MB |
| 10 services × high | 300 | 60s | 18,000 each | ~594 KB each | ~18 MB |

### Verify Bounded Growth

```bash
# Before test
redis-cli INFO memory | grep used_memory_human

# During peak load
redis-cli INFO memory | grep used_memory_human

# 2 minutes after test ends
redis-cli INFO memory | grep used_memory_human   # should return to pre-test level
```

**The critical check:** Memory after test ends + 60s should return to approximately the pre-test level. If it doesn't, cleanup isn't working.

---

## Part 5: Checklist Summary

| # | Test | What You're Verifying | Pass Criteria |
|---|------|-----------------------|---------------|
| 1 | Plain log (no latency) | Backward compat | No ZSET writes, no errors |
| 2 | Single latency field | Conditional writes | Only 1 ZSET populated |
| 3 | Uniform distribution | Percentile accuracy | P50≈300, P95≈480, P99≈496 |
| 4 | Bimodal distribution | Tail latency detection | P50 low, P95/P99 high |
| 5 | Outlier injection | P99 sensitivity | P99 spikes while P50/P95 stable |
| 6 | Multiple services | Per-service isolation | Each service has independent metrics |
| 7 | 60s expiry | Rolling cleanup | ZCARD drops after window expires |
| 8a-e | Edge cases | Schema validation | 0, large, decimal accepted; negative, string rejected |
| 9 | Artillery 500 req/s | Stress resilience | No crashes, bounded memory, accurate percentiles |
| 10 | Post-test memory | Cleanup verification | Memory returns to baseline after 60s idle |
