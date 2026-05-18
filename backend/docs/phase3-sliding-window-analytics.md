# Phase 3: Sliding Window Analytics — Walkthrough

## What Was Implemented

Replaced fixed-minute bucket alerting with rolling 60-second window evaluation using Redis Sorted Sets. Events at 12:40:59 and 12:41:01 are now correctly evaluated together.

**Coexistence:** Buckets (`tb:*`) continue to be written. The evaluator switched its READ source to sliding windows (`sw:*`).

---

## The Problem Solved

```
Old: 12:40:59 → bucket 1240 (85 logs)    12:41:01 → bucket 1241 (23 logs)
     Neither bucket reaches 100 threshold → NO ALERT (gap!)

New: 12:40:59 + 12:41:01 → same 60s window (108 logs, 8 errors)
     errorRate = 7.4% → CRITICAL alert fires ✅
```

---

## Dual-ZSET Architecture

```
sw:{service}       ← ZSET: ALL events     (score=epoch_ms, member=streamId)
sw:{service}:err   ← ZSET: ERROR events   (same structure)
sw:services        ← SET: service name registry
```

**Why two ZSETs?**
- Single ZSET: counting errors = iterate all members + filter → O(N) at 18K entries
- Dual ZSETs: `ZCOUNT` on each → O(log N) for both counts

**Memory:** ~554 KB per service at 300 req/s (60s window). Trivial.

---

## Files

| File | Action | Description |
|------|--------|-------------|
| `src/services/slidingWindowAggregator.js` | **NEW** | `recordEvent()` + `getRollingMetrics()` + `cleanupWindow()` |
| `src/config/index.js` | **MODIFY** | Added `slidingWindow` config |
| `src/workers/logConsumer.js` | **MODIFY** | Added `recordEvent()` call (best-effort) |
| `src/services/alertEvaluator.js` | **MODIFY** | Migrated from SCAN `tb:*` to SMEMBERS `sw:services` + ZCOUNT |

**Untouched:** `bucketAggregator.js`, `worker.js`, `mongo.js`

---

## Pipeline per Log (single round-trip)

For an error log:
```
1. ZADD sw:auth-service {timestamp_ms} {streamId}
2. ZADD sw:auth-service:err {timestamp_ms} {streamId}
3. SADD sw:services "auth-service"
4. ZREMRANGEBYSCORE sw:auth-service -inf {cutoff}
5. ZREMRANGEBYSCORE sw:auth-service:err -inf {cutoff}
```

---

## Alert Document (updated fields)

```json
{
  "alertType": "error_rate_threshold",
  "dataSource": "sliding_window",
  "windowSeconds": 60
}
```

---

## Redis Commands

```bash
# Service registry
redis-cli SMEMBERS "sw:services"

# Window sizes
redis-cli ZCARD "sw:auth-service"
redis-cli ZCARD "sw:auth-service:err"

# Recent entries
redis-cli ZREVRANGE "sw:auth-service" 0 4 WITHSCORES

# Verify buckets still written (coexistence)
redis-cli SCAN 0 MATCH "tb:*"
```
