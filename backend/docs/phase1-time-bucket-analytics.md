# Phase 1: Time Bucket Analytics — Walkthrough

## What Was Implemented

Per-minute, per-service Redis Hash buckets for real-time log aggregation. Each bucket tracks total logs, error logs, and bucket start time using atomic Redis operations.

---

## Redis Key Structure

```
tb:{service}:{YYYYMMDDHHmm}   ← Hash with 180s TTL

Fields:
  total   — total log count in this minute
  errors  — error-level log count
  start   — ISO timestamp of bucket start (set once via HSETNX)
```

**Example:**
```
tb:auth-service:202605151135
  total:  347
  errors: 12
  start:  2026-05-15T11:35:00.000Z
```

---

## Files

| File | Action | Description |
|------|--------|-------------|
| `src/services/bucketAggregator.js` | **NEW** | `updateBucket()` — pipelined HSETNX + HINCRBY + EXPIRE |
| `src/config/index.js` | **MODIFY** | Added `timeBucket` config (keyPrefix, ttlSeconds, flushIntervalMs) |
| `src/workers/logConsumer.js` | **MODIFY** | Added best-effort `updateBucket()` call after MongoDB insert |

---

## Key Design Decisions

- **Redis Pipeline:** All 4 commands (HSETNX, HINCRBY×2, EXPIRE) batched in a single round-trip (~0.1ms overhead)
- **HSETNX for `start`:** Only written once per bucket — idempotent
- **EXPIRE refreshed on every write:** Safety net TTL (180s) prevents memory leaks from orphaned buckets
- **Best-effort:** Bucket update failure never blocks the log pipeline (independent try/catch)
- **UTC timestamps:** Consistent bucket boundaries regardless of server timezone

---

## Redis Commands for Inspection

```bash
# List all bucket keys
redis-cli SCAN 0 MATCH "tb:*"

# Inspect a specific bucket
redis-cli HGETALL "tb:auth-service:202605151135"

# Check TTL remaining
redis-cli TTL "tb:auth-service:202605151135"

# Memory usage
redis-cli MEMORY USAGE "tb:auth-service:202605151135"
```
