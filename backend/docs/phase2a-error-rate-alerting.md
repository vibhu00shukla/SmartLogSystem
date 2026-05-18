# Phase 2A: Error Rate Alert Detection — Walkthrough

## What Was Implemented

A decoupled alert evaluation system that periodically scans Redis time buckets, computes per-service error rates, and persists alert documents to MongoDB — with Redis-based cooldowns and severity escalation.

---

## Architecture

```
Worker Process
├── logConsumer (blocking XREADGROUP loop)
│   └── updateBucket() → Redis Hashes
│
└── alertEvaluator (setInterval 30s, independent)
    ├── SCAN tb:* → HGETALL → computeSeverity
    ├── cooldown check (alert:cd:{service})
    ├── insertAlert → MongoDB
    └── SET cooldown with TTL
```

---

## Alert Thresholds

| Severity | Error Rate | Numeric Rank |
|----------|-----------|--------------|
| WARNING | ≥ 2% | 1 |
| CRITICAL | ≥ 5% | 2 |
| SEVERE | ≥ 10% | 3 |

**Minimum request threshold:** 100 (avoids false positives from low traffic)

---

## Cooldown & Escalation

- **Cooldown key:** `alert:cd:{service}` — String with 300s TTL
- **Value:** The severity level (e.g., "CRITICAL")
- **Suppression:** Same or lower severity → suppressed during cooldown
- **Escalation:** Higher severity → overrides cooldown, fires new alert

Example: WARNING cooldown active → CRITICAL spike → cooldown overridden, CRITICAL alert fires.

---

## Files

| File | Action | Description |
|------|--------|-------------|
| `src/services/alertEvaluator.js` | **NEW** | Core evaluator: SCAN, thresholds, cooldown, escalation, MongoDB persistence |
| `src/config/index.js` | **MODIFY** | Added `alerting` config (thresholds, cooldown, eval interval) |
| `src/db/mongo.js` | **MODIFY** | Added `alerts` collection + compound indexes + `insertAlert()` |
| `src/worker.js` | **MODIFY** | Starts evaluator on boot, clears interval on shutdown |

---

## MongoDB Alert Document

```json
{
  "service": "auth-service",
  "severity": "CRITICAL",
  "errorRate": 5.66,
  "totalLogs": 106,
  "errorLogs": 6,
  "timestamp": "2026-05-16T11:15:30Z",
  "alertType": "error_rate_threshold",
  "bucketKey": "tb:auth-service:202605161115",
  "bucketMinute": "202605161115",
  "evaluatedAt": "2026-05-16T11:15:30Z"
}
```

---

## Redis Commands

```bash
# List active cooldowns
redis-cli SCAN 0 MATCH "alert:cd:*"

# Check cooldown value and TTL
redis-cli GET "alert:cd:auth-service"
redis-cli TTL "alert:cd:auth-service"
```
