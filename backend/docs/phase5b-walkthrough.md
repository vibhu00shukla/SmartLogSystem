# Phase 5B: Endpoint-Level Observability Walkthrough

I have fully implemented endpoint-level latency percentiles and rolling registries into the APM pipeline, as per your approved plan.

## What Was Accomplished

1. **ZSET Endpoint Registries**
   - Implemented `endpointRegistry.js` utilizing Redis ZSETs.
   - `ep:services` maintains the list of services sending endpoint telemetry.
   - `ep:endpoints:{service}` maintains the list of active endpoints per service.
   - Using ZSETs ensures endpoints seamlessly expire (24-hour TTL) if traffic to them stops, preventing memory bloat from deprecated/ephemeral routes.

2. **Ingestion Pipeline Upgrades**
   - `latencyAggregator.js` updated to accept optional `endpoint` values.
   - Logs with an `endpoint` field now write their latency to **both** the service-level (`lat:resp:{service}`) and endpoint-level (`lat:resp:{service}:{endpoint}`) sliding windows.
   - The same 60s retention-buffer cleanup is applied to endpoint ZSETs to prevent stale memory build-up.

3. **Evaluator Iteration Changes**
   - Refactored `alertEvaluator.js` Path 3.
   - Now loops over the service-level percentiles exactly as before.
   - Then queries `endpointRegistry` and loops through every active endpoint for that service to calculate and log its individual P50/P95/P99 latency distribution.
   - Skips evaluation if the service drops below the `minRequestThreshold`, protecting system cycles during idle periods.

## File Changes Overview

- `src/config/index.js` — Added registry configuration mapping (`ep:services`, `ep:endpoints`) and TTL.
- `src/services/endpointRegistry.js` — **[NEW]** Complete registry logic using pipelined ZADD/ZREMRANGEBYSCORE.
- `src/services/latencyAggregator.js` — Dual-write logging for both service and endpoint scopes.
- `src/services/alertEvaluator.js` — Updated loop to iterate and compute endpoint metrics cleanly.

## Testing It Yourself

Since you prefer to handle the testing, here is how you can verify Phase 5B:

### 1. Ingest Endpoint Data
Use your Artillery/Postman to send logs that include the `endpoint` field.
```json
{
  "service": "auth-service",
  "endpoint": "/api/v1/login",
  "level": "info",
  "message": "User login success",
  "responseTime": 142.5,
  "statusCode": 200
}
```

### 2. Verify Redis Keys
Check that the data exists across both scopes:
```bash
# Service-level ZSET
redis-cli ZCARD lat:resp:auth-service

# Endpoint-level ZSET
redis-cli ZCARD lat:resp:auth-service:/api/v1/login

# Endpoint Registry 
redis-cli ZRANGE ep:services 0 -1
redis-cli ZRANGE ep:endpoints:auth-service 0 -1
```

### 3. Observe the Worker Evaluator
Watch the `node src/worker.js` logs every 30 seconds. You should now see granular outputs:
```
🔍 Alert evaluator — scanning sliding windows…
   📋 Services registered: [auth-service]
   📐 [auth-service] responseTime: P50=80ms P95=150ms P99=300ms (min=50ms max=350ms samples=200)
   📐 [auth-service][/api/v1/login] responseTime: P50=90ms P95=160ms P99=320ms (min=50ms max=350ms samples=100)
🔍 Evaluation complete
```
