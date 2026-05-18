# Phase 5B: Endpoint-Level Latency Observability

This plan extends the current service-level latency analytics to support endpoint-level metrics (P50/P95/P99) while fully preserving the existing architecture.

## User Review Required

> [!WARNING]
> **Endpoint Cardinality Explosion**
> The design accepts the `endpoint` field directly from telemetry. If clients send raw URLs containing IDs (e.g., `/api/v1/users/12345` instead of `/api/v1/users/:id`), this will create a new Redis key and registry entry for every single request. This would cause memory exhaustion and crash the evaluator loop. We must ensure the upstream APM clients are sending **normalized route paths**.

## Open Questions

1. **Endpoint Error Rates:** Your prompt strictly specifies endpoint-level *latency* windows. Do you also want endpoint-level *error counting* (sliding windows for error rate / thresholds), or are we sticking strictly to latency percentiles for now?
2. **Alerting on Endpoints:** Should we implement alerting (Threshold/EWMA) on these endpoint-level percentiles in this phase, or just compute and log them (Phase 5B) and save alerting for 5C?

## Proposed Changes

### 1. Redis Key Design & Registry Strategy

I agree with your proposed structure, but I highly recommend **using a ZSET for the endpoint registry instead of a normal SET**. This allows us to time-out stale or deprecated endpoints automatically so the evaluator doesn't scan them forever.

**Latency Windows (Existing + New):**
- `lat:resp:{service}` (Existing service-level)
- `lat:resp:{service}:{endpoint}` (New endpoint-level)
- `lat:db:{service}:{endpoint}`
- `lat:ext:{service}:{endpoint}`

**Registries (New):**
- `ep:services` → ZSET of services that have endpoint telemetry (score = timestamp).
- `ep:endpoints:{service}` → ZSET of endpoints for a specific service (score = timestamp).

### 2. Implementation Roadmap

#### [MODIFY] `src/config/index.js`
- Add registry key prefixes (`ep:services`, `ep:endpoints`).
- Define endpoint registry TTL (e.g., 24 hours). We don't want to scan an endpoint that hasn't seen traffic in a day.

#### [MODIFY] `src/services/latencyAggregator.js`
- **Key Generation:** Update `getLatencyKey` to optionally accept an `endpoint` parameter.
- **Recording (`recordLatency`):** 
  - If the `endpoint` field exists in the log, write the latency sample to the service-level key AND the endpoint-level key.
  - Update the `ep:services` and `ep:endpoints:{service}` ZSET registries with the current timestamp.
  - Apply the exact same ZREMRANGEBYSCORE retention-buffer cleanup to the endpoint ZSETs.
- **Percentiles (`computePercentiles` & `cleanupLatencyWindows`):**
  - Update signatures to accept an optional `endpoint`.
  - Fetch/cleanup the endpoint-specific key if provided.

#### [NEW] `src/services/endpointRegistry.js`
- Expose methods to read the active registries for the evaluator:
  - `getEndpointServices()` → Returns active services from `ep:services`.
  - `getEndpointsForService(service)` → Returns active endpoints from `ep:endpoints:{service}`.
  - `cleanupRegistries()` → Removes stale endpoints/services older than the TTL.

#### [MODIFY] `src/services/alertEvaluator.js`
- **Path 3 Evolution:**
  - Keep the existing service-level percentile logging exactly as it is.
  - Add a sub-loop: For each service, fetch its active endpoints from the registry.
  - For each endpoint, call `cleanupLatencyWindows(redis, service, endpoint)` and `computePercentiles(redis, service, endpoint)`.
  - Log the result cleanly: `📐 [auth-service][/login] responseTime: P50=80ms...`

## Verification Plan

### Automated Tests
- Send simulated traffic containing the `endpoint` field using PowerShell/Artillery.
- Verify that both service-level (`lat:resp:svc`) and endpoint-level (`lat:resp:svc:/login`) keys are populated correctly.
- Verify the evaluator logs output for both scopes.

### Manual Verification
- Observe Redis memory to ensure the ZSET registries are effectively expiring stale endpoints.
- Send logs *without* the endpoint field to prove the service-level logic still works and doesn't break or pollute the registries.
