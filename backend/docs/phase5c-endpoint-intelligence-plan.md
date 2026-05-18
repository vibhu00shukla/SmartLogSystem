# Phase 5C: Endpoint Intelligence & Adaptive Alerting

This phase evolves our endpoint-level observability into actionable operational intelligence by introducing static latency thresholds, adaptive EWMA baselines, and cross-metric correlation (Latency + Errors).

## User Review Required & Suggestions

You asked if anything in your prompt restricts the system from being optimal. Here are three crucial architectural suggestions to ensure Phase 5C is robust:

> [!IMPORTANT]
> **1. The Correlation Dilemma (Endpoint Errors)**
> In Phase 5B, we deferred *endpoint-level error counting*. However, your Phase 5C goal includes "latency + error correlation classification." 
> *If we don't track errors per endpoint, we can only correlate Endpoint Latency with Service-Level Errors.* This causes false positives (e.g., Endpoint A is slow, but Endpoint B is the one crashing). 
> **Suggestion:** To make the "INCIDENT" classification accurate, we **must** implement endpoint-level error sliding windows (`sw:{service}:{endpoint}:err`) alongside this phase.

> [!WARNING]
> **2. EWMA & Cooldown Memory Leaks (Cardinality)**
> Endpoints can be ephemeral. In Phase 5B, we fixed the registry cardinality using a ZSET TTL. However, the proposed `ewma:endpoint:{service}:{endpoint}` hash and cooldown keys will remain in Redis forever if an endpoint stops receiving traffic.
> **Suggestion:** Every time we update an endpoint's EWMA baseline or set a cooldown, we must apply an explicit `EXPIRE` (TTL) of 24-48 hours. If the endpoint dies, its baseline gracefully evaporates.

> [!TIP]
> **3. Which Metric for EWMA?**
> Adaptive baselines (EWMA) on P95 or P99 are notoriously noisy because tail latency inherently spikes. 
> **Suggestion:** Apply the EWMA adaptive baseline strictly to the **P50 (Median) latency**. This represents the typical user experience shifting. Keep the static thresholds focused on the **P95/P99** bounds.

---

## 1. Redis Key Design

- **Cooldowns:** `alert:ep:lat:{service}:{endpoint}` (Static), `alert:ep:adp:{service}:{endpoint}` (Adaptive)
- **EWMA Baselines:** `ewma:lat:{service}:{endpoint}` (Tracks P50 latency)
- **Endpoint Errors (Proposed):** `sw:{service}:{endpoint}:err` (Required for accurate correlation)

*Note: All endpoint-specific keys will carry a rolling 24-hour TTL to prevent memory leaks.*

## 2. Classification Model & Correlation

Alerts will be classified and persisted with the following structure:

1. **`latency_threshold`**: Static P95/P99 bounds exceeded (WARNING, CRITICAL, SEVERE, TAIL_SPIKE).
2. **`latency_anomaly`**: P50 latency deviates significantly from its EWMA baseline (e.g., 3x higher).
3. **`incident_correlation`**: An endpoint triggers a `latency_threshold` **AND** has an `error_rate` > 5% simultaneously. This escalates to an immediate `CRITICAL` or `SEVERE` alert, bypassing normal cooldowns.

## 3. Evaluator Flow Design

The `evaluateService` loop will be cleanly extended. After computing service-level metrics:

1. **Endpoint Iteration:** Loop through `ep:endpoints:{service}`.
2. **Metric Computation:** Compute P50/P95/P99 latency. *(If approved: compute endpoint error rate).*
3. **Static Evaluation:** Check P95 against 500/2000/5000ms thresholds and P99 against 10000ms.
4. **Adaptive Evaluation:** Update the P50 EWMA baseline (`ewma:lat:{service}:{endpoint}`). Check for 3x deviation.
5. **Correlation:** If latency thresholds are crossed AND error rate is high -> Upgrade to `incident_correlation`.
6. **Cooldown Check:** Check `alert:ep:lat:...` or `alert:ep:adp:...`.
7. **Persistence:** Dispatch to MongoDB via `insertAlert()` with the optional `endpoint` field populated.

## 4. Alert Persistence Schema

We will update the MongoDB alert insertion logic to support:
```json
{
  "service": "auth-service",
  "endpoint": "/api/v1/login",
  "type": "incident_correlation",
  "level": "critical",
  "message": "Endpoint latency P95 (2100ms) and Error Rate (8%) are critical.",
  "timestamp": "..."
}
```

## 5. Implementation Roadmap

1. **[MODIFY] `src/config/index.js`**: Add static endpoint thresholds and EWMA config for latency.
2. **[MODIFY] `src/services/slidingWindowAggregator.js` (If approved)**: Add logic to track `sw:{service}:{endpoint}:err` for accurate error correlation.
3. **[NEW] `src/services/endpointAlerting.js`**: Extract endpoint threshold, EWMA, and correlation logic into a clean, modular file to avoid bloating `alertEvaluator.js`.
4. **[MODIFY] `src/services/alertEvaluator.js`**: Integrate the new `endpointAlerting` logic inside the existing Path 3 endpoint loop.
5. **[MODIFY] `src/db/mongo.js`**: Update `insertAlert` to accept and persist the `endpoint` field.
