# Phase 6A: Distributed Tracing & Request Correlation

This phase introduces lightweight distributed tracing to reconstruct request journeys across multiple microservices using a shared `traceId`.

## User Review Required & Suggestions

You asked if anything in the prompt restricts the system from being optimal. The prompt is excellent for keeping things simple (avoiding DAGs), but here are three crucial suggestions to ensure the architecture actually works at scale:

> [!IMPORTANT]
> **1. The "Trace Closure" Problem (How do we know a trace is finished?)**
> Microservice logs arrive asynchronously. If Service A and Service B both log `traceId=123`, we don't definitively know if Service C is about to log it too. 
> **Suggestion:** We should use a **Timeout-Based Closure Strategy**. `trace:active` must be a ZSET where the score is the timestamp of the *last received log*. An evaluator sweeps this ZSET, and if a trace hasn't received a new log in **15 seconds**, it is considered "complete." The system then computes the critical path, saves the summary, and deletes the Redis keys.

> [!WARNING]
> **2. Evaluator Loop Bloat**
> The current `alertEvaluator.js` runs every 30 seconds to handle sliding windows, EWMA, and endpoint intelligence. If you have 500 active requests per second, sweeping and summarizing thousands of traces in the same 30s loop will cause immense lag.
> **Suggestion:** We must decouple trace reconstruction into its own dedicated loop (e.g., `traceEvaluator.js` running every 5 seconds).

> [!TIP]
> **3. Mongo Trace Summaries**
> We already save all individual raw logs to MongoDB. However, if we want fast historical querying of traces, querying raw logs and aggregating them on the fly is extremely slow.
> **Suggestion:** When a trace closes, the evaluator should write a **single summarized document** to a new `trace_summaries` MongoDB collection containing the E2E latency, the critical path service, and the hop count.

---

## 1. Redis Key Strategy

We will store the bare minimum data required to reconstruct the timeline in Redis to preserve memory.

- **`trace:active` (ZSET)**: Tracks active traces. `score` = Timestamp of the last received log event. `member` = `traceId`.
- **`trace:events:{traceId}` (LIST)**: A Redis List containing stringified lightweight event objects.
  - Payload: `{ "svc": "auth", "ep": "/login", "lat": 120, "ts": 1716000000000, "err": false }`

## 2. Mongo Persistence Strategy

- **Raw Logs (`logs` collection)**: Already index `traceId`. Remains untouched.
- **Trace Summaries (`trace_summaries` collection)**: **[NEW]**
  - Contains: `traceId`, `startTime`, `endTime`, `durationMs`, `hopCount`, `criticalPath` (service with highest latency), `hasError`.

## 3. Evaluator / Reconstruction Flow

A new decoupled `traceEvaluator` will run on a fast interval (e.g., every 5s):
1. **Sweep:** `ZRANGEBYSCORE trace:active -inf <Now - 15 seconds>` to find traces that have gone "cold" (assumed complete).
2. **Reconstruct:** For each cold `traceId`, fetch all events from `trace:events:{traceId}` using `LRANGE`.
3. **Analyze:**
   - Sort events by timestamp to reconstruct the timeline.
   - Compute `durationMs` (End Time - Start Time). Note: In async architectures, total latency might be longer than the sum of individual latencies, or shorter if parallelized.
   - Find **Critical Path** (the specific event/service with the highest `responseTime`).
4. **Persist:** Insert the generated trace summary into MongoDB.
5. **Cleanup:** Delete `trace:events:{traceId}` and remove the `traceId` from `trace:active`.

## 4. Ingestion Lifecycle

During log ingestion (`logConsumer.js`):
1. Check if the log contains a `traceId`.
2. Extract routing info (`service`, `endpoint`, `responseTime`, `timestamp`).
3. Pipeline:
   - `RPUSH trace:events:{traceId} {payload}`
   - `ZADD trace:active {current_timestamp} {traceId}`
   - *Self-healing TTL:* `EXPIRE trace:events:{traceId} 300` (5 min fallback TTL in case the evaluator crashes, preventing memory leaks).

## 5. Implementation Roadmap

1. **[MODIFY] `src/config/index.js`**: Add tracing configurations (closure timeout, loop intervals).
2. **[NEW] `src/services/traceAggregator.js`**: Functions to ingest trace events into Redis.
3. **[MODIFY] `src/workers/logConsumer.js`**: Wire in `traceAggregator.recordTraceEvent()`.
4. **[MODIFY] `src/db/mongo.js`**: Add `trace_summaries` collection and index.
5. **[NEW] `src/services/traceEvaluator.js`**: The dedicated loop for closing, summarizing, and persisting traces.
6. **[MODIFY] `src/worker.js`**: Initialize the new `traceEvaluator` alongside the existing `alertEvaluator`.
