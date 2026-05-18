# Phase 6A: Distributed Tracing & Request Correlation

The platform now natively supports lightweight distributed request correlation. Rather than just understanding isolated service health, the system now reconstructs the journey of a single user request across multiple microservices.

## Architectural Changes

1. **`traceAggregator.js` (Ingestion)**
   - Wired seamlessly into the `logConsumer` pipeline.
   - For any log containing a `traceId` and `responseTime`, it records a lightweight event payload (Service, Endpoint, Latency, Error status) to a Redis LIST (`trace:events:{traceId}`).
   - It also touches a Redis ZSET (`trace:active`) with the `traceId` and the current timestamp so the system knows when the trace was last active.
   - **Safety First**: It sets a 5-minute fallback TTL on the event lists so that if the evaluator crashes, Redis won't OOM (Out Of Memory).

2. **`traceEvaluator.js` (Analysis & Closure)**
   - A completely decoupled intelligence loop that runs independently every 5 seconds (configurable).
   - Sweeps the `trace:active` ZSET to find any traces that haven't received a new log event in over 15 seconds.
   - For each "cold" trace, it:
     - Fetches all historical events for that trace from Redis.
     - Sorts them chronologically to reconstruct the timeline.
     - Identifies the total duration (first event to last event).
     - Identifies the **Critical Path** (the specific service/endpoint that was the slowest hop).
     - Records a unique `Set` of all `servicesVisited`.
   - Cleans up Redis memory gracefully.

3. **MongoDB Integration**
   - We created a new `trace_summaries` collection specifically optimized for querying completed request journeys without having to aggregate millions of raw logs on the fly.
   - Added unique indexes on `traceId` and sorted indexes on `startTime`.

## Testing the Trace Engine

You can test this right now. Assuming you have 3 services (`api-gateway`, `auth-svc`, `db-svc`), send 3 consecutive logs into Redis using the *same* `traceId`. Wait 15 seconds.

If you watch the worker terminal, you will see output like this:
```bash
🔍 TraceEvaluator: Found 1 cold traces for closure.
   🔗 Trace [uuid-1234] Closed: hops=3 duration=180ms critical=db-svc/query error=false
```

And if you check your MongoDB `log-intelligence` database, you'll find a beautiful document in `trace_summaries`:
```json
{
  "traceId": "uuid-1234",
  "startTime": "2026-05-19T00:00:00.000Z",
  "endTime": "2026-05-19T00:00:00.180Z",
  "durationMs": 180,
  "hopCount": 3,
  "servicesVisited": ["api-gateway", "auth-svc", "db-svc"],
  "criticalPath": "db-svc/query",
  "hasError": false
}
```

This decoupled architecture perfectly sets the stage for future visualization capabilities while keeping your real-time sliding windows and EWMA engines extremely fast and completely unblocked!
