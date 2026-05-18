# Phase 5C: Endpoint Intelligence Walkthrough

I have successfully evolved endpoint observability into actionable operational intelligence. This means the system doesn't just log endpoint latency anymore—it actively evaluates it, correlates it, and fires alerts.

## What Was Accomplished

1. **Endpoint-Level Error Tracking**
   - Modified `slidingWindowAggregator.js` to create dual-ZSETs for endpoints (`sw:{service}:{endpoint}` and `sw:{service}:{endpoint}:err`).
   - This ensures exact error rates are calculated per route, rather than averaging service-wide errors across innocent endpoints.

2. **The Intelligence Engine (`endpointAlerting.js`)**
   - **Static Evaluation**: Checks endpoint P95 latencies against WARNING (500ms), CRITICAL (2000ms), and SEVERE (5000ms) thresholds. Also checks P99 for TAIL_SPIKE (>10s).
   - **Adaptive Evaluation**: Computes an EWMA baseline using **only the P50 median latency** to avoid tail-noise. Alerts if the P50 deviates 3x above its historical norm.
   - **Incident Correlation**: If a latency threshold is breached AND the endpoint's specific error rate exceeds 5%, it creates a highly-critical `incident_correlation` alert.

3. **Memory Safety & Cooldowns**
   - Both the P50 EWMA hash and the alert cooldown keys use an explicit `EXPIRE` rolling TTL of 48 hours and 30 minutes, respectively. This completely eliminates cardinality leaks from dead routes.
   - Cooldowns successfully prevent alert spam while intelligence algorithms work.

4. **Database Readiness**
   - Updated the MongoDB compound index in `mongo.js` to include the `endpoint` field for fast querying.
   - The alert documents pushed to MongoDB now gracefully include the `endpoint`, specific metric classifications, and accurate timestamps.

## Testing It Yourself

You can verify the intelligence engine by submitting artificial telemetry with your Postman or Artillery load generator.

### 1. Triggering an Adaptive Anomaly (P50 Jump)
Send a burst of traffic where the P50 latency is around `50ms`. Wait for the evaluator to run (30s) so it learns the baseline.
Then, send a burst where the latency is uniformly `200ms`. 
You will see the evaluator log an **Adaptive latency anomaly** because the median latency jumped > 3x.

### 2. Triggering an Incident Correlation
Send a burst of traffic where:
1. `responseTime` is > 2000ms (Critical Threshold)
2. `level` is set to `"error"` for more than 5% of the logs.
The evaluator will recognize both conditions on the specific endpoint and log:
`🚨 [auth-service][/api/v1/login] ALERT: SEVERE - INCIDENT: Endpoint latency P95 (2500ms) and Error Rate (10.00%) are degrading simultaneously.`

### 3. Verify MongoDB & Redis
You can inspect the newly created intelligence structures:
```bash
# Check the P50 Baseline Hash and its TTL
redis-cli HGETALL ewma:lat:auth-service:/api/v1/login
redis-cli TTL ewma:lat:auth-service:/api/v1/login

# Check Endpoint specific errors
redis-cli ZCARD sw:auth-service:/api/v1/login:err
```
