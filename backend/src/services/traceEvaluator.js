'use strict';

/**
 * traceEvaluator.js — Phase 6A
 *
 * Dedicated decoupled loop for evaluating completed traces.
 * Runs independently of the alertEvaluator to prevent blocking.
 *
 * Flow:
 * 1. Sweep trace:active for traces inactive for > 15 seconds.
 * 2. Reconstruct trace from trace:events:{traceId} list.
 * 3. Analyze: E2E duration, critical path (slowest), hop count, services visited.
 * 4. Save to trace_summaries in MongoDB.
 * 5. Cleanup Redis.
 */

const config = require('../config');
const { insertTraceSummary } = require('../db/mongo');

/**
 * Find traces that haven't received a new log event in the configured timeout.
 */
async function getColdTraces(redis) {
  const { active } = config.tracing.keyPrefixes;
  const cutoffMs = Date.now() - (config.tracing.closureTimeoutSeconds * 1000);
  
  // Get all traceIds whose last event was before the cutoff
  return redis.zrangebyscore(active, '-inf', cutoffMs);
}

/**
 * Fetch all events for a traceId and parse them.
 */
async function getTraceEvents(redis, traceId) {
  const listKey = `${config.tracing.keyPrefixes.events}:${traceId}`;
  const rawEvents = await redis.lrange(listKey, 0, -1);
  return rawEvents.map(e => JSON.parse(e));
}

/**
 * Analyze a reconstructed trace timeline.
 */
function analyzeTrace(traceId, events) {
  if (!events || events.length === 0) return null;

  // Sort chronologically
  events.sort((a, b) => a.ts - b.ts);

  const startTime = events[0].ts;
  const endTime = events[events.length - 1].ts;
  const durationMs = endTime - startTime;

  let hasError = false;
  let criticalPath = null;
  let maxLatency = -1;
  const servicesVisited = new Set();

  for (const ev of events) {
    if (ev.err) hasError = true;
    servicesVisited.add(ev.svc);

    if (ev.lat > maxLatency) {
      maxLatency = ev.lat;
      criticalPath = ev.ep ? `${ev.svc}${ev.ep}` : ev.svc;
    }
  }

  return {
    traceId,
    startTime: new Date(startTime).toISOString(),
    endTime: new Date(endTime).toISOString(),
    durationMs,
    hopCount: events.length,
    servicesVisited: Array.from(servicesVisited),
    criticalPath,
    hasError,
    createdAt: new Date().toISOString()
  };
}

/**
 * Delete a processed trace from Redis memory.
 */
async function cleanupTrace(redis, traceId) {
  const { active, events } = config.tracing.keyPrefixes;
  const listKey = `${events}:${traceId}`;

  const pipeline = redis.pipeline();
  pipeline.zrem(active, traceId);
  pipeline.del(listKey);
  await pipeline.exec();
}

/**
 * Main trace evaluation loop.
 */
async function evaluateTraces(redis) {
  try {
    const coldTraces = await getColdTraces(redis);

    if (coldTraces.length > 0) {
      console.log(`\n🔍 TraceEvaluator: Found ${coldTraces.length} cold traces for closure.`);
    }

    for (const traceId of coldTraces) {
      try {
        const events = await getTraceEvents(redis, traceId);
        const summary = analyzeTrace(traceId, events);

        if (summary) {
          // Persist summary
          await insertTraceSummary(summary);
          
          console.log(
            `   🔗 Trace [${traceId}] Closed: hops=${summary.hopCount} ` +
            `duration=${summary.durationMs}ms critical=${summary.criticalPath} error=${summary.hasError}`
          );
        }

        // Cleanup regardless of whether summary generated successfully to avoid memory leaks
        await cleanupTrace(redis, traceId);
      } catch (err) {
        console.error(`   ❌ Trace [${traceId}] evaluation failed:`, err.message);
        // Force cleanup to avoid poison pill traces blocking forever
        await cleanupTrace(redis, traceId);
      }
    }
  } catch (err) {
    console.error('❌ TraceEvaluator sweep failed:', err.message);
  }
}

/**
 * Start the decoupled interval loop.
 */
function startTraceEvaluator(redis) {
  console.log(`\n🚀 Starting TraceEvaluator loop (interval: ${config.tracing.evaluatorIntervalMs}ms)`);
  setInterval(() => {
    evaluateTraces(redis);
  }, config.tracing.evaluatorIntervalMs);
}

module.exports = {
  startTraceEvaluator,
  evaluateTraces // exported for testing
};
