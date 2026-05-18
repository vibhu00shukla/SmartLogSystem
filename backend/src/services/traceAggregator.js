'use strict';

/**
 * traceAggregator.js — Phase 6A
 *
 * Ingests individual trace events from logConsumer into Redis.
 * Uses a lightweight structure:
 * 1. trace:active (ZSET) — tracking trace lifetimes for closure
 * 2. trace:events:{traceId} (LIST) — storing minimal event payloads
 */

const config = require('../config');

/**
 * Record a trace event if the log has a traceId.
 *
 * @param {import('ioredis').Redis} redis
 * @param {object} logDoc
 */
async function recordTraceEvent(redis, logDoc) {
  const { traceId, service, endpoint, responseTime, timestamp, level } = logDoc;

  // We only trace requests with a traceId and latency (we care about duration)
  if (!traceId) return;

  const scoreMs = new Date(timestamp).getTime();
  const { active, events } = config.tracing.keyPrefixes;
  const listKey = `${events}:${traceId}`;

  const payload = {
    svc: service,
    ep: endpoint || null,
    lat: responseTime || 0,
    ts: scoreMs,
    err: level === 'error'
  };

  const pipeline = redis.pipeline();
  
  // 1. Add/update the trace in the active ZSET (score = latest event timestamp)
  pipeline.zadd(active, scoreMs, traceId);
  
  // 2. Append event to the trace's list
  pipeline.rpush(listKey, JSON.stringify(payload));
  
  // 3. Set a fallback TTL to prevent memory leaks if traceEvaluator crashes
  pipeline.expire(listKey, config.tracing.fallbackTtlSeconds);

  await pipeline.exec();
}

module.exports = {
  recordTraceEvent
};
