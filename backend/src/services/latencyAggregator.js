'use strict';

/**
 * latencyAggregator.js — Latency Sliding Window Service
 *
 * Maintains per-service rolling 60-second latency windows using
 * Redis Sorted Sets. Stores latency samples so the alert evaluator
 * can compute percentile metrics (P50, P95, P99) for real-time
 * application performance monitoring.
 *
 * Redis key design:
 *
 *   lat:resp:{service}   — response time samples
 *   lat:db:{service}     — database query time samples
 *   lat:ext:{service}    — external API call time samples
 *
 *   Score  = epoch milliseconds (timestamp — for time-based cleanup)
 *   Member = "{streamId}:{latencyMs}" (latency encoded for extraction)
 *
 * Why encode latency in the member?
 *   Redis ZSETs sort by score, not member. With timestamp as score
 *   (needed for ZREMRANGEBYSCORE cleanup), we can't sort by latency
 *   natively. Instead, we extract latency values from members and
 *   sort them in-memory for percentile computation. At 18,000
 *   members (300 req/s × 60s), sorting takes ~1ms — trivial.
 *
 * Conditional writes:
 *   Not every log has latency data. A simple debug message produces
 *   no ZSET writes. The pipeline is only created when at least one
 *   latency field is present in the normalised log document.
 *
 * Coexistence:
 *   This module operates independently of slidingWindowAggregator.js
 *   (error counting) and bucketAggregator.js (per-minute hashes).
 *   All three systems write to Redis on every log; each reads its
 *   own key namespace.
 */

const config = require('../config');
const { registerEndpoint } = require('./endpointRegistry');

// ─────────────────────────────────────────────────────────────
// Key Helpers
// ─────────────────────────────────────────────────────────────

/**
 * Build the Redis ZSET key for a specific latency type, service, and optionally endpoint.
 *
 * @param {string} type — one of: 'responseTime', 'dbQueryTime', 'externalApiTime'
 * @param {string} service — e.g. "auth-service"
 * @param {string} [endpoint] — e.g. "/api/v1/login"
 * @returns {string} — e.g. "lat:resp:auth-service" or "lat:resp:auth-service:/api/v1/login"
 */
function getLatencyKey(type, service, endpoint = null) {
  const prefix = config.latency.keyPrefixes[type];
  if (!prefix) {
    throw new Error(`Unknown latency type: ${type}`);
  }
  return endpoint ? `${prefix}:${service}:${endpoint}` : `${prefix}:${service}`;
}

/**
 * The three latency field names we track.
 * Used for iteration in record/cleanup/percentile functions.
 */
const LATENCY_FIELDS = ['responseTime', 'dbQueryTime', 'externalApiTime'];

// ─────────────────────────────────────────────────────────────
// Event Recording (Hot Path)
// ─────────────────────────────────────────────────────────────

/**
 * Record latency samples into per-service and per-endpoint Redis ZSETs.
 *
 * Called once per processed log, right after recordEvent().
 * Only creates a pipeline if at least one latency field is present
 * in the log document — debug/info logs without APM data skip
 * entirely (zero Redis operations).
 *
 * For each present latency field:
 *   1. ZADD lat:{type}:{service} {timestamp_ms} "{streamId}:{latencyMs}"
 *   2. ZREMRANGEBYSCORE lat:{type}:{service} -inf {cutoff} (cleanup)
 *   3. If endpoint exists: ZADD lat:{type}:{service}:{endpoint} ...
 *   4. If endpoint exists: ZREMRANGEBYSCORE lat:{type}:{service}:{endpoint} ...
 *
 * Member format: "{streamId}:{latencyMs}"
 *   - streamId provides uniqueness (already globally unique)
 *   - latencyMs is extracted later for percentile computation
 *   - Example: "1716000060001-0:142.5"
 *
 * @param {import('ioredis').Redis} redis — ioredis client
 * @param {object} logDoc — normalised log document
 *   Must have: .service, .timestamp, ._streamId
 *   Optional:  .responseTime, .dbQueryTime, .externalApiTime, .endpoint
 * @returns {Promise<void>}
 */
async function recordLatency(redis, logDoc) {
  const { service, endpoint, timestamp, _streamId } = logDoc;

  // ── Check which latency fields are present ────────────────
  // Build a list of [fieldName, value] pairs for only the fields
  // that have non-null values. If none are present, skip entirely.
  const presentFields = [];
  for (const field of LATENCY_FIELDS) {
    if (logDoc[field] != null && !isNaN(logDoc[field])) {
      presentFields.push([field, logDoc[field]]);
    }
  }

  // No latency data → skip pipeline entirely (zero Redis cost)
  if (presentFields.length === 0) {
    return;
  }

  // Convert ISO timestamp to epoch milliseconds for the ZSET score
  const scoreMs = new Date(timestamp).getTime();

  // Compute the cutoff timestamp for cleanup with a retention buffer (+60s)
  // to prevent race conditions where telemetry expires before the evaluator observes it.
  const retentionMs = (config.latency.windowSeconds + 60) * 1000;
  const cutoffMs = Date.now() - retentionMs;

  // ── Redis Pipeline ──────────────────────────────────────
  // Batch all commands into a single round-trip.
  const pipeline = redis.pipeline();

  for (const [field, value] of presentFields) {
    // 1. Service-level recording
    const svcKey = getLatencyKey(field, service);
    const member = `${_streamId}:${value}`;
    pipeline.zadd(svcKey, scoreMs, member);
    pipeline.zremrangebyscore(svcKey, '-inf', cutoffMs);

    // 2. Endpoint-level recording (Phase 5B)
    if (endpoint) {
      const epKey = getLatencyKey(field, service, endpoint);
      pipeline.zadd(epKey, scoreMs, member);
      pipeline.zremrangebyscore(epKey, '-inf', cutoffMs);
    }
  }

  // Register the endpoint in the registry if it exists
  if (endpoint) {
    registerEndpoint(pipeline, service, endpoint, scoreMs);
  }

  // Execute the pipeline
  await pipeline.exec();

  // Log which fields were recorded (compact format)
  const summary = presentFields.map(([f, v]) => `${f}=${v}ms`).join(' ');
  const epDisplay = endpoint ? `[${endpoint}] ` : '';
  console.log(
    `⏱️  Latency recorded [${service}] ${epDisplay}${summary}`
  );
}

// ─────────────────────────────────────────────────────────────
// Percentile Computation (used by alertEvaluator)
// ─────────────────────────────────────────────────────────────

/**
 * Extract the latency value from a ZSET member string.
 *
 * Member format: "{streamId}:{latencyMs}"
 * The streamId itself contains a hyphen (e.g., "1716000060001-0"),
 * so we split on the LAST colon to get the latency portion.
 *
 * @param {string} member — e.g. "1716000060001-0:142.5"
 * @returns {number} — latency in milliseconds, e.g. 142.5
 */
function extractLatency(member) {
  const lastColon = member.lastIndexOf(':');
  return parseFloat(member.substring(lastColon + 1));
}

/**
 * Compute a specific percentile from a sorted array of values.
 *
 * Uses the nearest-rank method:
 *   index = ceil(percentile × N) - 1
 *
 * @param {number[]} sorted — sorted array of latency values (ascending)
 * @param {number} p — percentile as a decimal (e.g., 0.95 for P95)
 * @returns {number} — the percentile value
 */
function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  const index = Math.ceil(p * sorted.length) - 1;
  return sorted[Math.max(0, index)];
}

/**
 * Compute P50, P95, P99 for all latency types for a given service (and optional endpoint).
 *
 * For each latency type (responseTime, dbQueryTime, externalApiTime):
 *   1. ZRANGEBYSCORE to get all members in the 60s window
 *   2. Extract latency from each member (split on last colon)
 *   3. Sort numerically (ascending)
 *   4. Compute P50, P95, P99 using nearest-rank method
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service — service name
 * @param {string} [endpoint] — optional endpoint path
 * @returns {Promise<object|null>} — percentile metrics or null if no data
 */
async function computePercentiles(redis, service, endpoint = null) {
  const now = Date.now();
  const windowMs = config.latency.windowSeconds * 1000;
  const from = now - windowMs;

  const result = {};
  let hasData = false;

  for (const field of LATENCY_FIELDS) {
    const key = getLatencyKey(field, service, endpoint);

    // Fetch all members in the time window (score range)
    const members = await redis.zrangebyscore(key, from, '+inf');

    if (members.length === 0) {
      result[field] = null;
      continue;
    }

    hasData = true;

    // Extract latency values from member strings and sort
    const values = members
      .map(extractLatency)
      .filter((v) => !isNaN(v))
      .sort((a, b) => a - b);

    if (values.length === 0) {
      result[field] = null;
      continue;
    }

    // Compute percentiles
    result[field] = {
      p50:     parseFloat(percentile(values, 0.50).toFixed(2)),
      p95:     parseFloat(percentile(values, 0.95).toFixed(2)),
      p99:     parseFloat(percentile(values, 0.99).toFixed(2)),
      min:     parseFloat(values[0].toFixed(2)),
      max:     parseFloat(values[values.length - 1].toFixed(2)),
      samples: values.length,
    };
  }

  return hasData ? result : null;
}

/**
 * Defensive cleanup — remove stale entries from a service's latency ZSETs.
 *
 * Called by the alert evaluator before percentile computation, to
 * handle edge cases where a service stopped sending and no
 * insert-time cleanup ran.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service
 * @param {string} [endpoint]
 * @returns {Promise<void>}
 */
async function cleanupLatencyWindows(redis, service, endpoint = null) {
  // Add a retention buffer so data remains available for the evaluator loop
  const retentionMs = (config.latency.windowSeconds + 60) * 1000;
  const cutoffMs = Date.now() - retentionMs;

  const pipeline = redis.pipeline();
  for (const field of LATENCY_FIELDS) {
    const key = getLatencyKey(field, service, endpoint);
    pipeline.zremrangebyscore(key, '-inf', cutoffMs);
  }
  await pipeline.exec();
}

// ─────────────────────────────────────────────────────────────
// Exports
// ─────────────────────────────────────────────────────────────

module.exports = {
  recordLatency,
  computePercentiles,
  cleanupLatencyWindows,
  getLatencyKey,
  extractLatency,
  LATENCY_FIELDS,
};
