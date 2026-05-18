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

// ─────────────────────────────────────────────────────────────
// Key Helpers
// ─────────────────────────────────────────────────────────────

/**
 * Build the Redis ZSET key for a specific latency type and service.
 *
 * @param {string} type — one of: 'responseTime', 'dbQueryTime', 'externalApiTime'
 * @param {string} service — e.g. "auth-service"
 * @returns {string} — e.g. "lat:resp:auth-service"
 */
function getLatencyKey(type, service) {
  const prefix = config.latency.keyPrefixes[type];
  if (!prefix) {
    throw new Error(`Unknown latency type: ${type}`);
  }
  return `${prefix}:${service}`;
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
 * Record latency samples into per-service Redis ZSETs.
 *
 * Called once per processed log, right after recordEvent().
 * Only creates a pipeline if at least one latency field is present
 * in the log document — debug/info logs without APM data skip
 * entirely (zero Redis operations).
 *
 * For each present latency field:
 *   1. ZADD lat:{type}:{service} {timestamp_ms} "{streamId}:{latencyMs}"
 *   2. ZREMRANGEBYSCORE lat:{type}:{service} -inf {cutoff} (cleanup)
 *
 * Member format: "{streamId}:{latencyMs}"
 *   - streamId provides uniqueness (already globally unique)
 *   - latencyMs is extracted later for percentile computation
 *   - Example: "1716000060001-0:142.5"
 *
 * @param {import('ioredis').Redis} redis — ioredis client
 * @param {object} logDoc — normalised log document
 *   Must have: .service, .timestamp, ._streamId
 *   Optional:  .responseTime, .dbQueryTime, .externalApiTime
 * @returns {Promise<void>}
 */
async function recordLatency(redis, logDoc) {
  const { service, timestamp, _streamId } = logDoc;

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

  // Compute the cutoff timestamp for cleanup (now - windowSeconds)
  const cutoffMs = Date.now() - (config.latency.windowSeconds * 1000);

  // ── Redis Pipeline ──────────────────────────────────────
  // Batch all commands into a single round-trip.
  const pipeline = redis.pipeline();

  for (const [field, value] of presentFields) {
    const key = getLatencyKey(field, service);

    // Member = "{streamId}:{latencyMs}" — latency encoded for extraction
    const member = `${_streamId}:${value}`;

    // 1. Record the latency sample
    pipeline.zadd(key, scoreMs, member);

    // 2. Cleanup: remove samples older than the window
    pipeline.zremrangebyscore(key, '-inf', cutoffMs);
  }

  // Execute the pipeline
  await pipeline.exec();

  // Log which fields were recorded (compact format)
  const summary = presentFields.map(([f, v]) => `${f}=${v}ms`).join(' ');
  console.log(
    `⏱️  Latency recorded [${service}] ${summary}`
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
 * Compute P50, P95, P99 for all latency types for a given service.
 *
 * For each latency type (responseTime, dbQueryTime, externalApiTime):
 *   1. ZRANGEBYSCORE to get all members in the 60s window
 *   2. Extract latency from each member (split on last colon)
 *   3. Sort numerically (ascending)
 *   4. Compute P50, P95, P99 using nearest-rank method
 *
 * At 300 req/s × 60s = 18,000 entries, the in-memory sort takes ~1ms.
 * This runs every 30s in the evaluator — trivial overhead.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service — service name
 * @returns {Promise<object|null>} — percentile metrics or null if no data
 */
async function computePercentiles(redis, service) {
  const now = Date.now();
  const windowMs = config.latency.windowSeconds * 1000;
  const from = now - windowMs;

  const result = {};
  let hasData = false;

  for (const field of LATENCY_FIELDS) {
    const key = getLatencyKey(field, service);

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
 * @returns {Promise<void>}
 */
async function cleanupLatencyWindows(redis, service) {
  const cutoffMs = Date.now() - (config.latency.windowSeconds * 1000);

  const pipeline = redis.pipeline();
  for (const field of LATENCY_FIELDS) {
    const key = getLatencyKey(field, service);
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
