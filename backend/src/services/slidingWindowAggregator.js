'use strict';

/**
 * slidingWindowAggregator.js — Rolling Window Analytics Service
 *
 * Maintains per-service rolling 60-second event windows using Redis
 * Sorted Sets (ZSETs). Solves the minute-boundary problem where
 * events at 12:40:59 and 12:41:01 would fall into different fixed
 * buckets even though they're only 2 seconds apart.
 *
 * Redis key design (dual-ZSET):
 *
 *   sw:{service}       — ALL events
 *     score  = epoch milliseconds (timestamp of the log event)
 *     member = Redis Stream entry ID (e.g. "1716000060001-0")
 *
 *   sw:{service}:err   — ERROR events only (subset of above)
 *     same score/member structure
 *
 *   sw:services         — SET of active service names (registry)
 *     used by the evaluator for efficient service discovery
 *
 * Why dual ZSETs instead of one?
 *   With a single ZSET, counting errors requires iterating ALL members
 *   in the window and filtering — O(N) at 18,000 entries for 300 req/s.
 *   Dual ZSETs allow O(log N) counting via ZCOUNT for both total and
 *   error counts.
 *
 * Performance:
 *   • All writes are batched in a Redis pipeline (single round-trip).
 *   • ZREMRANGEBYSCORE runs on every insert to keep the window tight.
 *   • At 300 req/s, each ZSET holds ~18,000 members (~500 KB) — trivial.
 *   • SADD to the service registry is O(1) and idempotent.
 *
 * Coexistence with time buckets:
 *   This module operates independently of bucketAggregator.js.
 *   Both systems write to Redis on every log; the alert evaluator
 *   reads from sliding windows for more accurate real-time detection.
 */

const config = require('../config');

// ─────────────────────────────────────────────────────────────
// Key Helpers
// ─────────────────────────────────────────────────────────────

/**
 * Build the Redis ZSET key for all events of a service.
 *
 * @param {string} service — e.g. "auth-service"
 * @returns {string} — e.g. "sw:auth-service"
 */
function getAllEventsKey(service) {
  return `${config.slidingWindow.keyPrefix}:${service}`;
}

/**
 * Build the Redis ZSET key for error events of a service.
 *
 * @param {string} service — e.g. "auth-service"
 * @returns {string} — e.g. "sw:auth-service:err"
 */
function getErrorEventsKey(service) {
  return `${config.slidingWindow.keyPrefix}:${service}${config.slidingWindow.errorSuffix}`;
}

// ─────────────────────────────────────────────────────────────
// Event Recording (Hot Path)
// ─────────────────────────────────────────────────────────────

/**
 * Record a log event into the sliding window ZSETs.
 *
 * Called once per processed log, right after updateBucket().
 * All commands are batched in a single Redis pipeline:
 *
 *   1. ZADD sw:{service} {timestamp_ms} {streamId}       — record event
 *   2. ZADD sw:{service}:err {timestamp_ms} {streamId}   — (error only)
 *   3. SADD sw:services {service}                         — register service
 *   4. ZREMRANGEBYSCORE sw:{service} -inf {cutoff}        — cleanup old events
 *   5. ZREMRANGEBYSCORE sw:{service}:err -inf {cutoff}    — cleanup old errors
 *
 * @param {import('ioredis').Redis} redis — ioredis client
 * @param {object} logDoc — normalised log document
 *   Must have: .service, .level, .timestamp, ._streamId
 * @returns {Promise<void>}
 */
async function recordEvent(redis, logDoc) {
  const { service, level, timestamp, _streamId } = logDoc;

  // Convert ISO timestamp to epoch milliseconds for the ZSET score
  const scoreMs = new Date(timestamp).getTime();

  // The stream entry ID is the perfect ZSET member:
  // already unique, compact (~15-20 bytes), no generation cost
  const member = _streamId;

  // Compute the cutoff timestamp for cleanup (now - windowSeconds)
  const cutoffMs = Date.now() - (config.slidingWindow.windowSeconds * 1000);

  // Key references
  const allKey = getAllEventsKey(service);
  const errKey = getErrorEventsKey(service);
  const registryKey = config.slidingWindow.serviceRegistryKey;

  const isError = level === 'error';

  // ── Redis Pipeline ──────────────────────────────────────
  // Batch all commands into a single round-trip.
  const pipeline = redis.pipeline();

  // 1. Record the event in the all-events ZSET
  pipeline.zadd(allKey, scoreMs, member);

  // 2. If error-level, also record in the errors-only ZSET
  if (isError) {
    pipeline.zadd(errKey, scoreMs, member);
  }

  // 3. Register the service name (idempotent SET add)
  pipeline.sadd(registryKey, service);

  // 4. Cleanup: remove events older than the window
  //    This keeps memory tight and runs efficiently — at steady state
  //    each call removes ~5 entries (300 req/s / 60s ≈ 5 per insert).
  pipeline.zremrangebyscore(allKey, '-inf', cutoffMs);
  pipeline.zremrangebyscore(errKey, '-inf', cutoffMs);

  // Execute the pipeline
  await pipeline.exec();

  console.log(
    `🪟 Window updated  [${allKey}]  member=${member}  error=${isError}`
  );
}

// ─────────────────────────────────────────────────────────────
// Rolling Metric Queries (used by alertEvaluator)
// ─────────────────────────────────────────────────────────────

/**
 * Get rolling metrics for a service within the sliding window.
 *
 * Uses ZCOUNT to count members whose score (timestamp) falls
 * within the [now - windowSeconds, now] range. This is O(log N)
 * for each call — extremely fast even with 18,000+ members.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service — service name
 * @returns {Promise<{ total: number, errors: number, windowSeconds: number }>}
 */
async function getRollingMetrics(redis, service) {
  const allKey = getAllEventsKey(service);
  const errKey = getErrorEventsKey(service);

  const now = Date.now();
  const windowMs = config.slidingWindow.windowSeconds * 1000;
  const from = now - windowMs;

  // ZCOUNT is O(log N) — counts members in the score range [from, +inf]
  // Using pipeline to batch both counts into one round-trip
  const pipeline = redis.pipeline();
  pipeline.zcount(allKey, from, '+inf');
  pipeline.zcount(errKey, from, '+inf');

  const results = await pipeline.exec();

  // pipeline.exec() returns [[err, result], [err, result]]
  const total  = results[0][1] || 0;
  const errors = results[1][1] || 0;

  return {
    total,
    errors,
    windowSeconds: config.slidingWindow.windowSeconds,
  };
}

/**
 * Get all registered service names from the service registry SET.
 *
 * @param {import('ioredis').Redis} redis
 * @returns {Promise<string[]>} — array of service names
 */
async function getServiceRegistry(redis) {
  return redis.smembers(config.slidingWindow.serviceRegistryKey);
}

/**
 * Defensive cleanup — remove stale entries from a service's ZSETs.
 *
 * Called by the alert evaluator before counting, to handle edge cases
 * where a service stopped sending logs and no insert-time cleanup ran.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service
 * @returns {Promise<void>}
 */
async function cleanupWindow(redis, service) {
  const cutoffMs = Date.now() - (config.slidingWindow.windowSeconds * 1000);
  const allKey = getAllEventsKey(service);
  const errKey = getErrorEventsKey(service);

  const pipeline = redis.pipeline();
  pipeline.zremrangebyscore(allKey, '-inf', cutoffMs);
  pipeline.zremrangebyscore(errKey, '-inf', cutoffMs);
  await pipeline.exec();
}

// ─────────────────────────────────────────────────────────────
// Exports
// ─────────────────────────────────────────────────────────────

module.exports = {
  recordEvent,
  getRollingMetrics,
  getServiceRegistry,
  cleanupWindow,
  getAllEventsKey,
  getErrorEventsKey,
};
