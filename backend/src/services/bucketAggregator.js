'use strict';

/**
 * bucketAggregator.js — Redis Time Bucket Aggregation Service
 *
 * Maintains per-minute, per-service counters in Redis Hashes.
 *
 * Redis key pattern:
 *   tb:{service}:{YYYYMMDDHHmm}
 *
 * Each key is a Hash with fields:
 *   total  — total log count in this minute
 *   errors — error-level log count
 *   start  — ISO timestamp of bucket start (set once)
 *
 * Design notes:
 *   • Uses Redis pipeline to batch HSETNX + HINCRBY + EXPIRE
 *     into a single round-trip (~0.1ms overhead per log).
 *   • HSETNX for `start` ensures it is only written once per bucket.
 *   • EXPIRE is refreshed on every write — the TTL is a safety net
 *     so orphaned buckets don't leak memory. Under normal operation
 *     the flusher (Phase 2) deletes buckets after persisting them.
 *   • All operations are atomic — safe for concurrent workers
 *     incrementing the same bucket.
 */

const config = require('../config');

// ─────────────────────────────────────────────────────────────
// Key Generation
// ─────────────────────────────────────────────────────────────

/**
 * Format a Date into a compact minute string: YYYYMMDDHHmm
 *
 * Uses UTC to ensure consistent bucket boundaries regardless
 * of the server's local timezone.
 *
 * @param {Date} date
 * @returns {string} e.g. "202605151135"
 */
function formatMinute(date) {
  const y = date.getUTCFullYear();
  const M = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  const h = String(date.getUTCHours()).padStart(2, '0');
  const m = String(date.getUTCMinutes()).padStart(2, '0');
  return `${y}${M}${d}${h}${m}`;
}

/**
 * Build the full Redis key for a given service and timestamp.
 *
 * @param {string} service — service name (e.g. "auth-service")
 * @param {Date}   [timestamp=new Date()] — point in time for the bucket
 * @returns {string} e.g. "tb:auth-service:202605151135"
 */
function getTimeBucketKey(service, timestamp = new Date()) {
  const prefix = config.timeBucket.keyPrefix;
  const minute = formatMinute(timestamp);
  return `${prefix}:${service}:${minute}`;
}

/**
 * Convenience wrapper — returns the key for the current minute.
 *
 * @param {string} service
 * @returns {string}
 */
function getCurrentMinuteKey(service) {
  return getTimeBucketKey(service, new Date());
}

// ─────────────────────────────────────────────────────────────
// Bucket Update
// ─────────────────────────────────────────────────────────────

/**
 * Atomically update the time bucket for the given log document.
 *
 * Called once per processed log, right after the MongoDB insert.
 * Uses an ioredis pipeline to send all commands in a single
 * network round-trip:
 *
 *   HSETNX key start <ISO>      — set bucket start (only first time)
 *   HINCRBY key total 1          — always increment total
 *   HINCRBY key errors <0|1>     — increment errors only for error-level
 *   EXPIRE key <ttl>             — refresh safety TTL
 *
 * @param {import('ioredis').Redis} redis — ioredis client
 * @param {object} logDoc — normalised log document (must have .service, .level, .timestamp)
 * @returns {Promise<void>}
 */
async function updateBucket(redis, logDoc) {
  const { service, level, timestamp } = logDoc;

  // Determine which minute this log belongs to
  const logTime = new Date(timestamp);
  const key = getTimeBucketKey(service, logTime);

  // Compute the bucket start as the top of the minute (UTC)
  const bucketStart = new Date(logTime);
  bucketStart.setUTCSeconds(0, 0);

  // Is this an error-level log?
  const isError = level === 'error' ? 1 : 0;

  // ── Redis Pipeline ──────────────────────────────────────
  // All four commands are batched into a single round-trip.
  const pipeline = redis.pipeline();

  // Set the bucket start timestamp (only on first write)
  pipeline.hsetnx(key, 'start', bucketStart.toISOString());

  // Increment total log count
  pipeline.hincrby(key, 'total', 1);

  // Increment error count (0 or 1)
  pipeline.hincrby(key, 'errors', isError);

  // Refresh TTL — safety net for orphan cleanup
  pipeline.expire(key, config.timeBucket.ttlSeconds);

  // Execute all commands atomically
  await pipeline.exec();

  console.log(
    `📊 Bucket updated  [${key}]  total+1  errors+${isError}`
  );
}

// ─────────────────────────────────────────────────────────────
// Exports
// ─────────────────────────────────────────────────────────────

module.exports = {
  getTimeBucketKey,
  getCurrentMinuteKey,
  formatMinute,
  updateBucket,
};
