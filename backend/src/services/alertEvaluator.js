'use strict';

/**
 * alertEvaluator.js — Error Rate Alert Detection Service
 *
 * Periodically scans Redis time buckets (tb:{service}:{minute}),
 * computes per-service error rates, and generates alert documents
 * when thresholds are breached.
 *
 * Architecture:
 *   • Runs on a 30-second setInterval, fully DECOUPLED from the
 *     log ingestion pipeline. The logConsumer is never touched.
 *   • Reads bucket hashes via HGETALL — atomic, no race with HINCRBY.
 *   • Uses Redis cooldown keys (alert:cd:{service}) with TTL to prevent
 *     repeated alerts for the same service.
 *   • Supports severity ESCALATION — if a service's error rate climbs
 *     from WARNING to SEVERE during an active cooldown, the cooldown
 *     is overridden and a new, higher-severity alert fires.
 *
 * Redis keys used:
 *   READ:   tb:{service}:{YYYYMMDDHHmm}    (time bucket hashes)
 *   WRITE:  alert:cd:{service}              (cooldown keys with 5-min TTL)
 *
 * Failure model:
 *   • Each bucket is evaluated independently — one failure doesn't
 *     block others.
 *   • The entire evaluation cycle is wrapped in try/catch — a crash
 *     just means the next 30s tick retries.
 *   • Alert persistence failures are logged but never propagated.
 */

const config = require('../config');
const { insertAlert } = require('../db/mongo');
const { formatMinute } = require('./bucketAggregator');

// ─────────────────────────────────────────────────────────────
// Severity Ranking
// ─────────────────────────────────────────────────────────────

/**
 * Numeric ranking for severity comparison.
 * Higher number = more severe.
 * Used for escalation logic: if current > stored, override cooldown.
 */
const SEVERITY_RANK = {
  WARNING:  1,
  CRITICAL: 2,
  SEVERE:   3,
};

// ─────────────────────────────────────────────────────────────
// Pure Functions
// ─────────────────────────────────────────────────────────────

/**
 * Compute the severity level for a given error rate.
 *
 * Evaluates highest threshold first so the most severe match wins.
 *   >= 10% → SEVERE
 *   >= 5%  → CRITICAL
 *   >= 2%  → WARNING
 *   < 2%   → null (no alert)
 *
 * @param {number} errorRate — error rate as a percentage (0-100)
 * @returns {string|null} — severity string or null if below all thresholds
 */
function computeSeverity(errorRate) {
  const { thresholds } = config.alerting;

  // Evaluate highest-first to return the most severe match
  if (errorRate >= thresholds.SEVERE)   return 'SEVERE';
  if (errorRate >= thresholds.CRITICAL) return 'CRITICAL';
  if (errorRate >= thresholds.WARNING)  return 'WARNING';

  return null; // below all thresholds
}

/**
 * Build the Redis cooldown key for a given service.
 *
 * @param {string} service
 * @returns {string} e.g. "alert:cd:auth-service"
 */
function getCooldownKey(service) {
  return `${config.alerting.cooldownKeyPrefix}:${service}`;
}

/**
 * Parse a time bucket Redis key into its components.
 *
 * Key format: {prefix}:{service}:{YYYYMMDDHHmm}
 * The minute is always the last 12-char segment.
 * The service name may contain colons (defensive parsing).
 *
 * @param {string} key — e.g. "tb:auth-service:202605151135"
 * @returns {{ service: string, minute: string } | null}
 */
function parseBucketKey(key) {
  const prefix = config.timeBucket.keyPrefix;

  // Strip the prefix and leading colon
  if (!key.startsWith(prefix + ':')) return null;
  const rest = key.slice(prefix.length + 1); // "auth-service:202605151135"

  // The minute is always the last 12 characters (YYYYMMDDHHmm)
  if (rest.length < 13) return null; // minimum: "x:202605151135" = 15 chars
  const minute = rest.slice(-12);
  const service = rest.slice(0, -(12 + 1)); // strip ":202605151135"

  if (!service || !minute) return null;
  return { service, minute };
}

/**
 * Check if a bucket minute is within the active TTL window.
 *
 * Only evaluates buckets created within the last `ttlSeconds` to avoid
 * processing stale/expired historical data unnecessarily.
 *
 * @param {string} minute — bucket minute string (YYYYMMDDHHmm)
 * @returns {boolean} — true if bucket is recent enough to evaluate
 */
function isBucketRecent(minute) {
  // Parse the minute string back to a Date (UTC)
  const year  = parseInt(minute.slice(0, 4), 10);
  const month = parseInt(minute.slice(4, 6), 10) - 1; // 0-indexed
  const day   = parseInt(minute.slice(6, 8), 10);
  const hour  = parseInt(minute.slice(8, 10), 10);
  const min   = parseInt(minute.slice(10, 12), 10);

  const bucketTime = new Date(Date.UTC(year, month, day, hour, min, 0));
  const now = new Date();

  // Bucket age in seconds
  const ageSeconds = (now.getTime() - bucketTime.getTime()) / 1000;

  // Only evaluate if within the TTL window
  return ageSeconds <= config.timeBucket.ttlSeconds;
}

// ─────────────────────────────────────────────────────────────
// Core Evaluation Logic
// ─────────────────────────────────────────────────────────────

/**
 * Evaluate a single time bucket and generate an alert if warranted.
 *
 * Steps:
 *   1. HGETALL to read total, errors, start
 *   2. Check minimum request threshold
 *   3. Compute error rate and severity
 *   4. Check cooldown (with escalation support)
 *   5. Persist alert to MongoDB
 *   6. Set cooldown key with TTL
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} bucketKey — e.g. "tb:auth-service:202605151135"
 * @param {string} service — service name
 * @param {string} minute — bucket minute string
 * @returns {Promise<void>}
 */
async function evaluateBucket(redis, bucketKey, service, minute) {
  // ── 1. Read bucket data ──────────────────────────────────
  const data = await redis.hgetall(bucketKey);

  // Empty hash = key expired between SCAN and HGETALL (rare but possible)
  if (!data || !data.total) {
    return;
  }

  const total  = parseInt(data.total, 10);
  const errors = parseInt(data.errors, 10) || 0;
  const start  = data.start || null;

  // ── 2. Minimum request threshold ─────────────────────────
  // Don't evaluate services with too few requests — avoids false positives
  // from low-traffic services (e.g., 1 error out of 2 requests = 50%).
  if (total < config.alerting.minRequestThreshold) {
    console.log(
      `   ⏭️  [${service}] total=${total} < ${config.alerting.minRequestThreshold} min threshold — skipping`
    );
    return;
  }

  // ── 3. Compute error rate and severity ────────────────────
  const errorRate = (errors / total) * 100;
  const severity = computeSeverity(errorRate);

  if (!severity) {
    // Below all thresholds — no alert needed
    console.log(
      `   ✅ [${service}] errorRate=${errorRate.toFixed(2)}% — below thresholds`
    );
    return;
  }

  // ── 4. Check cooldown with escalation ─────────────────────
  const cooldownKey = getCooldownKey(service);
  const existingCooldown = await redis.get(cooldownKey);

  if (existingCooldown) {
    const existingRank = SEVERITY_RANK[existingCooldown] || 0;
    const currentRank  = SEVERITY_RANK[severity] || 0;

    if (currentRank <= existingRank) {
      // Same or lower severity — cooldown is still active, skip
      console.log(
        `   🔇 [${service}] ${severity} alert suppressed — cooldown active (stored=${existingCooldown}, TTL=${await redis.ttl(cooldownKey)}s)`
      );
      return;
    }

    // Current severity is HIGHER than stored → escalation!
    console.log(
      `   ⬆️  [${service}] ESCALATION: ${existingCooldown} → ${severity} — overriding cooldown`
    );
  }

  // ── 5. Build and persist alert document ───────────────────
  const now = new Date().toISOString();

  const alertDoc = {
    service,
    severity,
    errorRate:    parseFloat(errorRate.toFixed(4)),
    totalLogs:    total,
    errorLogs:    errors,
    timestamp:    now,
    bucketKey,
    alertType:    'error_rate_threshold',
    bucketMinute: minute,
    bucketStart:  start,
    evaluatedAt:  now,
  };

  try {
    const result = await insertAlert(alertDoc);
    console.log(
      `   🚨 ALERT [${severity}] service=${service} errorRate=${errorRate.toFixed(2)}% ` +
      `(${errors}/${total}) — persisted as ${result.insertedId}`
    );
  } catch (dbErr) {
    // Alert persistence failure must NOT break the evaluation cycle.
    // Log and continue — the next cycle will re-evaluate.
    console.error(
      `   ❌ Failed to persist alert for [${service}]:`, dbErr.message
    );
    return; // Don't set cooldown if persistence failed — allows retry
  }

  // ── 6. Set cooldown — prevents repeated alerts ────────────
  // Store the severity as the value so escalation logic can compare.
  // EX = TTL in seconds. Key auto-expires after cooldown period.
  await redis.set(
    cooldownKey,
    severity,
    'EX',
    config.alerting.cooldownSeconds
  );

  console.log(
    `   🔕 Cooldown set: ${cooldownKey} = ${severity} (TTL=${config.alerting.cooldownSeconds}s)`
  );
}

/**
 * Run a full evaluation cycle across ALL recent time buckets.
 *
 * Uses SCAN (never KEYS) to iterate bucket keys safely in production.
 * Only processes buckets within the active TTL window — stale/expired
 * historical buckets are skipped.
 *
 * @param {import('ioredis').Redis} redis
 * @returns {Promise<void>}
 */
async function evaluateAllBuckets(redis) {
  const prefix = config.timeBucket.keyPrefix;
  const pattern = `${prefix}:*`;

  console.log('\n🔍 Alert evaluator — scanning buckets…');

  let cursor = '0';
  let bucketsScanned = 0;
  let bucketsEvaluated = 0;
  let bucketsSkippedStale = 0;

  // SCAN loop — iterates through all matching keys in batches
  do {
    // SCAN cursor MATCH pattern COUNT hint
    // COUNT is a hint, not a guarantee — Redis may return more or fewer
    const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 200);
    cursor = nextCursor;

    for (const key of keys) {
      bucketsScanned++;

      // Parse the key to extract service and minute
      const parsed = parseBucketKey(key);
      if (!parsed) {
        console.log(`   ⚠️  Skipping unparseable key: ${key}`);
        continue;
      }

      const { service, minute } = parsed;

      // ── Filter: only evaluate recent buckets ──────────────
      // Skip buckets older than the TTL window to avoid processing
      // stale historical data unnecessarily.
      if (!isBucketRecent(minute)) {
        bucketsSkippedStale++;
        console.log(`   ⏩ [${service}] bucket ${minute} is stale — skipping`);
        continue;
      }

      bucketsEvaluated++;

      try {
        await evaluateBucket(redis, key, service, minute);
      } catch (bucketErr) {
        // Per-bucket failure: log and continue to next bucket
        console.error(`   ❌ Error evaluating [${key}]:`, bucketErr.message);
      }
    }
  } while (cursor !== '0');

  console.log(
    `🔍 Evaluation complete — scanned=${bucketsScanned} evaluated=${bucketsEvaluated} stale_skipped=${bucketsSkippedStale}\n`
  );
}

// ─────────────────────────────────────────────────────────────
// Scheduler
// ─────────────────────────────────────────────────────────────

/**
 * Start the periodic alert evaluation loop.
 *
 * Runs evaluateAllBuckets() on a fixed interval (default 30s).
 * Returns the interval ID so the caller can clear it on shutdown.
 *
 * The first evaluation is slightly delayed (5s) to let the worker
 * finish startup and begin processing logs before we scan buckets.
 *
 * @param {import('ioredis').Redis} redis
 * @returns {NodeJS.Timeout} — interval ID for cleanup
 */
function startAlertEvaluator(redis) {
  const intervalMs = config.alerting.evalIntervalMs;

  console.log(
    `🚨 Alert evaluator started — evaluating every ${intervalMs / 1000}s ` +
    `(thresholds: WARNING≥${config.alerting.thresholds.WARNING}%, ` +
    `CRITICAL≥${config.alerting.thresholds.CRITICAL}%, ` +
    `SEVERE≥${config.alerting.thresholds.SEVERE}%) ` +
    `(min requests: ${config.alerting.minRequestThreshold}) ` +
    `(cooldown: ${config.alerting.cooldownSeconds}s)`
  );

  // Slight delay before first evaluation — let the worker warm up
  setTimeout(() => {
    evaluateAllBuckets(redis).catch((err) => {
      console.error('❌ Alert evaluation cycle failed:', err.message);
    });
  }, 5000);

  // Periodic evaluation
  const intervalId = setInterval(() => {
    evaluateAllBuckets(redis).catch((err) => {
      console.error('❌ Alert evaluation cycle failed:', err.message);
    });
  }, intervalMs);

  return intervalId;
}

// ─────────────────────────────────────────────────────────────
// Exports
// ─────────────────────────────────────────────────────────────

module.exports = {
  evaluateAllBuckets,
  startAlertEvaluator,
  computeSeverity,
  getCooldownKey,
  parseBucketKey,
  isBucketRecent,
  SEVERITY_RANK,
};
