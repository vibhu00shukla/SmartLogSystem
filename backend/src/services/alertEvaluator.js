'use strict';

/**
 * alertEvaluator.js — Dual-Path Alert Detection Service
 *
 * Periodically evaluates per-service error rates and generates alerts
 * through TWO independent detection paths:
 *
 *   Path 1: FIXED THRESHOLD ALERTS (Phase 2A)
 *     ≥2% → WARNING, ≥5% → CRITICAL, ≥10% → SEVERE
 *     Cooldown key:  alert:cd:{service}
 *     Alert type:    error_rate_threshold
 *
 *   Path 2: ADAPTIVE ANOMALY ALERTS (Phase 4)
 *     EWMA baseline tracks "normal" error rate per service.
 *     Fires when current rate ≥ 3× baseline AND rate ≥ 1%.
 *     Cooldown key:  alert:cd:adp:{service}
 *     Alert type:    adaptive_deviation
 *
 * Both paths run in every evaluation cycle. They have SEPARATE
 * cooldown namespaces so one never suppresses the other.
 *
 * Data source: Rolling 60-second sorted sets (Phase 3).
 *
 * Architecture:
 *   • Runs on a 30-second setInterval, fully DECOUPLED from the
 *     log ingestion pipeline. The logConsumer is never touched.
 *   • Discovers services via the sw:services registry SET.
 *   • Counts events via ZCOUNT on sorted sets — O(log N).
 *   • Updates EWMA baseline on EVERY cycle (always learning).
 *   • Supports severity ESCALATION for threshold alerts.
 *   • Cold-start grace period for adaptive alerts (5 cycles).
 *
 * Redis keys used:
 *   READ:   sw:{service}            (all-events sorted set)
 *   READ:   sw:{service}:err        (error-events sorted set)
 *   READ:   sw:services             (service registry SET)
 *   READ:   alert:cd:{service}      (threshold cooldown)
 *   READ:   alert:cd:adp:{service}  (adaptive cooldown)
 *   READ:   ewma:{service}          (EWMA baseline hash)
 *   WRITE:  alert:cd:{service}      (threshold cooldown with TTL)
 *   WRITE:  alert:cd:adp:{service}  (adaptive cooldown with TTL)
 *   WRITE:  ewma:{service}          (EWMA baseline hash, no TTL)
 *
 * Failure model:
 *   • Each service is evaluated independently — one failure doesn't
 *     block others.
 *   • EWMA update failures don't block threshold alerts.
 *   • Alert persistence failures are logged but never propagated.
 */

const config = require('../config');
const { insertAlert } = require('../db/mongo');
const {
  getRollingMetrics,
  getServiceRegistry,
  cleanupWindow,
} = require('./slidingWindowAggregator');
const {
  updateBaseline,
  detectAnomaly,
  getAdaptiveCooldownKey,
} = require('./ewmaBaseline');
const {
  computePercentiles,
  cleanupLatencyWindows,
} = require('./latencyAggregator');
const {
  getEndpointsForService,
  cleanupRegistries,
} = require('./endpointRegistry');

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
 * Build the Redis cooldown key for threshold alerts.
 *
 * @param {string} service
 * @returns {string} e.g. "alert:cd:auth-service"
 */
function getCooldownKey(service) {
  return `${config.alerting.cooldownKeyPrefix}:${service}`;
}

// ─────────────────────────────────────────────────────────────
// Path 1: Fixed Threshold Alerting (unchanged logic from Phase 2A)
// ─────────────────────────────────────────────────────────────

/**
 * Handle fixed-threshold alert logic for a single service.
 *
 * This is the EXACT SAME logic from Phase 2A, extracted into a
 * named function so the EWMA update and adaptive check can also
 * run in the same evaluation cycle.
 *
 * Steps:
 *   1. Compute severity from error rate
 *   2. Check threshold cooldown (with escalation)
 *   3. Persist alert to MongoDB
 *   4. Set threshold cooldown
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service
 * @param {number} errorRate — error rate (%)
 * @param {number} total — total logs in window
 * @param {number} errors — error logs in window
 * @param {number} windowSeconds — window size
 * @returns {Promise<void>}
 */
async function handleThresholdAlert(redis, service, errorRate, total, errors, windowSeconds) {
  const severity = computeSeverity(errorRate);

  if (!severity) {
    // Below all thresholds — no threshold alert needed
    console.log(
      `   ✅ [${service}] errorRate=${errorRate.toFixed(2)}% (${errors}/${total}) window=${windowSeconds}s — below thresholds`
    );
    return;
  }

  // ── Check cooldown with escalation ────────────────────────
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

  // ── Build and persist alert document ──────────────────────
  const now = new Date().toISOString();

  const alertDoc = {
    service,
    severity,
    errorRate:     parseFloat(errorRate.toFixed(4)),
    totalLogs:     total,
    errorLogs:     errors,
    timestamp:     now,
    alertType:     'error_rate_threshold',
    dataSource:    'sliding_window',
    windowSeconds,
    evaluatedAt:   now,
  };

  try {
    const result = await insertAlert(alertDoc);
    console.log(
      `   🚨 ALERT [${severity}] service=${service} errorRate=${errorRate.toFixed(2)}% ` +
      `(${errors}/${total}) window=${windowSeconds}s — persisted as ${result.insertedId}`
    );
  } catch (dbErr) {
    console.error(
      `   ❌ Failed to persist threshold alert for [${service}]:`, dbErr.message
    );
    return; // Don't set cooldown if persistence failed — allows retry
  }

  // ── Set threshold cooldown ────────────────────────────────
  await redis.set(
    cooldownKey,
    severity,
    'EX',
    config.alerting.cooldownSeconds
  );

  console.log(
    `   🔕 Threshold cooldown set: ${cooldownKey} = ${severity} (TTL=${config.alerting.cooldownSeconds}s)`
  );
}

// ─────────────────────────────────────────────────────────────
// Path 2: Adaptive Anomaly Alerting (Phase 4)
// ─────────────────────────────────────────────────────────────

/**
 * Handle adaptive anomaly detection for a single service.
 *
 * Uses the EWMA baseline (already updated earlier in the cycle)
 * to detect when the current error rate significantly exceeds
 * what the system has learned as "normal" for this service.
 *
 * Has its own cooldown namespace (alert:cd:adp:{service}) so it
 * operates completely independently of threshold alerts.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service
 * @param {number} errorRate — current error rate (%)
 * @param {number} total — total logs in window
 * @param {number} errors — error logs in window
 * @param {number} windowSeconds — window size
 * @param {{ baseline: number, previousBaseline: number, samples: number }} ewmaResult
 * @returns {Promise<void>}
 */
async function handleAdaptiveAlert(redis, service, errorRate, total, errors, windowSeconds, ewmaResult) {
  const { baseline, samples } = ewmaResult;

  // ── Anomaly detection (3 safety guards checked inside) ────
  const anomaly = detectAnomaly(errorRate, baseline, samples);

  if (!anomaly.isAnomaly) {
    // Log the reason for transparency
    console.log(
      `   🔬 [${service}] adaptive: ${anomaly.reason} — no anomaly`
    );
    return;
  }

  // ── Check adaptive cooldown ───────────────────────────────
  // Separate namespace from threshold cooldowns — the two systems
  // never interfere with each other.
  const adaptiveCdKey = getAdaptiveCooldownKey(service);
  const existingAdaptiveCd = await redis.get(adaptiveCdKey);

  if (existingAdaptiveCd) {
    console.log(
      `   🔇 [${service}] ANOMALY alert suppressed — adaptive cooldown active (TTL=${await redis.ttl(adaptiveCdKey)}s)`
    );
    return;
  }

  // ── Build and persist adaptive alert document ─────────────
  const now = new Date().toISOString();

  const alertDoc = {
    service,
    severity:             'ANOMALY',
    errorRate:            parseFloat(errorRate.toFixed(4)),
    totalLogs:            total,
    errorLogs:            errors,
    timestamp:            now,
    alertType:            'adaptive_deviation',
    dataSource:           'sliding_window',
    windowSeconds,
    ewmaBaseline:         parseFloat(baseline.toFixed(6)),
    deviationMultiplier:  parseFloat(anomaly.deviation.toFixed(4)),
    evaluatedAt:          now,
  };

  try {
    const result = await insertAlert(alertDoc);
    console.log(
      `   🔬 ANOMALY [${service}] rate=${errorRate.toFixed(2)}% baseline=${baseline.toFixed(2)}% ` +
      `deviation=${anomaly.deviation.toFixed(2)}× — persisted as ${result.insertedId}`
    );
  } catch (dbErr) {
    console.error(
      `   ❌ Failed to persist adaptive alert for [${service}]:`, dbErr.message
    );
    return; // Don't set cooldown if persistence failed — allows retry
  }

  // ── Set adaptive cooldown ─────────────────────────────────
  await redis.set(
    adaptiveCdKey,
    'ANOMALY',
    'EX',
    config.ewma.adaptiveCooldownSeconds
  );

  console.log(
    `   🔕 Adaptive cooldown set: ${adaptiveCdKey} = ANOMALY (TTL=${config.ewma.adaptiveCooldownSeconds}s)`
  );
}

// ─────────────────────────────────────────────────────────────
// Core Evaluation Logic
// ─────────────────────────────────────────────────────────────

/**
 * Evaluate a single service — runs BOTH threshold and adaptive paths.
 *
 * Flow:
 *   1. Defensive cleanup of stale ZSET entries
 *   2. Get rolling metrics (ZCOUNT-based, O(log N))
 *   3. Check minimum request threshold
 *   4. Compute error rate
 *   5. UPDATE EWMA BASELINE (always — learns from every meaningful window)
 *   6. THRESHOLD ALERTING (fixed thresholds with escalation)
 *   7. ADAPTIVE ANOMALY ALERTING (EWMA deviation detection)
 *
 * Steps 5-7 ALWAYS run — there are no early returns after the error
 * rate is computed. This ensures the EWMA baseline continuously
 * learns regardless of whether any alerts fire.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service — service name
 * @returns {Promise<void>}
 */
async function evaluateService(redis, service) {
  // ── 1. Defensive cleanup ─────────────────────────────────
  // Remove entries older than the window, in case the service
  // stopped sending logs and no insert-time cleanup ran.
  await cleanupWindow(redis, service);

  // ── 2. Get rolling metrics ────────────────────────────────
  // ZCOUNT-based: O(log N) for both total and error counts.
  const metrics = await getRollingMetrics(redis, service);
  const { total, errors, windowSeconds } = metrics;

  // Empty window = no recent events (service went idle)
  if (total === 0) {
    return;
  }

  // ── 3. Minimum request threshold ─────────────────────────
  // Don't evaluate services with too few requests in the window.
  // Also don't update EWMA with noisy low-sample data — a window
  // with 5 requests where 1 is an error (20%) would poison the baseline.
  if (total < config.alerting.minRequestThreshold) {
    console.log(
      `   ⏭️  [${service}] total=${total} < ${config.alerting.minRequestThreshold} min threshold (window=${windowSeconds}s) — skipping alerts/EWMA`
    );
  } else {
    // ── 4. Compute error rate ─────────────────────────────────
    const errorRate = (errors / total) * 100;

    // ── 5. Update EWMA baseline (ALWAYS runs) ─────────────────
    // The baseline learns from every evaluation cycle where we have
    // enough traffic. This happens regardless of whether any alert
    // fires — the system is always learning "normal" behavior.
    let ewmaResult;
    try {
      ewmaResult = await updateBaseline(redis, service, errorRate);
      console.log(
        `   📊 EWMA [${service}] rate=${errorRate.toFixed(2)}% ` +
        `baseline=${ewmaResult.previousBaseline.toFixed(2)}→${ewmaResult.baseline.toFixed(2)}% ` +
        `samples=${ewmaResult.samples} α=${config.ewma.alpha}`
      );
    } catch (ewmaErr) {
      // EWMA update failure must NOT block threshold alerts.
      // Log and continue — threshold alerting is more critical.
      console.error(
        `   ⚠️  EWMA update failed for [${service}]:`, ewmaErr.message
      );
      ewmaResult = null;
    }

    // ── 6. Path 1: Fixed threshold alerting ───────────────────
    // Exact same logic as Phase 2A, now in a named helper function.
    // Has its own cooldown: alert:cd:{service}
    try {
      await handleThresholdAlert(redis, service, errorRate, total, errors, windowSeconds);
    } catch (threshErr) {
      console.error(
        `   ❌ Threshold alert handling failed for [${service}]:`, threshErr.message
      );
    }

    // ── 7. Path 2: Adaptive anomaly alerting ──────────────────
    // Only runs if EWMA was updated successfully.
    // Has its own cooldown: alert:cd:adp:{service}
    if (ewmaResult) {
      try {
        await handleAdaptiveAlert(redis, service, errorRate, total, errors, windowSeconds, ewmaResult);
      } catch (adaptiveErr) {
        console.error(
          `   ❌ Adaptive alert handling failed for [${service}]:`, adaptiveErr.message
        );
      }
    }
  }

  // ── 8. Path 3: Latency percentiles (Phase 5A/5B — logging only) ─
  // Compute P50/P95/P99 for all latency types at the service level,
  // and then iterate over active endpoints for endpoint-level metrics.
  try {
    // Service-level latency
    await cleanupLatencyWindows(redis, service);
    const latencyMetrics = await computePercentiles(redis, service);

    if (latencyMetrics) {
      for (const [field, metrics] of Object.entries(latencyMetrics)) {
        if (metrics) {
          console.log(
            `   📐 [${service}] ${field}: ` +
            `P50=${metrics.p50}ms P95=${metrics.p95}ms P99=${metrics.p99}ms ` +
            `(min=${metrics.min}ms max=${metrics.max}ms samples=${metrics.samples})`
          );
        }
      }
    }

    // Endpoint-level latency (Phase 5B)
    await cleanupRegistries(redis, service);
    const endpoints = await getEndpointsForService(redis, service);

    for (const endpoint of endpoints) {
      await cleanupLatencyWindows(redis, service, endpoint);
      const epMetrics = await computePercentiles(redis, service, endpoint);

      if (epMetrics) {
        for (const [field, metrics] of Object.entries(epMetrics)) {
          if (metrics) {
            console.log(
              `   📐 [${service}][${endpoint}] ${field}: ` +
              `P50=${metrics.p50}ms P95=${metrics.p95}ms P99=${metrics.p99}ms ` +
              `(min=${metrics.min}ms max=${metrics.max}ms samples=${metrics.samples})`
            );
          }
        }
      }
    }

  } catch (latErr) {
    console.error(
      `   ⚠️  Latency percentile calc failed for [${service}]:`, latErr.message
    );
  }
}

/**
 * Run a full evaluation cycle across ALL registered services.
 *
 * Uses the sw:services registry SET for service discovery —
 * no SCAN needed. For each service, reads rolling metrics via
 * ZCOUNT, updates EWMA, and runs both alert paths.
 *
 * @param {import('ioredis').Redis} redis
 * @returns {Promise<void>}
 */
async function evaluateAllServices(redis) {
  console.log('\n🔍 Alert evaluator — scanning sliding windows…');

  // ── Discover services from registry ───────────────────────
  const services = await getServiceRegistry(redis);

  if (!services || services.length === 0) {
    console.log('   ℹ️  No services registered in sw:services — nothing to evaluate');
    console.log('🔍 Evaluation complete — services=0\n');
    return;
  }

  console.log(`   📋 Services registered: [${services.join(', ')}]`);

  let evaluated = 0;

  for (const service of services) {
    try {
      await evaluateService(redis, service);
      evaluated++;
    } catch (serviceErr) {
      // Per-service failure: log and continue to next service
      console.error(`   ❌ Error evaluating [${service}]:`, serviceErr.message);
    }
  }

  console.log(
    `🔍 Evaluation complete — services=${services.length} evaluated=${evaluated}\n`
  );
}

// ─────────────────────────────────────────────────────────────
// Scheduler
// ─────────────────────────────────────────────────────────────

/**
 * Start the periodic alert evaluation loop.
 *
 * Runs evaluateAllServices() on a fixed interval (default 30s).
 * Returns the interval ID so the caller can clear it on shutdown.
 *
 * The first evaluation is slightly delayed (5s) to let the worker
 * finish startup and begin processing logs before we scan windows.
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
    `(EWMA: α=${config.ewma.alpha}, deviation≥${config.ewma.deviationThreshold}×, ` +
    `floor=${config.ewma.baselineFloor}%, cold-start=${config.ewma.minSamples} cycles) ` +
    `(min requests: ${config.alerting.minRequestThreshold}) ` +
    `(cooldowns: threshold=${config.alerting.cooldownSeconds}s, adaptive=${config.ewma.adaptiveCooldownSeconds}s)`
  );

  // Slight delay before first evaluation — let the worker warm up
  setTimeout(() => {
    evaluateAllServices(redis).catch((err) => {
      console.error('❌ Alert evaluation cycle failed:', err.message);
    });
  }, 5000);

  // Periodic evaluation
  const intervalId = setInterval(() => {
    evaluateAllServices(redis).catch((err) => {
      console.error('❌ Alert evaluation cycle failed:', err.message);
    });
  }, intervalMs);

  return intervalId;
}

// ─────────────────────────────────────────────────────────────
// Exports
// ─────────────────────────────────────────────────────────────

module.exports = {
  evaluateAllServices,
  startAlertEvaluator,
  computeSeverity,
  getCooldownKey,
  SEVERITY_RANK,
};
