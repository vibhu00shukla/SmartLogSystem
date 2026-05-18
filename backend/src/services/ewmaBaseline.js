'use strict';

/**
 * ewmaBaseline.js — EWMA Baseline & Adaptive Anomaly Detection
 *
 * Maintains per-service Exponentially Weighted Moving Average (EWMA)
 * baselines in Redis Hashes. The evaluator calls updateBaseline()
 * on every evaluation cycle to continuously learn what "normal"
 * error rates look like for each service.
 *
 * EWMA Formula:
 *   newBaseline = (currentRate × α) + (previousBaseline × (1 - α))
 *
 * With α = 0.5, each new observation carries equal weight to
 * the entire prior history. This provides balanced responsiveness
 * without excessive noise sensitivity.
 *
 * Redis key design:
 *
 *   ewma:{service}   — Hash with fields:
 *     baseline     — current EWMA error rate (%)
 *     samples      — number of evaluation cycles (for cold-start detection)
 *     updatedAt    — ISO timestamp of last update
 *
 * No TTL: baselines persist indefinitely. If a service goes idle
 * and comes back, the learned baseline is still valid and useful.
 * Memory cost: ~100 bytes per service (trivial).
 *
 * Safety features:
 *   • Baseline floor (0.5%): prevents division-by-zero when a service
 *     has never had errors (EWMA = 0 → deviation = infinity).
 *   • Cold-start grace period (5 cycles): EWMA trains silently before
 *     arming anomaly detection, avoiding false positives from unreliable
 *     1-sample baselines.
 *   • Minimum absolute rate (1%): even if deviation is 10×, an absolute
 *     error rate of 0.3% isn't actionable — this guard prevents noise.
 */

const config = require('../config');

// ─────────────────────────────────────────────────────────────
// Key Helpers
// ─────────────────────────────────────────────────────────────

/**
 * Build the Redis hash key for a service's EWMA baseline.
 *
 * @param {string} service — e.g. "auth-service"
 * @returns {string} — e.g. "ewma:auth-service"
 */
function getBaselineKey(service) {
  return `${config.ewma.keyPrefix}:${service}`;
}

/**
 * Build the Redis cooldown key for adaptive anomaly alerts.
 *
 * Uses a SEPARATE namespace from threshold cooldowns so the two
 * alert systems operate independently:
 *   threshold:  alert:cd:{service}
 *   adaptive:   alert:cd:adp:{service}
 *
 * @param {string} service
 * @returns {string} — e.g. "alert:cd:adp:auth-service"
 */
function getAdaptiveCooldownKey(service) {
  return `${config.ewma.adaptiveCooldownPrefix}:${service}`;
}

// ─────────────────────────────────────────────────────────────
// EWMA Calculation
// ─────────────────────────────────────────────────────────────

/**
 * Compute the new EWMA value given the current rate and previous baseline.
 *
 * Formula: newEWMA = (currentRate × α) + (previousBaseline × (1 - α))
 *
 * Pure function — no side effects.
 *
 * @param {number} currentRate — current error rate (%)
 * @param {number} previousBaseline — previous EWMA value (%)
 * @param {number} alpha — smoothing factor (0-1)
 * @returns {number} — new EWMA value (%)
 */
function calculateEwma(currentRate, previousBaseline, alpha) {
  return (currentRate * alpha) + (previousBaseline * (1 - alpha));
}

/**
 * Compute the deviation multiplier: how many times the current rate
 * exceeds the baseline.
 *
 * Uses the baseline floor to prevent division-by-zero when the
 * baseline is 0% (service has never had errors).
 *
 * Example:
 *   baseline = 1%, current = 8% → deviation = 8.0×
 *   baseline = 0%, floor = 0.5%, current = 3% → deviation = 6.0×
 *
 * @param {number} currentRate — current error rate (%)
 * @param {number} baseline — EWMA baseline (%)
 * @returns {number} — deviation multiplier
 */
function computeDeviation(currentRate, baseline) {
  // Use the floor to prevent division-by-zero and noise from tiny baselines
  const effectiveBaseline = Math.max(baseline, config.ewma.baselineFloor);
  return currentRate / effectiveBaseline;
}

// ─────────────────────────────────────────────────────────────
// Redis Operations
// ─────────────────────────────────────────────────────────────

/**
 * Read the current EWMA baseline data for a service.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service
 * @returns {Promise<{ baseline: number, samples: number, updatedAt: string } | null>}
 *   Returns null if no baseline exists (first evaluation ever).
 */
async function getBaseline(redis, service) {
  const key = getBaselineKey(service);
  const data = await redis.hgetall(key);

  // Empty hash = no baseline exists yet
  if (!data || !data.baseline) {
    return null;
  }

  return {
    baseline:  parseFloat(data.baseline),
    samples:   parseInt(data.samples, 10) || 0,
    updatedAt: data.updatedAt || null,
  };
}

/**
 * Update the EWMA baseline for a service.
 *
 * Called once per evaluation cycle (every 30s) for each service
 * that has enough traffic (above minRequestThreshold).
 *
 * On first call (no existing baseline): seeds the EWMA with the
 * current rate directly — no smoothing applied to the initial value.
 *
 * On subsequent calls: applies the EWMA formula to smooth the
 * current rate into the running baseline.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service
 * @param {number} currentRate — current error rate (%)
 * @returns {Promise<{ baseline: number, previousBaseline: number, samples: number }>}
 */
async function updateBaseline(redis, service, currentRate) {
  const key = getBaselineKey(service);
  const alpha = config.ewma.alpha;

  // Read existing baseline
  const existing = await getBaseline(redis, service);

  let newBaseline;
  let previousBaseline;
  let samples;

  if (!existing) {
    // ── First evaluation ever — seed the baseline ─────────
    // No smoothing on the first value — just accept it as-is.
    // The cold-start guard (minSamples) prevents premature alerting.
    newBaseline = currentRate;
    previousBaseline = 0;
    samples = 1;
  } else {
    // ── Apply EWMA formula ───────────────────────────────
    previousBaseline = existing.baseline;
    newBaseline = calculateEwma(currentRate, previousBaseline, alpha);
    samples = existing.samples + 1;
  }

  // ── Write updated baseline to Redis ────────────────────
  // HMSET is atomic — all three fields are written together.
  const now = new Date().toISOString();
  await redis.hmset(key, {
    baseline:  newBaseline.toFixed(6),
    samples:   samples.toString(),
    updatedAt: now,
  });

  return {
    baseline: newBaseline,
    previousBaseline,
    samples,
  };
}

// ─────────────────────────────────────────────────────────────
// Anomaly Detection
// ─────────────────────────────────────────────────────────────

/**
 * Determine if the current error rate constitutes an anomaly
 * relative to the EWMA baseline.
 *
 * Three conditions must ALL be met:
 *   1. Baseline is trained (samples >= minSamples)
 *   2. Current rate exceeds minimum absolute rate (e.g., >= 1%)
 *   3. Deviation multiplier exceeds threshold (e.g., >= 3×)
 *
 * @param {number} currentRate — current error rate (%)
 * @param {number} baseline — EWMA baseline (%)
 * @param {number} samples — number of evaluation cycles
 * @returns {{ isAnomaly: boolean, deviation: number, reason: string | null }}
 */
function detectAnomaly(currentRate, baseline, samples) {
  const { minSamples, deviationThreshold, minAbsoluteRate } = config.ewma;

  // Condition 1: Cold-start check
  if (samples < minSamples) {
    return {
      isAnomaly: false,
      deviation: 0,
      reason: `training (samples=${samples}/${minSamples})`,
    };
  }

  // Condition 2: Minimum absolute rate
  if (currentRate < minAbsoluteRate) {
    return {
      isAnomaly: false,
      deviation: computeDeviation(currentRate, baseline),
      reason: `rate=${currentRate.toFixed(2)}% below min absolute ${minAbsoluteRate}%`,
    };
  }

  // Condition 3: Deviation threshold
  const deviation = computeDeviation(currentRate, baseline);

  if (deviation < deviationThreshold) {
    return {
      isAnomaly: false,
      deviation,
      reason: `deviation=${deviation.toFixed(2)}× below ${deviationThreshold}× threshold`,
    };
  }

  // All conditions met → anomaly detected
  return {
    isAnomaly: true,
    deviation,
    reason: null,
  };
}

// ─────────────────────────────────────────────────────────────
// Exports
// ─────────────────────────────────────────────────────────────

module.exports = {
  getBaselineKey,
  getAdaptiveCooldownKey,
  calculateEwma,
  computeDeviation,
  getBaseline,
  updateBaseline,
  detectAnomaly,
};
