'use strict';

/**
 * endpointAlerting.js — Phase 5C
 *
 * Implements endpoint-level intelligence including:
 * 1. Static Thresholds (P95 / P99)
 * 2. Adaptive Anomaly Detection (P50 EWMA)
 * 3. Error Rate + Latency Correlation (INCIDENT)
 * 4. Cooldown management with TTLs
 */

const config = require('../config');
const { insertAlert } = require('../db/mongo');

// ─────────────────────────────────────────────────────────────
// Key Helpers
// ─────────────────────────────────────────────────────────────

function getEwmaKey(service, endpoint) {
  return `ewma:lat:${service}:${endpoint}`;
}

function getStaticCooldownKey(service, endpoint) {
  return `alert:ep:lat:${service}:${endpoint}`;
}

function getAdaptiveCooldownKey(service, endpoint) {
  return `alert:ep:adp:${service}:${endpoint}`;
}

// ─────────────────────────────────────────────────────────────
// EWMA Baseline (Latency P50)
// ─────────────────────────────────────────────────────────────

/**
 * Calculate EWMA: new = (current * alpha) + (previous * (1 - alpha))
 */
function calculateEwma(current, previous, alpha) {
  return (current * alpha) + (previous * (1 - alpha));
}

async function getBaseline(redis, service, endpoint) {
  const key = getEwmaKey(service, endpoint);
  const data = await redis.hgetall(key);
  if (!data || !data.baseline) return null;

  return {
    baseline: parseFloat(data.baseline),
    samples: parseInt(data.samples, 10) || 0,
  };
}

async function updateBaseline(redis, service, endpoint, currentP50) {
  const key = getEwmaKey(service, endpoint);
  const existing = await getBaseline(redis, service, endpoint);

  let newBaseline = currentP50;
  let newSamples = 1;

  if (existing) {
    newBaseline = calculateEwma(currentP50, existing.baseline, config.latency.ewma.alpha);
    newSamples = existing.samples + 1;
  }

  // Update hash and set rolling TTL to prevent memory leaks from ephemeral endpoints
  const pipeline = redis.pipeline();
  pipeline.hmset(key, {
    baseline: newBaseline,
    samples: newSamples,
    updatedAt: new Date().toISOString(),
  });
  pipeline.expire(key, config.latency.ewma.ttlSeconds);
  await pipeline.exec();

  return { baseline: newBaseline, previousBaseline: existing?.baseline || 0, samples: newSamples };
}

// ─────────────────────────────────────────────────────────────
// Cooldowns
// ─────────────────────────────────────────────────────────────

async function isOnCooldown(redis, key) {
  const exists = await redis.exists(key);
  return exists === 1;
}

async function setCooldown(redis, key) {
  // Use the latency.alerting.cooldownSeconds
  await redis.set(key, '1', 'EX', config.latency.alerting.cooldownSeconds);
}

// ─────────────────────────────────────────────────────────────
// Core Evaluation Logic
// ─────────────────────────────────────────────────────────────

/**
 * Evaluate an endpoint's performance metrics and generate alerts.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service
 * @param {string} endpoint
 * @param {object} metrics — { total, errors, windowSeconds } from slidingWindowAggregator
 * @param {object} latencyMetrics — { responseTime: { p50, p95, p99 }, ... }
 */
async function evaluateEndpoint(redis, service, endpoint, metrics, latencyMetrics) {
  // We only evaluate responseTime for alerting right now
  const responseTime = latencyMetrics?.responseTime;
  if (!responseTime) {
    console.log(`   ⏭️  [${service}][${endpoint}] No responseTime metrics — skipping intelligence`);
    return;
  }

  const { p50, p95, p99 } = responseTime;
  const { total, errors } = metrics;
  const errorRate = total > 0 ? (errors / total) * 100 : 0;

  console.log(`   🧠 [${service}][${endpoint}] Intelligence Engine: total=${total} errors=${errors} (${errorRate.toFixed(1)}%) p50=${p50}ms p95=${p95}ms p99=${p99}ms`);

  let alertToFire = null;
  let isStatic = false;

  // ── 1. Static Thresholds & Correlation ───────────────────
  const { warning, critical, severe } = config.latency.alerting.p95;
  const { tailSpike } = config.latency.alerting.p99;
  const { correlationErrorRate } = config.latency.alerting;

  let severity = null;
  let alertType = 'latency_threshold';
  let message = '';

  if (p99 >= tailSpike) {
    severity = 'SEVERE';
    message = `Endpoint tail latency spike: P99 is ${p99}ms (threshold: ${tailSpike}ms)`;
  } else if (p95 >= severe) {
    severity = 'SEVERE';
    message = `Endpoint latency is severe: P95 is ${p95}ms (threshold: ${severe}ms)`;
  } else if (p95 >= critical) {
    severity = 'CRITICAL';
    message = `Endpoint latency is critical: P95 is ${p95}ms (threshold: ${critical}ms)`;
  } else if (p95 >= warning) {
    severity = 'WARNING';
    message = `Endpoint latency is warning: P95 is ${p95}ms (threshold: ${warning}ms)`;
  }

  // Correlation: High latency + High errors = INCIDENT
  if (severity && errorRate >= correlationErrorRate) {
    alertType = 'incident_correlation';
    severity = severity === 'WARNING' ? 'CRITICAL' : 'SEVERE'; // Escalate severity
    message = `INCIDENT: Endpoint latency P95 (${p95}ms) and Error Rate (${errorRate.toFixed(2)}%) are degrading simultaneously.`;
  }

  if (severity) {
    alertToFire = { type: alertType, severity, message };
    isStatic = true;
  }

  // ── 2. Adaptive Anomaly (EWMA on P50) ────────────────────
  // Always update baseline to learn current behavior
  const ewmaResult = await updateBaseline(redis, service, endpoint, p50);
  
  console.log(
    `   📊 EWMA [${service}][${endpoint}] P50=${p50}ms ` +
    `baseline=${ewmaResult.previousBaseline.toFixed(2)}→${ewmaResult.baseline.toFixed(2)}ms ` +
    `samples=${ewmaResult.samples} α=${config.latency.ewma.alpha}`
  );
  
  if (!isStatic && ewmaResult.samples >= 5) {
    // Only check adaptive if we aren't already firing a static/incident alert
    // and if we've passed the cold-start grace period (5 samples)
    const deviation = p50 / Math.max(ewmaResult.previousBaseline, 10); // Floor baseline at 10ms to prevent div 0

    if (deviation >= config.latency.ewma.deviationMultiplier) {
      alertToFire = {
        type: 'latency_anomaly',
        severity: 'WARNING',
        message: `Adaptive latency anomaly: P50 (${p50}ms) is ${deviation.toFixed(1)}x higher than baseline (${ewmaResult.previousBaseline.toFixed(0)}ms).`
      };
    }
  }

  // ── 3. Cooldown & Dispatch ───────────────────────────────
  if (alertToFire) {
    const cdKey = isStatic ? getStaticCooldownKey(service, endpoint) : getAdaptiveCooldownKey(service, endpoint);
    
    // Check cooldown (Incident correlations bypass normal cooldowns for severe escalations if we wanted, 
    // but we'll apply standard cooldown for now to prevent spam).
    const coolingDown = await isOnCooldown(redis, cdKey);
    
    if (!coolingDown) {
      console.log(`   🚨 [${service}][${endpoint}] ALERT: ${alertToFire.severity} - ${alertToFire.message}`);
      
      const alertDoc = {
        service,
        endpoint,
        type: alertToFire.type,
        severity: alertToFire.severity,
        message: alertToFire.message,
        timestamp: new Date().toISOString()
      };

      await insertAlert(alertDoc);
      await setCooldown(redis, cdKey);
    } else {
      console.log(`   🔇 [${service}][${endpoint}] Alert suppressed (cooldown active)`);
    }
  }
}

module.exports = {
  evaluateEndpoint,
  getEwmaKey,
  getStaticCooldownKey,
  getAdaptiveCooldownKey
};
