'use strict';

//worker.js is the bootstrap file that initializes Redis, MongoDB, verifies the consumer group, and starts the continuous processing loop.
/**
 * Worker entry point — runs independently of the Fastify server.
 *
 * Usage:  node src/worker.js
 *
 * Flow:
 *   1. Load config (.env)
 *   2. Connect to Redis
 *   3. Connect to MongoDB
 *   4. Create / verify consumer group
 *   5. Start alert evaluator (periodic, decoupled from log pipeline)
 *   6. Enter the processing loop (XREADGROUP → normalise → insert → XACK)
 */

// Load .env before anything else
require('./config');

const { createRedisClient } = require('./db/redis');
const { connect: connectMongo, disconnect: disconnectMongo } = require('./db/mongo');
const { ensureConsumerGroup, processLogs } = require('./workers/logConsumer');
const { startAlertEvaluator } = require('./services/alertEvaluator');

async function main() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  Log Intelligence Platform — Worker Service');
  console.log('═══════════════════════════════════════════════════════\n');

  // ── 1. Redis ────────────────────────────────────────────
  redisRef = createRedisClient();

  // Wait for the "connect" event before proceeding
  await new Promise((resolve, reject) => {
    redisRef.once('connect', resolve);
    redisRef.once('error', reject);
  });

  // ── 2. MongoDB ──────────────────────────────────────────
  await connectMongo();

  // ── 3. Consumer group ───────────────────────────────────
  await ensureConsumerGroup(redisRef);

  // ── 4. Start alert evaluator (decoupled) ────────────────
  // Runs on a periodic interval, completely independent of the
  // log processing loop below. Scans Redis buckets for error rate
  // threshold breaches and persists alerts to MongoDB.
  alertIntervalRef = startAlertEvaluator(redisRef);

  // ── 5. Start processing ─────────────────────────────────
  // NOTE: processLogs() blocks forever (infinite XREADGROUP loop).
  // The alert evaluator runs alongside it via setInterval.
  console.log('');
  await processLogs(redisRef);
}

// ── Graceful shutdown ───────────────────────────────────────
let redisRef;
let alertIntervalRef;

async function shutdown(signal) {
  console.log(`\n🛑 Received ${signal} — shutting down worker…`);
  // Clear alert evaluator interval first
  if (alertIntervalRef) {
    clearInterval(alertIntervalRef);
    console.log('🔕 Alert evaluator stopped');
  }
  try {
    if (redisRef) await redisRef.quit();
  } catch { /* ignore */ }
  try {
    await disconnectMongo();
  } catch { /* ignore */ }
  process.exit(0);
}

['SIGINT', 'SIGTERM'].forEach((sig) => {
  process.on(sig, () => shutdown(sig));
});

// Start the worker
main().catch((err) => {
  console.error('💥 Worker failed to start:', err);
  process.exit(1);
});

