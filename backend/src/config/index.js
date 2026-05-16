const dotenv = require('dotenv');
dotenv.config();

const config = {
  // ── Server ──────────────────────────────────────────────
  port: parseInt(process.env.PORT, 10) || 3001,
  host: process.env.HOST || '0.0.0.0',

  // ── Redis ───────────────────────────────────────────────
  redis: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
  },

  // ── MongoDB (used later by worker service) ──────────────
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/log-intelligence',

  // ── Redis Stream name ───────────────────────────────────
  streamName: process.env.STREAM_NAME || 'logs-stream',

  // ── Worker / Consumer Group ─────────────────────────────
  consumerGroup: process.env.CONSUMER_GROUP || 'log-processing-group',
  consumerName: process.env.CONSUMER_NAME || 'worker-1',
  batchSize: parseInt(process.env.BATCH_SIZE, 10) || 10,
  blockTimeoutMs: parseInt(process.env.BLOCK_TIMEOUT_MS, 10) || 5000,

  // ── Time Bucket Analytics ─────────────────────────────
  // Per-minute Redis hash buckets for real-time service metrics.
  timeBucket: {
    keyPrefix: process.env.TB_KEY_PREFIX || 'tb',           // Redis key prefix
    ttlSeconds: parseInt(process.env.TB_TTL_SECONDS, 10) || 180, // 3 min safety TTL
    flushIntervalMs: parseInt(process.env.TB_FLUSH_INTERVAL_MS, 10) || 60_000, // flusher cycle
  },

  // ── Alert Detection (Phase 2A) ────────────────────────
  // Periodic error-rate evaluation against time bucket data.
  alerting: {
    evalIntervalMs: parseInt(process.env.ALERT_EVAL_INTERVAL_MS, 10) || 30_000,  // evaluate every 30s
    cooldownSeconds: parseInt(process.env.ALERT_COOLDOWN_SECONDS, 10) || 300,    // 5 min cooldown per service
    cooldownKeyPrefix: process.env.ALERT_CD_PREFIX || 'alert:cd',                // Redis cooldown key prefix
    minRequestThreshold: parseInt(process.env.ALERT_MIN_REQUESTS, 10) || 100,    // min total before evaluating

    // Error rate (%) thresholds — evaluated highest-first
    thresholds: {
      WARNING:  parseFloat(process.env.ALERT_WARN_PCT)  || 2,
      CRITICAL: parseFloat(process.env.ALERT_CRIT_PCT)  || 5,
      SEVERE:   parseFloat(process.env.ALERT_SEVERE_PCT) || 10,
    },
  },
};

module.exports = config;
