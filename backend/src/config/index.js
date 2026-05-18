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

  // ── Sliding Window Analytics (Phase 3) ────────────────
  // Rolling 60-second windows using Redis Sorted Sets.
  // Coexists with fixed-minute time buckets — both are written.
  slidingWindow: {
    keyPrefix: process.env.SW_KEY_PREFIX || 'sw',                  // ZSET key prefix
    errorSuffix: process.env.SW_ERROR_SUFFIX || ':err',            // suffix for error-only ZSET
    serviceRegistryKey: process.env.SW_REGISTRY_KEY || 'sw:services', // SET of active service names
    windowSeconds: parseInt(process.env.SW_WINDOW_SECONDS, 10) || 60, // rolling window size in seconds
  },

  // ── EWMA Adaptive Baselines (Phase 4) ──────────────────
  // Exponentially Weighted Moving Average for per-service baseline
  // error rates. Augments fixed-threshold alerting with adaptive
  // anomaly detection that learns "normal" behavior.
  ewma: {
    alpha: parseFloat(process.env.EWMA_ALPHA) || 0.5,                              // smoothing factor (0-1)
    keyPrefix: process.env.EWMA_KEY_PREFIX || 'ewma',                              // Redis hash key prefix
    minSamples: parseInt(process.env.EWMA_MIN_SAMPLES, 10) || 5,                   // cold-start: min cycles before adaptive alerts
    deviationThreshold: parseFloat(process.env.EWMA_DEVIATION_THRESHOLD) || 3,     // fire anomaly when current >= Nx baseline
    minAbsoluteRate: parseFloat(process.env.EWMA_MIN_ABSOLUTE_RATE) || 1,          // minimum error rate (%) to fire anomaly
    baselineFloor: parseFloat(process.env.EWMA_BASELINE_FLOOR) || 0.5,            // minimum baseline to prevent div-by-zero
    adaptiveCooldownPrefix: process.env.EWMA_CD_PREFIX || 'alert:cd:adp',         // adaptive cooldown key prefix
    adaptiveCooldownSeconds: parseInt(process.env.EWMA_CD_SECONDS, 10) || 300,    // adaptive cooldown TTL
  },

  // ── Latency Analytics (Phase 5A) ──────────────────────
  // Per-service latency sliding windows using Redis Sorted Sets.
  // Stores latency samples for percentile computation (P50/P95/P99).
  latency: {
    keyPrefixes: {
      responseTime:    process.env.LAT_RESP_PREFIX    || 'lat:resp',   // response time ZSET prefix
      dbQueryTime:     process.env.LAT_DB_PREFIX      || 'lat:db',     // DB query time ZSET prefix
      externalApiTime: process.env.LAT_EXT_PREFIX     || 'lat:ext',    // external API time ZSET prefix
    },
    windowSeconds: parseInt(process.env.LAT_WINDOW_SECONDS, 10) || 60,  // rolling window for latency samples
  },
};

module.exports = config;
