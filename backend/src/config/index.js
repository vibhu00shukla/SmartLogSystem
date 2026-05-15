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
};

module.exports = config;
