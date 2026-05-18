'use strict';

const config = require('../config');

/**
 * Push a validated log entry into the Redis Stream.
 *
 * Uses XADD with auto-generated ID (`*`).
 * Each field of the log object becomes a field in the stream entry.
 *
 * APM fields (Phase 5A) are added conditionally — only when present
 * in the validated payload. This keeps stream entries compact for
 * simple logs without latency data.
 *
 * @param {import('ioredis').Redis} redisClient
 * @param {object} logData — validated log payload
 * @returns {Promise<string>} — the stream entry ID assigned by Redis
 */
async function pushLog(redisClient, logData) {
  // ── Required fields (always present) ────────────────────
  const fields = [
    'service',   logData.service,
    'message',   logData.message,
    'level',     logData.level,
    'traceId',   logData.traceId,
    'timestamp', new Date().toISOString(),
  ];

  // ── Optional APM fields (Phase 5A) ──────────────────────
  // Only add to the stream if the field is present in the payload.
  // Redis Stream fields are always strings, so we stringify numbers.
  if (logData.responseTime !== undefined) {
    fields.push('responseTime', logData.responseTime.toString());
  }
  if (logData.dbQueryTime !== undefined) {
    fields.push('dbQueryTime', logData.dbQueryTime.toString());
  }
  if (logData.externalApiTime !== undefined) {
    fields.push('externalApiTime', logData.externalApiTime.toString());
  }
  if (logData.endpoint !== undefined) {
    fields.push('endpoint', logData.endpoint);
  }
  if (logData.statusCode !== undefined) {
    fields.push('statusCode', logData.statusCode.toString());
  }

  const entryId = await redisClient.xadd(config.streamName, '*', ...fields);
  return entryId;
}

module.exports = { pushLog };
