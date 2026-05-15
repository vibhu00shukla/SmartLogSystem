'use strict';

const config = require('../config');

/**
 * Push a validated log entry into the Redis Stream.
 *
 * Uses XADD with auto-generated ID (`*`).
 * Each field of the log object becomes a field in the stream entry.
 *
 * @param {import('ioredis').Redis} redisClient
 * @param {object} logData — validated log payload
 * @returns {Promise<string>} — the stream entry ID assigned by Redis
 */
async function pushLog(redisClient, logData) {
  const fields = [
    'service',   logData.service,
    'message',   logData.message,
    'level',     logData.level,
    'traceId',   logData.traceId,
    'timestamp', new Date().toISOString(),
  ];

  const entryId = await redisClient.xadd(config.streamName, '*', ...fields);
  return entryId;
}

module.exports = { pushLog };
