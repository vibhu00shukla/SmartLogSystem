'use strict';

/**
 * Normalise a raw Redis Stream entry into a clean log document
 * ready for MongoDB insertion.
 *
 * Redis Stream entries come as flat key-value arrays:
 *   ['service', 'auth', 'message', 'hello', ...]
 *
 * After ioredis parsing they arrive as arrays of [id, [field, value, ...]].
 *
 * @param {string} entryId   — Redis Stream entry ID (e.g. "1713900000000-0")
 * @param {string[]} fields  — flat array of alternating key/value pairs
 * @returns {object}         — normalised log document
 */
function normaliseLog(entryId, fields) {
  // Convert flat array → object: ['a','1','b','2'] → { a:'1', b:'2' }
  const raw = {};
  for (let i = 0; i < fields.length; i += 2) {
    raw[fields[i]] = fields[i + 1];
  }

  // Ensure timestamp is valid ISO-8601; fall back to current time
  let timestamp = raw.timestamp;
  if (!timestamp || isNaN(Date.parse(timestamp))) {
    timestamp = new Date().toISOString();
  }

  return {
    service: raw.service || 'unknown',
    message: raw.message || '',
    level: raw.level || 'info',
    traceId: raw.traceId || null,
    timestamp,
    processedAt: new Date().toISOString(),
    _streamId: entryId, // keep a reference back to the Redis entry
  };
}

module.exports = { normaliseLog };
