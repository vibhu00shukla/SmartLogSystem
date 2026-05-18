'use strict';

const config = require('../config');
const { normaliseLog } = require('./logNormaliser');
const { insertLog } = require('../db/mongo');
const { updateBucket } = require('../services/bucketAggregator');
const { recordEvent } = require('../services/slidingWindowAggregator');
const { recordLatency } = require('../services/latencyAggregator');
const { recordTraceEvent } = require('../services/traceAggregator');

/**
 * Ensure the consumer group exists on the stream.
 *
 * Uses XGROUP CREATE with MKSTREAM so the stream is created automatically
 * if it doesn't exist yet.  If the group already exists Redis throws
 * "BUSYGROUP", which we catch and ignore — this makes restarts safe.
 *
 * @param {import('ioredis').Redis} redis
 */
async function ensureConsumerGroup(redis) {
  try {
    // '0' = start reading from the very beginning of the stream
    await redis.xgroup('CREATE', config.streamName, config.consumerGroup, '0', 'MKSTREAM');
    console.log(`✅ Consumer group "${config.consumerGroup}" created on "${config.streamName}"`);
  } catch (err) {
    if (err.message.includes('BUSYGROUP')) {
      console.log(`ℹ️  Consumer group "${config.consumerGroup}" already exists — continuing`);
    } else {
      throw err; // unexpected error — let it bubble up
    }
  }
}

/**
 * Main processing loop.
 *
 * 1. XREADGROUP blocks for new messages.
 * 2. For each message: normalise → insert into MongoDB → XACK.
 * 3. If MongoDB insert fails the message is NOT acknowledged,
 *    so Redis will re-deliver it on the next read (pending entry list).
 *
 * @param {import('ioredis').Redis} redis
 */
async function processLogs(redis) {
  console.log('🔄 Worker listening for new log messages…\n');

  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      // XREADGROUP GROUP <group> <consumer> COUNT <n> BLOCK <ms> STREAMS <stream> >
      //   '>' means "only messages never delivered to this consumer"
      const results = await redis.xreadgroup(
        'GROUP',
        config.consumerGroup,
        config.consumerName,
        'COUNT',
        config.batchSize,
        'BLOCK',
        config.blockTimeoutMs,
        'STREAMS',
        config.streamName,
        '>'
      );

      // results is null when BLOCK timeout expires with no new data
      if (!results) continue;

      // results = [ [ streamName, [ [id, fields], [id, fields], … ] ] ]
      const entries = results[0][1];

      for (const [entryId, fields] of entries) {
        try {
          // ── Normalise ──────────────────────────────────────
          const logDoc = normaliseLog(entryId, fields);

          console.log(`📥 Received  [${entryId}]  service=${logDoc.service}  level=${logDoc.level}`);

          // ── Insert into MongoDB ────────────────────────────
          await insertLog(logDoc);
          console.log(`💾 Inserted  [${entryId}]  into MongoDB`);

          // ── Update time bucket (best-effort) ───────────────
          // Increments per-minute Redis hash counters for this service.
          // Wrapped in try/catch so a Redis hiccup doesn't block the
          // main pipeline — bucket analytics are non-critical.
          try {
            await updateBucket(redis, logDoc);
          } catch (bucketErr) {
            console.error(`⚠️  Bucket update failed [${entryId}]:`, bucketErr.message);
          }

          // ── Update sliding window (best-effort) ────────────
          // Records this event into per-service Redis ZSETs for
          // rolling 60-second analytics. Independent of bucket writes.
          try {
            await recordEvent(redis, logDoc);
          } catch (swErr) {
            console.error(`⚠️  Sliding window update failed [${entryId}]:`, swErr.message);
          }

          // ── Record latency samples (best-effort) ────────────
          // Stores latency values into per-service Redis ZSETs for
          // percentile computation. Only writes when APM fields are
          // present — skips entirely for logs without latency data.
          try {
            await recordLatency(redis, logDoc);
          } catch (latErr) {
            console.error(`⚠️  Latency recording failed [${entryId}]:`, latErr.message);
          }

          // ── Record trace event (Phase 6A) ───────────────────
          try {
            await recordTraceEvent(redis, logDoc);
          } catch (traceErr) {
            console.error(`⚠️  Trace event recording failed [${entryId}]:`, traceErr.message);
          }

          // ── ACK only after successful insert ───────────────
          await redis.xack(config.streamName, config.consumerGroup, entryId);
          console.log(`✅ ACKed     [${entryId}]\n`);
        } catch (msgErr) {
          // Per-message failure: log and move on.
          // The message stays in the Pending Entries List (PEL) and will
          // be re-delivered when the worker restarts or claims it.
          console.error(`❌ Failed to process [${entryId}]:`, msgErr.message);
        }
      }
    } catch (loopErr) {
      // Stream-level error (e.g. Redis disconnect).
      // Wait a bit and retry — ioredis will auto-reconnect.
      console.error('❌ Stream read error:', loopErr.message);
      await sleep(2000);
    }
  }
}

/** Simple async sleep helper. */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = { ensureConsumerGroup, processLogs };
