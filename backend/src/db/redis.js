'use strict';

const Redis = require('ioredis');
const config = require('../config');

/**
 * Creates and returns a shared ioredis client for the worker process.
 * (The Fastify server uses its own client via the redis plugin;
 *  the worker needs its own standalone client.)
 */
function createRedisClient() {
  const client = new Redis({
    host: config.redis.host,
    port: config.redis.port,
    maxRetriesPerRequest: null,
    retryStrategy(times) {
      const delay = Math.min(times * 200, 5000);
      console.log(`⚠️  Redis reconnecting — attempt ${times}, next in ${delay}ms`);
      return delay;
    },
  });

  client.on('connect', () => console.log('✅ Redis connected'));
  client.on('error', (err) => console.error('❌ Redis error:', err.message));

  return client;
}

module.exports = { createRedisClient };
