'use strict';

const fp = require('fastify-plugin');
const Redis = require('ioredis');
const config = require('../config');

/**
 * Fastify plugin — decorates the app with a shared ioredis client.
 * Access it anywhere via `fastify.redis`.
 */
async function redisPlugin(fastify) {
  const client = new Redis({
    host: config.redis.host,
    port: config.redis.port,
    maxRetriesPerRequest: null,       // let ioredis retry indefinitely
    retryStrategy(times) {
      const delay = Math.min(times * 200, 5000);
      fastify.log.warn(`Redis reconnecting — attempt ${times}, next in ${delay}ms`);
      return delay;
    },
  });

  client.on('connect', () => fastify.log.info('✅ Redis connected'));
  client.on('error', (err) => fastify.log.error({ err }, '❌ Redis error'));

  // Decorate so routes can use `fastify.redis`
  fastify.decorate('redis', client);

  // Graceful shutdown
  fastify.addHook('onClose', async () => {
    fastify.log.info('Closing Redis connection…');
    await client.quit();
  });
}

module.exports = fp(redisPlugin, {
  name: 'redis-plugin',
});
