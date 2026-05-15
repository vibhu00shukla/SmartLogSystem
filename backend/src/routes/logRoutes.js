'use strict';

const { logSchema } = require('../schemas/logSchema');
const { pushLog } = require('../services/redisProducer');

/**
 * Encapsulates the /api/v1/logs route.
 *
 * @param {import('fastify').FastifyInstance} fastify
 */
async function logRoutes(fastify) {
  // ── Health-check ────────────────────────────────────────
  fastify.get('/api/v1/health', async () => ({ status: 'ok' }));

  // ── Ingest a log entry ──────────────────────────────────
  fastify.post('/api/v1/logs', async (request, reply) => {
    // 1. Validate with Zod
    const result = logSchema.safeParse(request.body);

    if (!result.success) {
      return reply.status(400).send({
        status: 'error',
        message: 'Validation failed',
        issues: result.error.issues.map((i) => ({
          field: i.path.join('.'),
          message: i.message,
        })),
      });
    }

    // 2. Push to Redis Stream
    try {
      const entryId = await pushLog(fastify.redis, result.data);

      fastify.log.info(
        { entryId, service: result.data.service, level: result.data.level },
        'Log queued'
      );

      return reply.status(200).send({
        status: 'queued',
        id: entryId,
      });
    } catch (err) {
      fastify.log.error({ err }, 'Failed to push log to Redis');
      return reply.status(500).send({
        status: 'error',
        message: 'Internal server error — could not queue log',
      });
    }
  });
}

module.exports = logRoutes;
