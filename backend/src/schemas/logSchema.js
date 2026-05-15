'use strict';

const { z } = require('zod');

/**
 * Zod schema for incoming log payloads.
 *
 * service  — name of the originating micro-service
 * message  — human-readable log message
 * level    — severity (info | warn | error | debug)
 * traceId  — UUID used for distributed-trace correlation
 */
const logSchema = z.object({
  service: z
    .string({ required_error: 'service is required' })
    .min(1, 'service must not be empty')
    .max(128, 'service must be at most 128 characters'),

  message: z
    .string({ required_error: 'message is required' })
    .min(1, 'message must not be empty')
    .max(2048, 'message must be at most 2048 characters'),

  level: z.enum(['info', 'warn', 'error', 'debug'], {
    errorMap: () => ({ message: 'level must be one of: info, warn, error, debug' }),
  }),

  traceId: z
    .string({ required_error: 'traceId is required' })
    .uuid('traceId must be a valid UUID'),
});

module.exports = { logSchema };
