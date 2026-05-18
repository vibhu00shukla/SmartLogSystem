'use strict';

const { z } = require('zod');

/**
 * Zod schema for incoming log payloads.
 *
 * Required fields:
 *   service  — name of the originating micro-service
 *   message  — human-readable log message
 *   level    — severity (info | warn | error | debug)
 *   traceId  — UUID used for distributed-trace correlation
 *
 * Optional APM fields (Phase 5A):
 *   responseTime    — end-to-end response time in milliseconds
 *   dbQueryTime     — database query time in milliseconds
 *   externalApiTime — external API call time in milliseconds
 *   endpoint        — API endpoint path (e.g. "/api/v1/login")
 *   statusCode      — HTTP status code (100-599)
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

  // ── Optional APM / latency fields (Phase 5A) ──────────
  // Each latency field is independently optional — a service can send
  // responseTime without dbQueryTime, or any combination.

  responseTime: z
    .number({ invalid_type_error: 'responseTime must be a number' })
    .nonnegative('responseTime must be non-negative')
    .optional(),

  dbQueryTime: z
    .number({ invalid_type_error: 'dbQueryTime must be a number' })
    .nonnegative('dbQueryTime must be non-negative')
    .optional(),

  externalApiTime: z
    .number({ invalid_type_error: 'externalApiTime must be a number' })
    .nonnegative('externalApiTime must be non-negative')
    .optional(),

  endpoint: z
    .string()
    .max(256, 'endpoint must be at most 256 characters')
    .optional(),

  statusCode: z
    .number({ invalid_type_error: 'statusCode must be a number' })
    .int('statusCode must be an integer')
    .min(100, 'statusCode must be >= 100')
    .max(599, 'statusCode must be <= 599')
    .optional(),
});

module.exports = { logSchema };
