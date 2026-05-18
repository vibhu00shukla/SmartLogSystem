'use strict';

/**
 * endpointRegistry.js — Endpoint Registry Management
 *
 * Maintains a registry of services and endpoints that have reported
 * latency telemetry. This allows the alertEvaluator to efficiently
 * iterate over active endpoints and compute latency percentiles.
 *
 * We use ZSETs (Sorted Sets) with the timestamp as the score.
 * This enables automatic cleanup of "stale" endpoints (e.g., if a
 * route is deprecated or dynamically generated) to prevent memory
 * exhaustion over time.
 *
 * Redis structure:
 *   ep:services             — ZSET of services [score=timestamp, member=serviceName]
 *   ep:endpoints:{service}  — ZSET of endpoints for a service [score=timestamp, member=endpoint]
 */

const config = require('../config');

// ─────────────────────────────────────────────────────────────
// Key Helpers
// ─────────────────────────────────────────────────────────────

function getServicesKey() {
  return config.latency.registry.servicesKey;
}

function getEndpointsKey(service) {
  return `${config.latency.registry.endpointsPrefix}:${service}`;
}

// ─────────────────────────────────────────────────────────────
// Hot Path Registration (used by latencyAggregator)
// ─────────────────────────────────────────────────────────────

/**
 * Register an endpoint for a service.
 *
 * Called during log ingestion (recordLatency) when the endpoint field
 * is present. Adds/updates the ZSET scores to the current timestamp.
 * Note: Actual key cleanup is handled by the evaluator.
 *
 * @param {import('ioredis').Pipeline} pipeline - An active ioredis pipeline
 * @param {string} service - The service name
 * @param {string} endpoint - The endpoint path
 * @param {number} timestampMs - The timestamp of the log in ms
 */
function registerEndpoint(pipeline, service, endpoint, timestampMs) {
  pipeline.zadd(getServicesKey(), timestampMs, service);
  pipeline.zadd(getEndpointsKey(service), timestampMs, endpoint);
}

// ─────────────────────────────────────────────────────────────
// Retrieval & Cleanup (used by alertEvaluator)
// ─────────────────────────────────────────────────────────────

/**
 * Get the list of all services currently in the registry.
 *
 * @param {import('ioredis').Redis} redis
 * @returns {Promise<string[]>}
 */
async function getEndpointServices(redis) {
  return await redis.zrange(getServicesKey(), 0, -1);
}

/**
 * Get the list of all active endpoints for a given service.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service
 * @returns {Promise<string[]>}
 */
async function getEndpointsForService(redis, service) {
  return await redis.zrange(getEndpointsKey(service), 0, -1);
}

/**
 * Remove stale entries from the registries.
 *
 * Removes any service or endpoint that hasn't seen traffic
 * older than the TTL (e.g., 24 hours). This prevents endless
 * iteration over deprecated routes.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} service - The service to clean endpoints for
 * @returns {Promise<void>}
 */
async function cleanupRegistries(redis, service) {
  const cutoffMs = Date.now() - (config.latency.registry.ttlSeconds * 1000);
  
  const pipeline = redis.pipeline();
  pipeline.zremrangebyscore(getServicesKey(), '-inf', cutoffMs);
  pipeline.zremrangebyscore(getEndpointsKey(service), '-inf', cutoffMs);
  
  await pipeline.exec();
}

module.exports = {
  registerEndpoint,
  getEndpointServices,
  getEndpointsForService,
  cleanupRegistries,
};
