'use strict';

const { MongoClient } = require('mongodb');
const config = require('../config');

/** @type {MongoClient} */
let client;

/** @type {import('mongodb').Db} */
let db;

/** @type {import('mongodb').Collection} */
let logsCollection;

/** @type {import('mongodb').Collection} */
let alertsCollection;

/** @type {import('mongodb').Collection} */
let traceSummariesCollection;

/**
 * Connect to MongoDB and cache the client, db, and collection references.
 * Safe to call multiple times — returns immediately if already connected.
 */
async function connect() {
  if (db) return db;

  client = new MongoClient(config.mongoUri);
  await client.connect();

  // Database name is embedded in the URI, but we extract it explicitly
  // so the code is self-documenting.
  db = client.db('log-intelligence');
  logsCollection = db.collection('logs');
  alertsCollection = db.collection('alerts');

  // Create an index on traceId for fast lookups (idempotent)
  await logsCollection.createIndex({ traceId: 1 });
  // Create an index on timestamp for time-range queries
  await logsCollection.createIndex({ timestamp: -1 });

  // ── Alert indexes ─────────────────────────
  // Compound index: query alerts by service + endpoint + time range
  await alertsCollection.createIndex({ service: 1, endpoint: 1, timestamp: -1 });
  // Index: filter/sort alerts by severity
  await alertsCollection.createIndex({ severity: 1, timestamp: -1 });

  // ── Trace indexes (Phase 6A) ──────────────────────
  traceSummariesCollection = db.collection('trace_summaries');
  await traceSummariesCollection.createIndex({ traceId: 1 }, { unique: true });
  await traceSummariesCollection.createIndex({ startTime: -1 });

  console.log('✅ MongoDB connected — database: log-intelligence');
  return db;
}

/**
 * Insert a single log document into the `logs` collection.
 *
 * @param {object} logDoc — normalised log document
 * @returns {Promise<import('mongodb').InsertOneResult>}
 */
async function insertLog(logDoc) {
  return logsCollection.insertOne(logDoc);
}

/**
 * Insert a single alert document into the `alerts` collection.
 *
 * @param {object} alertDoc — alert document from alertEvaluator
 * @returns {Promise<import('mongodb').InsertOneResult>}
 */
async function insertAlert(alertDoc) {
  return alertsCollection.insertOne(alertDoc);
}

/**
 * Insert a single trace summary document.
 *
 * @param {object} traceDoc
 */
async function insertTraceSummary(traceDoc) {
  return traceSummariesCollection.insertOne(traceDoc);
}

/**
 * Gracefully close the MongoDB connection.
 */
async function disconnect() {
  if (client) {
    await client.close();
    client = null;
    db = null;
    logsCollection = null;
    alertsCollection = null;
    traceSummariesCollection = null;
    console.log('MongoDB connection closed');
  }
}

module.exports = { connect, insertLog, insertAlert, insertTraceSummary, disconnect };
