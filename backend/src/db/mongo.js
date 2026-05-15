'use strict';

const { MongoClient } = require('mongodb');
const config = require('../config');

/** @type {MongoClient} */
let client;

/** @type {import('mongodb').Db} */
let db;

/** @type {import('mongodb').Collection} */
let logsCollection;

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

  // Create an index on traceId for fast lookups (idempotent)
  await logsCollection.createIndex({ traceId: 1 });
  // Create an index on timestamp for time-range queries
  await logsCollection.createIndex({ timestamp: -1 });

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
 * Gracefully close the MongoDB connection.
 */
async function disconnect() {
  if (client) {
    await client.close();
    client = null;
    db = null;
    logsCollection = null;
    console.log('MongoDB connection closed');
  }
}

module.exports = { connect, insertLog, disconnect };
