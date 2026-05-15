'use strict';

const fastify = require('fastify');
const config = require('./config');

// ── Build the Fastify instance ────────────────────────────
const app = fastify({
  logger: {
    level: 'info',
    transport: {
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:standard',
        ignore: 'pid,hostname',
      },
    },
  },
});

// ── Register plugins ──────────────────────────────────────
app.register(require('./plugins/redis'));

// ── Register routes ───────────────────────────────────────
app.register(require('./routes/logRoutes'));

// ── Graceful shutdown ─────────────────────────────────────
const signals = ['SIGINT', 'SIGTERM'];
signals.forEach((signal) => {
  process.on(signal, async () => {
    app.log.info(`Received ${signal} — shutting down…`);
    await app.close();
    process.exit(0);
  });
});

// ── Start the server ──────────────────────────────────────
async function start() {
  try {
    await app.listen({ port: config.port, host: config.host });
    app.log.info(`🚀 Server listening on http://${config.host}:${config.port}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

start();
