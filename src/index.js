require('dotenv').config();
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const redisClient = require('../redis/client');
const { registerSocketHandlers } = require('./socket');
const { startMatchmaker }        = require('./matchmaker');

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3001;

const io = new Server(server, {
  cors: {
    origin: process.env.BACKEND_URL || 'http://localhost:3000',
    methods: ['GET', 'POST'],
  },
});

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ── Socket.IO ─────────────────────────────────────────────────────────────────
registerSocketHandlers(io);

// ── Boot ──────────────────────────────────────────────────────────────────────
async function start() {
  try {
    await redisClient.connect();
  } catch (err) {
    console.error('Failed to connect to Redis:', err.message);
    process.exit(1);
  }

  const matchmakerHandle = startMatchmaker();

  server.listen(PORT, () => {
    console.log(`Realtime server listening on port ${PORT}`);
  });

  // ── Graceful shutdown ────────────────────────────────────────────────────
  const shutdown = async (signal) => {
    console.log(`\n[server] ${signal} received — shutting down gracefully`);
    clearInterval(matchmakerHandle);
    server.close(async () => {
      await redisClient.quit();
      console.log('[server] clean exit');
      process.exit(0);
    });
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

start();
