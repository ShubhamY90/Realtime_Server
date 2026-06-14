require('dotenv').config();
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const redisClient = require('../redis/client');
const { registerSocketHandlers } = require('./socket');
const { startMatchmaker }        = require('./matchmaker');
const { startResultWorker } = require('./queue/resultWorker');
const { eloWorker }         = require('./queue/eloWorker');

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3001;

const configuredOrigins = process.env.CORS_ORIGINS
  ? process.env.CORS_ORIGINS.split(",")
  : [];
const corsOrigins = [
  "http://localhost:5173",
  "http://localhost:3000",
  "https://code-duel-f.vercel.app",
  "https://cod3duel.netlify.app",
  ...configuredOrigins
];

const corsOptions = {
  origin: corsOrigins,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: true,
};

const io = new Server(server, {
  cors: corsOptions,
});

// ── Health check ──────────────────────────────────────────────────────────────
app.get('/health', async (_req, res) => {
  const health = {
    status: 'UP',
    timestamp: new Date().toISOString(),
    uptime: Math.floor(process.uptime()),
    services: {},
  };

  // Redis ping
  try {
    const pong = await redisClient.ping();
    health.services.redis = pong === 'PONG' ? 'CONNECTED' : 'DEGRADED';
  } catch (err) {
    health.services.redis = 'ERROR';
    health.status = 'DEGRADED';
  }

  const statusCode = health.status === 'UP' ? 200 : 503;
  res.status(statusCode).json(health);
});

// ── Socket.IO ─────────────────────────────────────────────────────────────────
registerSocketHandlers(io);

// ── Internal HTTP endpoints ────────────────────────────────────────────────────
app.use(require('express').json());

const INTERNAL_SECRET = process.env.INTERNAL_SECRET || '';

function internalAuth(req, res, next) {
  if (req.headers['x-internal-secret'] !== INTERNAL_SECRET) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  next();
}

/**
 * POST /internal/match-result
 *
 * Called by the backend (Code_Duel_B /api/matches/complete) to enqueue
 * a BullMQ ELO-update job after a match completes.
 *
 * Body: { roomId, matchId, isDraw, matchType, winnerId, loserId, winnerRating, loserRating }
 */
const { addMatchResultJob } = require('./queue/producer');

app.post('/internal/match-result', internalAuth, async (req, res) => {
  const {
    roomId, matchId, isDraw = false, matchType = 'public',
    winnerId, loserId, winnerRating, loserRating,
  } = req.body;

  if (!roomId || !winnerId || !loserId || winnerRating == null || loserRating == null) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    await addMatchResultJob({ roomId, matchId, isDraw, matchType, winnerId, loserId, winnerRating, loserRating });
    console.log(`[internal/match-result] Enqueued ELO job for roomId=${roomId} isDraw=${isDraw}`);
    return res.status(200).json({ success: true });
  } catch (err) {
    console.error('[internal/match-result] Error:', err.message);
    return res.status(500).json({ error: err.message });
  }
});


// ── Boot ──────────────────────────────────────────────────────────────────────
async function start() {
  try {
    await redisClient.connect();
  } catch (err) {
    console.error('Failed to connect to Redis:', err.message);
    process.exit(1);
  }

  const matchmakerHandle = startMatchmaker(io);
  const resultWorker     = startResultWorker(io);
  // eloWorker auto-starts on require; grab reference for clean shutdown
  const _eloWorker       = eloWorker;

  server.listen(PORT, () => {
    console.log(`Realtime server listening on port ${PORT}`);
  });

  // ── Graceful shutdown ────────────────────────────────────────────────────
  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;   // prevent double-invoke from --watch
    shuttingDown = true;

    console.log(`\n[server] ${signal} received — shutting down gracefully`);

    // Hard kill-switch: if clean shutdown takes > 5s, force exit.
    // This prevents hanging forever when BullMQ blocking connections
    // or lingering Socket.IO connections stall the event loop.
    const forceExit = setTimeout(() => {
      console.error('[server] force exit after 5s timeout');
      process.exit(1);
    }, 5000);
    forceExit.unref(); // don't let this timeout keep the process alive on its own

    // 1. Stop matchmaker loop
    clearInterval(matchmakerHandle);

    // 2. Disconnect all Socket.IO clients — this unblocks server.close()
    await io.close();

    // 3. Close BullMQ workers (releases blocking Redis connections)
    await Promise.allSettled([
      resultWorker.close(),
      _eloWorker.close(),
    ]);

    // 4. Stop accepting HTTP connections and wait for in-flight requests
    server.close(async () => {
      // 5. Disconnect shared ioredis client
      await redisClient.quit().catch(() => {});
      console.log('[server] clean exit');
      clearTimeout(forceExit);
      process.exit(0);
    });
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

start();
