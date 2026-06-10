const redisClient = require('../../redis/client');
const { joinQueue, leaveQueue } = require('../matchmaker/queue');

const ONLINE_USERS_KEY = 'online_users';

// In-memory reverse map: socketId → userId
// Lets disconnect clean up without scanning Redis
const socketToUser = new Map();

/**
 * Register all socket event handlers on the given io instance.
 * @param {import('socket.io').Server} io
 */
function registerSocketHandlers(io) {
  io.on('connection', (socket) => {
    console.log(`[socket] client connected: ${socket.id}`);

    // ── register ────────────────────────────────────────────────────────────
    socket.on('register', async (userId) => {
      if (!userId || typeof userId !== 'string') {
        socket.emit('error', { message: 'register requires a valid userId string' });
        return;
      }

      try {
        // Store userId → socketId in Redis hash
        await redisClient.hset(ONLINE_USERS_KEY, userId, socket.id);

        // Keep reverse map for fast disconnect lookup
        socketToUser.set(socket.id, userId);

        // Join a room named after the userId for targeted emissions later
        socket.join(userId);

        console.log(`[socket] registered: userId=${userId} socketId=${socket.id}`);
        socket.emit('registered', { userId, socketId: socket.id });
      } catch (err) {
        console.error(`[socket] register error for ${userId}:`, err.message);
        socket.emit('error', { message: 'Registration failed, please retry' });
      }
    });

    // ── join-queue ───────────────────────────────────────────────────────────
    // Payload: { userId: string, rating: number }
    socket.on('join-queue', async ({ userId, rating } = {}) => {
      if (!userId || rating == null) {
        socket.emit('queue-error', { message: 'join-queue requires userId and rating' });
        return;
      }
      try {
        await joinQueue(userId, Number(rating));
        socket.emit('queue-joined', { userId, rating });
        console.log(`[socket] join-queue: userId=${userId} rating=${rating}`);
      } catch (err) {
        console.error(`[socket] join-queue error for ${userId}:`, err.message);
        socket.emit('queue-error', { message: err.message });
      }
    });

    // ── leave-queue ──────────────────────────────────────────────────────────
    // Payload: { userId: string }
    socket.on('leave-queue', async ({ userId } = {}) => {
      if (!userId) return;
      try {
        await leaveQueue(userId);
        socket.emit('queue-left', { userId });
        console.log(`[socket] leave-queue: userId=${userId}`);
      } catch (err) {
        console.error(`[socket] leave-queue error for ${userId}:`, err.message);
      }
    });

    // ── disconnect ──────────────────────────────────────────────────────────
    socket.on('disconnect', async (reason) => {
      const userId = socketToUser.get(socket.id);

      if (userId) {
        try {
          // Auto-remove from queue on disconnect so they don't stay as phantom players
          await leaveQueue(userId).catch(() => {});

          // Only remove the entry if this socket is still the current one
          // (guards against a race where the user reconnected quickly)
          const currentSocketId = await redisClient.hget(ONLINE_USERS_KEY, userId);
          if (currentSocketId === socket.id) {
            await redisClient.hdel(ONLINE_USERS_KEY, userId);
            console.log(`[socket] removed from online_users: userId=${userId}`);
          }
        } catch (err) {
          console.error(`[socket] disconnect cleanup error for ${userId}:`, err.message);
        }

        socketToUser.delete(socket.id);
      }

      console.log(`[socket] client disconnected: ${socket.id} (userId=${userId ?? 'unregistered'}) — ${reason}`);
    });
  });
}

module.exports = { registerSocketHandlers };

