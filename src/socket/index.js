const redisClient = require('../../redis/client');

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

    // ── disconnect ──────────────────────────────────────────────────────────
    socket.on('disconnect', async (reason) => {
      const userId = socketToUser.get(socket.id);

      if (userId) {
        try {
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
