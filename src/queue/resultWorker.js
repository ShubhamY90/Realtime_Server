const { Worker } = require('bullmq');
const redisClient = require('../../redis/client');

const ONLINE_KEY = 'online_users';

// Separate connection options for the worker (BullMQ manages its own pool)
const connection = (() => {
  const url = process.env.REDIS_URL || '';
  if (url.startsWith('rediss://') || url.startsWith('redis://')) {
    const parsed = new URL(url);
    return {
      host:     parsed.hostname,
      port:     Number(parsed.port) || (url.startsWith('rediss://') ? 6380 : 6379),
      password: parsed.password || undefined,
      username: parsed.username || undefined,
      tls:      url.startsWith('rediss://') ? {} : undefined,
    };
  }
  return { host: process.env.REDIS_HOST || 'localhost', port: Number(process.env.REDIS_PORT) || 6379 };
})();

/**
 * Start the BullMQ worker that listens on the "results" queue.
 * When a result job arrives it looks up the player's current socket ID
 * from Redis "online_users" and emits the result directly to their socket.
 *
 * Expected job data shape:
 * {
 *   jobId:         string,
 *   userId:        string,
 *   roomId:        string,
 *   status:        'accepted' | 'wrong_answer' | 'runtime_error' | 'tle' | …,
 *   testsPassed:   number,
 *   totalTests:    number,
 *   executionTime: number,   // ms
 * }
 *
 * @param {import('socket.io').Server} io
 * @returns {import('bullmq').Worker}
 */
function startResultWorker(io) {
  const worker = new Worker(
    'results',
    async (job) => {
      const {
        jobId,
        userId,
        roomId,
        status,
        testsPassed,
        totalTests,
        executionTime,
      } = job.data;

      console.log(
        `[resultWorker] 📦 Result received — jobId=${jobId} ` +
        `userId=${userId} status=${status}`
      );

      // Look up the player's current socket ID
      const socketId = await redisClient.hget(ONLINE_KEY, userId);

      if (!socketId) {
        // Player disconnected before result arrived — log and discard
        console.warn(
          `[resultWorker] ⚠️  userId=${userId} is offline, ` +
          `dropping result for jobId=${jobId}`
        );
        return; // job completes cleanly (no retry needed — player is gone)
      }

      io.to(socketId).emit('submission-result', {
        jobId,
        userId,
        roomId,
        status,
        testsPassed,
        totalTests,
        executionTime,
      });

      console.log(
        `[resultWorker] ✅ Emitted submission-result to ` +
        `userId=${userId} (${socketId}) — jobId=${jobId}`
      );
    },
    {
      connection,
      concurrency: 10,  // process up to 10 result jobs in parallel
    }
  );

  worker.on('failed', (job, err) => {
    console.error(
      `[resultWorker] ❌ Job ${job?.id} failed: ${err.message}`
    );
  });

  worker.on('error', (err) => {
    console.error('[resultWorker] Worker error:', err.message);
  });

  console.log('[resultWorker] 🚀 Listening on "results" queue');
  return worker;
}

module.exports = { startResultWorker };
