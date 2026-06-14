const { Queue } = require('bullmq');

// Supports both plain redis:// (local) and Upstash rediss:// (TLS, cloud).
// BullMQ manages its own internal Redis connections; we pass options rather
// than a shared ioredis instance so it can control blocking reads itself.
const redisUrl = process.env.REDIS_URL || '';
let connection;
if (redisUrl.startsWith('rediss://') || redisUrl.startsWith('redis://')) {
  const parsed = new URL(redisUrl);
  connection = {
    host:     parsed.hostname,
    port:     Number(parsed.port) || (redisUrl.startsWith('rediss://') ? 6380 : 6379),
    password: parsed.password || undefined,
    username: parsed.username || undefined,
    tls:      redisUrl.startsWith('rediss://') ? {} : undefined,
  };
} else {
  // Legacy fallback: plain host + port env vars
  connection = {
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT) || 6379,
  };
}

// ── Submissions queue ─────────────────────────────────────────────────────────
const submissionsQueue = new Queue('submissions', {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
    removeOnComplete: { count: 500 },
    removeOnFail:     { count: 200 },
  },
});

submissionsQueue.on('error', (err) => {
  console.error('[producer] submissions queue error:', err.message);
});

/**
 * Add a code submission job to the "submissions" BullMQ queue.
 *
 * @param {{
 *   jobId:     string,
 *   userId:    string,
 *   roomId:    string,
 *   language:  string,
 *   code:      string,
 *   problemId: string,
 * }} payload
 * @returns {Promise<import('bullmq').Job>}
 */
async function addSubmissionJob(payload) {
  const { jobId, userId, roomId, language, code, problemId } = payload;

  if (!jobId || !userId || !roomId || !language || !code || !problemId) {
    throw new TypeError(
      'addSubmissionJob: payload is missing required fields ' +
      '(jobId, userId, roomId, language, code, problemId)'
    );
  }

  const job = await submissionsQueue.add(
    'submit',           // job name (used for filtering/monitoring)
    payload,
    { jobId }           // use caller-supplied jobId as BullMQ job ID
  );

  console.log(
    `[producer] ✅ Queued submission — jobId=${jobId} userId=${userId} ` +
    `roomId=${roomId} language=${language}`
  );

  return job;
}

// ── Match-results queue ───────────────────────────────────────────────────────
const matchResultsQueue = new Queue('match-results', {
  connection,
  defaultJobOptions: {
    attempts: 5,                                    // ELO update must succeed
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: { count: 200 },
    removeOnFail:     { count: 100 },
  },
});

matchResultsQueue.on('error', (err) => {
  console.error('[producer] match-results queue error:', err.message);
});

/**
 * Push a match-result job so the ELO worker can update both players' ratings.
 *
 * @param {{
 *   roomId:       string,
 *   matchId?:     string,
 *   winnerId:     string,
 *   loserId:      string,
 *   winnerRating: number,
 *   loserRating:  number,
 *   isDraw:       boolean,
 *   matchType:    'public' | 'private',
 * }} payload
 * @returns {Promise<import('bullmq').Job>}
 */
async function addMatchResultJob(payload) {
  const { roomId, winnerId, loserId, winnerRating, loserRating } = payload;

  if (!roomId || !winnerId || !loserId ||
      winnerRating == null || loserRating == null) {
    throw new TypeError(
      'addMatchResultJob: payload is missing required fields ' +
      '(roomId, winnerId, loserId, winnerRating, loserRating)'
    );
  }

  const job = await matchResultsQueue.add('elo-update', payload, {
    jobId: `elo-${roomId}`,   // idempotent — one ELO update per room
  });

  console.log(
    `[producer] ✅ Queued match-result — roomId=${roomId} isDraw=${payload.isDraw ?? false} ` +
    `winner=${winnerId}(${winnerRating}) loser=${loserId}(${loserRating})`
  );

  return job;
}

module.exports = { addSubmissionJob, submissionsQueue, addMatchResultJob, matchResultsQueue };

