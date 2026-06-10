const redisClient    = require('../../redis/client');
const { joinQueue }  = require('./queue');
const { createRoom } = require('../http/backend');

const QUEUE_KEY     = 'matchmaking_queue';
const ONLINE_KEY    = 'online_users';
const RATING_WINDOW = 200;   // max rating gap for a valid match
const TICK_INTERVAL = 2000;  // ms between queue scans

// ── Lua script ────────────────────────────────────────────────────────────────
// Atomically verifies both players are still in the queue (and their scores
// haven't changed since we read them), then removes both in one round-trip.
//
// KEYS[1]  = queue key
// ARGV[1]  = userId of player 1
// ARGV[2]  = expected score of player 1
// ARGV[3]  = userId of player 2
// ARGV[4]  = expected score of player 2
//
// Returns:
//   1  → both removed successfully
//   0  → one or both players were already gone / score mismatch (stale read)
const ATOMIC_MATCH_SCRIPT = `
local score1 = redis.call('ZSCORE', KEYS[1], ARGV[1])
local score2 = redis.call('ZSCORE', KEYS[1], ARGV[3])

if score1 == false or score2 == false then
  return 0
end

if score1 ~= ARGV[2] or score2 ~= ARGV[4] then
  return 0
end

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[3])
return 1
`;

// ── Match handler ─────────────────────────────────────────────────────────────
/**
 * Called when two players are atomically dequeued.
 *
 * Steps:
 *  1. Look up both socket IDs in Redis "online_users".
 *  2. If both are online → emit "match-found" to each with opponentId + roomId.
 *  3. If one is offline → put the other back in the queue and log the reason.
 *
 * @param {import('socket.io').Server} io
 * @param {{ userId: string, rating: number }} player1
 * @param {{ userId: string, rating: number }} player2
 */
async function onMatchFound(io, player1, player2) {
  console.log(
    `[matchmaker] 🎮 Match found! ` +
    `${player1.userId}(${player1.rating}) vs ${player2.userId}(${player2.rating})`
  );

  // 1. Fetch both socket IDs in one round-trip (pipeline)
  const [socketId1, socketId2] = await redisClient.hmget(
    ONLINE_KEY,
    player1.userId,
    player2.userId
  );

  const p1Online = Boolean(socketId1);
  const p2Online = Boolean(socketId2);

  // 2. Both online — create a room then emit match-found to each
  if (p1Online && p2Online) {
    let roomId = null;
    try {
      roomId = await createRoom(player1.userId, player2.userId);
      console.log(`[matchmaker] 🏠 Room created: ${roomId}`);
    } catch (err) {
      // Backend is unavailable or not yet implemented — re-queue both
      // players so they aren't silently dropped from the system
      console.error(`[matchmaker] ❌ createRoom failed: ${err.message}`);
      await Promise.all([
        joinQueue(player1.userId, player1.rating),
        joinQueue(player2.userId, player2.rating),
      ]);
      return;
    }

    io.to(socketId1).emit('match-found', { opponentId: player2.userId, roomId });
    io.to(socketId2).emit('match-found', { opponentId: player1.userId, roomId });

    console.log(
      `[matchmaker] ✅ Notified ${player1.userId} (${socketId1}) ` +
      `and ${player2.userId} (${socketId2}) — roomId: ${roomId}`
    );
    return;
  }

  // 3. One or both players have disconnected — re-queue the survivor
  if (!p1Online && !p2Online) {
    console.warn(
      `[matchmaker] ⚠️  Both ${player1.userId} and ${player2.userId} ` +
      `are offline — dropping match`
    );
    return;
  }

  if (!p1Online) {
    console.warn(
      `[matchmaker] ⚠️  ${player1.userId} is offline — ` +
      `re-queuing ${player2.userId}`
    );
    await joinQueue(player2.userId, player2.rating);
    return;
  }

  // !p2Online
  console.warn(
    `[matchmaker] ⚠️  ${player2.userId} is offline — ` +
    `re-queuing ${player1.userId}`
  );
  await joinQueue(player1.userId, player1.rating);
}

// ── Core tick ─────────────────────────────────────────────────────────────────
/**
 * One matchmaking tick:
 * 1. Read the full queue ordered by rating ascending.
 * 2. Walk adjacent pairs — first pair within RATING_WINDOW wins.
 * 3. Attempt atomic removal via Lua; retry next tick if the read was stale.
 *
 * @param {import('socket.io').Server} io
 */
async function matchmakingTick(io) {
  let raw;
  try {
    // ZRANGE … WITHSCORES → [userId, score, userId, score, …]
    raw = await redisClient.zrange(QUEUE_KEY, 0, -1, 'WITHSCORES');
  } catch (err) {
    console.error('[matchmaker] Redis read error:', err.message);
    return;
  }

  // Build typed array
  const players = [];
  for (let i = 0; i < raw.length; i += 2) {
    players.push({ userId: raw[i], rating: Number(raw[i + 1]) });
  }

  if (players.length < 2) return; // 0 or 1 players — nothing to match, exit silently

  // Find first adjacent pair within the rating window
  for (let i = 0; i < players.length - 1; i++) {
    const p1 = players[i];
    const p2 = players[i + 1];

    if (p2.rating - p1.rating <= RATING_WINDOW) {
      // Attempt atomic removal
      let removed;
      try {
        removed = await redisClient.eval(
          ATOMIC_MATCH_SCRIPT,
          1,                  // number of KEYS
          QUEUE_KEY,          // KEYS[1]
          p1.userId,          // ARGV[1]
          String(p1.rating),  // ARGV[2]
          p2.userId,          // ARGV[3]
          String(p2.rating)   // ARGV[4]
        );
      } catch (err) {
        console.error('[matchmaker] Lua eval error:', err.message);
        return;
      }

      if (removed === 1) {
        await onMatchFound(io, p1, p2);
      } else {
        // Stale read — another instance beat us or a player left; try next tick
        console.warn(
          `[matchmaker] Stale read for ${p1.userId} vs ${p2.userId}, retrying next tick`
        );
      }

      // Only one match per tick to keep latency predictable
      return;
    }
  }
}

// ── Public API ────────────────────────────────────────────────────────────────
/**
 * Start the matchmaking loop.
 *
 * @param {import('socket.io').Server} io  - Socket.IO server instance
 * @returns {NodeJS.Timeout}               - Interval handle for clearInterval()
 */
function startMatchmaker(io) {
  console.log(
    `[matchmaker] started — tick every ${TICK_INTERVAL}ms, window ±${RATING_WINDOW}`
  );
  const handle = setInterval(() => matchmakingTick(io), TICK_INTERVAL);
  // Prevent the interval from keeping Node alive if everything else exits
  if (handle.unref) handle.unref();
  return handle;
}

module.exports = { startMatchmaker, onMatchFound };
