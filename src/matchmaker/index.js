const redisClient = require('../../redis/client');

const QUEUE_KEY      = 'matchmaking_queue';
const RATING_WINDOW  = 200;   // max rating gap for a valid match
const TICK_INTERVAL  = 2000;  // ms between queue scans

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

// ── Stub — replace in next step with real game-room creation ──────────────────
function onMatchFound(player1, player2) {
  console.log(
    `[matchmaker] 🎮 Match found! ` +
    `${player1.userId}(${player1.rating}) vs ${player2.userId}(${player2.rating})`
  );
}

// ── Core tick ─────────────────────────────────────────────────────────────────
/**
 * One matchmaking tick:
 * 1. Read the full queue ordered by rating ascending.
 * 2. Walk adjacent pairs — first pair within RATING_WINDOW wins.
 * 3. Attempt atomic removal via Lua; retry next tick if the read was stale.
 */
async function matchmakingTick() {
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
          1,             // number of KEYS
          QUEUE_KEY,     // KEYS[1]
          p1.userId,     // ARGV[1]
          String(p1.rating), // ARGV[2]
          p2.userId,     // ARGV[3]
          String(p2.rating)  // ARGV[4]
        );
      } catch (err) {
        console.error('[matchmaker] Lua eval error:', err.message);
        return;
      }

      if (removed === 1) {
        onMatchFound(p1, p2);
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
 * Returns the interval handle so callers can clearInterval() on shutdown.
 *
 * @returns {NodeJS.Timeout}
 */
function startMatchmaker() {
  console.log(
    `[matchmaker] started — tick every ${TICK_INTERVAL}ms, window ±${RATING_WINDOW}`
  );
  const handle = setInterval(matchmakingTick, TICK_INTERVAL);
  // Prevent the interval from keeping Node alive if everything else exits
  if (handle.unref) handle.unref();
  return handle;
}

module.exports = { startMatchmaker, onMatchFound };
