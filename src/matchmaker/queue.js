const redisClient = require('../../redis/client');

const QUEUE_KEY = 'matchmaking_queue';

/**
 * Add a player to the matchmaking queue.
 * Uses ZADD with the player's rating as the score so the sorted set
 * is ordered by skill — ready for range-based matching later.
 *
 * @param {string} userId
 * @param {number} rating  - numeric ELO / MMR score
 * @returns {Promise<number>} 1 if newly added, 0 if score was updated
 */
async function joinQueue(userId, rating) {
  if (!userId || typeof userId !== 'string') {
    throw new TypeError('joinQueue: userId must be a non-empty string');
  }
  const score = Number(rating);
  if (!Number.isFinite(score)) {
    throw new TypeError('joinQueue: rating must be a finite number');
  }

  // ZADD returns 1 for new member, 0 for update
  const result = await redisClient.zadd(QUEUE_KEY, score, userId);
  console.log(`[queue] ${result === 1 ? 'joined' : 'updated'}: userId=${userId} rating=${score}`);
  return result;
}

/**
 * Remove a player from the matchmaking queue.
 *
 * @param {string} userId
 * @returns {Promise<number>} 1 if removed, 0 if they weren't in the queue
 */
async function leaveQueue(userId) {
  if (!userId || typeof userId !== 'string') {
    throw new TypeError('leaveQueue: userId must be a non-empty string');
  }

  const result = await redisClient.zrem(QUEUE_KEY, userId);
  console.log(`[queue] ${result === 1 ? 'left' : 'not in queue'}: userId=${userId}`);
  return result;
}

/**
 * Return every player currently in the queue, ordered by rating ascending.
 *
 * @returns {Promise<Array<{ userId: string, rating: number }>>}
 */
async function getQueueState() {
  // ZRANGE with WITHSCORES returns [member, score, member, score, ...]
  const raw = await redisClient.zrange(QUEUE_KEY, 0, -1, 'WITHSCORES');

  const players = [];
  for (let i = 0; i < raw.length; i += 2) {
    players.push({
      userId: raw[i],
      rating: Number(raw[i + 1]),
    });
  }

  return players;
}

module.exports = { joinQueue, leaveQueue, getQueueState };
