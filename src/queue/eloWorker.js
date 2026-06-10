const { Worker, Queue } = require('bullmq');
const { calculateElo }  = require('../elo');
const { updateRatings } = require('../http/backend');

const connection = {
  host: process.env.REDIS_HOST || 'localhost',
  port: Number(process.env.REDIS_PORT) || 6379,
};

// ── ELO worker ────────────────────────────────────────────────────────────────
/**
 * Listens on "match-results" queue.
 *
 * Expected job.data shape:
 * {
 *   roomId:       string,
 *   matchId?:     string,
 *   winnerId:     string,
 *   loserId:      string,
 *   winnerRating: number,
 *   loserRating:  number,
 *   isDraw:       boolean,       // true → both get 0 delta, matchesTied++ 
 *   matchType:    'public' | 'private',
 * }
 */
const eloWorker = new Worker(
  'match-results',
  async (job) => {
    const {
      roomId,
      matchId,
      winnerId,
      loserId,
      winnerRating,
      loserRating,
      isDraw    = false,
      matchType = 'public',
    } = job.data;

    console.log(
      `\n[eloWorker] 🏆 Processing match-result — roomId=${roomId} ` +
      `isDraw=${isDraw} matchType=${matchType} ` +
      `winner=${winnerId}(${winnerRating}) loser=${loserId}(${loserRating})`
    );

    if (isDraw) {
      // Tie: no rating change — both players stay at their current rating
      console.log(`[eloWorker] 🤝 Draw detected — no rating change`);

      await updateRatings({
        roomId,
        matchId,
        isDraw:          true,
        matchType,
        winnerId,
        loserId,
        newWinnerRating: winnerRating,
        newLoserRating:  loserRating,
        winnerDelta:     0,
        loserDelta:      0,
      });

      console.log(`[eloWorker] ✅ Tie recorded for roomId=${roomId}`);
      return;
    }

    // Win/Loss: Calculate new ratings
    const { newWinnerRating, newLoserRating, winnerDelta, loserDelta } =
      calculateElo(winnerRating, loserRating);

    console.log(
      `[eloWorker] 📊 ELO delta — ` +
      `${winnerId}: ${winnerRating} → ${newWinnerRating} (${winnerDelta > 0 ? '+' : ''}${winnerDelta}) | ` +
      `${loserId}: ${loserRating} → ${newLoserRating} (${loserDelta})`
    );

    // Persist to backend
    await updateRatings({
      roomId,
      matchId,
      isDraw:   false,
      matchType,
      winnerId,
      loserId,
      newWinnerRating,
      newLoserRating,
      winnerDelta,
      loserDelta,
    });

    console.log(`[eloWorker] ✅ Ratings updated for roomId=${roomId}`);
  },
  {
    connection,
    concurrency: 5,
  }
);

eloWorker.on('failed', (job, err) => {
  console.error(`[eloWorker] ❌ Job ${job?.id} failed: ${err.message}`);
});

eloWorker.on('error', (err) => {
  console.error('[eloWorker] Worker error:', err.message);
});

console.log('[eloWorker] 🚀 Listening on "match-results" queue');

module.exports = { eloWorker };
