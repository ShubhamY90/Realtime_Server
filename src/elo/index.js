/**
 * Standard ELO rating calculation.
 *
 * Expected score formula:  E = 1 / (1 + 10^((opponentRating - playerRating) / 400))
 * New rating formula:      R' = R + K * (actual - expected)
 *
 * @param {number} winnerRating  - current ELO of the winner
 * @param {number} loserRating   - current ELO of the loser
 * @param {number} kFactor       - sensitivity constant (default 32)
 * @returns {{
 *   newWinnerRating: number,
 *   newLoserRating:  number,
 *   winnerDelta:     number,
 *   loserDelta:      number,
 * }}
 */
function calculateElo(winnerRating, loserRating, kFactor = 32) {
  // Expected scores (probability of winning)
  const expectedWinner = 1 / (1 + Math.pow(10, (loserRating - winnerRating) / 400));
  const expectedLoser  = 1 / (1 + Math.pow(10, (winnerRating - loserRating) / 400));

  // Actual scores: winner = 1, loser = 0
  const winnerDelta = Math.round(kFactor * (1 - expectedWinner));
  const loserDelta  = Math.round(kFactor * (0 - expectedLoser));   // always negative

  const newWinnerRating = winnerRating + winnerDelta;
  const newLoserRating  = Math.max(0, loserRating + loserDelta);   // floor at 0

  return { newWinnerRating, newLoserRating, winnerDelta, loserDelta };
}

module.exports = { calculateElo };
