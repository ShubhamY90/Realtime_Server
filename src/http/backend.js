const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:4000';

const CREATE_ROOM_PATH   = '/api/rooms/create-internal';
const UPDATE_RATINGS_PATH = '/api/users/update-ratings-internal';

// ── Shared helper ─────────────────────────────────────────────────────────────
async function internalPost(path, body) {
  const url = `${BACKEND_URL}${path}`;
  let res;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-internal-secret': process.env.INTERNAL_SECRET || '',
      },
      body: JSON.stringify(body),
    });
  } catch (networkErr) {
    throw new Error(`[backend] Network error calling ${url}: ${networkErr.message}`);
  }

  if (!res.ok) {
    let detail = '';
    try {
      const b = await res.json();
      detail = b.message || JSON.stringify(b);
    } catch {
      detail = await res.text().catch(() => '');
    }
    throw new Error(`[backend] POST ${path} failed — HTTP ${res.status}: ${detail}`);
  }

  return res.json();
}

// ── createRoom ────────────────────────────────────────────────────────────────
/**
 * Ask the backend to create a new game room for two matched players.
 *
 * @param {string} player1Id
 * @param {string} player2Id
 * @returns {Promise<string>} roomId returned by the backend
 */
async function createRoom(player1Id, player2Id) {
  const data = await internalPost(CREATE_ROOM_PATH, { player1Id, player2Id });

  if (!data.roomId) {
    throw new Error(
      `[backend] createRoom response missing roomId: ${JSON.stringify(data)}`
    );
  }

  return data.roomId;
}

// ── updateRatings ─────────────────────────────────────────────────────────────
/**
 * Ask the backend to persist new ELO ratings for both players in Firestore.
 *
 * @param {{
 *   roomId:          string,
 *   matchId?:        string,
 *   winnerId:        string,
 *   loserId:         string,
 *   newWinnerRating: number,
 *   newLoserRating:  number,
 *   winnerDelta:     number,
 *   loserDelta:      number,
 *   isDraw:          boolean,
 *   matchType:       'public' | 'private',
 * }} payload
 * @returns {Promise<void>}
 */
async function updateRatings(payload) {
  await internalPost(UPDATE_RATINGS_PATH, payload);
}

module.exports = { createRoom, updateRatings };

