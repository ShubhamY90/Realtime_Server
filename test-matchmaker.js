/**
 * Integration test for the matchmaker loop.
 * Seed Redis with players, start the matchmaker, wait for it to fire.
 *
 * Run:  node test-matchmaker.js
 * (server does NOT need to be running separately)
 */
require('dotenv').config();
const redisClient = require('./redis/client');
const { joinQueue, getQueueState } = require('./src/matchmaker/queue');
const { startMatchmaker } = require('./src/matchmaker/index');

const PLAYERS = [
  { userId: 'alice',   rating: 1200 },
  { userId: 'bob',     rating: 1350 },  // within 200 of alice ✓
  { userId: 'charlie', rating: 1800 },  // too far from everyone
];

async function run() {
  await redisClient.connect();

  // Clean slate
  await redisClient.del('matchmaking_queue');

  // Seed queue
  console.log('\n── Seeding queue ─────────────────────────────────────────');
  for (const p of PLAYERS) await joinQueue(p.userId, p.rating);
  console.table(await getQueueState());

  // Start matchmaker — it ticks every 2 s
  console.log('\n── Starting matchmaker (waiting up to 4 s) ───────────────');
  const handle = startMatchmaker();

  await new Promise((r) => setTimeout(r, 4500));
  clearInterval(handle);

  console.log('\n── Queue after matchmaker ran ────────────────────────────');
  console.table(await getQueueState());  // charlie should be the only one left

  await redisClient.quit();
  console.log('\n✅ Matchmaker integration test complete.\n');
  process.exit(0);
}

run().catch((err) => { console.error(err); process.exit(1); });
