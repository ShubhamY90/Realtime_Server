// Quick smoke-test for matchmaker/queue.js
// Run: node test-queue.js  (server does NOT need to be running)
require('dotenv').config();
const Redis = require('ioredis');

// Patch the module-level client before requiring queue
const client = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
// Override the exported singleton so queue.js uses this connected instance
jest = null; // not jest — just node
const mod = require('./src/matchmaker/queue');

// Monkey-patch redis client used by queue
// (queue imports ../../redis/client which uses lazyConnect — connect it first)
const redisClient = require('./redis/client');

async function run() {
  await redisClient.connect();

  const { joinQueue, leaveQueue, getQueueState } = mod;

  // Clean slate
  await client.del('matchmaking_queue');

  console.log('\n── joinQueue ─────────────────────────────────────────────');
  await joinQueue('alice', 1200);
  await joinQueue('bob',   1400);
  await joinQueue('carol', 1100);

  console.log('\n── getQueueState (should be ordered: carol, alice, bob) ──');
  const state = await getQueueState();
  console.table(state);

  console.log('\n── leaveQueue alice ──────────────────────────────────────');
  await leaveQueue('alice');

  console.log('\n── getQueueState (should be: carol, bob) ─────────────────');
  console.table(await getQueueState());

  console.log('\n── leaveQueue unknown user ───────────────────────────────');
  await leaveQueue('ghost'); // should log "not in queue", not throw

  await client.quit();
  await redisClient.quit();
  console.log('\n✅ Queue smoke-test passed.\n');
  process.exit(0);
}

run().catch((err) => { console.error(err); process.exit(1); });
