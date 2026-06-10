/**
 * Quick manual test for Socket.IO registration flow.
 * Run WHILE the server is already running (npm run dev).
 *
 *   node test-client.js
 */

const { io } = require('socket.io-client');
const Redis = require('ioredis');
require('dotenv').config();

const SERVER = 'http://localhost:3001';
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

const USERS = ['alice', 'bob'];
const sockets = [];

function connect(userId) {
  return new Promise((resolve) => {
    const socket = io(SERVER, { transports: ['websocket'] });

    socket.on('connect', () => {
      console.log(`\n[${userId}] connected  →  socketId: ${socket.id}`);
      socket.emit('register', userId);
    });

    socket.on('registered', async (data) => {
      console.log(`[${userId}] ✅ registered:`, data);
      resolve(socket);
    });

    socket.on('error', (err) => {
      console.error(`[${userId}] ❌ error:`, err);
    });
  });
}

async function checkRedis(label) {
  const all = await redis.hgetall('online_users');
  console.log(`\n── Redis online_users (${label}) ──`);
  if (!all || Object.keys(all).length === 0) {
    console.log('  (empty)');
  } else {
    Object.entries(all).forEach(([uid, sid]) =>
      console.log(`  ${uid}  →  ${sid}`)
    );
  }
}

async function run() {
  // 1. Connect both users
  for (const userId of USERS) {
    const socket = await connect(userId);
    sockets.push(socket);
  }

  // 2. Verify both appear in Redis
  await checkRedis('both connected');

  // 3. Disconnect alice
  console.log('\n── Disconnecting alice… ──');
  sockets[0].disconnect();

  // Wait for server cleanup
  await new Promise((r) => setTimeout(r, 500));

  // 4. Verify only bob remains
  await checkRedis('after alice disconnects');

  // 5. Teardown
  sockets[1].disconnect();
  await new Promise((r) => setTimeout(r, 300));
  await redis.quit();

  console.log('\n✅ Test complete.\n');
  process.exit(0);
}

run().catch((err) => {
  console.error('Test failed:', err);
  process.exit(1);
});
