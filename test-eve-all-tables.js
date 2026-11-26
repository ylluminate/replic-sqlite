/**
 * Test replic-sqlite discovers all Eve's _patches tables
 */
const Database = require('better-sqlite3');
const SQLiteOnSteroid = require('./lib/index.js');
const path = require('path');

const MIND_DB = path.join(process.env.HOME, '.ai/identities/eve/memory/mind.db');
const PEER_ID = 1;

const db = new Database(MIND_DB);
const app = SQLiteOnSteroid(db, PEER_ID);

// Initialize
app.migrate([
  { up: '-- replic-sqlite init', down: '' }
]);

// List all patches tables
const patchTables = db.prepare(`
  SELECT name FROM sqlite_master 
  WHERE type='table' AND name LIKE '%_patches' AND name != 'pending_patches'
  ORDER BY name
`).all();

console.log('Patch tables discovered by replic-sqlite:');
patchTables.forEach(t => console.log('  -', t.name.replace('_patches', '')));

const ts = Date.now();
// Test upsert for each table (with all required fields)
const tests = [
  { table: 'percepts', data: { id: 'test-' + ts, time_start: new Date().toISOString() }},
  { table: 'learnings', data: { id: 'test-' + ts, insight: 'Test learning', category: 'note', current_version: 1 }},
  { table: 'handoffs', data: { id: 'test-' + ts, task_summary: 'Test handoff' }},
  { table: 'presence', data: { session_id: 'test-' + ts, channel: 'test', user: 'test', started_at: new Date().toISOString(), last_heartbeat: new Date().toISOString() }},
];

console.log('\nTesting upserts:');
let passed = 0;
let failed = 0;

function runTest(test, callback) {
  app.upsert(test.table, test.data, (err) => {
    if (err) {
      console.log(`  ✗ ${test.table}: ${err.message}`);
      failed++;
    } else {
      console.log(`  ✓ ${test.table}`);
      passed++;
      // Cleanup - use the correct primary key
      const pk = test.table === 'presence' ? 'session_id' : 'id';
      const pkVal = test.data[pk];
      db.prepare(`DELETE FROM ${test.table} WHERE ${pk} = ?`).run(pkVal);
      db.prepare(`DELETE FROM ${test.table}_patches WHERE ${pk} = ?`).run(pkVal);
    }
    callback();
  });
}

// Run tests sequentially
let i = 0;
function next() {
  if (i < tests.length) {
    runTest(tests[i++], next);
  } else {
    console.log(`\nResults: ${passed} passed, ${failed} failed`);
    db.close();
    process.exit(failed > 0 ? 1 : 0);
  }
}
next();
