/**
 * Test replic-sqlite with Eve's mind.db percepts table
 */
const Database = require('better-sqlite3');
const SQLiteOnSteroid = require('./lib/index.js');
const path = require('path');
const os = require('os');

// Use MIND_DB env var or default to identity-based path
const MIND_DB = process.env.MIND_DB || path.join(os.homedir(), '.ai/identities/eve/memory/mind.db');
const PEER_ID = parseInt(process.env.PEER_ID || '1', 10);

console.log('Connecting to:', MIND_DB);
console.log('Peer ID:', PEER_ID);

// Open database
const db = new Database(MIND_DB);

// Initialize replic-sqlite
const app = SQLiteOnSteroid(db, PEER_ID);

// CRITICAL: Call migrate with at least one migration to set version
console.log('Initializing replic-sqlite (calling migrate)...');
const migrateResult = app.migrate([
  { up: '-- replic-sqlite init', down: '' }
]);
console.log('Migration result:', migrateResult);

// List all patches tables
const patchTables = db.prepare(`
  SELECT name FROM sqlite_master 
  WHERE type='table' AND name LIKE '%_patches' AND name != 'pending_patches'
`).all();
console.log('Patch tables found:', patchTables.map(t => t.name.replace('_patches', '')));

// Test: Upsert a test percept
const testPercept = {
  id: 'test-percept-' + Date.now(),
  time_start: new Date().toISOString(),
  time_end: null,
  source_device: 'test',
  modalities: '["test"]',
  auto_caption: 'Test percept from replic-sqlite',
  scene_entities: '[]',
  emotional_color: 'neutral',
  created_at: new Date().toISOString(),
  deletedAt: null
};

console.log('\nUpserting test percept:', testPercept.id);

try {
  app.upsert('percepts', testPercept, (err, sessionToken) => {
    if (err) {
      console.error('Upsert error:', err);
      db.close();
      process.exit(1);
    }
    console.log('Upsert successful! Session token:', sessionToken);
    
    // Verify it's in the patches table
    const patches = db.prepare('SELECT COUNT(*) as cnt FROM percepts_patches WHERE id = ?').get(testPercept.id);
    console.log('Patches recorded:', patches.cnt);
    
    // Verify it's in the main table
    const main = db.prepare('SELECT id, auto_caption FROM percepts WHERE id = ?').get(testPercept.id);
    console.log('Main table record:', main);
    
    // Clean up test data
    db.prepare('DELETE FROM percepts WHERE id = ?').run(testPercept.id);
    db.prepare('DELETE FROM percepts_patches WHERE id = ?').run(testPercept.id);
    console.log('\nTest data cleaned up');
    
    db.close();
    console.log('\n✓ replic-sqlite working with percepts!');
  });
} catch (err) {
  console.error('Error:', err);
  db.close();
  process.exit(1);
}
