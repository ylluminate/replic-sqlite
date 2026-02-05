module replic

// SQLite Replication Engine
//
// Conflict-free multi-writer replication for SQLite using CRDTs.
// Each row is replicated conflict-free using patch tables with:
// - _patchedAt: HLC timestamp
// - _sequenceId: Monotonic sequence per peer
// - _peerId: Unique peer identifier

import db.sqlite
import rand
import time
import json

// Message types for replication protocol
pub const msg_patch = 10
pub const msg_ping = 20
pub const msg_missing_patch = 30

// Peer stat indices
const last_patch_at_timestamp = 0
const last_sequence_id = 1
const guaranteed_contiguous_patch_at = 2
const guaranteed_contiguous_seq_id = 3
const last_message_timestamp = 4

// Default configuration values
const default_heartbeat_interval_ms = 5000
const default_max_patch_retention_ms = i64(25 * 60 * 60 * 1000) // 25 hours
const default_max_patch_per_retransmission = 2000

// Config holds replication configuration
pub struct Config {
pub mut:
	heartbeat_interval_ms      int = default_heartbeat_interval_ms
	max_patch_retention_ms     i64 = default_max_patch_retention_ms
	max_patch_per_retransmission int = default_max_patch_per_retransmission
	debug                      bool
}

// PeerStats tracks synchronization state for a peer
pub struct PeerStats {
pub mut:
	last_patch_at           i64 // Most recent patch timestamp
	last_seq_id             i64 // Most recent sequence ID
	guaranteed_contiguous_at i64 // Guaranteed contiguous timestamp
	guaranteed_contiguous_seq i64 // Guaranteed contiguous sequence ID
	last_message_time       i64 // Last message received time (epoch ms)
}

// Patch represents a replicated change
pub struct Patch {
pub:
	msg_type  int    @[json: 'type']
	at        i64    // HLC timestamp
	peer      i64    // Source peer ID
	seq       i64    // Sequence ID
	ver       int    // Database version
	tab       string // Table name
	delta     string // JSON-encoded row data
}

// MissingPatchRequest requests retransmission of patches
pub struct MissingPatchRequest {
pub:
	msg_type int @[json: 'type']
	peer     i64 // Peer to request from
	min_seq  i64 // Minimum sequence ID
	max_seq  i64 // Maximum sequence ID
	for_peer i64 // Peer requesting the patches
}

// Replicator manages SQLite replication
pub struct Replicator {
pub:
	peer_id  i64
	config   Config
mut:
	db       sqlite.DB
	hlc      HLCState
	db_version int = 1
	last_sequence_id i64 = -1
	last_patch_at    i64 = -1
	peer_stats       map[i64]PeerStats
	// Timing for maintenance
	last_delete_old_patches i64
	last_detect_missing     i64
	last_ping_stat          i64
}

// new creates a new Replicator
pub fn new(db sqlite.DB, peer_id i64, config Config) !Replicator {
	mut r := Replicator{
		peer_id: if peer_id > 0 { peer_id } else { generate_peer_id() }
		config: config
		db: db
		hlc: new_hlc_state()
	}
	return r
}

// generate_peer_id creates a unique peer identifier
// Format: (timestamp << 13) + random(0, 8191)
pub fn generate_peer_id() i64 {
	ts := time.now().unix_milli() - unix_timestamp_offset
	ts_shifted := ts * i64(8192) // 2^13
	random_part := i64(rand.u32() % 8092)
	return ts_shifted + random_part
}

// migrate applies migrations and prepares statements
pub fn (mut r Replicator) migrate(migrations []Migration) !MigrationResult {
	// Create migrations table if needed
	r.db.exec('
		CREATE TABLE IF NOT EXISTS migrations (
			id INTEGER PRIMARY KEY,
			up TEXT NOT NULL,
			down TEXT NOT NULL,
			applied_at TEXT DEFAULT CURRENT_TIMESTAMP
		)
	') or { return error('failed to create migrations table: ${err}') }

	// Get current version
	existing := r.db.exec('SELECT MAX(id) as v FROM migrations') or {
		return error('failed to get migrations: ${err}')
	}
	last_applied := if existing.len > 0 && existing[0].vals[0] != '' {
		existing[0].vals[0].int()
	} else {
		0
	}

	target := migrations.len
	r.db_version = if target > 0 { target } else { 1 }

	// Apply up migrations
	if target > last_applied {
		for i in last_applied .. target {
			m := migrations[i]
			r.db.exec(m.up) or { return error('migration ${i + 1} failed: ${err}') }
			r.db.exec("INSERT INTO migrations (id, up, down) VALUES (${i + 1}, '${escape_sql(m.up)}', '${escape_sql(m.down)}')") or {
				return error('failed to record migration: ${err}')
			}
		}
	} else if target < last_applied {
		// Apply down migrations (rollback)
		rows := r.db.exec('SELECT id, down FROM migrations WHERE id > ${target} ORDER BY id DESC') or {
			return error('failed to get migrations for rollback: ${err}')
		}
		for row in rows {
			down_sql := row.vals[1]
			r.db.exec(down_sql) or { return error('rollback failed: ${err}') }
			r.db.exec('DELETE FROM migrations WHERE id = ${row.vals[0]}') or {
				return error('failed to delete migration record: ${err}')
			}
		}
	}

	// Initialize peer sequence from existing data
	r.init_peer_sequence()!

	return MigrationResult{
		previous_version: last_applied
		current_version: target
	}
}

// Migration defines a database migration
pub struct Migration {
pub:
	up   string
	down string
}

// MigrationResult holds migration outcome
pub struct MigrationResult {
pub:
	previous_version int
	current_version  int
}

// init_peer_sequence loads last sequence ID from database
fn (mut r Replicator) init_peer_sequence() ! {
	// Find the highest sequence ID for this peer across all patch tables
	tables := r.db.exec("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_patches'") or {
		return
	}

	mut max_seq := i64(0)
	mut max_at := i64(0)

	for table in tables {
		name := table.vals[0]
		result := r.db.exec('SELECT MAX(_sequenceId), MAX(_patchedAt) FROM ${name} WHERE _peerId = ${r.peer_id}') or {
			continue
		}
		if result.len > 0 && result[0].vals[0] != '' {
			seq := result[0].vals[0].i64()
			at := result[0].vals[1].i64()
			if seq > max_seq {
				max_seq = seq
				max_at = at
			}
		}
	}

	r.last_sequence_id = max_seq
	r.last_patch_at = max_at
}

// upsert inserts or updates a row with replication tracking
pub fn (mut r Replicator) upsert(table string, row_data map[string]string) !string {
	if r.db_version == 0 {
		return error('database version not set - call migrate first')
	}
	if r.last_sequence_id == -1 {
		return error('system not initialized - call migrate first')
	}

	patch_at := r.hlc.create()
	seq := r.last_sequence_id + 1

	// Build INSERT statement for patch table
	mut cols := []string{}
	mut vals := []string{}
	cols << '_patchedAt'
	cols << '_sequenceId'
	cols << '_peerId'
	vals << '${patch_at}'
	vals << '${seq}'
	vals << '${r.peer_id}'

	for k, v in row_data {
		cols << k
		vals << "'${escape_sql(v)}'"
	}

	patch_table := '${table}_patches'
	sql_stmt := 'INSERT INTO ${patch_table} (${cols.join(", ")}) VALUES (${vals.join(", ")})'

	r.db.exec(sql_stmt) or { return error('failed to save patch: ${err}') }

	r.last_sequence_id = seq
	r.last_patch_at = patch_at

	// Apply patches to main table
	r.apply_patches(table, patch_at)!

	// Return session token for read-your-write consistency
	return '${r.peer_id}.${seq}'
}

// apply_patches merges patches into the main table using CRDT rules
fn (mut r Replicator) apply_patches(table string, from_timestamp i64) ! {
	// Get column info for the table
	cols_result := r.db.exec("PRAGMA table_info('${table}')") or { return }

	mut cols := []string{}
	mut pk_cols := []string{}
	mut update_clauses := []string{}

	for row in cols_result {
		col_name := row.vals[1]
		is_pk := row.vals[5].int() > 0
		cols << col_name
		if is_pk {
			pk_cols << col_name
		} else {
			// For non-PK columns, use keep_last CRDT
			update_clauses << '${col_name} = coalesce(excluded.${col_name}, ${col_name})'
		}
	}

	if pk_cols.len == 0 {
		return error('table ${table} has no primary key')
	}

	// Note: This simplified version doesn't use the keep_last() aggregate
	// For production, load the C extension for proper CRDT behavior
	patch_table := '${table}_patches'

	sql_stmt := '
		INSERT INTO ${table} (${cols.join(", ")})
		SELECT ${cols.join(", ")}
		FROM ${patch_table}
		WHERE _patchedAt >= ${from_timestamp}
		ON CONFLICT (${pk_cols.join(", ")}) DO UPDATE SET
			${update_clauses.join(",\n")}
	'

	r.db.exec(sql_stmt) or { return error('failed to apply patches: ${err}') }
}

// receive_patch processes a patch from a remote peer
pub fn (mut r Replicator) receive_patch(patch Patch) ! {
	if patch.peer == r.peer_id {
		return // Ignore our own patches
	}

	// Update HLC
	r.hlc.receive(patch.at)

	// Store in patch table
	patch_table := '${patch.tab}_patches'

	// Parse delta JSON
	delta := json.decode(map[string]string, patch.delta) or {
		return error('invalid patch delta: ${err}')
	}

	mut cols := []string{}
	mut vals := []string{}
	cols << '_patchedAt'
	cols << '_sequenceId'
	cols << '_peerId'
	vals << '${patch.at}'
	vals << '${patch.seq}'
	vals << '${patch.peer}'

	for k, v in delta {
		cols << k
		vals << "'${escape_sql(v)}'"
	}

	sql_stmt := 'INSERT INTO ${patch_table} (${cols.join(", ")}) VALUES (${vals.join(", ")})'
	r.db.exec(sql_stmt) or { return error('failed to save received patch: ${err}') }

	// Track peer stats
	r.update_peer_stats(patch)

	// Apply to main table
	r.apply_patches(patch.tab, patch.at)!
}

// update_peer_stats updates tracking for a peer
fn (mut r Replicator) update_peer_stats(patch Patch) {
	mut stats := r.peer_stats[patch.peer] or { PeerStats{} }

	stats.last_message_time = time.now().unix_milli()

	gap := patch.seq - stats.guaranteed_contiguous_seq

	if gap == 1 {
		// Contiguous sequence
		stats.guaranteed_contiguous_seq = patch.seq
		stats.guaranteed_contiguous_at = patch.at
	}
	// If gap > 1, there are missing patches - handled by heartbeat

	if patch.seq > stats.last_seq_id {
		stats.last_seq_id = patch.seq
		stats.last_patch_at = patch.at
	}

	r.peer_stats[patch.peer] = stats
}

// add_remote_peer registers a new peer for replication
pub fn (mut r Replicator) add_remote_peer(remote_peer_id i64) {
	if remote_peer_id == r.peer_id {
		return
	}
	if remote_peer_id !in r.peer_stats {
		r.peer_stats[remote_peer_id] = PeerStats{}
	}
}

// status returns current replication status
pub fn (r Replicator) status() ReplicationStatus {
	return ReplicationStatus{
		peer_id: r.peer_id
		last_sequence_id: r.last_sequence_id
		last_patch_at: r.last_patch_at
		peer_stats: r.peer_stats.clone()
	}
}

// ReplicationStatus holds current sync state
pub struct ReplicationStatus {
pub:
	peer_id          i64
	last_sequence_id i64
	last_patch_at    i64
	peer_stats       map[i64]PeerStats
}

// heartbeat performs periodic maintenance
pub fn (mut r Replicator) heartbeat() {
	now := time.now().unix_milli()

	// Delete old patches hourly
	if now - r.last_delete_old_patches >= 3600000 {
		r.delete_old_patches()
		r.last_delete_old_patches = now
	}

	// Detect and request missing patches
	if now - r.last_detect_missing >= r.config.heartbeat_interval_ms {
		r.detect_missing_patches()
		r.last_detect_missing = now
	}
}

// delete_old_patches removes patches older than retention period
fn (mut r Replicator) delete_old_patches() {
	oldest := hlc_from(time.now().unix_milli() - r.config.max_patch_retention_ms, 0)

	tables := r.db.exec("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_patches'") or {
		return
	}

	for table in tables {
		r.db.exec('DELETE FROM ${table.vals[0]} WHERE _patchedAt < ${oldest.value}') or { continue }
	}
}

// detect_missing_patches finds gaps in peer sequences
fn (mut r Replicator) detect_missing_patches() {
	for peer_id, stats in r.peer_stats {
		if stats.guaranteed_contiguous_seq < stats.last_seq_id {
			// There are missing patches from this peer
			if r.config.debug {
				eprintln('Missing patches from peer ${peer_id}: ${stats.guaranteed_contiguous_seq + 1} to ${stats.last_seq_id}')
			}
		}
	}
}

// is_consistent checks if we have all patches up to a given sequence
pub fn (r Replicator) is_consistent(peer_id i64, seq_id i64) bool {
	stats := r.peer_stats[peer_id] or { return true }
	return stats.guaranteed_contiguous_seq >= seq_id
}

// Helper: escape SQL string
fn escape_sql(s string) string {
	return s.replace("'", "''")
}
