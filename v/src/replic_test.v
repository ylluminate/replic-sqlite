module replic

import db.sqlite

fn setup_test_db() !sqlite.DB {
	// Create in-memory database for testing
	mut db := sqlite.connect(':memory:')!
	return db
}

fn test_generate_peer_id_is_positive() {
	id := generate_peer_id()
	assert id > 0
}

fn test_generate_peer_id_is_unique() {
	id1 := generate_peer_id()
	id2 := generate_peer_id()
	// Very unlikely to be equal (uses timestamp + random)
	assert id1 != id2
}

fn test_new_replicator() {
	mut db := setup_test_db() or {
		assert false, 'failed to create test db'
		return
	}
	defer { db.close() or {} }

	config := Config{
		debug: true
	}

	r := new(db, 12345, config) or {
		assert false, 'failed to create replicator'
		return
	}

	assert r.peer_id == 12345
	assert r.config.debug == true
}

fn test_new_replicator_generates_peer_id() {
	mut db := setup_test_db() or { return }
	defer { db.close() or {} }

	r := new(db, 0, Config{}) or { return }

	// Should have generated a peer ID
	assert r.peer_id > 0
}

fn test_migration_creates_table() {
	mut db := setup_test_db() or { return }
	defer { db.close() or {} }

	mut r := new(db, 12345, Config{}) or { return }

	migrations := [
		Migration{
			up: '
				CREATE TABLE users (
					id INTEGER PRIMARY KEY,
					name TEXT NOT NULL
				);
				CREATE TABLE users_patches (
					_patchedAt INTEGER NOT NULL,
					_sequenceId INTEGER NOT NULL,
					_peerId INTEGER NOT NULL,
					id INTEGER,
					name TEXT,
					PRIMARY KEY (_patchedAt, _sequenceId, _peerId)
				);
			'
			down: 'DROP TABLE users_patches; DROP TABLE users;'
		},
	]

	result := r.migrate(migrations) or {
		assert false, 'migration failed: ${err}'
		return
	}

	assert result.previous_version == 0
	assert result.current_version == 1
	assert r.db_version == 1
}

fn test_status_returns_state() {
	mut db := setup_test_db() or { return }
	defer { db.close() or {} }

	r := new(db, 99999, Config{}) or { return }

	status := r.status()
	assert status.peer_id == 99999
	assert status.last_sequence_id == -1 // Not yet initialized
}

fn test_add_remote_peer() {
	mut db := setup_test_db() or { return }
	defer { db.close() or {} }

	mut r := new(db, 1, Config{}) or { return }

	r.add_remote_peer(2)
	r.add_remote_peer(3)
	r.add_remote_peer(1) // Should be ignored (self)

	status := r.status()
	assert 2 in status.peer_stats
	assert 3 in status.peer_stats
	assert 1 !in status.peer_stats // Self not added
}

fn test_is_consistent_unknown_peer() {
	mut db := setup_test_db() or { return }
	defer { db.close() or {} }

	r := new(db, 1, Config{}) or { return }

	// Unknown peer should return true (optimistic)
	assert r.is_consistent(999, 100) == true
}

fn test_escape_sql() {
	assert escape_sql("hello") == "hello"
	assert escape_sql("it's") == "it''s"
	assert escape_sql("O'Brien") == "O''Brien"
}
