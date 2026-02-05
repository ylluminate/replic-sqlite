module replic

import time

fn test_hlc_from_creates_valid_value() {
	// Create HLC from known timestamp
	ts := i64(1735689600000) + 1000 // 1 second after epoch
	h := hlc_from(ts, 0)

	// Should be able to extract back
	assert h.to_unix_timestamp() == ts
	assert h.to_counter() == 0
}

fn test_hlc_from_with_counter() {
	ts := i64(1735689600000) + 5000
	h := hlc_from(ts, 42)

	assert h.to_unix_timestamp() == ts
	assert h.to_counter() == 42
}

fn test_hlc_timestamp_extraction() {
	h := hlc_from(i64(1735689600000) + 12345, 99)

	// Timestamp should be 12345
	assert h.to_timestamp() == 12345
	// Counter should be 99
	assert h.to_counter() == 99
}

fn test_hlc_create_returns_incrementing_values() {
	mut state := new_hlc_state()

	v1 := state.create()
	v2 := state.create()
	v3 := state.create()

	// Values should be non-decreasing
	assert v2 >= v1
	assert v3 >= v2
}

fn test_hlc_receive_updates_highest() {
	mut state := new_hlc_state()

	// Create a remote HLC that's "in the future"
	future := hlc_from(time.now().unix_milli() + 10000, 0)
	state.receive(future.value)

	// Now create should return something >= that remote value
	created := state.create()
	assert created >= future.value
}

fn test_hlc_now() {
	h := hlc_now()

	// Should be close to current time (within 1 second)
	now := time.now().unix_milli()
	assert h.to_unix_timestamp() >= now - 1000
	assert h.to_unix_timestamp() <= now + 1000
}

fn test_clock_drift_initially_zero() {
	state := new_hlc_state()
	assert state.get_clock_drift_ms() == 0
}

fn test_reset_clears_state() {
	mut state := new_hlc_state()

	// Create some state
	future := hlc_from(time.now().unix_milli() + 5000, 0)
	state.receive(future.value)
	state.create()

	// Reset
	state.reset()

	// Should be back to initial state
	assert state.highest_remote_hlc == 0
	assert state.counter == 0
	assert state.clock_drift_hlc == 0
}
