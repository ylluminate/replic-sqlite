module replic

// Hybrid Logical Clocks (HLC)
//
// Generate and extract information from the HLC Number stored in patchedAt column
// This number is composed of 53 bits maximum (less than MAX_SAFE_INTEGER)
// 13 bits for the counter   : valid for 2^13 - 1 = 8191 patches at the same timestamp in milliseconds
// 40 bits for the timestamp : unix timestamp in ms starting from 2025-01-01 (1735689600000)
//
// HLC makes the system robust to clock drift across distributed nodes.

import time

// UNIX timestamp offset: 2025-01-01 00:00:00 UTC in milliseconds
pub const unix_timestamp_offset = i64(1735689600000)

// Maximum counter value (2^13 - 1)
const max_counter = 8191

// Bit shift amount for timestamp
const timestamp_shift = u64(13)

// Counter mask (2^13 - 1)
const counter_mask = i64(0x1FFF)

// HLCState holds the state for Hybrid Logical Clock
pub struct HLCState {
pub mut:
	highest_remote_hlc i64
	counter            i64
	clock_drift_hlc    i64
}

// new_hlc_state creates a new HLC state
pub fn new_hlc_state() HLCState {
	return HLCState{
		highest_remote_hlc: 0
		counter: 0
		clock_drift_hlc: 0
	}
}

// HLC represents a Hybrid Logical Clock value
pub struct HLC {
pub:
	value i64
}

// to_counter extracts the lowest 13 bits (counter) from an HLC value
pub fn (h HLC) to_counter() int {
	return int(h.value & counter_mask)
}

// to_timestamp extracts the timestamp portion (upper bits) from an HLC value
pub fn (h HLC) to_timestamp() i64 {
	return h.value >> timestamp_shift
}

// to_unix_timestamp converts HLC to actual Unix timestamp in milliseconds
pub fn (h HLC) to_unix_timestamp() i64 {
	return h.to_timestamp() + unix_timestamp_offset
}

// hlc_from creates an HLC value from a Unix timestamp and optional counter
pub fn hlc_from(unix_timestamp i64, counter int) HLC {
	// Multiply by 2^13 to shift timestamp into upper bits
	ts := (unix_timestamp - unix_timestamp_offset) * i64(8192) // 2^13 = 8192
	return HLC{
		value: ts + i64(counter)
	}
}

// now creates an HLC from the current time with counter 0
pub fn hlc_now() HLC {
	return hlc_from(time.now().unix_milli(), 0)
}

// receive updates the HLC state when receiving a remote HLC
pub fn (mut s HLCState) receive(remote_hlc i64) {
	if remote_hlc > s.highest_remote_hlc {
		remote := HLC{value: remote_hlc}
		current := HLC{value: s.highest_remote_hlc}
		if remote.to_timestamp() > current.to_timestamp() {
			s.counter = 0
		}
		s.highest_remote_hlc = remote_hlc
	}
}

// create generates a new HLC value, handling clock drift
pub fn (mut s HLCState) create() i64 {
	now_ms := time.now().unix_milli()
	now_hlc := (now_ms - unix_timestamp_offset) * i64(8192) // 2^13

	if now_hlc > s.highest_remote_hlc {
		s.counter = 0
		// If local time is ahead of remote, use local time
		return now_hlc
	}

	s.counter++
	if s.counter > max_counter {
		eprintln('WARN: clock skew too high. Make sure system time is synchronized with NTP')
	}

	s.clock_drift_hlc = s.highest_remote_hlc - now_hlc
	return s.highest_remote_hlc + s.counter
}

// get_clock_drift_ms returns the current clock drift in milliseconds
pub fn (s HLCState) get_clock_drift_ms() i64 {
	return HLC{value: s.clock_drift_hlc}.to_timestamp()
}

// reset resets HLC state (for testing)
pub fn (mut s HLCState) reset() {
	s.highest_remote_hlc = 0
	s.counter = 0
	s.clock_drift_hlc = 0
}
