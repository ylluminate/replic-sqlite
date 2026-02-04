# Integration Plan: replic-sqlite with continuous-ais

## Overview

This fork will be used to add CRDT replication to Eve's memory system.

## Phase 1: Temporary Node.js Bridge (Current)

Use replic-sqlite as-is via a Node.js sync daemon that:
1. Watches for changes to mind.db via SQLite triggers
2. Syncs patches between nodes
3. Python code continues to use mind.db normally

## Phase 2: V Language Rewrite (Future)

Rewrite replic-sqlite core in V for:
- Better integration with BEAM/Elixir control plane
- Native SQLite bindings (no Node.js dependency)
- Single binary deployment

## Tables to Sync

### CRDT-Safe (Multi-writer OK)
- `percepts` → `percepts_patches`
- `vitals` → `vitals_patches`  
- `presence` → `presence_patches`
- `sensor_*` → `sensor_*_patches`

### Single-Writer (executive_lock enforced)
- `executive_state` → `executive_state_patches`
- `learnings` → `learnings_patches`
- `handoffs` → `handoffs_patches`
- `soul_instructions` → `soul_instructions_patches`

## Required Schema Changes

For each synced table, add:

```sql
CREATE TABLE <table>_patches (
    _patchedAt    INTEGER NOT NULL,
    _sequenceId   INTEGER NOT NULL,
    _peerId       INTEGER NOT NULL,
    -- mirror all columns from original table
    deletedAt     INTEGER  -- soft delete marker
);
```

## Architecture

```
┌─────────────┐     ┌─────────────┐
│  Node A     │     │  Node B     │
│  (Python)   │     │  (Python)   │
│  mind.db    │     │  mind.db    │
└──────┬──────┘     └──────┬──────┘
       │                   │
       └───────┬───────────┘
               │
        ┌──────▼──────┐
        │ replic-node │  (Node.js sync daemon)
        │ or V daemon │  (future)
        └─────────────┘
```

## Notes

- executive_lock in Python ensures single-writer for truth-bearing tables
- CRDT handles transport/merge for all tables
- Soft deletes required (deletedAt column)
