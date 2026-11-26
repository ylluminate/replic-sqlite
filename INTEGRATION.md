# Integration: replic-sqlite with Eve's Memory System

## Status: MVP Working ✓

**Bidirectional sync verified 2026-02-04** between g1-mbp (spoke) and eve-mm1 (hub).

Successfully tested CRDT replication for 4 key tables:
- `percepts` (CRDT-safe, multi-writer)
- `learnings` (single-writer, executive_lock enforced)
- `handoffs` (single-writer)
- `presence` (CRDT-safe, multi-writer)

## Quick Start

### 1. Install Dependencies

```bash
cd ~/tank/ops/ai/replic-sqlite
npm install
```

### 2. Run Sync Daemon

**On hub node (eve-mm1):**
```bash
PEER_ID=1 MIND_DB=/Users/eve/.ai/memory/mind.db /opt/homebrew/bin/node eve-sync-daemon.js
```

**On spoke nodes (g1-mbp):**
```bash
PEER_ID=2 MIND_DB=/Users/u/.ai/identities/eve/memory/mind.db node eve-sync-daemon.js --hub=192.168.0.4
```

### 3. Test

**Via WebSocket write request:**
```javascript
const ws = new WebSocket('ws://localhost:8765');
ws.on('open', () => {
    ws.send(JSON.stringify({
        type: 'write',
        table: 'percepts',
        data: { id: 'test-' + Date.now(), time_start: new Date().toISOString() }
    }));
});
```

Data will sync to all connected peers within seconds.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  eve-mm1    │     │  g1-mbp     │     │  other...   │
│  PEER_ID=1  │◄───►│  PEER_ID=2  │◄───►│  PEER_ID=N  │
│  (hub)      │     │  (spoke)    │     │  (spoke)    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       ▼                   ▼                   ▼
   mind.db             mind.db             mind.db
   (shared)            (replica)           (replica)
```

## Tables with Replication Support

| Table | Type | Primary Key | Notes |
|-------|------|-------------|-------|
| percepts | CRDT-safe | id | Multi-writer OK |
| learnings | Single-writer | id | executive_lock enforced |
| handoffs | Single-writer | id | executive_lock enforced |
| presence | CRDT-safe | session_id | Multi-writer OK |

## Schema Requirements

Each synced table needs:

1. **A `_patches` companion table:**
```sql
CREATE TABLE tablename_patches (
    _patchedAt    INTEGER NOT NULL,  -- Unix timestamp ms
    _sequenceId   INTEGER NOT NULL,  -- Sequence per peer
    _peerId       INTEGER NOT NULL,  -- Peer ID
    -- Mirror all columns from main table
    deletedAt     INTEGER            -- Soft delete marker
);
CREATE INDEX tablename_patches_at_idx ON tablename_patches (_patchedAt);
```

2. **A `deletedAt` column** for soft deletes:
```sql
ALTER TABLE tablename ADD COLUMN deletedAt INTEGER;
```

## Adding More Tables

1. Create the `_patches` table with matching columns
2. Add `deletedAt` column to main table
3. Restart sync daemon (it auto-discovers `_patches` tables)

## Single-Writer Tables

For truth-bearing tables (learnings, handoffs, soul_instructions, etc.):

1. Python code must acquire `executive_lock` before writes
2. CRDT handles transport/merge
3. Fencing tokens (epoch) prevent stale writers after failover

## Writing Data (Important!)

The daemon's WebSocket accepts `write` messages to create patches that broadcast to peers.

**DON'T** create patches from a separate Node process - they won't have peers registered to broadcast to.

**DO** send write requests through the daemon's WebSocket:
```javascript
ws.send(JSON.stringify({ type: 'write', table: 'percepts', data: {...} }));
```

## Future Work

- [x] Bidirectional sync (g1-mbp ↔ eve-mm1) - DONE 2026-02-04
- [ ] Add executive_state replication
- [ ] Add soul_instructions replication
- [ ] Set up launchd/systemd services for daemon
- [ ] Add monitoring/metrics
- [ ] Python integration (write through daemon or port to Python)
- [ ] Phase 2: V language rewrite for BEAM integration
