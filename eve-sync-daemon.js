#!/usr/bin/env node
/**
 * Eve Memory Sync Daemon
 * 
 * Runs on each node to sync mind.db via replic-sqlite CRDT replication.
 * 
 * Usage:
 *   PEER_ID=1 node eve-sync-daemon.js              # Hub node (eve-mm1)
 *   PEER_ID=2 node eve-sync-daemon.js --hub=eve    # Spoke node
 */
const Database = require('better-sqlite3');
const SQLiteOnSteroid = require('./lib/index.js');
const WebSocket = require('ws');
const path = require('path');
const os = require('os');

// Configuration
const MIND_DB = process.env.MIND_DB || path.join(os.homedir(), '.ai/identities/eve/memory/mind.db');
const PEER_ID = parseInt(process.env.PEER_ID || '1', 10);
const WS_PORT = parseInt(process.env.WS_PORT || '8765', 10);
const HUB_HOST = process.argv.find(a => a.startsWith('--hub='))?.split('=')[1];

console.log('Eve Memory Sync Daemon');
console.log('  Database:', MIND_DB);
console.log('  Peer ID:', PEER_ID);
console.log('  WebSocket Port:', WS_PORT);
console.log('  Hub:', HUB_HOST || '(this is the hub)');

// Open database
const db = new Database(MIND_DB);

// Initialize replic-sqlite
const app = SQLiteOnSteroid(db, PEER_ID);

// Migrate (no-op since tables exist)
app.migrate([{ up: '-- Eve sync init', down: '' }]);

// Track connected peers
const peers = new Map();

// WebSocket server for incoming connections
const wss = new WebSocket.Server({ port: WS_PORT });

wss.on('connection', (ws, req) => {
  const peerAddr = req.socket.remoteAddress;
  console.log(`Peer connected from ${peerAddr}`);
  let connectedPeerId = null;
  
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'register') {
        // Peer registration
        connectedPeerId = msg.peerId;
        peers.set(msg.peerId, ws);
        app.addRemotePeer(msg.peerId, {
          send: (patch) => {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify(patch));
            }
          }
        }, { ip: peerAddr, port: WS_PORT });
        console.log(`Registered peer ${msg.peerId}`);
        // Send our peer ID back
        ws.send(JSON.stringify({ type: 'registered', hubPeerId: PEER_ID }));
      } else if (msg.type === 10) {  // PATCH message
        console.log('Received patch from peer:', msg.tab, msg.delta?.id);
        // Forward to replic-sqlite's internal handler
        app._onPatchReceivedFromPeers(msg);
      } else if (msg.type === 20) {  // PING message
        // Let replic handle ping/sync status
        console.log('Received ping from peer');
      } else if (msg.type === 'write') {
        // Local write request - upsert and broadcast to peers
        console.log('Write request:', msg.table, msg.data?.id);
        app.upsert(msg.table, msg.data, (err) => {
          if (err) {
            console.error('Write error:', err.message);
            ws.send(JSON.stringify({ type: 'write_error', error: err.message }));
          } else {
            console.log('Write success, patch broadcast to peers');
            ws.send(JSON.stringify({ type: 'write_ok', id: msg.data?.id }));
          }
        });
      } else {
        console.log('Unknown message type:', msg.type);
      }
    } catch (err) {
      console.error('Message error:', err.message);
    }
  });
  
  ws.on('close', () => {
    console.log(`Peer disconnected from ${peerAddr}`);
    if (connectedPeerId) {
      peers.delete(connectedPeerId);
    }
  });
  
  ws.on('error', (err) => {
    console.error(`Peer ${peerAddr} error:`, err.message);
  });
});

console.log(`WebSocket server listening on port ${WS_PORT}`);

// If we're a spoke node, connect to hub
if (HUB_HOST) {
  const hubUrl = `ws://${HUB_HOST}:${WS_PORT}`;
  console.log(`Connecting to hub at ${hubUrl}...`);
  
  function connectToHub() {
    const hubWs = new WebSocket(hubUrl);
    
    hubWs.on('open', () => {
      console.log('Connected to hub');
      // Register with hub
      hubWs.send(JSON.stringify({ type: 'register', peerId: PEER_ID }));
    });
    
    hubWs.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.type === 'registered') {
          // Hub registered us
          app.addRemotePeer(msg.hubPeerId, {
            send: (patch) => {
              if (hubWs.readyState === WebSocket.OPEN) {
                hubWs.send(JSON.stringify(patch));
              }
            }
          }, { ip: HUB_HOST, port: WS_PORT });
          console.log(`Registered with hub (peer ${msg.hubPeerId})`);
        } else if (msg.type === 10) {  // PATCH message
          console.log('Received patch from hub:', msg.tab, msg.delta?.id);
          app._onPatchReceivedFromPeers(msg);
        } else if (msg.type === 20) {  // PING
          console.log('Received ping from hub');
        }
      } catch (err) {
        console.error('Hub message error:', err.message);
      }
    });
    
    hubWs.on('close', () => {
      console.log('Disconnected from hub, reconnecting in 5s...');
      setTimeout(connectToHub, 5000);
    });
    
    hubWs.on('error', (err) => {
      console.error('Hub connection error:', err.message);
    });
  }
  
  connectToHub();
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down...');
  wss.close();
  db.close();
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

console.log('Ready for sync');
