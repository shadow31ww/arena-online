const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const rooms = new Map();
const PORT = process.env.PORT || 10000;

app.use(express.static(path.join(__dirname)));
app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size }));

function code() { return crypto.randomBytes(3).toString('hex').toUpperCase(); }
function send(ws, data) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(data)); }
function roomInfo(room) { return { room: room.code, seed: room.seed, mode: room.mode, maxPlayers: 2, playerCount: room.players.length }; }
function broadcast(room, data, except) { for (const p of room.players) if (p !== except) send(p, data); }
function removePlayer(ws) {
  const room = ws.room;
  if (!room) return;
  ws.room = null;
  room.players = room.players.filter(p => p !== ws);
  if (room.players.length === 0) rooms.delete(room.code);
  else {
    broadcast(room, { type: 'peer-left' });
    broadcast(room, { type: 'room-status', ...roomInfo(room) });
  }
}
wss.on('connection', ws => {
  ws.on('message', raw => {
    let d;
    try { d = JSON.parse(raw.toString()); } catch { return; }
    if (!d || typeof d.type !== 'string') return;

    if (d.type === 'create') {
      removePlayer(ws);
      let roomCode = String(d.room || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);
      if (!roomCode) roomCode = code();
      if (rooms.has(roomCode)) { send(ws, { type: 'error', message: 'Ese código ya está ocupado. Crea otra sala.' }); return; }
      const room = { code: roomCode, password: String(d.password || ''), mode: ['race','chameleon','battlezone','1v1fps'].includes(d.mode) ? d.mode : '1v1', seed: crypto.randomInt(1, 0x7fffffff), players: [], names: new Map(), host: ws, started: false, raceReady: new Set() };
      rooms.set(roomCode, room); room.players.push(ws); ws.room = room; ws.role = 'host'; ws.playerName = String(d.name || 'Jugador').slice(0, 24);
      send(ws, { type: 'created', ...roomInfo(room) });
      return;
    }

    if (d.type === 'join') {
      removePlayer(ws);
      const roomCode = String(d.room || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);
      const room = rooms.get(roomCode);
      if (!room) { send(ws, { type: 'error', message: 'No existe esa sala. Revisa el código.' }); return; }
      if (room.password !== String(d.password || '')) { send(ws, { type: 'error', message: 'La contraseña no coincide.' }); return; }
      if (room.started || room.players.length >= 2) { send(ws, { type: 'error', message: 'La sala ya está llena.' }); return; }
      if (room.mode !== d.mode && d.mode) { send(ws, { type: 'error', message: 'El modo no coincide con el anfitrión. Elige el mismo modo.' }); return; }
      room.players.push(ws); ws.room = room; ws.role = 'guest'; ws.playerName = String(d.name || 'Jugador').slice(0, 24);
      room.started = true;
      const host = room.host;
      send(host, { type: 'ready', role: 'host', peerName: ws.playerName, ...roomInfo(room) });
      send(ws, { type: 'ready', role: 'guest', peerName: host.playerName || 'Anfitrión', ...roomInfo(room) });
      return;
    }

    const room = ws.room;
    if (!room || !room.players.includes(ws)) { send(ws, { type: 'error', message: 'Primero crea o únete a una sala.' }); return; }

    if (d.type === 'race-ready' || d.type === 'race-client-ready') {
      room.raceReady.add(ws);
      if (room.raceReady.size === 2 && !room.raceStarted) {
        room.raceStarted = true;
        const trackIndex = Number.isInteger(d.trackIndex) ? d.trackIndex : 0;
        broadcast(room, { type: 'race-start', trackIndex });
      } else if (room.raceReady.size < 2) broadcast(room, { type: 'race-client-ready' }, ws);
      return;
    }
    if (d.type === 'rematch') {
      room.seed = crypto.randomInt(1, 0x7fffffff);
      room.raceReady.clear(); room.raceStarted = false;
      broadcast(room, { type: 'rematch-ready', seed: room.seed });
      return;
    }
    // Game state and combat/building events are forwarded to the other player.
    broadcast(room, d, ws);
  });
  ws.on('close', () => removePlayer(ws));
  ws.on('error', () => removePlayer(ws));
});

server.listen(PORT, '0.0.0.0', () => console.log(`Game server listening on ${PORT}`));
