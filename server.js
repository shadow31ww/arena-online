
const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: 100 * 1024 * 1024,
  perMessageDeflate: false
});

const rooms = new Map();
const PORT = process.env.PORT || 10000;

const CINEMA_MAX_FILE_BYTES = 400 * 1024 * 1024;
const CINEMA_CHUNK_BYTES = 1024 * 1024;

app.use(express.static(path.join(__dirname)));
app.get('/health', (_req, res) =>
  res.json({ ok: true, rooms: rooms.size })
);

function code() {
  return crypto.randomBytes(3).toString('hex').toUpperCase();
}

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function roomInfo(room) {
  return {
    room: room.code,
    seed: room.seed,
    mode: room.mode,
    maxPlayers: room.max || 2,
    playerCount: room.players.length
  };
}

function broadcast(room, data, except) {
  for (const p of room.players) {
    if (p !== except) send(p, data);
  }
}

function cinemaPeer(room, source) {
  return room.players.find(
    p => p !== source && p.readyState === WebSocket.OPEN
  );
}

function pauseSourceForBackpressure(source, target) {
  if (source._cinemaBackpressurePaused) return;
  if (!source._socket ||
      typeof source._socket.pause !== 'function') return;

  source._cinemaBackpressurePaused = true;
  source._socket.pause();

  const check = () => {
    if (source.readyState !== WebSocket.OPEN || !source._socket) {
      source._cinemaBackpressurePaused = false;
      return;
    }

    if (
      target.readyState !== WebSocket.OPEN ||
      target.bufferedAmount < 2 * 1024 * 1024
    ) {
      source._cinemaBackpressurePaused = false;
      source._socket.resume();
      return;
    }

    setTimeout(check, 20);
  };

  setTimeout(check, 20);
}

function removePlayer(ws) {
  const room = ws.room;
  if (!room) return;

  ws.room = null;
  room.players = room.players.filter(p => p !== ws);

  if (room.host === ws) {
    room.cinemaTransfer = null;
  }

  if (room.players.length === 0) {
    rooms.delete(room.code);
  } else {
    broadcast(room, { type: 'peer-left' });
    broadcast(room, {
      type: 'room-status',
      ...roomInfo(room)
    });
  }
}

wss.on('connection', ws => {
  ws.on('message', (raw, isBinary) => {
    const room = ws.room;

    // Transferencia binaria del vídeo por fragmentos.
    if (isBinary) {
      if (
        !room ||
        room.mode !== 'cinema' ||
        ws !== room.host ||
        !room.cinemaTransfer
      ) return;

      const transfer = room.cinemaTransfer;

      if (
        raw.length < 1 ||
        raw.length > CINEMA_CHUNK_BYTES ||
        transfer.receivedBytes + raw.length > transfer.totalBytes
      ) {
        room.cinemaTransfer = null;
        send(ws, {
          type: 'error',
          message: 'El fragmento de vídeo no es válido. Vuelve a compartir el archivo.'
        });
        return;
      }

      transfer.receivedBytes += raw.length;

      const target = cinemaPeer(room, ws);

      if (target) {
        target.send(raw, { binary: true });

        if (target.bufferedAmount > 8 * 1024 * 1024) {
          pauseSourceForBackpressure(ws, target);
        }
      }

      return;
    }

    let d;
    try {
      d = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (!d || typeof d.type !== 'string') return;

    // Crear sala.
    if (d.type === 'create') {
      removePlayer(ws);

      let roomCode = String(d.room || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '')
        .slice(0, 12);

      if (!roomCode) roomCode = code();

      if (rooms.has(roomCode)) {
        send(ws, {
          type: 'error',
          message: 'Ese código ya está ocupado. Crea otra sala.'
        });
        return;
      }

      const validModes = [
        '1v1',
        'race',
        'chameleon',
        'battlezone',
        '1v1fps',
        'ffa3',
        'cinema'
      ];

      const requestedMode = String(d.mode || '1v1');
      const roomMode = validModes.includes(requestedMode)
        ? requestedMode
        : '1v1';

      const room = {
        code: roomCode,
        password: String(d.password || ''),
        mode: roomMode,
        max: roomMode === 'ffa3' ? 3 : 2,
        seed: crypto.randomInt(1, 0x7fffffff),
        players: [],
        host: ws,
        started: false,
        raceReady: new Set(),
        cinemaTransfer: null
      };

      rooms.set(roomCode, room);
      room.players.push(ws);

      ws.room = room;
      ws.role = 'host';
      ws.playerName = String(d.name || 'Jugador').slice(0, 24);

      send(ws, {
        type: 'created',
        ...roomInfo(room)
      });

      return;
    }

    // Unirse a una sala.
    if (d.type === 'join') {
      removePlayer(ws);

      const roomCode = String(d.room || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '')
        .slice(0, 12);

      const targetRoom = rooms.get(roomCode);

      if (!targetRoom) {
        send(ws, {
          type: 'error',
          message: 'No existe esa sala. Revisa el código.'
        });
        return;
      }

      if (targetRoom.password !== String(d.password || '')) {
        send(ws, {
          type: 'error',
          message: 'La contraseña no coincide.'
        });
        return;
      }

      if (
        targetRoom.started ||
        targetRoom.players.length >= (targetRoom.max || 2)
      ) {
        send(ws, {
          type: 'error',
          message: 'La sala ya está llena.'
        });
        return;
      }

      // El modo del anfitrión manda.
      targetRoom.players.push(ws);
      ws.room = targetRoom;
      ws.role = 'guest';
      ws.playerName = String(d.name || 'Jugador').slice(0, 24);

      if (targetRoom.players.length < (targetRoom.max || 2)) {
        broadcast(targetRoom, {
          type: 'waiting',
          message: 'Esperando a que se llene la sala…',
          ...roomInfo(targetRoom)
        });
        return;
      }

      targetRoom.started = true;
      targetRoom.players.forEach((p, i) => {
        p.slot = i;
      });

      const peersOf = me =>
        targetRoom.players
          .filter(p => p !== me)
          .map(p => ({
            slot: p.slot,
            name: p.playerName || 'Jugador'
          }));

      for (const p of targetRoom.players) {
        const first = peersOf(p)[0];

        send(p, {
          type: 'ready',
          role: p === targetRoom.host ? 'host' : 'guest',
          slot: p.slot,
          peers: peersOf(p),
          peerName: first ? first.name : 'Rival',
          ...roomInfo(targetRoom)
        });
      }

      return;
    }

    if (!room || !room.players.includes(ws)) {
      send(ws, {
        type: 'error',
        message: 'Primero crea o únete a una sala.'
      });
      return;
    }

    // Comienzo de transferencia del vídeo.
    if (d.type === 'cinema-media-start') {
      if (room.mode !== 'cinema' || ws !== room.host) return;

      const totalBytes = Number(d.totalBytes);
      const totalChunks = Number(d.totalChunks);
      const chunkBytes = Number(d.chunkBytes);

      if (
        typeof d.transferId !== 'string' ||
        !d.transferId.startsWith('cine-') ||
        !Number.isSafeInteger(totalBytes) ||
        totalBytes < 1 ||
        totalBytes > CINEMA_MAX_FILE_BYTES ||
        chunkBytes !== CINEMA_CHUNK_BYTES ||
        totalChunks !== Math.ceil(totalBytes / CINEMA_CHUNK_BYTES) ||
        !/^((image|video)\/[a-zA-Z0-9.+-]+)$/.test(String(d.mime || ''))
      ) {
        send(ws, {
          type: 'error',
          message: 'El archivo multimedia no es válido o supera 400 MB.'
        });
        return;
      }

      room.cinemaTransfer = {
        id: d.transferId,
        totalBytes,
        receivedBytes: 0
      };

      broadcast(room, d, ws);
      return;
    }

    // Finalizar transferencia.
    if (d.type === 'cinema-media-end') {
      if (
        room.mode !== 'cinema' ||
        ws !== room.host ||
        !room.cinemaTransfer ||
        d.transferId !== room.cinemaTransfer.id
      ) return;

      if (
        room.cinemaTransfer.receivedBytes !==
        room.cinemaTransfer.totalBytes
      ) {
        room.cinemaTransfer = null;

        send(ws, {
          type: 'error',
          message: 'La transferencia no llegó completa al servidor. Vuelve a enviarla.'
        });
        return;
      }

      room.cinemaTransfer = null;
      broadcast(room, d, ws);
      return;
    }

    // Sincronizar reproducción y pausa.
    if (d.type === 'cinema-control') {
      if (room.mode === 'cinema') {
        broadcast(room, d, ws);
      }
      return;
    }

    // Preparación de carreras.
    if (
      d.type === 'race-ready' ||
      d.type === 'race-client-ready'
    ) {
      room.raceReady.add(ws);

      if (room.raceReady.size === 2 && !room.raceStarted) {
        room.raceStarted = true;

        const trackIndex = Number.isInteger(d.trackIndex)
          ? d.trackIndex
          : 0;

        broadcast(room, {
          type: 'race-start',
          trackIndex
        });
      } else if (room.raceReady.size < 2) {
        broadcast(room, {
          type: 'race-client-ready'
        }, ws);
      }

      return;
    }

    // Revancha.
    if (d.type === 'rematch') {
      room.seed = crypto.randomInt(1, 0x7fffffff);
      room.raceReady.clear();
      room.raceStarted = false;

      broadcast(room, {
        type: 'rematch-ready',
        seed: room.seed
      });

      return;
    }

    // Sincronización general del juego.
    broadcast(room, {
      ...d,
      from: ws.slot
    }, ws);
  });

  ws.on('close', () => removePlayer(ws));
  ws.on('error', () => removePlayer(ws));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Game server listening on ${PORT}`);
});
