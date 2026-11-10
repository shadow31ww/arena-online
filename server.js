
'use strict';

const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');
const path = require('path');

const app = express();
const server = http.createServer(app);

const GAME_MAX_PAYLOAD = 4 * 1024 * 1024;
const CINEMA_MAX_FILE_BYTES = 400 * 1024 * 1024;
const CINEMA_CHUNK_BYTES = 512 * 1024;
const CINEMA_FRAME_HEADER_BYTES = 4;
const MEDIA_MAX_PAYLOAD = CINEMA_CHUNK_BYTES + CINEMA_FRAME_HEADER_BYTES + 1024;

// Conexión independiente para el juego y el vídeo.
const gameWss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: GAME_MAX_PAYLOAD,
  perMessageDeflate: false
});

const mediaWss = new WebSocketServer({
  server,
  path: '/ws-media',
  maxPayload: MEDIA_MAX_PAYLOAD,
  perMessageDeflate: false
});

const rooms = new Map();
const PORT = process.env.PORT || 10000;

app.use(express.static(path.join(__dirname)));

app.get('/health', (_req, res) => {
  res.json({ ok: true, rooms: rooms.size });
});

function code() {
  return crypto.randomBytes(3).toString('hex').toUpperCase();
}

function token() {
  return crypto.randomBytes(32).toString('hex');
}

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(JSON.stringify(data));
    } catch (_) {}
  }
}

function sendMedia(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(JSON.stringify(data));
    } catch (_) {}
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
  for (const player of room.players) {
    if (player !== except) send(player, data);
  }
}

function mediaSocketFor(room, player) {
  const socket = room && room.mediaSockets && room.mediaSockets.get(player);
  return socket && socket.readyState === WebSocket.OPEN ? socket : null;
}

function mediaPeerSocket(room, player) {
  const peer = room.players.find(candidate => candidate !== player);
  return peer ? mediaSocketFor(room, peer) : null;
}

function removePlayer(ws) {
  const room = ws.room;
  if (!room) return;

  ws.room = null;
  room.players = room.players.filter(player => player !== ws);

  const mediaSocket = room.mediaSockets.get(ws);

  if (mediaSocket) {
    room.mediaSockets.delete(ws);
    mediaSocket.player = null;
    mediaSocket.room = null;

    try {
      mediaSocket.close(4001, 'Game connection closed');
    } catch (_) {}
  }

  room.cinemaTransfer = null;

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

function installHeartbeat(wss, label) {
  wss.on('connection', socket => {
    socket.isAlive = true;

    socket.on('pong', () => {
      socket.isAlive = true;
    });
  });

  const timer = setInterval(() => {
    for (const socket of wss.clients) {
      if (socket.isAlive === false) {
        console.warn(`[${label}] heartbeat timeout`);
        socket.terminate();
        continue;
      }

      socket.isAlive = false;

      try {
        socket.ping();
      } catch (_) {
        socket.terminate();
      }
    }
  }, 25000);

  if (typeof timer.unref === 'function') timer.unref();
}

installHeartbeat(gameWss, 'game-ws');
installHeartbeat(mediaWss, 'media-ws');

// ==========================================
// CONEXIÓN DEL JUEGO
// ==========================================

gameWss.on('connection', ws => {
  ws.on('message', (raw, isBinary) => {
    if (isBinary) {
      send(ws, {
        type: 'error',
        message: 'El vídeo usa un canal separado. Actualiza el HTML del juego.'
      });
      return;
    }

    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch (_) {
      return;
    }

    if (!data || typeof data.type !== 'string') return;

    // CREAR SALA
    if (data.type === 'create') {
      removePlayer(ws);

      let roomCode = String(data.room || '')
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

      const requestedMode = String(data.mode || '1v1');

      const roomMode = validModes.includes(requestedMode)
        ? requestedMode
        : '1v1';

      const room = {
        code: roomCode,
        password: String(data.password || ''),
        mode: roomMode,
        max: roomMode === 'ffa3' ? 3 : 2,
        seed: crypto.randomInt(1, 0x7fffffff),
        players: [],
        host: ws,
        started: false,
        raceReady: new Set(),
        mediaSockets: new Map(),
        cinemaTransfer: null
      };

      rooms.set(roomCode, room);
      room.players.push(ws);

      ws.room = room;
      ws.role = 'host';
      ws.mediaToken = token();
      ws.playerName = String(data.name || 'Jugador').slice(0, 24);

      send(ws, {
        type: 'created',
        ...roomInfo(room)
      });

      return;
    }

    // UNIRSE A SALA
    if (data.type === 'join') {
      removePlayer(ws);

      const roomCode = String(data.room || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '')
        .slice(0, 12);

      const room = rooms.get(roomCode);

      if (!room) {
        send(ws, {
          type: 'error',
          message: 'No existe esa sala. Revisa el código.'
        });
        return;
      }

      if (room.password !== String(data.password || '')) {
        send(ws, {
          type: 'error',
          message: 'La contraseña no coincide.'
        });
        return;
      }

      if (room.started || room.players.length >= (room.max || 2)) {
        send(ws, {
          type: 'error',
          message: 'La sala ya está llena.'
        });
        return;
      }

      // El invitado adopta el modo del anfitrión.
      room.players.push(ws);

      ws.room = room;
      ws.role = 'guest';
      ws.mediaToken = token();
      ws.playerName = String(data.name || 'Jugador').slice(0, 24);

      if (room.players.length < (room.max || 2)) {
        broadcast(room, {
          type: 'waiting',
          message: 'Esperando a que se llene la sala…',
          ...roomInfo(room)
        });

        return;
      }

      room.started = true;

      room.players.forEach((player, index) => {
        player.slot = index;
      });

      const peersOf = me => room.players
        .filter(player => player !== me)
        .map(player => ({
          slot: player.slot,
          name: player.playerName || 'Jugador'
        }));

      for (const player of room.players) {
        const first = peersOf(player)[0];

        send(player, {
          type: 'ready',
          role: player === room.host ? 'host' : 'guest',
          slot: player.slot,
          peers: peersOf(player),
          peerName: first ? first.name : 'Rival',
          mediaToken: player.mediaToken,
          ...roomInfo(room)
        });
      }

      return;
    }

    if (data.type === 'leave') {
      removePlayer(ws);
      return;
    }

    const room = ws.room;

    if (!room || !room.players.includes(ws)) {
      send(ws, {
        type: 'error',
        message: 'Primero crea o únete a una sala.'
      });

      return;
    }

    // CARRERAS
    if (
      data.type === 'race-ready' ||
      data.type === 'race-client-ready'
    ) {
      room.raceReady.add(ws);

      if (room.raceReady.size === 2 && !room.raceStarted) {
        room.raceStarted = true;

        const trackIndex = Number.isInteger(data.trackIndex)
          ? data.trackIndex
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

    // REVANCHA
    if (data.type === 'rematch') {
      room.seed = crypto.randomInt(1, 0x7fffffff);

      room.raceReady.clear();
      room.raceStarted = false;

      broadcast(room, {
        type: 'rematch-ready',
        seed: room.seed
      });

      return;
    }

    // EVENTOS NORMALES DEL JUEGO
    broadcast(room, {
      ...data,
      from: ws.slot
    }, ws);
  });

  ws.on('close', () => removePlayer(ws));

  ws.on('error', error => {
    console.error('[game-ws] socket error:', error.message);
    removePlayer(ws);
  });
});

// ==========================================
// CONEXIÓN EXCLUSIVA PARA EL CINE
// ==========================================

mediaWss.on('connection', media => {
  media.player = null;
  media.room = null;

  media.on('message', (raw, isBinary) => {

    // AUTENTICAR CANAL DE VÍDEO
    if (!media.player) {
      if (isBinary) {
        media.close(4400, 'Attach required');
        return;
      }

      let attach;

      try {
        attach = JSON.parse(raw.toString());
      } catch (_) {
        media.close(4400, 'Invalid attach');
        return;
      }

      if (!attach || attach.type !== 'attach') {
        media.close(4400, 'Attach required');
        return;
      }

      const room = rooms.get(
        String(attach.room || '').toUpperCase()
      );

      if (!room || room.mode !== 'cinema' || !room.started) {
        sendMedia(media, {
          type: 'media-error',
          message: 'La sala de cine no está activa.'
        });

        media.close(4404, 'Cinema room not found');
        return;
      }

      const player = room.players.find(candidate =>
        typeof candidate.mediaToken === 'string' &&
        candidate.mediaToken.length === 64 &&
        typeof attach.token === 'string' &&
        attach.token.length === 64 &&
        crypto.timingSafeEqual(
          Buffer.from(candidate.mediaToken),
          Buffer.from(attach.token)
        )
      );

      if (!player) {
        sendMedia(media, {
          type: 'media-error',
          message: 'No se pudo autenticar el canal del vídeo.'
        });

        media.close(4403, 'Unauthorized');
        return;
      }

      const previous = room.mediaSockets.get(player);

      if (previous && previous !== media) {
        try {
          previous.close(4002, 'Replaced by a new media connection');
        } catch (_) {}
      }

      media.player = player;
      media.room = room;

      room.mediaSockets.set(player, media);

      sendMedia(media, {
        type: 'media-attached'
      });

      const allAttached =
        room.players.length === 2 &&
        room.players.every(p => mediaSocketFor(room, p));

      if (allAttached) {
        const hostMedia = mediaSocketFor(room, room.host);

        sendMedia(hostMedia, {
          type: 'media-ready'
        });
      }

      return;
    }

    const room = media.room;
    const player = media.player;

    if (
      !room ||
      !player ||
      !room.players.includes(player) ||
      room.mediaSockets.get(player) !== media
    ) return;

    // FRAGMENTOS BINARIOS
    if (isBinary) {
      const transfer = room.cinemaTransfer;

      if (
        player !== room.host ||
        !transfer ||
        !transfer.ready ||
        raw.length <= CINEMA_FRAME_HEADER_BYTES ||
        raw.length > MEDIA_MAX_PAYLOAD
      ) return;

      const index = raw.readUInt32LE(0);
      const payloadBytes = raw.length - CINEMA_FRAME_HEADER_BYTES;

      const expectedBytes = Math.min(
        CINEMA_CHUNK_BYTES,
        transfer.totalBytes - index * CINEMA_CHUNK_BYTES
      );

      if (
        index !== transfer.receivedChunks ||
        index >= transfer.totalChunks ||
        payloadBytes !== expectedBytes
      ) {
        room.cinemaTransfer = null;

        sendMedia(media, {
          type: 'media-error',
          message: 'Se detectó un fragmento fuera de orden o incompleto. Reinicia el envío.'
        });

        const guestMedia = mediaPeerSocket(room, player);

        sendMedia(guestMedia, {
          type: 'media-error',
          message: 'El envío del anfitrión llegó incompleto.'
        });

        return;
      }

      const guestMedia = mediaPeerSocket(room, player);

      if (!guestMedia) {
        room.cinemaTransfer = null;

        sendMedia(media, {
          type: 'media-error',
          message: 'El otro jugador perdió el canal de vídeo.'
        });

        return;
      }

      transfer.receivedChunks += 1;
      transfer.receivedBytes += payloadBytes;

      try {
        guestMedia.send(raw, {
          binary: true,
          compress: false
        });
      } catch (error) {
        room.cinemaTransfer = null;

        sendMedia(media, {
          type: 'media-error',
          message: 'No se pudo retransmitir un fragmento del vídeo.'
        });
      }

      return;
    }

    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch (_) {
      return;
    }

    if (!data || typeof data.type !== 'string') return;

    // INICIAR TRANSFERENCIA
    if (data.type === 'cinema-media-start') {
      if (
        room.mode !== 'cinema' ||
        player !== room.host ||
        room.cinemaTransfer
      ) return;

      const totalBytes = Number(data.totalBytes);
      const totalChunks = Number(data.totalChunks);
      const chunkBytes = Number(data.chunkBytes);

      const guestMedia = mediaPeerSocket(room, player);

      if (!guestMedia) {
        sendMedia(media, {
          type: 'media-error',
          message: 'El invitado todavía no tiene conectado el canal del vídeo.'
        });

        return;
      }

      if (
        typeof data.transferId !== 'string' ||
        !/^cine-[a-z0-9-]{5,80}$/i.test(data.transferId) ||
        !Number.isSafeInteger(totalBytes) ||
        totalBytes < 1 ||
        totalBytes > CINEMA_MAX_FILE_BYTES ||
        chunkBytes !== CINEMA_CHUNK_BYTES ||
        !Number.isSafeInteger(totalChunks) ||
        totalChunks !== Math.ceil(totalBytes / CINEMA_CHUNK_BYTES) ||
        !/^((image|video)\/[a-zA-Z0-9.+-]+)$/.test(String(data.mime || ''))
      ) {
        sendMedia(media, {
          type: 'media-error',
          message: 'El archivo no es válido o supera el límite de 400 MB.'
        });

        return;
      }

      room.cinemaTransfer = {
        id: data.transferId,
        totalBytes,
        totalChunks,
        receivedBytes: 0,
        receivedChunks: 0,
        acked: new Set(),
        ready: false
      };

      sendMedia(guestMedia, data);
      return;
    }

    // EL INVITADO ESTÁ PREPARADO
    if (data.type === 'cinema-media-ready') {
      const transfer = room.cinemaTransfer;

      if (
        player === room.host ||
        !transfer ||
        data.transferId !== transfer.id
      ) return;

      transfer.ready = true;

      sendMedia(
        mediaSocketFor(room, room.host),
        data
      );

      return;
    }

    // CONFIRMAR RECEPCIÓN DE FRAGMENTOS
    if (data.type === 'cinema-media-ack') {
      const transfer = room.cinemaTransfer;

      if (
        player === room.host ||
        !transfer ||
        data.transferId !== transfer.id ||
        !Number.isInteger(data.index)
      ) return;

      if (
        data.index < 0 ||
        data.index >= transfer.receivedChunks
      ) return;

      if (!transfer.acked.has(data.index)) {
        transfer.acked.add(data.index);

        sendMedia(
          mediaSocketFor(room, room.host),
          data
        );
      }

      return;
    }

    // FINALIZAR TRANSFERENCIA
    if (data.type === 'cinema-media-end') {
      const transfer = room.cinemaTransfer;

      if (
        player !== room.host ||
        !transfer ||
        data.transferId !== transfer.id
      ) return;

      if (
        transfer.receivedBytes !== transfer.totalBytes ||
        transfer.receivedChunks !== transfer.totalChunks ||
        transfer.acked.size !== transfer.totalChunks
      ) {
        sendMedia(media, {
          type: 'media-error',
          message: 'No se confirmó la recepción de todos los fragmentos. Puedes volver a intentarlo.'
        });

        return;
      }

      const guestMedia = mediaPeerSocket(room, player);

      sendMedia(guestMedia, data);

      sendMedia(media, {
        type: 'cinema-media-complete',
        transferId: transfer.id
      });

      room.cinemaTransfer = null;
      return;
    }

    // ERROR DE TRANSFERENCIA
    if (data.type === 'cinema-media-fail') {
      const transfer = room.cinemaTransfer;

      if (
        player === room.host ||
        !transfer ||
        data.transferId !== transfer.id
      ) return;

      sendMedia(
        mediaSocketFor(room, room.host),
        data
      );

      room.cinemaTransfer = null;
      return;
    }

    // PAUSAR Y REPRODUCIR
    if (data.type === 'cinema-control') {
      sendMedia(
        mediaPeerSocket(room, player),
        data
      );
    }
  });

  media.on('close', () => {
    const room = media.room;
    const player = media.player;

    if (!room || !player) return;

    if (room.mediaSockets.get(player) === media) {
      room.mediaSockets.delete(player);
    }

    room.cinemaTransfer = null;

    const peer = room.players.find(
      candidate => candidate !== player
    );

    if (peer) {
      sendMedia(
        mediaSocketFor(room, peer),
        { type: 'media-peer-disconnected' }
      );
    }

    media.player = null;
    media.room = null;
  });

  media.on('error', error => {
    console.error('[media-ws] socket error:', error.message);
  });
});

// ==========================================
// INICIAR SERVIDOR
// ==========================================

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Game server listening on ${PORT}`);
  console.log('Game WebSocket: /ws');
  console.log('Cinema media WebSocket: /ws-media');
});
