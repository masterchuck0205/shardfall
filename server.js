'use strict';
/* Shardfall online server.
   The game itself runs deterministic lockstep on both players' computers; this server only
   creates 2-player rooms and relays validated messages between the two players. */
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const compression = require('compression');
const { Server } = require('socket.io');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const CORS_ORIGIN = (process.env.CORS_ORIGIN || '*').split(',').map(s => s.trim()).filter(Boolean);
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ';          // same letters the game's code box expects
const KINDS = new Set(['i', 'h', 'rdy', 'hi', 'p', 'q', 'go', 'rm']);   // lockstep input, desync hash, handshake, ping, start, rematch
const MAX_ROOMS = 500;

const app = express();
app.disable('x-powered-by');
app.use(compression());
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: CORS_ORIGIN.includes('*') ? '*' : CORS_ORIGIN },
  maxHttpBufferSize: 1e5, pingInterval: 8000, pingTimeout: 12000, transports: ['websocket', 'polling']
});
const rooms = new Map();    // code -> { host: socketId, guest: socketId|null }

app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size }));
app.use(express.static(path.join(__dirname, 'public'), { maxAge: '10m', setHeaders: (res, p) => { if (p.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache'); } }));

function genCode() {
  for (let n = 0; n < 200; n++) {
    let c = ''; const b = crypto.randomBytes(5);
    for (let i = 0; i < 5; i++) c += ALPHABET[b[i] % ALPHABET.length];
    if (!rooms.has(c)) return c;
  }
  return null;
}
function allow(sock) {                                   // token bucket: lockstep sends ~60 messages/s per player
  const d = sock.data, t = Date.now();
  d.tokens = Math.min(400, (d.tokens === undefined ? 400 : d.tokens) + ((t - (d.tt || t)) / 1000) * 250); d.tt = t;
  if (d.tokens < 1) return false; d.tokens -= 1; return true;
}
function validMsg(m) {
  if (!m || typeof m !== 'object' || !KINDS.has(m.k)) return false;
  try { return JSON.stringify(m).length < 1500; } catch (e) { return false; }
}
function leave(sock) {
  const code = sock.data.room; if (!code) return;
  sock.data.room = null; sock.leave(code);
  const r = rooms.get(code); if (!r) return;
  if (r.host === sock.id) {                              // host left: the room is over
    if (r.guest) io.to(r.guest).emit('peer:left');
    rooms.delete(code);
  } else if (r.guest === sock.id) {                      // guest left: host keeps the room open
    r.guest = null; io.to(r.host).emit('peer:left');
  }
}

io.on('connection', sock => {
  sock.on('room:create', (msg, ack) => {
    if (typeof ack !== 'function') return;
    if (sock.data.room) return ack({ ok: false, error: 'Already in a room.' });
    if (rooms.size >= MAX_ROOMS) return ack({ ok: false, error: 'Server is full, try again soon.' });
    const code = genCode(); if (!code) return ack({ ok: false, error: 'Could not open a room.' });
    rooms.set(code, { host: sock.id, guest: null }); sock.join(code); sock.data.room = code;
    ack({ ok: true, code });
  });
  sock.on('room:join', (msg, ack) => {
    if (typeof ack !== 'function') return;
    if (sock.data.room) return ack({ ok: false, error: 'Already in a room.' });
    const code = String((msg && msg.code) || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 6);
    const r = rooms.get(code);
    if (!r) return ack({ ok: false, error: 'No room with that code.' });
    if (r.guest) return ack({ ok: false, error: 'That room is already full.' });
    r.guest = sock.id; sock.join(code); sock.data.room = code;
    ack({ ok: true }); io.to(r.host).emit('peer:join');
  });
  sock.on('room:leave', () => leave(sock));
  sock.on('disconnect', () => leave(sock));
  sock.on('m', m => {                                    // relay to the other player in the room only
    const code = sock.data.room; if (!code) return; if (!allow(sock)) { if (process.env.DEBUG_DROPS) console.log('DROP rate', sock.data.tokens); return; } if (!validMsg(m)) { if (process.env.DEBUG_DROPS) console.log('DROP invalid', JSON.stringify(m).slice(0,200)); return; }
    sock.to(code).emit('m', m);
  });
});

server.listen(PORT, () => console.log('Shardfall server on port ' + PORT));
process.on('SIGTERM', () => { io.close(); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); });
module.exports = { server, io, rooms };
