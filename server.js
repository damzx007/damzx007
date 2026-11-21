'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');
const PORT = Number(process.env.PORT || 3000);
const indexPath = path.join(__dirname, 'index.html');
const rooms = new Map();
const joinRates = new Map();

const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if (pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    return res.end('{"ok":true}');
  }
  if ((pathname === '/' || pathname === '/index.html') && req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    return fs.createReadStream(indexPath).pipe(res);
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});
const wss = new WebSocketServer({ server, path: '/signal', maxPayload: 128 * 1024 });
function send(ws, value) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(value));
}
function getRoom(code) { return rooms.get(code); }
function roomFor(ws) { return ws.roomCode ? getRoom(ws.roomCode) : null; }
function rateLimited(ip) {
  const now = Date.now();
  const times = (joinRates.get(ip) || []).filter(t => now - t < 60_000);
  times.push(now); joinRates.set(ip, times);
  return times.length > 18;
}
wss.on('connection', (ws, req) => {
  ws.roomCode = null;
  ws.role = null;
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', message: 'bad-message' }); }
    if (!msg || typeof msg.type !== 'string') return;
    if (msg.type === 'create' || msg.type === 'join') {
      if (ws.roomCode) return;
      if (rateLimited(ip)) return send(ws, { type: 'error', message: 'slow-down' });
      const code = String(msg.room || '');
      if (!/^\d{6}$/.test(code)) return send(ws, { type: 'error', message: 'bad-room' });
      let room = getRoom(code);
      if (msg.type === 'create') {
        if (room) return send(ws, { type: 'code-taken' });
        room = { peers: [], ready: new Set() };
        rooms.set(code, room);
      } else if (!room) return send(ws, { type: 'not-found' });
      if (room.peers.length >= 2) return send(ws, { type: 'room-full' });
      ws.roomCode = code;
      ws.role = room.peers.length === 0 ? 'host' : 'guest';
      room.peers.push(ws);
      send(ws, { type: ws.role === 'host' ? 'waiting' : 'joined', room: code, role: ws.role });
      if (room.peers.length === 2) room.peers.forEach(peer => send(peer, { type: 'room-ready', role: peer.role }));
      return;
    }
    const room = roomFor(ws);
    if (!room) return;
    if (msg.type === 'media-ready') {
      room.ready.add(ws);
      if (room.peers.length === 2 && room.peers.every(peer => room.ready.has(peer))) {
        room.peers.forEach(peer => send(peer, { type: 'media-ready' }));
      }
      return;
    }
    if (msg.type === 'media-off') {
      room.ready.delete(ws);
      room.peers.filter(peer => peer !== ws).forEach(peer => send(peer, { type: 'peer-left' }));
      return;
    }
    if (msg.type === 'signal' && msg.data && ['offer', 'answer', 'candidate', 'sync-capture'].includes(msg.data.type)) {
      room.peers.filter(peer => peer !== ws).forEach(peer => send(peer, { type: 'signal', data: msg.data }));
    }
  });
  ws.on('close', () => {
    const room = roomFor(ws);
    if (!room) return;
    room.peers = room.peers.filter(peer => peer !== ws);
    room.ready.clear();
    room.peers.forEach(peer => send(peer, { type: 'peer-left' }));
    if (room.peers.length === 0) rooms.delete(ws.roomCode);
  });
});
server.listen(PORT, '0.0.0.0', () => console.log(`Together Frame listening on ${PORT}`));
