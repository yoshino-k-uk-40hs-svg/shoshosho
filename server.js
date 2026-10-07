const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const PUBLIC_DIR = path.join(__dirname, 'public');
const rooms = new Map();

// HTMLをRailwayから直接配信
const server = http.createServer((req, res) => {
  let pathname;
  try {
    pathname = decodeURIComponent(new URL(req.url, `http://${req.headers.host || 'localhost'}`).pathname);
  } catch {
    res.writeHead(400);
    return res.end('Bad Request');
  }

  if (pathname === '/') pathname = '/index.html';

  const filePath = path.normalize(path.join(PUBLIC_DIR, pathname));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    return res.end('Forbidden');
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      return res.end('Not Found');
    }

    const types = {
      '.html': 'text/html; charset=utf-8',
      '.js': 'text/javascript; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.json': 'application/json; charset=utf-8'
    };

    res.writeHead(200, {
      'Content-Type': types[path.extname(filePath)] || 'application/octet-stream',
      'Cache-Control': 'no-cache'
    });
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server });

function roomInfo(room) {
  return { type: 'room', players: room.clients.size };
}

function broadcast(room, message) {
  const data = JSON.stringify(message);
  for (const ws of room.clients) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(data); } catch (_) {}
    }
  }
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const code = (url.searchParams.get('room') || '').trim().toUpperCase();

  if (!code) return ws.close(1008, 'room required');

  if (!rooms.has(code)) {
    rooms.set(code, {
      clients: new Set(),
      state: null,
      finished: false
    });
  }

  const room = rooms.get(code);

  if (room.finished) {
    ws.send(JSON.stringify({
      type: 'error',
      message: 'このルームの対局は終了しています。'
    }));
    return ws.close(1000, 'game finished');
  }

  if (room.clients.size >= 2) {
    ws.send(JSON.stringify({
      type: 'error',
      message: 'このルームは満員です。'
    }));
    return ws.close(1008, 'room full');
  }

  room.clients.add(ws);
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.send(JSON.stringify(roomInfo(room)));

  if (room.state) {
    ws.send(JSON.stringify({ type: 'state', ...room.state }));
  }

  broadcast(room, roomInfo(room));

  ws.on('message', raw => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch (_) {
      return;
    }

    // ブラウザ側のハートビート
    if (message.type === 'ping') {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
      return;
    }

    // 対局状態を2人に同期
    if (message.type === 'state' && !room.finished) {
      room.state = {
        board: message.board,
        turn: message.turn,
        hands: message.hands,
        lastMove: message.lastMove || null
      };

      broadcast(room, { type: 'state', ...room.state });
      return;
    }

    // 対局終了
    if (message.type === 'gameOver') {
      room.finished = true;
      broadcast(room, {
        type: 'gameOver',
        reason: message.reason || '対局終了'
      });
    }
  });

  ws.on('close', () => {
    room.clients.delete(ws);

    if (room.clients.size > 0) {
      broadcast(room, roomInfo(room));
    } else if (!room.finished) {
      // 一時切断しても再接続できるよう60秒保持
      setTimeout(() => {
        if (room.clients.size === 0 && rooms.get(code) === room) {
          rooms.delete(code);
        }
      }, 60000);
    }
  });

  ws.on('error', () => {});
});

// WebSocketの接続維持
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 20000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Online shogi server on port ${PORT}`);
});
