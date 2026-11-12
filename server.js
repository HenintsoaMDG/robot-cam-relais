// Relais Robot CAM : ESP32 <-> navigateur, via WebSocket
// Variables d'environnement a definir sur Render :
//   VIEW_PASS : mot de passe pour piloter depuis la page web
//   ROBOT_KEY : cle secrete que l'ESP32 envoie pour se connecter
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const VIEW_PASS = process.env.VIEW_PASS || 'changeme';
const ROBOT_KEY = process.env.ROBOT_KEY || 'changeme';
const PAGE = fs.readFileSync(path.join(__dirname, 'index.html'));

// comparaison de secrets sans fuite de temps
function same(a, b) {
  const ha = crypto.createHash('sha256').update(a).digest();
  const hb = crypto.createHash('sha256').update(b).digest();
  return crypto.timingSafeEqual(ha, hb);
}

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.end('ok'); return; }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(PAGE);
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 512 * 1024 });
let robot = null;
const viewers = new Set();

function toRobot(msg) {
  if (robot && robot.readyState === 1) robot.send(msg);
}
function toViewers(msg) {
  for (const v of viewers) if (v.readyState === 1) v.send(msg);
}
// le robot n'envoie la video que si quelqu'un regarde (economise les donnees 4G)
function syncStreaming() { toRobot(viewers.size > 0 ? 'V1' : 'V0'); }

server.on('upgrade', (req, socket, head) => {
  const u = new URL(req.url, 'http://localhost');
  const key = u.searchParams.get('key') || '';
  if (u.pathname === '/robot' && same(key, ROBOT_KEY)) {
    wss.handleUpgrade(req, socket, head, onRobot);
  } else if (u.pathname === '/view' && same(key, VIEW_PASS)) {
    wss.handleUpgrade(req, socket, head, onViewer);
  } else {
    socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
    socket.destroy();
  }
});

function onRobot(ws) {
  if (robot) robot.terminate();          // un seul robot a la fois
  robot = ws;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  toViewers('robot:1');
  syncStreaming();
  ws.on('message', (data, isBinary) => {
    ws.isAlive = true;                    // tout message prouve que le robot est vivant
    if (!isBinary) return;
    for (const v of viewers) {
      // on saute l'image si le client est en retard (evite l'accumulation)
      if (v.readyState === 1 && v.bufferedAmount < 300000) v.send(data, { binary: true });
    }
  });
  ws.on('close', () => {
    if (robot === ws) { robot = null; toViewers('robot:0'); }
  });
  ws.on('error', () => {});
}

function onViewer(ws) {
  viewers.add(ws);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.send(robot ? 'robot:1' : 'robot:0');
  syncStreaming();
  ws.on('message', (data, isBinary) => {
    ws.isAlive = true;
    if (isBinary) return;
    const m = data.toString();
    // seules les commandes valides passent : F,200  B,150  L,200  R,200  S,0
    if (/^[FBLRS],\d{1,3}$/.test(m) || m === 'A') toRobot(m);   // 'A' = accuse d'image
  });
  ws.on('close', () => { viewers.delete(ws); syncStreaming(); });
  ws.on('error', () => {});
}

// ping regulier : detecte les connexions mortes et garde le tunnel ouvert
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log('Relais Robot CAM sur le port ' + PORT));
