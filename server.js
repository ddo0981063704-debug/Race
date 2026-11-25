// Приёмник подарков TikTok LIVE для «Гонки подарков».
// Открывает игру (index.html) и передаёт в неё подарки из эфира.
// Запуск: npm install && node server.js   (на Render всё делается само)
const http = require('http');
const fs = require('fs');
const path = require('path');

let TL = {};
try { TL = require('tiktok-live-connector'); }
catch (e) { console.error('Не найден пакет tiktok-live-connector. Выполните: npm install', e.message); }
const Conn = TL.TikTokLiveConnection || TL.WebcastPushConnection;
const WE = TL.WebcastEvent || {}, CE = TL.ControlEvent || {};
const EV = {
  GIFT: WE.GIFT || 'gift',
  END: WE.STREAM_END || 'streamEnd',
  DISC: CE.DISCONNECTED || 'disconnected',
  ERR: CE.ERROR || 'error',
};
const PORT = process.env.PORT || 3000;
const GAME = path.join(__dirname, 'index.html');
const rooms = new Map();
const cool = new Map(), fails = new Map(), block = new Map(); // время последней попытки и число неудач подряд (по нику) // ник → { clients, conn, state, retry, stop }

function send(res, obj) { try { res.write('data: ' + JSON.stringify(obj) + '\n\n'); } catch (e) {} }
function broadcast(room, obj) { for (const r of room.clients) send(r, obj); }
function status(room, state, message) {
  room.state = { event: 'status', data: { state, message } };
  broadcast(room, room.state);
  console.log('[@' + room.nick + '] ' + message);
}

function pickGift(d) {
  const det = d.giftDetails || {}, ext = d.extendedGiftInfo || {}, u = d.user || {};
  return {
    user: { uniqueId: u.uniqueId || d.uniqueId, nickname: u.nickname || d.nickname },
    giftId: d.giftId,
    giftName: det.giftName || d.giftName || ext.name || '',
    giftType: det.giftType != null ? det.giftType : d.giftType,
    repeatCount: d.repeatCount || 1,
    repeatEnd: !!d.repeatEnd,
    groupId: d.groupId != null ? String(d.groupId) : undefined,
  };
}

function connect(room) {
  clearTimeout(room.retry);
  if (!Conn) { status(room, 'err', 'на сервере не установлен tiktok-live-connector'); return; }
  const opts = {};
  if (process.env.SIGN_API_KEY) opts.signApiKey = process.env.SIGN_API_KEY;
  const conn = new Conn(room.nick, opts);
  room.conn = conn;
  status(room, 'wait', 'подключаюсь к эфиру @' + room.nick + '…');

  conn.on(EV.GIFT, (d) => { if (room.conn === conn) broadcast(room, { event: 'gift', data: pickGift(d) }); });
  conn.on(EV.END, () => { if (room.conn === conn) retryLater(room, 'эфир завершён — жду новый эфир…', 30000); });
  conn.on(EV.DISC, () => { if (room.conn === conn) retryLater(room, 'связь с эфиром потеряна — переподключаюсь…', 10000); });
  conn.on(EV.ERR, (e) => console.error('[@' + room.nick + '] ошибка:', e && (e.message || e)));

  cool.set(room.nick, Date.now());
  conn.connect().then(() => {
    if (room.conn !== conn) return;
    fails.set(room.nick, 0);
    status(room, 'on', '✔ подключено к эфиру @' + room.nick);
  }).catch((err) => {
    if (room.conn !== conn) return;
    const m = String((err && (err.message || err.info)) || err);
    const n = (fails.get(room.nick) || 0) + 1; fails.set(room.nick, n);
    if (/rate.?limit|too many/i.test(m)) {
      // дневной лимит сервиса подписи: не тратим попытки, ждём долго
      block.set(room.nick, Date.now() + 30 * 60000);
      return retryLater(room, '⚠ лимит подключений на сегодня исчерпан (Rate Limited). Повторю через 30 мин. ' +
        (process.env.SIGN_API_KEY ? '' : 'Совет: добавьте свой бесплатный ключ SIGN_API_KEY.'), 30 * 60000);
    }
    const offline = /offline|not.{0,10}live|isn.?t online|ended|room ?id/i.test(m);
    const wait = Math.min(5 * 60000, 60000 * Math.pow(2, Math.min(n - 1, 3))); // 1, 2, 4, 5 мин
    retryLater(room, (offline ? '@' + room.nick + ' сейчас не в эфире — жду начала эфира'
                              : 'не удалось подключиться: ' + m.slice(0, 140)) + ' (повтор через ' + Math.round(wait / 60000) + ' мин)', wait);
  });
}

function retryLater(room, msg, ms) {
  const c = room.conn; room.conn = null;
  try { c && c.disconnect && c.disconnect(); } catch (e) {}
  status(room, 'wait', msg);
  clearTimeout(room.retry);
  if (room.clients.size) room.retry = setTimeout(() => { room.retry = null; if (room.clients.size) connect(room); }, ms);
}

function join(nick, res) {
  let room = rooms.get(nick);
  if (!room) { room = { nick, clients: new Set(), conn: null, state: null }; rooms.set(nick, room); }
  clearTimeout(room.stop);
  room.clients.add(res);
  if (room.state) send(res, room.state);
  // не подключаемся чаще, чем разрешает пауза (иначе быстро кончается дневной лимит)
  if (!room.conn && !room.retry) {
    const last = cool.get(nick) || 0, n = fails.get(nick) || 0;
    const gap = n ? Math.min(5 * 60000, 60000 * Math.pow(2, Math.min(n - 1, 3))) : 15000;
    const left = Math.max(last + gap, block.get(nick) || 0) - Date.now();
    if (left > 0) { room.retry = setTimeout(() => { room.retry = null; if (room.clients.size && !room.conn) connect(room); }, left);
      if (!room.state) status(room, 'wait', 'жду ' + Math.ceil(left / 1000) + ' с перед новой попыткой…'); }
    else connect(room);
  }
  return room;
}

function leave(room, res) {
  room.clients.delete(res);
  if (room.clients.size) return;
  room.stop = setTimeout(() => {
    if (room.clients.size) return;
    clearTimeout(room.retry);
    const c = room.conn; room.conn = null;
    try { c && c.disconnect && c.disconnect(); } catch (e) {}
    rooms.delete(room.nick);
    console.log('[@' + room.nick + '] отключено (игра закрыта)');
  }, 60000);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/health' || url.pathname === '/ping') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ race: true, connector: !!Conn }));
  }
  if (url.pathname === '/events') {
    const nick = String(url.searchParams.get('u') || '').trim().replace(/^@/, '');
    if (!/^[\w.]{1,40}$/.test(nick)) { res.writeHead(400); return res.end('bad nick'); }
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.write('retry: 5000\n\n');
    const room = join(nick, res);
    const ka = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 20000);
    req.on('close', () => { clearInterval(ka); leave(room, res); });
    return;
  }
  const STATIC = { '/manifest.webmanifest': 'application/manifest+json', '/icon-180.png': 'image/png', '/icon-192.png': 'image/png', '/icon-512.png': 'image/png' };
  if (STATIC[url.pathname]) {
    return fs.readFile(path.join(__dirname, url.pathname.slice(1)), (err, buf) => {
      if (err) { res.writeHead(404); return res.end('not found'); }
      res.writeHead(200, { 'Content-Type': STATIC[url.pathname], 'Cache-Control': 'public, max-age=86400' });
      res.end(buf);
    });
  }
  if (url.pathname === '/' || url.pathname === '/index.html') {
    return fs.readFile(GAME, (err, buf) => {
      if (err) { res.writeHead(500); return res.end('index.html не найден рядом с server.js'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(buf);
    });
  }
  res.writeHead(404); res.end('not found');
});

server.listen(PORT, () => console.log('Гонка подарков: http://localhost:' + PORT));
