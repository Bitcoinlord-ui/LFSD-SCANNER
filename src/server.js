import express from 'express';
import { WebSocket } from 'ws';
import fs from 'node:fs';
import path from 'node:path';

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const APP_ID = process.env.DERIV_APP_ID || '1089';
const SYMBOLS = (process.env.DERIV_SYMBOLS || 'cryBTCUSD,frxXAUUSD').split(',').map(s => s.trim()).filter(Boolean);
const DATA_DIR = process.env.DATA_DIR || '/data';
const HISTORY_FILE = path.join(DATA_DIR, 'signal-history.json');

function loadSignalHistory() {
  try {
    if (!fs.existsSync(HISTORY_FILE)) return [];
    const parsed = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
    return Array.isArray(parsed) ? parsed : [];
  } catch (err) {
    console.error('Signal history load failed:', err.message);
    return [];
  }
}

let signalHistory = loadSignalHistory();
const signalKeys = new Set(signalHistory.map(x => x.signalKey).filter(Boolean));

function persistSignal() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const tmp = `${HISTORY_FILE}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(signalHistory, null, 2), 'utf8');
    fs.renameSync(tmp, HISTORY_FILE);
    return true;
  } catch (err) {
    console.error('Signal history persist failed:', err.message);
    return false;
  }
}

const assets = Object.fromEntries(SYMBOLS.map(symbol => [symbol, {
  symbol,
  name: symbol === 'frxXAUUSD' ? 'GOLD' : symbol === 'cryBTCUSD' ? 'BTC/USD' : symbol,
  connection: 'CONNECTING', price: null, change24h: null,
  candles5: [], candles15: [], activeZone: null, setupStatus: 'WAITING', signal: null,
  lastUpdate: null, lastSignalKey: null, history: []
}]));

const state = {
  connection: 'CONNECTING', assets, activeAsset: SYMBOLS[0], liveTrading: false,
  riskPerTrade: 1, maxRiskPerTrade: 10, minRR: 2, lot: 0.02,
  history: signalHistory, lastUpdate: null, uptimeStarted: new Date().toISOString(),
  historyPersistence: { file: HISTORY_FILE, loaded: signalHistory.length, durable: false },
  patConnected: false
};

let deriv = null;
let token = null;
const tf = (ms, t) => Math.floor(t / ms) * ms;
const candle = (ms, p, t) => ({ time: tf(ms, t), open: p, high: p, low: p, close: p });

function addTick(asset, p, t) {
  asset.price = p; asset.lastUpdate = new Date().toISOString(); state.lastUpdate = asset.lastUpdate;
  for (const [arr, ms] of [[asset.candles5, 300000], [asset.candles15, 900000]]) {
    const c = arr.at(-1), k = tf(ms, t);
    if (!c || c.time !== k) { arr.push(candle(ms, p, t)); if (arr.length > 300) arr.shift(); }
    else { c.high = Math.max(c.high, p); c.low = Math.min(c.low, p); c.close = p; }
  }
  analyze(asset);
}

function analyze(a) {
  const m15 = a.candles15, m5 = a.candles5;
  if (m15.length < 15 || m5.length < 15) return;
  const z = m15.at(-2), prior = m15.at(-3);
  if (!a.activeZone && z) {
    const range = z.high - z.low;
    if (range > 0 && Math.abs(z.close - z.open) <= range * 0.45) {
      a.activeZone = { zoneId: `${a.symbol}-${z.time}`, type: z.close >= z.open ? 'demand' : 'supply', low: z.low, high: z.high, formationTime: z.time, sourceCandle: z.time, status: 'ACTIVE' };
      a.setupStatus = 'ZONE IDENTIFIED';
    }
  }
  const zone = a.activeZone;
  if (!zone || a.price == null) return;
  const invalid = zone.type === 'demand' ? a.price < zone.low : a.price > zone.high;
  if (invalid) { zone.status = 'INVALIDATED'; a.activeZone = null; a.setupStatus = 'RESET'; a.signal = null; return; }
  if (a.price >= zone.low && a.price <= zone.high) a.setupStatus = 'ZONE ENTRY';

  const r = m5.at(-2), q = m5.at(-3);
  if (!r || !q) return;
  const bullishReject = r.close > r.open && r.low < q.low && r.close > q.close;
  const bearishReject = r.close < r.open && r.high > q.high && r.close < q.close;
  const rejection = zone.type === 'demand' ? bullishReject : bearishReject;
  if (!rejection) return;
  a.setupStatus = '5M REJECTION';

  const bos = zone.type === 'demand' ? r.close > q.high : r.close < q.low;
  if (!bos) return;
  a.setupStatus = '5M BOS/CHOCH';

  const closed15 = m15.at(-2);
  const zoneBreakout = zone.type === 'demand' ? closed15.close > zone.high : closed15.close < zone.low;
  if (zoneBreakout) { a.activeZone = null; a.setupStatus = 'RESET'; return; }

  const key = `${zone.zoneId}-${r.time}-${zone.type}`;
  if (a.lastSignalKey === key || signalKeys.has(key)) return;
  const entry = a.price;
  const buffer = Math.max((zone.high - zone.low) * 0.1, entry * 0.0002);
  const sl = zone.type === 'demand' ? zone.low - buffer : zone.high + buffer;
  const risk = Math.abs(entry - sl);
  if (!risk) return;
  const tp = zone.type === 'demand' ? entry + risk * 2 : entry - risk * 2;
  const signal = { signalKey: key, asset: a.name, symbol: a.symbol, side: zone.type === 'demand' ? 'BUY' : 'SELL', entry, SL: sl, TP: tp, RR: 2, lot: state.lot, confidence: 70, timestamp: new Date().toISOString(), setup: '15M Zone → 5M Rejection → 5M BOS/CHOCH', result: 'OPEN', status: 'QUALIFIED' };

  a.signal = signal; a.lastSignalKey = key; a.setupStatus = 'QUALIFIED SIGNAL';
  signalHistory.unshift(signal);
  signalKeys.add(key);
  state.history = signalHistory;
  const durable = persistSignal();
  state.historyPersistence.durable = durable;
  state.historyPersistence.loaded = signalHistory.length;
  a.history.unshift(signal);
}

function connect() {
  state.connection = 'CONNECTING';
  deriv = new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`);
  deriv.on('open', () => {
    state.connection = 'LIVE';
    for (const symbol of SYMBOLS) {
      const a = assets[symbol]; a.connection = 'LIVE';
      deriv.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
      deriv.send(JSON.stringify({ ticks_history: symbol, adjust_start_time: 1, count: 300, end: 'latest', style: 'ticks' }));
    }
  });
  deriv.on('message', raw => {
    try {
      const m = JSON.parse(raw);
      const symbol = m.echo_req?.ticks || m.echo_req?.ticks_history || m.tick?.symbol;
      const a = assets[symbol];
      if (!a) return;
      if (m.tick) addTick(a, Number(m.tick.quote), Number(m.tick.epoch) * 1000);
      if (m.history?.times) for (let i = 0; i < m.history.times.length; i++) addTick(a, Number(m.history.prices[i]), Number(m.history.times[i]) * 1000);
    } catch { state.connection = 'DATA_ERROR'; }
  });
  deriv.on('close', () => { state.connection = 'RECONNECTING'; for (const a of Object.values(assets)) a.connection = 'RECONNECTING'; setTimeout(connect, 3000); });
  deriv.on('error', () => { state.connection = 'RECONNECTING'; });
}

app.get('/', (req, res) => {
  try {
    let html = fs.readFileSync(path.join(process.cwd(), 'public', 'index.html'), 'utf8');
    html = html.replace(".filter(x=>x.status==='QUALIFIED').slice(0,20)", ".filter(x=>x.status==='QUALIFIED')");
    res.type('html').send(html);
  } catch { res.sendFile(path.join(process.cwd(), 'public', 'index.html')); }
});
app.use(express.static('public'));

app.get('/api/state', (req, res) => {
  const safe = JSON.parse(JSON.stringify(state));
  for (const a of Object.values(safe.assets)) { a.candles5 = a.candles5.slice(-80); a.candles15 = a.candles15.slice(-80); }
  res.json(safe);
});
app.get('/api/history', (req, res) => res.json({ count: signalHistory.length, signals: signalHistory }));
app.post('/api/live', (req, res) => { state.liveTrading = Boolean(req.body?.enabled); res.json({ liveTrading: state.liveTrading }); });

// PAT support: accepts either {pat:"..."} or the legacy {token:"..."} field.
// PATs are validated against Deriv's authenticated REST API when a current
// DERIV_APP_ID is configured. The PAT is never persisted to signal history.
app.post('/api/token', async (req, res) => {
  const supplied = typeof req.body?.pat === 'string' ? req.body.pat.trim() : typeof req.body?.token === 'string' ? req.body.token.trim() : '';
  if (!supplied) return res.status(400).json({ ok: false, error: 'Enter your Deriv PAT first.' });
  if (!/^PAT/i.test(supplied)) return res.status(400).json({ ok: false, error: 'This field expects a Deriv PAT (Personal Access Token) beginning with PAT.' });
  if (APP_ID === '1089') return res.status(400).json({ ok: false, error: 'PAT authentication needs your current Deriv App ID. Set DERIV_APP_ID in Railway, then reconnect the PAT.' });
  try {
    const r = await fetch('https://api.derivws.com/trading/v1/options/accounts', { headers: { 'Authorization': `Bearer ${supplied}`, 'Deriv-App-ID': APP_ID, 'Content-Type': 'application/json' } });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) {
      const message = body?.errors?.[0]?.message || `Deriv rejected the PAT (HTTP ${r.status}).`;
      return res.status(r.status === 401 ? 401 : 400).json({ ok: false, error: message });
    }
    token = supplied;
    state.patConnected = true;
    return res.json({ ok: true, authenticated: true, storedInMemory: true, accountCount: Array.isArray(body?.data) ? body.data.length : undefined });
  } catch (err) {
    return res.status(502).json({ ok: false, error: `Unable to reach Deriv authentication API: ${err.message}` });
  }
});
app.post('/api/confirm', (req, res) => {
  if (!state.liveTrading) return res.status(400).json({ error: 'Live trading is disabled' });
  if (!token) return res.status(400).json({ error: 'Deriv PAT not connected' });
  return res.status(403).json({ error: 'Fresh per-trade confirmation is required; unattended real-money execution is disabled.' });
});
app.get('/health', (req, res) => res.json({ ok: true, connection: state.connection, assets: SYMBOLS, uptime: process.uptime(), signalHistoryCount: signalHistory.length, historyFile: HISTORY_FILE, patConnected: state.patConnected }));

app.listen(PORT, () => { console.log(`LFSD scanner listening on ${PORT}`); console.log(`Loaded ${signalHistory.length} persisted qualified signals from ${HISTORY_FILE}`); connect(); });
