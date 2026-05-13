/**
 * IoT Backend for ESP32-CAM Exam Monitor
 *
 * Features:
 * - Serves a tiny web UI
 * - REST endpoints for device management
 * - Command dispatching and polling
 * - Health monitoring and event tracking
 * - Frame streaming relay
 * - Proper IoT protocols and best practices
 *
 * Run:
 *   npm install express cors dotenv
 *   node index.js
 *
 * Open:
 *   http://localhost:3000
 *
 * API Endpoints:
 *   GET  /health - Backend health check
 *   GET  /healthz - Kubernetes health probe
 *   POST /api/device/command - Queue command for device
 *   GET  /api/device/command - Device polls for pending command
 *   POST /api/device/health - Receive device health report
 *   POST /api/device/event - Receive device event
 *   POST /api/device/frame - Receive JPEG frame
 *   GET  /api/device/stream - MJPEG stream relay
 */

const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DEVICE_ID = 'examcam-001';
const DEVICE_TOKEN = 'CHANGE_THIS_TOKEN';

// IoT Device State
const state = {
  // Command queue
  pendingCommands: new Map(),  // device_id -> command
  
  // Device health and status
  deviceHealth: new Map(),     // device_id -> latest health object
  deviceEvents: new Map(),     // device_id -> array of recent events
  deviceFrames: new Map(),     // device_id -> latest frame buffer
  
  // Connection tracking
  deviceConnections: new Map(), // device_id -> { connected, lastPoll, firstSeen }
  
  // System logs
  logs: [],
  
  // Constants
  MAX_LOGS: 50,
  MAX_EVENTS_PER_DEVICE: 20
};

// =========================
// LOGGING
// =========================
function log(level, message) {
  const timestamp = new Date().toISOString();
  const logLine = `[${timestamp}] [${level}] ${message}`;
  
  state.logs.unshift(logLine);
  state.logs = state.logs.slice(0, state.MAX_LOGS);
  
  console.log(logLine);
}

function info(msg) { log('INFO', msg); }
function warn(msg) { log('WARN', msg); }
function error(msg) { log('ERROR', msg); }

// =========================
// HTTP RESPONSE HELPERS
// =========================
function sendJson(res, code, obj, headers = {}) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
    'Access-Control-Allow-Origin': '*',
    'X-Timestamp': new Date().toISOString(),
    ...headers
  });
  res.end(body);
}

function sendText(res, code, text, contentType = 'text/plain', headers = {}) {
  res.writeHead(code, {
    'Content-Type': contentType,
    'Content-Length': Buffer.byteLength(text),
    'Access-Control-Allow-Origin': '*',
    'X-Timestamp': new Date().toISOString(),
    ...headers
  });
  res.end(text);
}

// =========================
// HTTP REQUEST HELPERS
// =========================
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

// =========================
// DEVICE MANAGEMENT
// =========================
function initDeviceIfNeeded(deviceId) {
  if (!state.deviceConnections.has(deviceId)) {
    state.deviceConnections.set(deviceId, {
      connected: true,
      firstSeen: new Date().toISOString(),
      lastPoll: null,
      lastHealth: null
    });
    state.deviceHealth.set(deviceId, null);
    state.deviceEvents.set(deviceId, []);
  }
}

function recordEvent(deviceId, eventObj) {
  initDeviceIfNeeded(deviceId);
  const events = state.deviceEvents.get(deviceId) || [];
  events.unshift({
    timestamp: new Date().toISOString(),
    ...eventObj
  });
  state.deviceEvents.set(deviceId, events.slice(0, state.MAX_EVENTS_PER_DEVICE));
}

function getDeviceStatus(deviceId) {
  const conn = state.deviceConnections.get(deviceId);
  const health = state.deviceHealth.get(deviceId);
  
  if (!conn) return null;
  
  return {
    device_id: deviceId,
    connected: conn.connected,
    first_seen: conn.firstSeen,
    last_poll: conn.lastPoll,
    last_health: conn.lastHealth,
    current_state: health?.state || 'unknown',
    current_ip: health?.ip || 'unknown',
    streaming: health?.streaming || false
  };
}

// =========================
// COMMAND QUEUEING
// =========================
function queueCommand(deviceId, command, token = null) {
  if (command === '/start_test' || command === '/start-test') {
    if (!token || token !== DEVICE_TOKEN) {
      return { ok: false, error: 'Invalid token for /start_test' };
    }
  }
  
  state.pendingCommands.set(deviceId, command);
  recordEvent(deviceId, {
    event_type: 'command_queued',
    command: command
  });
  
  info(`Command queued for ${deviceId}: ${command}`);
  return { ok: true, command_queued: command, device_id: deviceId };
}

function getNextCommand(deviceId) {
  initDeviceIfNeeded(deviceId);
  const cmd = state.pendingCommands.get(deviceId) || 'NONE';
  
  if (cmd !== 'NONE') {
    state.pendingCommands.delete(deviceId);
  }
  
  // Update connection state
  const conn = state.deviceConnections.get(deviceId);
  if (conn) {
    conn.connected = true;
    conn.lastPoll = new Date().toISOString();
  }
  
  return cmd;
}

// =========================
// HTML DASHBOARD
// =========================
function getPageHtml(req) {
  const protocol = req.headers['x-forwarded-proto'] || 'http';
  const host = req.headers['x-forwarded-host'] || req.headers['host'] || 'localhost';
  const backendUrl = `${protocol}://${host}`;
  
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ESP32 Exam Monitor - Backend</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
    h1 { color: #333; margin-top: 0; }
    .container { max-width: 1200px; margin: 0 auto; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
    .row.full { grid-template-columns: 1fr; }
    .card { background: white; border-radius: 8px; padding: 16px; box-shadow: 0 2px 8px rgba(0,0,0,.1); }
    button { margin: 4px; padding: 10px 14px; border: 0; border-radius: 6px; cursor: pointer; font-weight: 500; }
    .btn-blue { background: #2196F3; color: white; }
    .btn-green { background: #4CAF50; color: white; }
    .btn-red { background: #f44336; color: white; }
    .btn-gray { background: #757575; color: white; }
    button:hover { opacity: 0.9; }
    input { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px; margin: 6px 0; }
    pre { background: #1e1e1e; color: #d4d4d4; padding: 12px; border-radius: 6px; overflow: auto; max-height: 300px; font-size: 12px; }
    .status-box { background: #e3f2fd; padding: 12px; border-left: 4px solid #2196F3; border-radius: 4px; margin: 10px 0; }
    .info { color: #666; font-size: 14px; margin: 0; }
    .badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }
    .badge-green { background: #c8e6c9; color: #2e7d32; }
    .badge-red { background: #ffcdd2; color: #c62828; }
    .badge-blue { background: #bbdefb; color: #1565c0; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
    th { background: #f5f5f5; font-weight: 600; }
    tr:hover { background: #fafafa; }
  </style>
</head>
<body>
  <div class="container">
    <h1>🎥 ESP32 Exam Monitor - Backend Dashboard</h1>
    
    <div class="row">
      <div class="card">
        <h2>Device: ${DEVICE_ID}</h2>
        <div class="status-box">
          <p class="info"><b>Backend API URL:</b><br/>
          <code>${backendUrl}</code></p>
          <p class="info" style="font-size: 12px; margin-top: 8px;">Update ESP32 BACKEND_HOST to point here</p>
        </div>
        <div id="device-status">Loading...</div>
      </div>
      
      <div class="card">
        <h2>Commands</h2>
        <button class="btn-blue" onclick="sendCommand('/health')">/health</button>
        <button class="btn-blue" onclick="sendCommand('/health-detailed')">/health-detailed</button><br/>
        <button class="btn-green" onclick="sendStartTest()">/start_test</button>
        <button class="btn-red" onclick="sendCommand('/abort')">/abort</button>
        <button class="btn-gray" onclick="sendCommand('/end_test')">/end_test</button>
      </div>
    </div>

    <div class="row full">
      <div class="card">
        <h2>Device Health</h2>
        <pre id="health-data">No health data yet</pre>
      </div>
    </div>

    <div class="row full">
      <div class="card">
        <h2>Recent Events</h2>
        <pre id="events-data">No events yet</pre>
      </div>
    </div>

    <div class="row full">
      <div class="card">
        <h2>System Logs</h2>
        <pre id="logs-data">No logs yet</pre>
      </div>
    </div>
  </div>

  <script>
    const DEVICE_ID = '${DEVICE_ID}';
    const DEVICE_TOKEN = '${DEVICE_TOKEN}';

    async function sendCommand(cmd) {
      const res = await fetch('/api/commands', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ device_id: DEVICE_ID, command: cmd })
      });
      const data = await res.json();
      alert(JSON.stringify(data, null, 2));
      refresh();
    }

    async function sendStartTest() {
      const res = await fetch('/api/commands', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ device_id: DEVICE_ID, command: '/start_test', token: DEVICE_TOKEN })
      });
      const data = await res.json();
      alert(JSON.stringify(data, null, 2));
      refresh();
    }

    async function refresh() {
      // Device status
      const statusRes = await fetch('/api/status').then(r => r.json());
      document.getElementById('device-status').innerHTML = 
        statusRes.devices && statusRes.devices[DEVICE_ID] ? 
        \`<pre>\${JSON.stringify(statusRes.devices[DEVICE_ID], null, 2)}\</pre>\` : 
        '<p>Device not connected</p>';

      // Health
      const healthRes = await fetch('/api/health').then(r => r.text());
      document.getElementById('health-data').textContent = healthRes || 'No health data';

      // Events
      const eventsRes = await fetch('/api/events').then(r => r.text());
      document.getElementById('events-data').textContent = eventsRes || 'No events';

      // Logs
      const logsRes = await fetch('/api/logs').then(r => r.text());
      document.getElementById('logs-data').textContent = logsRes || 'No logs';
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>`;
}

// =========================
// REQUEST ROUTER
// =========================
const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  const method = req.method || 'GET';
  const pathname = parsed.pathname || '/';
  const query = parsed.query || {};

  try {
    // Enable CORS preflight
    if (method === 'OPTIONS') {
      res.writeHead(200, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, X-Device-Token'
      });
      return res.end();
    }

    // ===== HEALTH =====
    if (method === 'GET' && pathname === '/health') {
      return sendJson(res, 200, { ok: true, service: 'iot-backend', status: 'running' });
    }

    if (method === 'GET' && pathname === '/healthz') {
      return sendText(res, 200, 'ok');
    }

    // ===== DASHBOARD =====
    if (method === 'GET' && pathname === '/') {
      return sendText(res, 200, getPageHtml(req), 'text/html');
    }

    // ===== STATUS / MONITORING =====
    if (method === 'GET' && pathname === '/api/status') {
      const devices = {};
      for (const [deviceId, conn] of state.deviceConnections) {
        devices[deviceId] = getDeviceStatus(deviceId);
      }
      return sendJson(res, 200, { ok: true, devices });
    }

    if (method === 'GET' && pathname === '/api/health') {
      const deviceId = DEVICE_ID;
      const health = state.deviceHealth.get(deviceId);
      return sendText(res, 200, health ? JSON.stringify(health, null, 2) : 'No health data', 'text/plain');
    }

    if (method === 'GET' && pathname === '/api/events') {
      const deviceId = DEVICE_ID;
      const events = state.deviceEvents.get(deviceId) || [];
      return sendText(res, 200, JSON.stringify(events, null, 2), 'text/plain');
    }

    if (method === 'GET' && pathname === '/api/logs') {
      return sendText(res, 200, state.logs.join('\n'), 'text/plain');
    }

    // ===== DEVICE COMMAND POLLING =====
    if (method === 'GET' && pathname === '/api/device/command') {
      const deviceId = query.device_id;
      if (!deviceId) return sendJson(res, 400, { ok: false, error: 'device_id required' });
      if (deviceId !== DEVICE_ID) return sendJson(res, 403, { ok: false, error: 'device not authorized' });
      
      initDeviceIfNeeded(deviceId);
      const cmd = getNextCommand(deviceId);
      
      info(`Device poll from ${deviceId}: sending command "${cmd}"`);
      
      // Return as plain text for simplicity
      return sendText(res, 200, cmd);
    }

    // ===== BACKEND COMMAND QUEUEING =====
    if (method === 'POST' && pathname === '/api/commands') {
      const body = await readBody(req);
      const data = JSON.parse(body.toString('utf8') || '{}');
      
      const deviceId = data.device_id || DEVICE_ID;
      const command = data.command;
      const token = data.token;

      if (!command) return sendJson(res, 400, { ok: false, error: 'command required' });

      const result = queueCommand(deviceId, command, token);
      return sendJson(res, result.ok ? 200 : 401, result);
    }

    // For backwards compatibility
    if (method === 'POST' && pathname === '/api/device/command') {
      const body = await readBody(req);
      const data = JSON.parse(body.toString('utf8') || '{}');
      
      const deviceId = data.device_id || DEVICE_ID;
      const command = data.command;
      const token = data.token;

      if (!command) return sendJson(res, 400, { ok: false, error: 'command required' });

      const result = queueCommand(deviceId, command, token);
      return sendJson(res, result.ok ? 200 : 401, result);
    }

    // ===== DEVICE HEALTH REPORTS =====
    if (method === 'POST' && pathname === '/api/device/health') {
      const body = await readBody(req);
      const data = JSON.parse(body.toString('utf8') || '{}');
      
      const deviceId = data.device_id || DEVICE_ID;
      initDeviceIfNeeded(deviceId);
      
      state.deviceHealth.set(deviceId, data);
      const conn = state.deviceConnections.get(deviceId);
      if (conn) conn.lastHealth = new Date().toISOString();
      
      recordEvent(deviceId, { event_type: 'health_report', state: data.state, streaming: data.streaming });
      
      info(`Health report from ${deviceId}: state=${data.state}, streaming=${data.streaming}`);
      return sendJson(res, 200, { ok: true });
    }

    // ===== DEVICE EVENTS =====
    if (method === 'POST' && pathname === '/api/device/event') {
      const body = await readBody(req);
      const data = JSON.parse(body.toString('utf8') || '{}');
      
      const deviceId = data.device_id || DEVICE_ID;
      initDeviceIfNeeded(deviceId);
      
      recordEvent(deviceId, data);
      
      info(`Event from ${deviceId}: ${data.event || 'unknown'}`);
      return sendJson(res, 200, { ok: true });
    }

    // ===== FRAME UPLOAD =====
    if (method === 'POST' && pathname === '/api/device/frame') {
      const deviceId = query.device_id || DEVICE_ID;
      const buf = await readBody(req);
      
      initDeviceIfNeeded(deviceId);
      state.deviceFrames.set(deviceId, buf);
      
      info(`Frame received from ${deviceId}: ${buf.length} bytes`);
      return sendJson(res, 200, { ok: true, bytes: buf.length });
    }

    // ===== FRAME RETRIEVAL =====
    if (method === 'GET' && pathname === '/api/device/latest-frame') {
      const deviceId = query.device_id || DEVICE_ID;
      const frame = state.deviceFrames.get(deviceId);
      
      if (!frame) return sendText(res, 404, 'No frame available');
      
      res.writeHead(200, {
        'Content-Type': 'image/jpeg',
        'Content-Length': frame.length,
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store'
      });
      return res.end(frame);
    }

    // ===== MJPEG STREAM RELAY =====
    if (method === 'GET' && pathname === '/api/device/stream') {
      const boundary = '--mjpegboundary\r\n';
      res.writeHead(200, {
        'Content-Type': 'multipart/x-mixed-replace; boundary=mjpegboundary',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
        'Connection': 'close'
      });

      const deviceId = DEVICE_ID;
      
      const frameInterval = setInterval(() => {
        const frame = state.deviceFrames.get(deviceId);
        if (frame) {
          res.write(boundary);
          res.write('Content-Type: image/jpeg\r\n');
          res.write(`Content-Length: ${frame.length}\r\n`);
          res.write(`X-Timestamp: ${Date.now()}\r\n\r\n`);
          res.write(frame);
          res.write('\r\n');
        }
      }, 50);  // 20 FPS

      req.on('close', () => {
        clearInterval(frameInterval);
      });
      return;
    }

    // Fallback
    return sendJson(res, 404, { ok: false, error: 'Not found' });

  } catch (err) {
    error(`Request error: ${err.message}`);
    return sendJson(res, 500, { ok: false, error: err.message });
  }
});

server.listen(PORT, "0.0.0.0", () => {
  info(`Backend service listening on port ${PORT}`);
});