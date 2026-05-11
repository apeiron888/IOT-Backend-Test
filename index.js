/**
 * Minimal ESP32-CAM backend + frontend dashboard
 *
 * Features:
 * - Serves a tiny web UI
 * - Accepts commands for ESP32
 * - ESP32 polls for pending command
 * - Receives health/events/frame uploads
 * - Shows latest frame in browser
 *
 * Run:
 *   node server.js
 *
 * Open:
 *   http://localhost:3000
 *
 * ESP32 endpoints expected:
 *   GET  /api/device/command?device_id=examcam-001
 *   POST /api/device/health
 *   POST /api/device/event
 *   POST /api/device/frame?device_id=examcam-001   (JPEG body)
 */

const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DEVICE_ID = 'examcam-001';
const DEVICE_TOKEN = 'CHANGE_THIS_TOKEN';

const state = {
  pendingCommand: null,
  lastHealth: null,
  lastEvent: null,
  lastFrame: null,
  lastFrameType: null,
  logs: [],
  deviceConnected: false,
  lastPoll: null
};

function log(line) {
  const msg = `[${new Date().toISOString()}] ${line}`;
  state.logs.unshift(msg);
  state.logs = state.logs.slice(0, 30);
  console.log(msg);
}

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
    'Access-Control-Allow-Origin': '*'
  });
  res.end(body);
}

function sendText(res, code, text, contentType = 'text/plain') {
  res.writeHead(code, {
    'Content-Type': contentType,
    'Content-Length': Buffer.byteLength(text),
    'Access-Control-Allow-Origin': '*'
  });
  res.end(text);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function pageHtml() {
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ESP32 Exam Dashboard</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
    .card { background: white; border-radius: 12px; padding: 16px; box-shadow: 0 2px 10px rgba(0,0,0,.08); }
    button { margin: 4px; padding: 10px 14px; border: 0; border-radius: 8px; cursor: pointer; }
    .blue { background: #1976d2; color: white; }
    .green { background: #2e7d32; color: white; }
    .red { background: #c62828; color: white; }
    .gray { background: #555; color: white; }
    pre { white-space: pre-wrap; word-break: break-word; background: #111; color: #0f0; padding: 12px; border-radius: 8px; max-height: 320px; overflow: auto; }
    img { width: 100%; max-width: 640px; border-radius: 10px; background: #ddd; }
    input { width: 100%; padding: 10px; border-radius: 8px; border: 1px solid #ccc; margin: 6px 0; }
    .muted { color: #666; font-size: 14px; }
  </style>
</head>
<body>
  <h1>ESP32 Exam Dashboard</h1>
  <p class="muted">Device: <b>${DEVICE_ID}</b> | Backend: <b>http://localhost:${PORT}</b></p>

  <div class="row">
    <div class="card">
      <h2>Commands</h2>
      <button class="blue" onclick="sendCmd('/health')">/health</button>
      <button class="gray" onclick="sendCmd('/health-detailed')">/health-detailed</button>
      <br/>
      <input id="token" placeholder="token for /start-test" value="${DEVICE_TOKEN}" />
      <button class="green" onclick="sendStartTest()">/start-test</button>
      <button class="red" onclick="sendCmd('/abort')">/abort</button>
      <button class="gray" onclick="sendCmd('/end-test')">/end-test</button>
      <p class="muted">ESP32 polls this backend for the next command.</p>
    </div>

    <div class="card">
      <h2>Latest Frame (Direct from ESP32)</h2>
      <p class="muted">Connect directly to ESP32 at its IP on port 81</p>
      <pre id="streamInfo">To view stream:\n\nhttp://192.168.1.12:81/stream\n\nor click button below:</pre>
      <button class="blue" onclick="window.open('http://192.168.1.12:81/stream', '_blank')">Open Direct Stream</button>
      <p class="muted">⚠️ Replace 192.168.1.12 with your ESP32's actual IP if different</p>
    </div>
  </div>

  <div class="row" style="margin-top:16px;">
    <div class="card">
      <h2>Device Status</h2>
      <pre id="status">Loading...</pre>
    </div>
    <div class="card">
      <h2>Last Health</h2>
      <pre id="health">No data yet</pre>
    </div>
  </div>

  <div class="row" style="margin-top:16px;">
    <div class="card" style="grid-column: 1 / -1;">
      <h2>Logs</h2>
      <pre id="logs">No logs yet</pre>
    </div>
  </div>

  <script>
    async function sendCmd(command) {
      const res = await fetch('/api/device/command', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ device_id: '${DEVICE_ID}', command })
      });
      const data = await res.json();
      alert(JSON.stringify(data, null, 2));
    }

    async function sendStartTest() {
      const token = document.getElementById('token').value;
      const res = await fetch('/api/device/command', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ device_id: '${DEVICE_ID}', command: '/start-test', token })
      });
      const data = await res.json();
      alert(JSON.stringify(data, null, 2));
    }

    async function refresh() {
      const h = await fetch('/api/device/health/latest').then(r => r.text());
      document.getElementById('health').textContent = h || 'No data yet';
      const logs = await fetch('/api/device/logs').then(r => r.text());
      document.getElementById('logs').textContent = logs || 'No logs yet';
      const status = await fetch('/api/device/status').then(r => r.text());
      document.getElementById('status').textContent = status || 'No status';
      // Frame is now MJPEG stream - no need to refresh it
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>`;
}

function notFound(res) {
  sendText(res, 404, 'Not found');
}

const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  const method = req.method || 'GET';
  const pathname = parsed.pathname || '/';

  try {
    if (method === 'GET' && pathname === '/') {
      return sendText(res, 200, pageHtml(), 'text/html');
    }

    if (method === 'GET' && pathname === '/api/device/command') {
      const deviceId = parsed.query.device_id;
      if (deviceId !== DEVICE_ID) return sendText(res, 400, 'invalid device_id');
      
      if (!state.deviceConnected) {
        state.deviceConnected = true;
        log('Device connected');
      }
      state.lastPoll = new Date().toISOString();
      
      const cmd = state.pendingCommand || 'NONE';
      if (cmd !== 'NONE') {
        log(`Sending command to device: ${cmd}`);
      }
      state.pendingCommand = null;
      return sendText(res, 200, cmd);
    }

    if (method === 'POST' && pathname === '/api/device/command') {
      const body = await readBody(req);
      const data = JSON.parse(body.toString('utf8') || '{}');
      if (data.device_id !== DEVICE_ID) return sendJson(res, 400, { ok: false, error: 'invalid device_id' });

      if (data.command === '/start-test') {
        if (data.token !== DEVICE_TOKEN) {
          log('Rejected /start-test due to invalid token');
          return sendJson(res, 401, { ok: false, error: 'invalid token' });
        }
        state.pendingCommand = `/start-test|token=${DEVICE_TOKEN}`;
        log('Queued /start-test');
        return sendJson(res, 200, { ok: true, queued: state.pendingCommand });
      }

      state.pendingCommand = data.command;
      log(`Queued command: ${data.command}`);
      return sendJson(res, 200, { ok: true, queued: data.command });
    }

    if (method === 'POST' && pathname === '/api/device/health') {
      const body = await readBody(req);
      const text = body.toString('utf8');
      state.lastHealth = text;
      log('Health received');
      return sendJson(res, 200, { ok: true });
    }

    if (method === 'GET' && pathname === '/api/device/health/latest') {
      return sendText(res, 200, state.lastHealth || 'No health yet', 'text/plain');
    }

    if (method === 'POST' && pathname === '/api/device/event') {
      const body = await readBody(req);
      const text = body.toString('utf8');
      state.lastEvent = text;
      log('Event received');
      return sendJson(res, 200, { ok: true });
    }

    if (method === 'GET' && pathname === '/api/device/logs') {
      return sendText(res, 200, state.logs.join('\n'), 'text/plain');
    }

    if (method === 'GET' && pathname === '/api/device/status') {
      const status = `Connected: ${state.deviceConnected}
Last Poll: ${state.lastPoll || 'Never'}
Last Frame: ${state.lastFrame ? state.lastFrame.length + ' bytes' : 'None'}
Pending Command: ${state.pendingCommand || 'None'}`;
      return sendText(res, 200, status, 'text/plain');
    }

    if (method === 'POST' && pathname === '/api/device/frame') {
      const deviceId = parsed.query.device_id;
      if (deviceId !== DEVICE_ID) return sendText(res, 400, 'invalid device_id');
      const buf = await readBody(req);
      state.lastFrame = buf;
      state.lastFrameType = req.headers['content-type'] || 'image/jpeg';
      log(`Frame received (${buf.length} bytes)`);
      return sendJson(res, 200, { ok: true, bytes: buf.length });
    }

    if (method === 'GET' && pathname === '/api/device/latest-frame') {
      if (!state.lastFrame) return sendText(res, 404, 'No frame yet');
      res.writeHead(200, {
        'Content-Type': state.lastFrameType || 'image/jpeg',
        'Content-Length': state.lastFrame.length,
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store'
      });
      return res.end(state.lastFrame);
    }

    if (method === 'GET' && pathname === '/api/device/stream') {
      // MJPEG stream endpoint - sends continuous stream of frames
      const boundary = '--mjpegboundary';
      res.writeHead(200, {
        'Content-Type': 'multipart/x-mixed-replace; boundary=' + boundary,
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
        'Connection': 'close'
      });

      let frameInterval = setInterval(() => {
        if (state.lastFrame) {
          res.write(boundary + '\r\n');
          res.write('Content-Type: ' + (state.lastFrameType || 'image/jpeg') + '\r\n');
          res.write('Content-Length: ' + state.lastFrame.length + '\r\n');
          res.write('X-Timestamp: ' + Date.now() + '\r\n\r\n');
          res.write(state.lastFrame);
          res.write('\r\n');
        }
      }, 50);  // Send frame every 50ms (20 FPS)

      req.on('close', () => {
        clearInterval(frameInterval);
      });
      return;
    }

    return notFound(res);
  } catch (err) {
    console.error(err);
    return sendJson(res, 500, { ok: false, error: String(err.message || err) });
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("Running on all interfaces");
});