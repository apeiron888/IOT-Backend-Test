/**
 * IoT Backend for ESP32-CAM Exam Monitor
 *
 * Streaming Modes:
 * 1. DIRECT LOCAL: Stream directly from ESP32 on LAN (ultra-low latency, ~30ms)
 * 2. WEBSOCKET RELAY: Real-time binary frame push over internet (~100-300ms)
 * 3. MJPEG HTTP: Compatibility fallback (~1-2s latency)
 *
 * Performance:
 * - Local LAN: ~15-30ms latency (direct from ESP32)
 * - Internet WS: ~50-200ms latency (relay via backend)
 * - Internet HTTP: ~2-5s latency (polling model)
 *
 * Features:
 * - Serves web UI with adaptive streaming mode selection
 * - REST endpoints for device management
 * - Real-time binary WebSocket frame streaming
 * - Command dispatching and polling
 * - Health monitoring and event tracking
 * - Proper IoT protocols and best practices
 *
 * Local Run:
 *   node index.js
 *   Open: http://localhost:3000
 *
 * Render Cloud Deployment:
 *   Backend URL: https://your-service.onrender.com
 *   For local LAN: Stream directly from ESP32 port 81 (no internet needed)
 *   For internet: Use WebSocket relay through backend
 *
 * API Endpoints:
 *   GET  /health - Backend health check
 *   GET  /healthz - Kubernetes health probe
 *   POST /api/device/command - Queue command for device
 *   GET  /api/device/command - Device polls for pending command
 *   POST /api/device/health - Receive device health report
 *   POST /api/device/event - Receive device event
 *   POST /api/device/frame - Receive JPEG frame
 *   GET  /api/device/stream - MJPEG relay (deprecated)
 *   WS   /api/device/stream-ws - WebSocket real-time binary relay
 */

const http = require('http');
const url = require('url');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ===== CONFIGURATION FROM ENVIRONMENT =====
// These can be set via .env file (locally) or Render environment variables
const PORT = process.env.PORT || 3000;
const DEVICE_ID = process.env.DEVICE_ID || 'examcam-001';
const DEVICE_TOKEN = process.env.DEVICE_TOKEN || 'CHANGE_THIS_TOKEN';
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS ? 
  process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim()) : 
  ['*']; // '*' for localhost, specify domains for production
const ENV = process.env.NODE_ENV || 'development';
const IS_PRODUCTION = ENV === 'production';

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
  
  // WebSocket streaming clients
  streamClients: new Map(),    // device_id -> Set of { ws, lastFrameTime }
  
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
  const origin = ALLOWED_ORIGINS.includes('*') ? '*' : ALLOWED_ORIGINS[0];
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(body),
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Credentials': 'true',
    'Cache-Control': 'no-store, no-cache',
    'X-Timestamp': new Date().toISOString(),
    ...headers
  });
  res.end(body);
}

function sendText(res, code, text, contentType = 'text/plain', headers = {}) {
  const origin = ALLOWED_ORIGINS.includes('*') ? '*' : ALLOWED_ORIGINS[0];
  res.writeHead(code, {
    'Content-Type': contentType,
    'Content-Length': Buffer.byteLength(text),
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Credentials': 'true',
    'Cache-Control': 'no-store, no-cache',
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
        <button class="btn-blue" onclick="openLiveStream()" style="margin-top: 10px;">Open Live Stream</button>
      </div>
    </div>

    <div class="row full">
      <div class="card">
        <h2>Live Stream - Adaptive Streaming</h2>
        <p class="info" id="stream-mode-info">Detecting network location...</p>
        
        <div style="margin-bottom:10px;">
          <button class="btn-blue" id="btn-local" onclick="switchToLocalStream()" style="display:none;">Use Direct Local (ESP32)</button>
          <button class="btn-blue" id="btn-websocket" onclick="switchToWebSocketStream()" style="display:none;">Use WebSocket Relay</button>
          <button class="btn-gray" id="btn-fallback" onclick="switchToMJPEGStream()" style="display:none;">Use HTTP Fallback</button>
          <button class="btn-blue" onclick="sendTestFrame()" style="margin-left:5px;">📤 Send Test Frame</button>
        </div>

        <div style="background:#111; border-radius:8px; padding:8px; overflow:auto; text-align:center;">
          <!-- Local Direct Stream (MJPEG from ESP32) -->
          <img id="local-stream" style="display:none; max-width:100%; width:100%; border-radius:6px; background:#000;" />
          
          <!-- WebSocket Canvas Stream -->
          <canvas id="live-stream-ws" style="display:none; max-width:100%; width:100%; border-radius:6px; background:#000;" width="640" height="480"></canvas>
          
          <!-- HTTP Fallback MJPEG -->
          <img id="live-stream" style="display:none; max-width:100%; width:100%; border-radius:6px; background:#000;" />
          
          <!-- Loading placeholder -->
          <div id="stream-loading" style="width:100%; height:480px; display:flex; align-items:center; justify-content:center; background:#000; border-radius:6px; color:#666;">
            <p>Stream loading...</p>
          </div>
        </div>
        
        <p id="stream-status" style="margin-top:8px; color:#aaa; font-size:12px; text-align:center;">Status: Initializing...</p>
      </div>
    </div>

    <div class="row full">
      <div class="card">
        <h2>Streaming Modes</h2>
        <table style="width:100%; border-collapse:collapse;">
          <tr style="border-bottom:1px solid #333;">
            <th style="text-align:left; padding:8px;">Mode</th>
            <th style="text-align:left; padding:8px;">Latency</th>
            <th style="text-align:left; padding:8px;">Use Case</th>
            <th style="text-align:left; padding:8px;">Status</th>
          </tr>
          <tr style="border-bottom:1px solid #333;">
            <td style="padding:8px;">Direct (ESP32 LAN)</td>
            <td style="padding:8px;">15-30ms ⚡</td>
            <td style="padding:8px;">Local network only</td>
            <td style="padding:8px;"><span id="status-local">Detecting...</span></td>
          </tr>
          <tr style="border-bottom:1px solid #333;">
            <td style="padding:8px;">WebSocket Relay</td>
            <td style="padding:8px;">50-200ms 🌐</td>
            <td style="padding:8px;">Internet access</td>
            <td style="padding:8px;"><span id="status-ws">Detecting...</span></td>
          </tr>
          <tr>
            <td style="padding:8px;">HTTP MJPEG</td>
            <td style="padding:8px;">2-5s ⏱️</td>
            <td style="padding:8px;">Firewall workaround</td>
            <td style="padding:8px;"><span id="status-http">Available</span></td>
          </tr>
        </table>
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

    async function sendTestFrame() {
      try {
        const res = await fetch(\`/api/device/test-frame?device_id=\${DEVICE_ID}\`, {
          method: 'POST'
        });
        const data = await res.json();
        console.log('Test frame sent:', data);
        updateStreamStatus('✅ Test frame broadcasted! Should see it on WebSocket canvas.');
      } catch (err) {
        console.error('Error sending test frame:', err);
        updateStreamStatus('❌ Error sending test frame');
      }
    }

    function openLiveStream() {
      window.open('/api/device/stream', '_blank', 'noopener,noreferrer');
    }


    // ===== ADAPTIVE STREAMING - Auto-detect and choose optimal mode =====
    let currentMode = 'detecting';
    let wsStream = null;
    let frameCount = 0;
    let lastFrameTime = 0;

    // Detect if we're on local network (192.168.x.x, 10.x.x.x, 172.16-31.x.x, 127.x.x.x, localhost)
    function isLocalNetwork() {
      const host = window.location.hostname;
      return /^(localhost|127\.0\.0\.1|192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[01])\.|\[::1\])/.test(host);
    }

    function updateStreamStatus(msg) {
      const status = document.getElementById('stream-status');
      if (status) status.textContent = msg;
      console.log('[STREAM]', msg);
    }

    // ===== MODE 1: DIRECT LOCAL (Ultra-fast - 15-30ms) =====
    function switchToLocalStream() {
      if (currentMode === 'local') return;
      stopCurrentStream();
      
      const localStreamImg = document.getElementById('local-stream');
      const canvasWs = document.getElementById('live-stream-ws');
      const httpImg = document.getElementById('live-stream');
      const loading = document.getElementById('stream-loading');
      
      localStreamImg.style.display = 'block';
      canvasWs.style.display = 'none';
      httpImg.style.display = 'none';
      loading.style.display = 'none';
      
      // ESP32 streams on port 81 directly
      const esp32Ip = window.location.hostname;
      const streamUrl = \`http://\${esp32Ip}:81/stream\`;
      
      localStreamImg.src = streamUrl;
      currentMode = 'local';
      updateStreamStatus('🚀 Direct Local Stream (15-30ms) - Streaming from ESP32 port 81');
      document.getElementById('btn-websocket').style.display = 'inline-block';
      document.getElementById('btn-fallback').style.display = 'inline-block';
      document.getElementById('btn-local').style.display = 'none';
    }

    // ===== MODE 2: WEBSOCKET RELAY (Fast - 50-200ms) =====
    function switchToWebSocketStream() {
      if (currentMode === 'websocket') return;
      stopCurrentStream();
      
      const localStreamImg = document.getElementById('local-stream');
      const canvasWs = document.getElementById('live-stream-ws');
      const httpImg = document.getElementById('live-stream');
      const loading = document.getElementById('stream-loading');
      
      localStreamImg.style.display = 'none';
      canvasWs.style.display = 'block';
      httpImg.style.display = 'none';
      loading.style.display = 'none';
      
      currentMode = 'websocket';
      frameCount = 0;
      connectWebSocketStream();
      document.getElementById('btn-local').style.display = 'inline-block';
      document.getElementById('btn-fallback').style.display = 'inline-block';
      document.getElementById('btn-websocket').style.display = 'none';
    }

    // ===== MODE 3: HTTP MJPEG FALLBACK (Slow - 2-5s) =====
    function switchToMJPEGStream() {
      if (currentMode === 'mjpeg') return;
      stopCurrentStream();
      
      const localStreamImg = document.getElementById('local-stream');
      const canvasWs = document.getElementById('live-stream-ws');
      const httpImg = document.getElementById('live-stream');
      const loading = document.getElementById('stream-loading');
      
      localStreamImg.style.display = 'none';
      canvasWs.style.display = 'none';
      httpImg.style.display = 'block';
      loading.style.display = 'none';
      
      httpImg.src = '/api/device/stream?t=' + Date.now();
      currentMode = 'mjpeg';
      updateStreamStatus('⏱️ HTTP MJPEG Fallback (2-5s) - Use if WebSocket blocked');
      document.getElementById('btn-local').style.display = 'inline-block';
      document.getElementById('btn-websocket').style.display = 'inline-block';
      document.getElementById('btn-fallback').style.display = 'none';
    }

    function stopCurrentStream() {
      if (wsStream) {
        wsStream.close();
        wsStream = null;
      }
    }

    // ===== WEBSOCKET STREAMING DETAILS =====
    function connectWebSocketStream() {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = \`\${protocol}//\${window.location.host}/api/device/stream-ws?device_id=\${DEVICE_ID}\`;
      
      updateStreamStatus('🔗 WebSocket: Connecting to relay...');
      wsStream = new WebSocket(wsUrl);
      wsStream.binaryType = 'arraybuffer';
      
      wsStream.onopen = () => {
        console.log('[WS] Connected');
        updateStreamStatus('🟢 WebSocket Connected - Waiting for frames (50-200ms latency)');
      };
      
      wsStream.onmessage = (event) => {
        try {
          const data = new Uint8Array(event.data);
          if (data.length < 2) return;
          
          const frameType = data[0];
          const frameData = data.slice(1);
          
          if (frameType === 0 && frameData.length > 0) {
            const blob = new Blob([frameData], { type: 'image/jpeg' });
            const url = URL.createObjectURL(blob);
            const canvas = document.getElementById('live-stream-ws');
            if (canvas && currentMode === 'websocket') {
              const img = new Image();
              img.onload = () => {
                const ctx = canvas.getContext('2d');
                ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
                URL.revokeObjectURL(url);
                frameCount++;
                lastFrameTime = Date.now();
                if (frameCount % 10 === 0) {
                  updateStreamStatus(\`🟢 WebSocket Relay Active - \${frameCount} frames received\`);
                }
              };
              img.src = url;
            }
          }
        } catch (e) {
          console.error('[WS] Message error:', e);
        }
      };
      
      wsStream.onerror = (err) => {
        console.error('[WS] Error:', err);
        updateStreamStatus('🔴 WebSocket Error - Retrying in 3s...');
      };
      
      wsStream.onclose = () => {
        console.log('[WS] Closed');
        if (currentMode === 'websocket') {
          updateStreamStatus('🔴 WebSocket Disconnected - Reconnecting in 2s...');
          setTimeout(connectWebSocketStream, 2000);
        }
      };
    }

    // ===== STREAM MODE AUTO-DETECTION =====
    async function initializeStreaming() {
      const isLocal = isLocalNetwork();
      const localStream = document.getElementById('local-stream');
      const wsBtn = document.getElementById('btn-websocket');
      const localBtn = document.getElementById('btn-local');
      const fallbackBtn = document.getElementById('btn-fallback');
      
      updateStreamStatus('🔍 Detecting network location and stream availability...');
      
      if (isLocal) {
        console.log('[STREAM] Local network detected - testing direct ESP32 stream...');
        
        // Test if ESP32 is reachable on port 81
        try {
          const testUrl = \`http://\${window.location.hostname}:81/stream\`;
          const response = await fetch(testUrl, { method: 'HEAD', mode: 'no-cors' });
          document.getElementById('status-local').textContent = '✅ Available';
          switchToLocalStream();
          wsBtn.style.display = 'inline-block';
          fallbackBtn.style.display = 'inline-block';
        } catch (e) {
          console.log('[STREAM] ESP32 port 81 not reachable, falling back to WebSocket');
          document.getElementById('status-local').textContent = '⏳ Unavailable';
          switchToWebSocketStream();
          localBtn.style.display = 'inline-block';
          fallbackBtn.style.display = 'inline-block';
        }
      } else {
        console.log('[STREAM] Internet access detected - using WebSocket relay');
        document.getElementById('stream-mode-info').innerHTML = 
          '🌐 <strong>Internet Mode:</strong> Using real-time WebSocket relay (50-200ms). Best performance for cloud viewing.';
        document.getElementById('status-local').textContent = '🔒 Not available (local only)';
        switchToWebSocketStream();
        localBtn.style.display = 'none';
        fallbackBtn.style.display = 'inline-block';
      }
      
      document.getElementById('status-ws').textContent = '✅ Available';
      document.getElementById('status-http').textContent = '✅ Available';
    }

    // Start streaming detection
    setTimeout(initializeStreaming, 300);

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
// WEBSOCKET FRAME STREAMING
// =========================
function createWebSocketKey(key) {
  const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
  return crypto.createHash('sha1').update(key + GUID).digest('base64');
}

function sendWebSocketFrame(ws, data, opcode = 0x82) {  // 0x82 = binary frame
  if (!ws || ws.destroyed) return;
  
  try {
    let frame = Buffer.alloc(2);
    frame[0] = opcode;  // Binary frame
    
    let payloadLen = data.length;
    if (payloadLen < 126) {
      frame[1] = payloadLen | 0x80;  // Mask bit set
      frame = Buffer.concat([frame, Buffer.alloc(4), data]);  // 4-byte mask key
    } else if (payloadLen < 65536) {
      let header = Buffer.alloc(4);
      header[0] = opcode;
      header[1] = 126 | 0x80;  // Mask bit set
      header.writeUInt16BE(payloadLen, 2);
      frame = Buffer.concat([header, Buffer.alloc(4), data]);
    } else {
      let header = Buffer.alloc(10);
      header[0] = opcode;
      header[1] = 127 | 0x80;  // Mask bit set
      header.writeBigUInt64BE(BigInt(payloadLen), 2);
      frame = Buffer.concat([header, Buffer.alloc(4), data]);
    }
    
    ws.write(frame);
  } catch (err) {
    // WebSocket closed or error
  }
}

function broadcastFrameToClients(deviceId, frameBuffer) {
  const clients = state.streamClients.get(deviceId);
  if (!clients || clients.size === 0) return;
  
  // Send frame to all connected WebSocket clients
  const frameMsg = Buffer.concat([
    Buffer.from([0]), // Frame type marker (0 = JPEG frame)
    frameBuffer
  ]);
  
  for (const client of clients) {
    sendWebSocketFrame(client.ws, frameMsg);
  }
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
      
      // Broadcast frame to all WebSocket clients in real-time
      broadcastFrameToClients(deviceId, buf);
      
      info(`Frame received from ${deviceId}: ${buf.length} bytes, ${state.streamClients.get(deviceId)?.size || 0} WebSocket clients`);
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

    // ===== TEST FRAME GENERATOR (for testing WebSocket without ESP32) =====
    if (method === 'POST' && pathname === '/api/device/test-frame') {
      const deviceId = query.device_id || DEVICE_ID;
      
      // Generate a simple test JPEG frame (1x1 pixel, red)
      const testJpeg = Buffer.from([
        0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01,
        0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43,
        0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08, 0x07, 0x07, 0x07, 0x09,
        0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D, 0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12,
        0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D, 0x1A, 0x1C, 0x1C, 0x20,
        0x24, 0x2E, 0x27, 0x20, 0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29,
        0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27, 0x39, 0x3D, 0x38, 0x32,
        0x3C, 0x2E, 0x33, 0x34, 0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01,
        0x00, 0x01, 0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00, 0x1F, 0x00, 0x00,
        0x01, 0x05, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0xFF, 0xC4, 0x00, 0xB5, 0x10, 0x00, 0x02, 0x01, 0x03,
        0x03, 0x02, 0x04, 0x03, 0x05, 0x05, 0x04, 0x04, 0x00, 0x00, 0x01, 0x7D,
        0x01, 0x02, 0x03, 0x00, 0x04, 0x11, 0x05, 0x12, 0x21, 0x31, 0x41, 0x06,
        0x13, 0x51, 0x61, 0x07, 0x22, 0x71, 0x14, 0x32, 0x81, 0x91, 0xA1, 0x08,
        0x23, 0x42, 0xB1, 0xC1, 0x15, 0x52, 0xD1, 0xF0, 0x24, 0x33, 0x62, 0x72,
        0x82, 0x09, 0x0A, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x25, 0x26, 0x27, 0x28,
        0x29, 0x2A, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x43, 0x44, 0x45,
        0x46, 0x47, 0x48, 0x49, 0x4A, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59,
        0x5A, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6A, 0x73, 0x74, 0x75,
        0x76, 0x77, 0x78, 0x79, 0x7A, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
        0x8A, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0xA2, 0xA3,
        0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6,
        0xB7, 0xB8, 0xB9, 0xBA, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9,
        0xCA, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xE1, 0xE2,
        0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xF1, 0xF2, 0xF3, 0xF4,
        0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01,
        0x00, 0x00, 0x3F, 0x00, 0xFB, 0xD3, 0xFF, 0xD9
      ]);
      
      initDeviceIfNeeded(deviceId);
      state.deviceFrames.set(deviceId, testJpeg);
      broadcastFrameToClients(deviceId, testJpeg);
      
      info(`Test frame sent to ${deviceId}. WebSocket clients: ${state.streamClients.get(deviceId)?.size || 0}`);
      return sendJson(res, 200, { ok: true, info: 'Test frame broadcasted to all WebSocket clients' });
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

// =========================
// WEBSOCKET UPGRADE HANDLER
// =========================
server.on('upgrade', (req, socket, head) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;
  const query = parsedUrl.query;

  // WebSocket frame stream endpoint
  if (pathname === '/api/device/stream-ws') {
    const deviceId = query.device_id || DEVICE_ID;
    
    // Verify device token if provided
    const token = req.headers['x-device-token'] || query.token;
    if (token && token !== DEVICE_TOKEN) {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }

    // Perform WebSocket handshake
    const key = req.headers['sec-websocket-key'];
    const responseKey = createWebSocketKey(key);
    
    const responseHeaders = [
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      'Sec-WebSocket-Accept: ' + responseKey
    ].join('\r\n') + '\r\n\r\n';
    
    socket.write(responseHeaders);
    
    // Track WebSocket client
    initDeviceIfNeeded(deviceId);
    if (!state.streamClients.has(deviceId)) {
      state.streamClients.set(deviceId, new Set());
    }
    
    const wsClient = { ws: socket, deviceId, lastFrameTime: 0 };
    state.streamClients.get(deviceId).add(wsClient);
    
    info(`WebSocket client connected for ${deviceId}. Active clients: ${state.streamClients.get(deviceId).size}`);
    
    // Send latest frame if available
    const latestFrame = state.deviceFrames.get(deviceId);
    if (latestFrame) {
      broadcastFrameToClients(deviceId, latestFrame);
    }
    
    // Handle socket close
    socket.on('close', () => {
      const clients = state.streamClients.get(deviceId);
      if (clients) {
        clients.delete(wsClient);
        info(`WebSocket client disconnected from ${deviceId}. Active clients: ${clients.size}`);
      }
    });
    
    socket.on('error', (err) => {
      warn(`WebSocket error for ${deviceId}: ${err.message}`);
      const clients = state.streamClients.get(deviceId);
      if (clients) clients.delete(wsClient);
    });
  } else {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
  }
});

server.listen(PORT, "0.0.0.0", () => {
  info(`===================================`);
  info(`IoT Backend Service Starting`);
  info(`===================================`);
  info(`Environment: ${ENV}`);
  info(`Port: ${PORT}`);
  info(`Device ID: ${DEVICE_ID}`);
  info(`Token: ${DEVICE_TOKEN.substring(0, 8)}...`);
  info(`Allowed Origins: ${ALLOWED_ORIGINS.join(', ')}`);
  info(`===================================`);
  
  if (IS_PRODUCTION) {
    info(`🚀 PRODUCTION MODE - Running on Render`);
    info(`Backend URL: https://your-service.onrender.com`);
  } else {
    info(`🔧 DEVELOPMENT MODE`);
    info(`Local access: http://localhost:${PORT}`);
  }
  
  info(`===================================`);
});