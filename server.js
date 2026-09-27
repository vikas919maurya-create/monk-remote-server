// Personal Remote Access - WebSocket Relay Server
// Bridges Android phone ↔ Web browser for personal remote access

require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
// Serve web dashboard - works both locally and in Cloud Run container
const fs = require('fs');
const publicDir = fs.existsSync(path.join(__dirname, 'public'))
  ? path.join(__dirname, 'public')
  : path.join(__dirname, '..', 'web');
app.use(express.static(publicDir));

const server = http.createServer(app);

// Two WebSocket servers on different paths
const wssPhone = new WebSocketServer({ noServer: true });
const wssWeb = new WebSocketServer({ noServer: true });

// ─── State ─────────────────────────────────────────────────────────────────────
// deviceId → { ws, pin, battery: {level, charging}, lastHeartbeat, deviceInfo }
const phones = new Map();
// sessionId → { phoneDeviceId, webWs }
const sessions = new Map();
// webWs → deviceId (reverse lookup)
const webToDevice = new Map();

// ─── Helpers ───────────────────────────────────────────────────────────────────
function isOnline(deviceId) {
  const phone = phones.get(deviceId);
  if (!phone) return false;
  return (Date.now() - phone.lastHeartbeat) < 10000;
}

function sendJson(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function getSessionByPhone(deviceId) {
  for (const [sid, s] of sessions) {
    if (s.phoneDeviceId === deviceId) return { sessionId: sid, ...s };
  }
  return null;
}

function getSessionByWeb(webWs) {
  const deviceId = webToDevice.get(webWs);
  if (!deviceId) return null;
  return getSessionByPhone(deviceId);
}

function removeSession(webWs) {
  const deviceId = webToDevice.get(webWs);
  if (deviceId) {
    for (const [sid, s] of sessions) {
      if (s.phoneDeviceId === deviceId) {
        sessions.delete(sid);
        break;
      }
    }
    webToDevice.delete(webWs);
    // Notify phone that web disconnected
    const phone = phones.get(deviceId);
    if (phone) {
      sendJson(phone.ws, { type: 'session_ended' });
    }
    console.log(`[Session] Ended for device ${deviceId}`);
  }
}

// ─── Phone WebSocket Handler ───────────────────────────────────────────────────
wssPhone.on('connection', (ws) => {
  let deviceId = null;
  console.log('[Phone] New connection');

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // Binary data = screen frame → forward to paired web client
      if (!deviceId) return;
      const session = getSessionByPhone(deviceId);
      if (session && session.webWs && session.webWs.readyState === WebSocket.OPEN) {
        session.webWs.send(data, { binary: true });
      }
      return;
    }

    // Text data = JSON message
    try {
      const msg = JSON.parse(data.toString());

      switch (msg.type) {
        case 'register':
          deviceId = msg.deviceId;
          phones.set(deviceId, {
            ws,
            pin: msg.pin,
            battery: { level: -1, charging: false },
            lastHeartbeat: Date.now(),
            deviceInfo: null,
          });
          console.log(`[Phone] Registered: ${deviceId}`);
          sendJson(ws, { type: 'registered', deviceId });
          break;

        case 'heartbeat':
          if (deviceId && phones.has(deviceId)) {
            phones.get(deviceId).lastHeartbeat = Date.now();
          }
          break;

        case 'battery':
          if (deviceId && phones.has(deviceId)) {
            phones.get(deviceId).battery = {
              level: msg.level,
              charging: msg.charging,
            };
            // Forward to web client if connected
            const session = getSessionByPhone(deviceId);
            if (session) {
              sendJson(session.webWs, {
                type: 'battery',
                level: msg.level,
                charging: msg.charging,
              });
            }
          }
          break;

        case 'device_info':
          if (deviceId && phones.has(deviceId)) {
            phones.get(deviceId).deviceInfo = {
              width: msg.width,
              height: msg.height,
              model: msg.model,
            };
            const session2 = getSessionByPhone(deviceId);
            if (session2) {
              sendJson(session2.webWs, {
                type: 'device_info',
                width: msg.width,
                height: msg.height,
                model: msg.model,
              });
            }
          }
          break;

        default:
          // Forward any other message to web client
          const s = getSessionByPhone(deviceId);
          if (s) sendJson(s.webWs, msg);
      }
    } catch (e) {
      console.error('[Phone] Invalid JSON:', e.message);
    }
  });

  ws.on('close', () => {
    console.log(`[Phone] Disconnected: ${deviceId}`);
    if (deviceId) {
      // Notify web client
      const session = getSessionByPhone(deviceId);
      if (session) {
        sendJson(session.webWs, { type: 'phone_disconnected' });
        removeSession(session.webWs);
      }
      phones.delete(deviceId);
    }
  });

  ws.on('error', (err) => {
    console.error(`[Phone] Error: ${err.message}`);
  });
});

// ─── Web WebSocket Handler ─────────────────────────────────────────────────────
wssWeb.on('connection', (ws) => {
  console.log('[Web] New connection');

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      switch (msg.type) {
        case 'connect': {
          const { deviceId, pin } = msg;
          const phone = phones.get(deviceId);

          if (!phone) {
            sendJson(ws, { type: 'error', message: 'Device not found' });
            return;
          }
          if (!isOnline(deviceId)) {
            sendJson(ws, { type: 'error', message: 'Device is offline' });
            return;
          }
          if (phone.pin !== pin) {
            sendJson(ws, { type: 'error', message: 'Invalid PIN' });
            return;
          }

          // Create session
          const sessionId = `${deviceId}-${Date.now()}`;
          sessions.set(sessionId, {
            phoneDeviceId: deviceId,
            webWs: ws,
          });
          webToDevice.set(ws, deviceId);

          console.log(`[Session] Started: ${sessionId}`);

          sendJson(ws, { type: 'session_started', deviceId });

          // Notify phone to start streaming
          sendJson(phone.ws, { type: 'start_stream' });

          // Send current battery and device info
          if (phone.battery.level >= 0) {
            sendJson(ws, {
              type: 'battery',
              level: phone.battery.level,
              charging: phone.battery.charging,
            });
          }
          if (phone.deviceInfo) {
            sendJson(ws, {
              type: 'device_info',
              ...phone.deviceInfo,
            });
          }
          break;
        }

        case 'disconnect': {
          removeSession(ws);
          sendJson(ws, { type: 'disconnected' });
          // Tell phone to stop streaming
          const dId = webToDevice.get(ws);
          if (dId) {
            const ph = phones.get(dId);
            if (ph) sendJson(ph.ws, { type: 'stop_stream' });
          }
          break;
        }

        // Input commands → forward to phone
        case 'tap':
        case 'swipe':
        case 'scroll':
        case 'type_text':
        case 'keypress':
        case 'volume': {
          const dId = webToDevice.get(ws);
          if (dId) {
            const phone = phones.get(dId);
            if (phone) sendJson(phone.ws, msg);
          }
          break;
        }

        default:
          console.log('[Web] Unknown message type:', msg.type);
      }
    } catch (e) {
      console.error('[Web] Invalid JSON:', e.message);
    }
  });

  ws.on('close', () => {
    console.log('[Web] Disconnected');
    removeSession(ws);
  });

  ws.on('error', (err) => {
    console.error(`[Web] Error: ${err.message}`);
  });
});

// ─── HTTP Upgrade Routing ──────────────────────────────────────────────────────
server.on('upgrade', (request, socket, head) => {
  const { pathname } = new URL(request.url, `http://${request.headers.host}`);

  if (pathname === '/phone') {
    wssPhone.handleUpgrade(request, socket, head, (ws) => {
      wssPhone.emit('connection', ws, request);
    });
  } else if (pathname === '/web') {
    wssWeb.handleUpgrade(request, socket, head, (ws) => {
      wssWeb.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

// ─── REST API ──────────────────────────────────────────────────────────────────
app.get('/api/status/:deviceId', (req, res) => {
  const { deviceId } = req.params;
  const phone = phones.get(deviceId);

  if (!phone) {
    return res.json({ online: false, battery: null, deviceInfo: null });
  }

  res.json({
    online: isOnline(deviceId),
    battery: phone.battery,
    deviceInfo: phone.deviceInfo,
  });
});

app.get('/api/devices', (req, res) => {
  const devices = [];
  for (const [deviceId, phone] of phones) {
    devices.push({
      deviceId,
      online: isOnline(deviceId),
      battery: phone.battery,
      deviceInfo: phone.deviceInfo,
    });
  }
  res.json(devices);
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

// ─── Cleanup stale devices every 30 seconds ────────────────────────────────────
setInterval(() => {
  for (const [deviceId, phone] of phones) {
    if (Date.now() - phone.lastHeartbeat > 60000) {
      console.log(`[Cleanup] Removing stale device: ${deviceId}`);
      if (phone.ws.readyState === WebSocket.OPEN) phone.ws.close();
      phones.delete(deviceId);
    }
  }
}, 30000);

// ─── Start Server ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════╗
║   Personal Remote Access Relay Server      ║
║   Running on port ${PORT}                     ║
║                                            ║
║   Phone connects to:  ws://HOST:${PORT}/phone  ║
║   Web connects to:    ws://HOST:${PORT}/web    ║
║   Dashboard:          http://HOST:${PORT}      ║
╚════════════════════════════════════════════╝
  `);
});
