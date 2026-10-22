const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;

const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('HAVE THEIR WSS server is running');
});

const wss = new WebSocketServer({ server: httpServer });

const rooms = new Map();
const clients = new Set();

function broadcast(packet, exceptWs) {
    const s = JSON.stringify(packet);
    for (const c of clients) {
        if (c === exceptWs) continue;
        if (c.readyState === 1) {
            try { c.send(s); } catch (e) {}
        }
    }
}

wss.on('connection', (ws) => {
    clients.add(ws);
    ws.roomId = null;

    ws.on('message', (buf) => {
        let d;
        try { d = JSON.parse(buf.toString()); } catch (e) { return; }
        if (!d || d.__mt !== 1) return;

        switch (d.type) {
            case 'get_rooms': {
                const list = [];
                const now = Date.now();
                for (const [id, r] of rooms) {
                    if (now - r.updated > 15000) { rooms.delete(id); continue; }
                    list.push({ id, name: r.name, players: r.players.size, started: r.started });
                }
                ws.send(JSON.stringify({
                    __mt: 1, type: 'rooms_list',
                    rooms: list, senderId: 'server', t: now
                }));
                break;
            }
            case 'create_room':
            case 'announce_room': {
                if (!d.roomId) break;
                let r = rooms.get(d.roomId);
                if (!r) {
                    r = { name: d.roomName || 'Комната', players: new Set(), updated: Date.now(), started: false };
                    rooms.set(d.roomId, r);
                }
                r.name = d.roomName || r.name;
                r.updated = Date.now();
                if (typeof d.started === 'boolean') r.started = d.started;
                r.players.add(ws);
                ws.roomId = d.roomId;
                broadcast({ ...d, type: 'announce_room', players: r.players.size }, ws);
                break;
            }
            case 'join_room': {
                const r = rooms.get(d.roomId);
                if (!r) {
                    ws.send(JSON.stringify({ __mt: 1, type: 'error', message: 'Комната не найдена', senderId: 'server' }));
                    break;
                }
                if (r.players.size >= 5) {
                    ws.send(JSON.stringify({ __mt: 1, type: 'error', message: 'Комната заполнена', senderId: 'server' }));
                    break;
                }
                r.players.add(ws);
                ws.roomId = d.roomId;
                r.updated = Date.now();
                broadcast({ ...d, players: r.players.size }, ws);
                break;
            }
            case 'leave_room':
            case 'player_disconnected': {
                const id = d.roomId || ws.roomId;
                ws.roomId = null;
                const r = rooms.get(id);
                if (r) {
                    r.players.delete(ws);
                    if (r.players.size === 0) rooms.delete(id);
                    else r.updated = Date.now();
                }
                broadcast({ ...d, type: 'leave_room' }, ws);
                break;
            }
            default:
                broadcast(d, ws);
        }
    });

    ws.on('close', () => {
        clients.delete(ws);
        if (!ws.roomId) return;
        const id = ws.roomId;
        const r = rooms.get(id);
        if (r) {
            r.players.delete(ws);
            if (r.players.size === 0) rooms.delete(id);
        }
        broadcast({ __mt: 1, type: 'leave_room', roomId: id, senderId: 'server', t: Date.now() }, ws);
    });
});

httpServer.listen(PORT, () => {
    console.log('WSS server on port', PORT);
});
