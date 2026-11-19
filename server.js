// عبسي وحظ — سيرفر LAN + WebSocket (من غير أي مكتبات خارجية)
// التشغيل:  node server.js        (أو PORT=8080 node server.js)
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto"), os = require("os");
const PORT = +process.env.PORT || 3000, DIR = __dirname, GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const page = () => {
  const f = path.join(DIR, "index.html");
  if (fs.existsSync(f)) return f;
  const a = fs.readdirSync(DIR).find(x => x.endsWith(".html"));
  return a && path.join(DIR, a);
};

// مكتبة three.js: بتتحمّل مرة واحدة (محتاج إنترنت أول مرة بس) وبعدها بتتوزع للأجهزة من غير إنترنت
const THREE_FILE = path.join(DIR, "three.min.js");
if (!fs.existsSync(THREE_FILE)) {
  require("https").get("https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js", r => {
    if (r.statusCode !== 200) { r.resume(); return console.log("⚠️ مقدرتش أنزّل three.js (كود " + r.statusCode + ")"); }
    const o = fs.createWriteStream(THREE_FILE); r.pipe(o); o.on("finish", () => console.log("✅ three.js اتحمّلت وبتتوزع محليًا"));
  }).on("error", () => console.log("⚠️ مفيش إنترنت دلوقتي — نزّل three.min.js يدويًا جنب server.js"));
}
const srv = http.createServer((req, res) => {
  if (req.url === "/health") return res.end("ok");
  if (req.url === "/favicon.ico") { res.writeHead(204); return res.end(); }
  if (req.url.split("?")[0] === "/three.min.js") {
    if (!fs.existsSync(THREE_FILE)) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { "Content-Type": "application/javascript", "Cache-Control": "max-age=86400" });
    return fs.createReadStream(THREE_FILE).pipe(res);
  }
  const SONGS = path.join(DIR, "songs");
  if (req.url === "/songs.json") {
    let a = []; try { a = fs.readdirSync(SONGS).filter(x => /\.(mp3|m4a|ogg|wav|aac)$/i.test(x)); } catch {}
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" }); return res.end(JSON.stringify(a));
  }
  if (req.url.startsWith("/songs/")) {
    const f = path.join(SONGS, path.basename(decodeURIComponent(req.url.slice(7).split("?")[0])));
    if (!fs.existsSync(f)) { res.writeHead(404); return res.end(); }
    const st = fs.statSync(f), mt = { ".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".ogg": "audio/ogg", ".wav": "audio/wav", ".aac": "audio/aac" }[path.extname(f).toLowerCase()] || "application/octet-stream", rg = req.headers.range;
    if (rg) {
      const [x, y] = rg.replace("bytes=", "").split("-"), a0 = +x || 0, b0 = y ? +y : st.size - 1;
      res.writeHead(206, { "Content-Type": mt, "Content-Range": `bytes ${a0}-${b0}/${st.size}`, "Accept-Ranges": "bytes", "Content-Length": b0 - a0 + 1 });
      return fs.createReadStream(f, { start: a0, end: b0 }).pipe(res);
    }
    res.writeHead(200, { "Content-Type": mt, "Content-Length": st.size, "Accept-Ranges": "bytes" });
    return fs.createReadStream(f).pipe(res);
  }
  const f = page();
  if (!f) { res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" }); return res.end("حط ملف اللعبة (.html) جنب server.js"); }
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
  fs.createReadStream(f).pipe(res);
});

/* ---------- WebSocket (RFC 6455 مبسّط) ---------- */
const enc = str => {
  const p = Buffer.from(str), n = p.length;
  let h;
  if (n < 126) h = Buffer.from([0x81, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, p]);
};
const rooms = new Map();
const send = (c, o) => { if (!c.sock.destroyed) c.sock.write(enc(JSON.stringify(o))); };
const list = r => [...r.values()].map(c => ({ peer: c.id, presence: c.presence }));
const bcast = (r, o) => r.forEach(c => send(c, o));

function onMsg(c, txt) {
  let m; try { m = JSON.parse(txt); } catch { return; }
  if (m.t === "join" && !c.room) {
    const name = String(m.room || "").slice(0, 40);
    if (!name) return;
    let r = rooms.get(name); if (!r) rooms.set(name, r = new Map());
    if (r.size >= 12) { send(c, { t: "full" }); return c.sock.end(); }
    c.room = name; c.id = crypto.randomBytes(3).toString("hex"); r.set(c.id, c);
    send(c, { t: "hello", id: c.id, peers: list(r) });
    bcast(r, { t: "peers", peers: list(r), joined: [c.id], left: [] });
  } else if (!c.room) return;
  else if (m.t === "pres") {
    c.presence = m.data && typeof m.data === "object" ? m.data : {};
    const r = rooms.get(c.room); bcast(r, { t: "peers", peers: list(r), joined: [], left: [] });
  } else if (m.t === "ev" && typeof m.ev === "string") {
    bcast(rooms.get(c.room), { t: "ev", ev: m.ev.slice(0, 20), from: c.id, data: m.data });
  }
}

function parse(c) {
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1], op = b0 & 15, fin = b0 & 128, mk = b1 & 128;
    let len = b1 & 127, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > 4e6) return c.sock.destroy();
    const need = off + (mk ? 4 : 0) + len;
    if (c.buf.length < need) return;
    let pl = Buffer.from(c.buf.subarray(off + (mk ? 4 : 0), need));
    if (mk) { const k = c.buf.subarray(off, off + 4); for (let i = 0; i < pl.length; i++) pl[i] ^= k[i & 3]; }
    c.buf = c.buf.subarray(need);
    if (op === 8) return c.sock.end();
    if (op === 9) { if (pl.length < 126 && !c.sock.destroyed) c.sock.write(Buffer.concat([Buffer.from([0x8a, pl.length]), pl])); continue; }
    if (op === 1 || op === 0) {
      if (op === 1) c.frag = [];
      if (!c.frag) continue;
      c.frag.push(pl);
      if (!fin) continue;
      pl = Buffer.concat(c.frag); c.frag = null;
      onMsg(c, pl.toString());
    }
  }
}

srv.on("upgrade", (req, sock) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) return sock.destroy();
  sock.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
    crypto.createHash("sha1").update(key + GUID).digest("base64") + "\r\n\r\n");
  sock.setNoDelay(true);
  const c = { sock, buf: Buffer.alloc(0), room: null, id: null, presence: {}, frag: null };
  sock.on("data", d => { c.buf = Buffer.concat([c.buf, d]); parse(c); });
  const bye = () => {
    if (!c.room) return;
    const r = rooms.get(c.room); if (!r || !r.delete(c.id)) return;
    if (!r.size) rooms.delete(c.room);
    else bcast(r, { t: "peers", peers: list(r), joined: [], left: [{ peer: c.id }] });
    c.room = null;
  };
  sock.on("close", bye); sock.on("error", bye);
  c.ping = setInterval(() => { if (!sock.destroyed) sock.write(Buffer.from([0x89, 0])); }, 25000);
  sock.on("close", () => clearInterval(c.ping));
});

process.on("uncaughtException", e => console.log("⚠️ خطأ اتجاهل علشان السيرفر يكمّل:", e.message));
srv.on("clientError", (e, sock) => { try { sock.destroy(); } catch {} });
srv.on("error", e => {
  console.log(e.code === "EADDRINUSE" ? "⚠️ البورت " + PORT + " مشغول — جرّب: PORT=8080 node server.js" : "⚠️ " + e.message);
  process.exit(1);
});
srv.listen(PORT, "0.0.0.0", () => {
  console.log("\n🎮 عبسي وحظ — سيرفر LAN شغال\n");
  console.log("   على الجهاز ده:   http://localhost:" + PORT);
  for (const l of Object.values(os.networkInterfaces()))
    for (const a of l || []) if (a.family === "IPv4" && !a.internal) console.log("   على باقي الأجهزة: http://" + a.address + ":" + PORT);
  console.log("\n   (كل اللاعبين لازم يكونوا على نفس الواي فاي)\n");
});
