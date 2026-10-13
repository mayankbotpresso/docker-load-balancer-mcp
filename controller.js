"use strict";

// Central controller + load balancer for container-a / container-b.
//   - Load balancer:   http://localhost:9080  (proxies to the containers)
//   - Dashboard + API: http://localhost:9081
// No npm dependencies: only Node built-ins. Docker is driven via `docker compose`.

const http = require("http");
const fs = require("fs");
const path = require("path");
const { execFile } = require("child_process");

const LB_PORT = Number(process.env.LB_PORT) || 9080;
const UI_PORT = Number(process.env.UI_PORT) || 9081;
const COMPOSE_DIR = path.resolve(__dirname, "..");
const COMPOSE_PROJECT = "ab-demo";
const BACKEND_HOST = process.env.BACKEND_HOST || "127.0.0.1";

const HEALTH_INTERVAL_MS = 1000;
const HEALTH_TIMEOUT_MS = 1000;
const HEALTH_FAILS_TO_UNHEALTHY = 2;
const DOCKER_POLL_MS = 2000;
const START_HEALTH_TIMEOUT_MS = 60000;
const STOP_DRAIN_TIMEOUT_MS = 60000;
const MAX_EVENTS = 300;

// No keep-alive: draining/stopped containers must never be reached via a stale pooled socket.
const agent = new http.Agent({ keepAlive: false });

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

function makeContainer(id, service, port) {
  return {
    id, service, port,
    dockerState: "unknown", running: false,
    healthy: false, healthFails: 0, healthDetail: "not checked", checking: false,
    draining: false, op: null, // op: "starting" | "draining" | "stopping" | null
    inFlight: 0, requests: 0, success: 0, failed: 0,
  };
}

const containers = {
  A: makeContainer("A", "container-a", 3001),
  B: makeContainer("B", "container-b", 3002),
};
const other = (c) => (c.id === "A" ? containers.B : containers.A);

let split = { A: 50, B: 50 }; // configured split
let rollout = null;           // active rollout
let lastRollout = null;       // most recent finished rollout (for the UI)
let dockerError = null;
const lb = { total: 0, unavailable: 0, badGateway: 0, retries: 0 };
const events = [];

// ---------------------------------------------------------------------------
// Event log
// ---------------------------------------------------------------------------

function logEvent(action, source, result, detail = "") {
  const e = { time: new Date().toISOString(), action, source, result, detail };
  events.unshift(e);
  if (events.length > MAX_EVENTS) events.pop();
  console.log(`[${e.time}] ${action.padEnd(16)} ${source.padEnd(12)} ${result.padEnd(9)} ${detail}`);
  broadcastSoon();
}

class Rejection extends Error {
  constructor(message, status = 409) { super(message); this.status = status; }
}

// Accepts "A", "a", "container-a" ... so dashboard, curl and MCP callers all resolve the same way.
function resolveContainer(raw, action) {
  const key = String(raw ?? "").trim().toUpperCase().replace(/^CONTAINER-/, "");
  if (containers[key]) return containers[key];
  throw new Rejection(raw ? `${action}: unknown container "${raw}" (use A or B)` : `${action}: a container (A or B) is required`, 400);
}

const outcome = (action, source, ok, detail) => {
  logEvent(action, source, ok ? "ok" : "failed", detail);
  return { ok, detail };
};

// Long-running actions (start, stop, rollout) return an operation id that callers can poll.
const operations = new Map();
let opSeq = 0;
function trackOperation(kind, target, source, done, extra = {}) {
  const op = {
    id: `op-${++opSeq}`, kind, target, source,
    status: "running", detail: "", startedAt: new Date().toISOString(), endedAt: null, ...extra,
  };
  operations.set(op.id, op);
  if (operations.size > 50) operations.delete(operations.keys().next().value);
  done.then((r) => Object.assign(op, {
    status: r.status || (r.ok ? "succeeded" : "failed"),
    detail: r.detail,
    endedAt: new Date().toISOString(),
  }));
  return op.id;
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

const isEligible = (c) => c.running && c.healthy && !c.draining;

function statusOf(c) {
  if (c.op) return c.op;
  if (c.draining) return "draining";
  if (!c.running) return "stopped";
  return c.healthy ? "healthy" : "unhealthy";
}

function effectiveSplit() {
  const a = isEligible(containers.A);
  const b = isEligible(containers.B);
  if (a && b) return { A: split.A, B: split.B, reason: "as configured" };
  if (!a && !b) return { A: 0, B: 0, reason: "no healthy containers - returning 503" };
  const up = a ? containers.A : containers.B;
  const down = other(up);
  const eff = { [up.id]: 100, [down.id]: 0 };
  eff.reason = split[up.id] === 100 ? "as configured" : `failover: ${down.id} is ${statusOf(down)}, all traffic to ${up.id}`;
  return eff;
}

// Smooth weighted round-robin: deterministic, so a 70/30 split really is 7 of every 10 requests.
const wrr = { A: 0, B: 0 };
function pickBackend() {
  const eff = effectiveSplit();
  let total = 0;
  let best = null;
  for (const c of Object.values(containers)) {
    const w = eff[c.id];
    if (!w) continue;
    wrr[c.id] += w;
    total += w;
    if (!best || wrr[c.id] > wrr[best.id]) best = c;
  }
  if (best) wrr[best.id] -= total;
  return best;
}

let lastEffKey = null;
function checkEffective() {
  const eff = effectiveSplit();
  const key = `${eff.A}/${eff.B}`;
  if (key === lastEffKey) return;
  lastEffKey = key;
  wrr.A = 0;
  wrr.B = 0;
  const result = eff.A + eff.B === 0 ? "failed" : eff.reason === "as configured" ? "ok" : "warning";
  logEvent("effective-split", "router", result, `A ${eff.A}% / B ${eff.B}% (${eff.reason})`);
}

// ---------------------------------------------------------------------------
// Load balancer (reverse proxy)
// ---------------------------------------------------------------------------

function sendJson(res, status, body, headers = {}) {
  if (res.headersSent) return res.end();
  res.writeHead(status, { "Content-Type": "application/json", ...headers });
  res.end(JSON.stringify(body));
}

function forward(req, res, c, allowRetry) {
  c.inFlight++;
  c.requests++;
  let settled = false;
  const settle = (ok) => {
    if (settled) return;
    settled = true;
    c.inFlight--;
    if (ok) c.success++; else c.failed++;
  };
  const idempotent = req.method === "GET" || req.method === "HEAD";

  const upstream = http.request({
    host: BACKEND_HOST,
    port: c.port,
    method: req.method,
    path: req.url,
    headers: { ...req.headers, "x-forwarded-for": req.socket.remoteAddress },
    agent,
  }, (upRes) => {
    res.writeHead(upRes.statusCode, upRes.headers);
    upRes.pipe(res);
    upRes.on("end", () => settle(upRes.statusCode < 500));
    upRes.on("error", () => { settle(false); res.destroy(); });
  });

  upstream.on("error", (err) => {
    settle(false);
    if (res.destroyed || res.writableEnded) return;
    if (res.headersSent) return res.destroy();
    // Connection-level failure before any response: retry a safe request once on the other container.
    const alt = other(c);
    if (allowRetry && idempotent && isEligible(alt)) {
      lb.retries++;
      return forward(req, res, alt, false);
    }
    lb.badGateway++;
    sendJson(res, 502, { error: "Bad Gateway", detail: `container ${c.id}: ${err.code || err.message}` });
  });

  res.on("close", () => {
    if (!settled) { upstream.destroy(); settle(false); }
  });

  if (idempotent) upstream.end();
  else req.pipe(upstream);
}

const lbServer = http.createServer((req, res) => {
  lb.total++;
  const c = pickBackend();
  if (!c) {
    lb.unavailable++;
    return sendJson(res, 503, { error: "Service Unavailable", detail: "No healthy containers available" }, { "Retry-After": "1" });
  }
  forward(req, res, c, true);
});

// ---------------------------------------------------------------------------
// Docker
// ---------------------------------------------------------------------------

function docker(args, timeout = 180000) {
  return new Promise((resolve, reject) => {
    execFile("docker", args, { cwd: COMPOSE_DIR, timeout, windowsHide: true }, (err, stdout, stderr) => {
      if (err) {
        const msg = (stderr || err.message).trim().split("\n").filter(Boolean).pop() || err.message;
        reject(new Error(msg));
      } else resolve(stdout);
    });
  });
}
const compose = (args) => docker(["compose", ...args]);

let dockerPolling = null;
function pollDocker() {
  if (!dockerPolling) dockerPolling = doPollDocker().finally(() => { dockerPolling = null; });
  return dockerPolling;
}

async function doPollDocker() {
  try {
    const out = await docker(["ps", "-a", "--filter", `label=com.docker.compose.project=${COMPOSE_PROJECT}`, "--format", "{{json .}}"], 15000);
    const states = {};
    for (const line of out.split("\n").filter((l) => l.trim())) {
      const row = JSON.parse(line);
      const m = /com\.docker\.compose\.service=([^,]+)/.exec(row.Labels || "");
      if (m) states[m[1]] = row.State;
    }
    for (const c of Object.values(containers)) {
      const s = states[c.service] || "not created";
      if (s !== c.dockerState) {
        logEvent("docker-state", "docker", s === "running" ? "ok" : "info", `${c.service}: ${c.dockerState} -> ${s}`);
        c.dockerState = s;
        c.running = s === "running";
        if (!c.running) { c.healthy = false; c.healthFails = 0; c.healthDetail = "not running"; }
      }
    }
    if (dockerError) logEvent("docker", "docker", "ok", "Docker reachable again");
    dockerError = null;
  } catch (err) {
    if (dockerError !== err.message) logEvent("docker", "docker", "failed", err.message);
    dockerError = err.message;
  }
  checkEffective();
}

// ---------------------------------------------------------------------------
// Health checks
// ---------------------------------------------------------------------------

function healthResult(c, ok, detail) {
  c.checking = false;
  c.healthDetail = detail;
  if (ok) {
    c.healthFails = 0;
    if (!c.healthy && c.running) {
      c.healthy = true;
      logEvent("health", "health-check", "ok", `${c.id} is healthy`);
    }
  } else {
    c.healthFails++;
    if (c.healthy && c.healthFails >= HEALTH_FAILS_TO_UNHEALTHY) {
      c.healthy = false;
      logEvent("health", "health-check", "failed", `${c.id} is unhealthy (${detail})`);
    }
  }
  checkEffective();
}

function checkHealth(c) {
  if (!c.running || c.checking) return;
  c.checking = true;
  const req = http.get({ host: BACKEND_HOST, port: c.port, path: "/health", agent, timeout: HEALTH_TIMEOUT_MS }, (res) => {
    res.resume();
    healthResult(c, res.statusCode === 200, `HTTP ${res.statusCode}`);
  });
  req.on("timeout", () => req.destroy(new Error("timeout")));
  req.on("error", (err) => healthResult(c, false, err.code || err.message));
}

function waitFor(cond, timeoutMs, abortCond) {
  return new Promise((resolve) => {
    const started = Date.now();
    const check = () => {
      if (cond()) return done("ok");
      if (abortCond && abortCond()) return done("aborted");
      if (Date.now() - started >= timeoutMs) return done("timeout");
    };
    const timer = setInterval(check, 250);
    const done = (r) => { clearInterval(timer); resolve(r); };
    check();
  });
}

// ---------------------------------------------------------------------------
// Actions: split, start, stop, force-unhealthy
// ---------------------------------------------------------------------------

function setSplit(body, source) {
  const missing = ["A", "B"].filter((k) => body[k] === undefined || body[k] === null || body[k] === "");
  if (missing.length) throw new Rejection(`set split: percentage for ${missing.join(" and ")} is required (A + B must equal 100)`, 400);
  const desc = `A ${body.A}% / B ${body.B}%`;
  const a = Number(body.A);
  const b = Number(body.B);
  if (![a, b].every((v) => Number.isInteger(v) && v >= 0 && v <= 100)) {
    throw new Rejection(`${desc}: percentages must be whole numbers from 0 to 100`, 400);
  }
  if (a + b !== 100) throw new Rejection(`${desc}: must add up to exactly 100 (got ${a + b})`, 400);
  if (rollout) throw new Rejection(`${desc}: a rollout to ${rollout.target} is in progress`);
  for (const [id, w] of [["A", a], ["B", b]]) {
    const c = containers[id];
    if (w > 0 && !isEligible(c)) throw new Rejection(`${desc}: container ${id} is ${statusOf(c)} and cannot receive traffic`);
  }
  split = { A: a, B: b };
  logEvent("set-split", source, "ok", desc);
  checkEffective();
  return { message: `Split set to ${desc}` };
}

function startContainer(c, source, body = {}) {
  const timeoutSec = body.timeoutSec === undefined || body.timeoutSec === "" ? START_HEALTH_TIMEOUT_MS / 1000 : Number(body.timeoutSec);
  if (!Number.isInteger(timeoutSec) || timeoutSec < 5 || timeoutSec > 300) {
    throw new Rejection(`start ${c.id}: timeout must be a whole number of seconds from 5 to 300`, 400);
  }
  if (rollout) throw new Rejection(`start ${c.id}: a rollout to ${rollout.target} is in progress`);
  if (c.op) throw new Rejection(`start ${c.id}: container is busy (${c.op})`);
  if (c.running) throw new Rejection(`start ${c.id}: container is already running`);

  c.op = "starting";
  logEvent("start", source, "info", `starting ${c.service} (health timeout ${timeoutSec}s)`);
  const done = (async () => {
    try {
      await compose(["up", "-d", "--no-deps", c.service]);
      await pollDocker();
      const r = await waitFor(() => c.healthy, timeoutSec * 1000);
      if (r !== "ok") return outcome("start", source, false, `${c.service} started but not healthy within ${timeoutSec}s`);
      // A newly started container gets 0% unless it is the only one running.
      if (!other(c).running) split = { [c.id]: 100, [other(c).id]: 0 };
      return outcome("start", source, true, `${c.service} is running and healthy (configured weight ${split[c.id]}%)`);
    } catch (err) {
      return outcome("start", source, false, `${c.service}: ${err.message}`);
    } finally {
      c.op = null;
      checkEffective();
    }
  })();
  return { message: `Starting ${c.service}...`, operation: trackOperation("start", c.id, source, done) };
}

function stopContainer(c, source) {
  if (rollout) throw new Rejection(`stop ${c.id}: a rollout to ${rollout.target} is in progress`);
  if (c.op) throw new Rejection(`stop ${c.id}: container is busy (${c.op})`);
  if (!c.running) throw new Rejection(`stop ${c.id}: container is already stopped`);
  if (isEligible(c) && !isEligible(other(c))) {
    throw new Rejection(`stop ${c.id}: it is the last healthy container (${other(c).id} is ${statusOf(other(c))})`);
  }

  // 1. Remove from routing, and move the configured weight to the other container.
  c.op = "draining";
  c.draining = true;
  split = { [c.id]: 0, [other(c).id]: 100 };
  checkEffective();
  logEvent("stop", source, "info", `${c.id} removed from routing, draining ${c.inFlight} in-flight request(s)`);

  const done = (async () => {
    try {
      // 2. Let in-flight requests finish.
      const r = await waitFor(() => c.inFlight === 0, STOP_DRAIN_TIMEOUT_MS);
      const drainNote = r === "ok"
        ? `${c.id} drained`
        : `${c.id} still had ${c.inFlight} in-flight after ${STOP_DRAIN_TIMEOUT_MS / 1000}s; stopped anyway`;
      logEvent("drain", source, r === "ok" ? "ok" : "warning", drainNote);
      // 3. Stop.
      c.op = "stopping";
      await compose(["stop", c.service]);
      await pollDocker();
      return outcome("stop", source, true, `${c.service} stopped (${drainNote})`);
    } catch (err) {
      return outcome("stop", source, false, `${c.service}: ${err.message}`);
    } finally {
      c.op = null;
      c.draining = false;
      checkEffective();
    }
  })();
  return { message: `Stopping ${c.service} (draining first)...`, operation: trackOperation("stop", c.id, source, done) };
}

function containerCall(c, endpoint, source) {
  const action = endpoint === "fail" ? "force-unhealthy" : "recover";
  if (!c.running) return Promise.reject(new Rejection(`${action} ${c.id}: container is not running`));
  return new Promise((resolve, reject) => {
    const req = http.request({ host: BACKEND_HOST, port: c.port, method: "POST", path: `/${endpoint}`, agent, timeout: 3000 }, (res) => {
      res.resume();
      if (res.statusCode !== 200) return reject(new Error(`${c.id} answered HTTP ${res.statusCode}`));
      logEvent(action, source, "ok", endpoint === "fail" ? `${c.id} will now fail its health check` : `${c.id} will pass its health check again`);
      resolve({ message: `${action} sent to ${c.id}` });
    });
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", reject);
    req.end();
  });
}

// ---------------------------------------------------------------------------
// Safe rollout: start target -> wait healthy -> move traffic -> drain old -> stop old
// ---------------------------------------------------------------------------

function startRollout(body, source) {
  const target = resolveContainer(body.target, "rollout target");
  const old = other(target);
  if (body.from !== undefined && body.from !== "" && resolveContainer(body.from, "rollout from") !== old) {
    throw new Rejection(`rollout: from and target must be different containers (got ${body.from} -> ${body.target})`, 400);
  }
  const timeoutSec = body.timeoutSec === undefined || body.timeoutSec === "" ? 30 : Number(body.timeoutSec);
  if (!Number.isInteger(timeoutSec) || timeoutSec < 5 || timeoutSec > 300) {
    throw new Rejection("rollout: timeout must be a whole number of seconds from 5 to 300", 400);
  }
  if (rollout) throw new Rejection(`rollout to ${target.id}: a rollout to ${rollout.target} is already in progress`);
  for (const c of [target, old]) {
    if (c.op) throw new Rejection(`rollout to ${target.id}: container ${c.id} is busy (${c.op})`);
  }
  if (split[target.id] === 100 && isEligible(target) && !old.running) {
    throw new Rejection(`rollout to ${target.id}: ${target.id} already serves 100% and ${old.id} is stopped, nothing to do`);
  }

  const r = {
    target: target.id, from: old.id, timeoutSec, source,
    status: "running", detail: "", startedAt: new Date().toISOString(), endedAt: null,
    steps: [
      `Start ${target.id}`,
      `Wait for ${target.id} healthy`,
      `Move traffic to ${target.id}`,
      `Drain ${old.id}`,
      `Stop ${old.id}`,
    ].map((name) => ({ name, status: "pending", detail: "" })),
  };
  rollout = r;
  logEvent("rollout", source, "info", `started ${old.id} -> ${target.id} (timeout ${timeoutSec}s per step)`);

  const done = runRollout(r, target, old)
    .catch((err) => {
      target.op = null;
      old.op = null;
      old.draining = false;
      finishRollout(r, "failed", err.message);
    })
    .then(() => ({ status: r.status, detail: r.detail }));
  return { message: `Rollout ${old.id} -> ${target.id} started`, operation: trackOperation("rollout", target.id, source, done, { rollout: r }) };
}

async function runRollout(r, target, old) {
  const timeoutMs = r.timeoutSec * 1000;
  const step = (i, status, detail = "") => {
    Object.assign(r.steps[i], { status, detail });
    if (status !== "active") {
      const result = { done: "ok", skipped: "info", failed: "failed" }[status] || "info";
      logEvent("rollout-step", r.source, result, `${r.steps[i].name}: ${status}${detail ? ` (${detail})` : ""}`);
    } else broadcastSoon();
  };

  // 1. Start target
  step(0, "active");
  let startedByUs = false;
  if (target.running) {
    step(0, "skipped", `${target.service} already running`);
  } else {
    target.op = "starting";
    await compose(["up", "-d", "--no-deps", target.service]);
    startedByUs = true;
    await pollDocker();
    step(0, "done", `${target.service} started`);
  }

  // 2. Wait until healthy
  step(1, "active", `waiting up to ${r.timeoutSec}s`);
  const healthy = await waitFor(() => target.healthy, timeoutMs);
  target.op = null;
  if (healthy !== "ok") {
    step(1, "failed", `not healthy after ${r.timeoutSec}s`);
    let note = "traffic unchanged";
    if (startedByUs) {
      await compose(["stop", target.service]);
      await pollDocker();
      note += `, ${target.id} stopped again`;
    }
    return finishRollout(r, "aborted", `${target.id} did not become healthy within ${r.timeoutSec}s; ${note}`);
  }
  step(1, "done", `${target.id} is healthy`);

  // 3. Move traffic
  step(2, "active");
  const prevSplit = { ...split };
  split = { [target.id]: 100, [old.id]: 0 };
  checkEffective();
  step(2, "done", `A ${split.A}% / B ${split.B}%`);

  if (!old.running) {
    step(3, "skipped", `${old.id} not running`);
    step(4, "skipped", `${old.id} already stopped`);
    return finishRollout(r, "succeeded", `all traffic on ${target.id}`);
  }

  // 4. Drain old. Roll back if the new container goes unhealthy or the drain times out.
  old.op = "draining";
  old.draining = true;
  step(3, "active", `${old.inFlight} in-flight`);
  const drained = await waitFor(() => old.inFlight === 0, timeoutMs, () => !isEligible(target));
  if (drained !== "ok") {
    const why = drained === "timeout"
      ? `${old.inFlight} request(s) still in flight after ${r.timeoutSec}s`
      : `${target.id} became ${statusOf(target)} during drain`;
    old.op = null;
    old.draining = false;
    split = prevSplit;
    checkEffective();
    step(3, "failed", why);
    return finishRollout(r, "aborted", `${why}; rolled back to A ${split.A}% / B ${split.B}%`);
  }
  step(3, "done", "all in-flight requests finished");

  // 5. Stop old
  old.op = "stopping";
  step(4, "active");
  await compose(["stop", old.service]);
  await pollDocker();
  old.op = null;
  old.draining = false;
  step(4, "done", `${old.service} stopped`);
  finishRollout(r, "succeeded", `all traffic on ${target.id}, ${old.id} stopped`);
}

function finishRollout(r, status, detail) {
  r.status = status;
  r.detail = detail;
  r.endedAt = new Date().toISOString();
  for (const s of r.steps) if (s.status === "pending" || s.status === "active") s.status = "cancelled";
  lastRollout = r;
  rollout = null;
  checkEffective();
  logEvent("rollout", r.source, { succeeded: "ok", aborted: "aborted" }[status] || "failed", detail);
}

// ---------------------------------------------------------------------------
// Traffic generator (sends real requests through the load balancer)
// ---------------------------------------------------------------------------

const gen = {
  running: false, rpm: 120, path: "/", credit: 0, timer: null,
  sent: 0, success: 0, failed: 0, inFlight: 0, byContainer: { A: 0, B: 0 }, window: [],
};

function genRecord(ok, container) {
  gen.inFlight--;
  if (ok) gen.success++; else gen.failed++;
  if (container === "A" || container === "B") gen.byContainer[container]++;
  gen.window.push({ t: Date.now(), ok });
}

function genSend() {
  gen.sent++;
  gen.inFlight++;
  let recorded = false;
  const record = (ok, container) => { if (!recorded) { recorded = true; genRecord(ok, container); } };
  const req = http.get({ host: "127.0.0.1", port: LB_PORT, path: gen.path, agent, timeout: 70000 }, (res) => {
    const container = res.headers["x-container"];
    res.resume();
    res.on("end", () => record(res.statusCode < 400, container));
    res.on("error", () => record(false, container));
  });
  req.on("timeout", () => req.destroy(new Error("timeout")));
  req.on("error", () => record(false));
}

function startTraffic(body, source) {
  const rpm = Number(body.rpm);
  const p = String(body.path || "/");
  if (!Number.isInteger(rpm) || rpm < 1 || rpm > 6000) throw new Rejection("traffic: requests per minute must be 1 to 6000", 400);
  if (!p.startsWith("/")) throw new Rejection("traffic: path must start with /", 400);
  gen.rpm = rpm;
  gen.path = p;
  if (!gen.running) {
    gen.running = true;
    gen.credit = 1; // first request goes out immediately
    gen.timer = setInterval(() => {
      gen.credit += gen.rpm / 600; // ticks every 100ms
      while (gen.credit >= 1) { gen.credit--; genSend(); }
    }, 100);
  }
  logEvent("traffic-start", source, "ok", `${rpm} req/min -> GET ${p}`);
  return { message: `Traffic generator: ${rpm} req/min` };
}

function stopTraffic(source) {
  if (!gen.running) throw new Rejection("traffic: generator is not running", 400);
  clearInterval(gen.timer);
  gen.running = false;
  logEvent("traffic-stop", source, "ok", `stopped after ${gen.sent} request(s)`);
  return { message: "Traffic generator stopped" };
}

function resetCounters(source) {
  Object.assign(gen, { sent: 0, success: 0, failed: 0, byContainer: { A: 0, B: 0 }, window: [] });
  Object.assign(lb, { total: 0, unavailable: 0, badGateway: 0, retries: 0 });
  for (const c of Object.values(containers)) Object.assign(c, { requests: 0, success: 0, failed: 0 });
  logEvent("reset-counters", source, "ok", "request counters reset");
  return { message: "Counters reset" };
}

// ---------------------------------------------------------------------------
// Dashboard + API server
// ---------------------------------------------------------------------------

function snapshot() {
  const cutoff = Date.now() - 60000;
  while (gen.window.length && gen.window[0].t < cutoff) gen.window.shift();
  const okLast = gen.window.filter((w) => w.ok).length;
  return {
    lbPort: LB_PORT,
    dockerError,
    containers: Object.values(containers).map((c) => ({
      id: c.id, service: c.service, port: c.port, status: statusOf(c),
      dockerState: c.dockerState, healthy: c.healthy, healthDetail: c.healthDetail, draining: c.draining,
      inFlight: c.inFlight, requests: c.requests, success: c.success, failed: c.failed,
    })),
    split,
    effective: effectiveSplit(),
    lb,
    rollout: rollout || lastRollout,
    gen: {
      running: gen.running, rpm: gen.rpm, path: gen.path,
      sent: gen.sent, success: gen.success, failed: gen.failed, inFlight: gen.inFlight, byContainer: gen.byContainer,
      last60: { completed: gen.window.length, success: okLast, failed: gen.window.length - okLast },
    },
    events: events.slice(0, 150),
  };
}

const streams = new Set();
let broadcastPending = false;
function broadcast() {
  broadcastPending = false;
  const data = `data: ${JSON.stringify(snapshot())}\n\n`;
  for (const res of streams) res.write(data);
}
function broadcastSoon() {
  if (broadcastPending) return;
  broadcastPending = true;
  setTimeout(broadcast, 50);
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > 10000) req.destroy();
    });
    req.on("end", () => {
      if (!raw) return resolve({});
      try { resolve(JSON.parse(raw)); } catch { reject(new Rejection("invalid JSON body", 400)); }
    });
    req.on("error", reject);
  });
}

const routes = {
  "/api/split": (b, s) => setSplit(b, s),
  "/api/rollout": (b, s) => startRollout(b, s),
  "/api/traffic/start": (b, s) => startTraffic(b, s),
  "/api/traffic/stop": (b, s) => stopTraffic(s),
  "/api/counters/reset": (b, s) => resetCounters(s),
};

function getEvents(query) {
  const limit = query.has("limit") ? Number(query.get("limit")) : 50;
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_EVENTS) {
    throw new Rejection(`get logs: limit must be a whole number from 1 to ${MAX_EVENTS}`, 400);
  }
  const only = query.get("source");
  const list = only ? events.filter((e) => e.source === only) : events;
  return { events: list.slice(0, limit), matching: list.length };
}

const INDEX_HTML = path.join(__dirname, "public", "index.html");
const SOURCES = new Set(["ui", "mcp"]); // anything else (curl, scripts) is logged as "api"

const uiServer = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  const { pathname } = url;
  const source = SOURCES.has(req.headers["x-source"]) ? req.headers["x-source"] : "api";

  if (req.method === "GET" && pathname === "/") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
    return fs.createReadStream(INDEX_HTML).pipe(res);
  }
  if (req.method === "GET" && pathname === "/api/stream") {
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-store", Connection: "keep-alive" });
    res.write(`data: ${JSON.stringify(snapshot())}\n\n`);
    streams.add(res);
    req.on("close", () => streams.delete(res));
    return;
  }
  const opMatch = /^\/api\/operations\/([\w-]+)$/.exec(pathname);
  if (req.method === "GET" && opMatch) {
    const op = operations.get(opMatch[1]);
    if (!op) return sendJson(res, 404, { ok: false, error: `unknown operation ${opMatch[1]}` });
    const s = snapshot();
    return sendJson(res, 200, { ok: true, operation: op, containers: s.containers, split: s.split, effective: s.effective });
  }

  let action;
  let handler;
  if (req.method === "GET" && pathname === "/api/state") {
    action = "get-status";
    handler = () => snapshot();
  } else if (req.method === "GET" && pathname === "/api/events") {
    action = "get-logs";
    handler = () => getEvents(url.searchParams);
  } else if (req.method === "POST") {
    const m = /^\/api\/containers\/([^/]*)\/(start|stop|fail|recover)$/.exec(pathname);
    if (m) {
      action = m[2];
      handler = (b, s) => {
        const c = resolveContainer(decodeURIComponent(m[1]), m[2]);
        if (m[2] === "start") return startContainer(c, s, b);
        if (m[2] === "stop") return stopContainer(c, s);
        return containerCall(c, m[2], s);
      };
    } else if (routes[pathname]) {
      action = pathname.replace("/api/", "");
      handler = routes[pathname];
    }
  }
  if (!handler) return sendJson(res, 404, { ok: false, error: "not found" });

  try {
    const body = req.method === "POST" ? await readJson(req) : {};
    const result = await handler(body, source);
    if (req.method === "GET") {
      // Reads by an AI agent belong in its audit trail; dashboard and curl reads are not logged.
      if (source === "mcp") logEvent(action, source, "info", action === "get-status" ? "status read" : `read ${result.events.length} event(s)`);
      return sendJson(res, 200, result);
    }
    sendJson(res, 200, { ok: true, ...result });
  } catch (err) {
    const status = err instanceof Rejection ? err.status : 500;
    logEvent(action, source, status === 500 ? "failed" : "rejected", err.message);
    sendJson(res, status, { ok: false, error: err.message });
  }
});

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

async function main() {
  logEvent("controller", "controller", "info", `starting (compose dir: ${COMPOSE_DIR})`);
  await pollDocker();
  // Initial configured split follows whatever is already running.
  const running = Object.values(containers).filter((c) => c.running);
  if (running.length === 1) split = { [running[0].id]: 100, [other(running[0]).id]: 0 };

  for (const [server, port] of [[lbServer, LB_PORT], [uiServer, UI_PORT]]) {
    server.on("error", (err) => {
      console.error(err.code === "EADDRINUSE"
        ? `Port ${port} is already in use. Set LB_PORT / UI_PORT to free ports and try again.`
        : err.message);
      process.exit(1);
    });
  }

  setInterval(pollDocker, DOCKER_POLL_MS);
  setInterval(() => Object.values(containers).forEach(checkHealth), HEALTH_INTERVAL_MS);
  setInterval(broadcast, 1000);

  lbServer.listen(LB_PORT, () => logEvent("controller", "controller", "ok", `load balancer listening on http://localhost:${LB_PORT}`));
  uiServer.listen(UI_PORT, () => logEvent("controller", "controller", "ok", `dashboard on http://localhost:${UI_PORT}`));
}

process.on("SIGINT", () => {
  console.log("Controller shutting down (containers are left as they are)");
  process.exit(0);
});

main();
