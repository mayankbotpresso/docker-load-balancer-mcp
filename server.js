const express = require("express");

const app = express();
const PORT = process.env.PORT || 3000;
const NAME = process.env.CONTAINER_NAME || "unknown";

let healthy = true;

// Every response says which container handled it.
app.use((req, res, next) => {
  res.set("X-Container", NAME);
  next();
});

app.get("/", (req, res) => {
  res.json({ container: NAME, message: `Hello from Container ${NAME}` });
});

// Readiness / health check: 200 when healthy, 503 after /fail.
app.get("/health", (req, res) => {
  res.status(healthy ? 200 : 503).json({ container: NAME, healthy });
});

// Slow request, e.g. /delay?ms=10000 (default 5000ms, max 60000ms).
app.get("/delay", (req, res) => {
  const ms = Math.min(Number(req.query.ms) || 5000, 60000);
  setTimeout(() => {
    res.json({ container: NAME, delayedMs: ms });
  }, ms);
});

// Force this container to report unhealthy.
app.post("/fail", (req, res) => {
  healthy = false;
  res.json({ container: NAME, healthy });
});

// Restore health (handy for resetting the demo).
app.post("/recover", (req, res) => {
  healthy = true;
  res.json({ container: NAME, healthy });
});

const server = app.listen(PORT, () => {
  console.log(`Container ${NAME} listening on port ${PORT}`);
});

// Graceful shutdown: stop accepting new connections, let in-flight requests finish.
process.on("SIGTERM", () => {
  console.log(`Container ${NAME} received SIGTERM, draining...`);
  server.close(() => {
    console.log(`Container ${NAME} drained, exiting`);
    process.exit(0);
  });
});
