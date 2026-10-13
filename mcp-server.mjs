// MCP server for the A/B controller, over stdio.
//
// This is a thin client: every tool calls the controller's HTTP API (the same code paths the
// dashboard buttons use) with "X-Source: mcp", so validation, routing rules and event logging
// all happen in controller.js and every MCP action lands in the central Event Log as "mcp".
// Arguments are deliberately NOT validated here: an invalid request must still reach the
// controller so that its rejection is logged.
//
// stdout carries the MCP protocol, so this file must never console.log; use console.error.

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";

const CONTROLLER_URL = new URL(process.env.CONTROLLER_URL || "http://localhost:9081");
const POLL_MS = 500;

class ToolError extends Error {}

const text = (value) => ({ content: [{ type: "text", text: typeof value === "string" ? value : JSON.stringify(value, null, 2) }] });
const failure = (message) => ({ content: [{ type: "text", text: message }], isError: true });

// `mcp: true` marks the request as an MCP action so the controller logs it with source "mcp".
// Internal polling of an operation's progress is not an action and is sent without it.
async function controller(method, path, { body, mcp = false } = {}) {
  let res;
  try {
    res = await fetch(new URL(path, CONTROLLER_URL), {
      method,
      headers: { "Content-Type": "application/json", ...(mcp ? { "X-Source": "mcp" } : {}) },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(15000),
    });
  } catch (err) {
    throw new ToolError(
      `Cannot reach the controller at ${CONTROLLER_URL.origin} (${err.cause?.code || err.message}). ` +
      "Start it with: node ab-demo/controller/controller.js",
    );
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new ToolError(`Rejected by controller: ${data.error || `HTTP ${res.status}`}`);
  return data;
}

// Polls a long-running controller operation until it finishes or `maxWaitMs` passes,
// sending MCP progress notifications when the client asked for them.
async function waitForOperation(opId, maxWaitMs, progress) {
  const deadline = Date.now() + maxWaitMs;
  let lastMessage = "";
  for (;;) {
    const data = await controller("GET", `/api/operations/${opId}`);
    const op = data.operation;
    if (op.status !== "running") return { ...data, finished: true };
    const active = op.rollout?.steps.find((s) => s.status === "active");
    const message = active ? `${active.name}${active.detail ? ` (${active.detail})` : ""}` : `${op.kind} ${op.target}: running`;
    if (message !== lastMessage) {
      lastMessage = message;
      await progress(message);
    }
    if (Date.now() >= deadline) return { ...data, finished: false };
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}

function containerSummary(c, s) {
  return {
    id: c.id,
    service: c.service,
    status: c.status,
    docker_state: c.dockerState,
    healthy: c.healthy,
    health_check: c.healthDetail,
    configured_percent: s.split[c.id],
    effective_percent: s.effective[c.id],
    in_flight: c.inFlight,
    requests: { total: c.requests, success: c.success, failed: c.failed },
  };
}

// Runs an action that returns an operation id, then waits for its real outcome.
async function runOperation(path, body, maxWaitMs, progress) {
  const started = await controller("POST", path, { body, mcp: true });
  const result = await waitForOperation(started.operation, maxWaitMs, progress);
  const op = result.operation;
  const summary = {
    operation: op.id,
    action: op.kind,
    status: result.finished ? op.status : "still running",
    detail: result.finished ? op.detail : "Still in progress; call get_status to follow it.",
    ...(op.rollout ? { steps: op.rollout.steps } : {}),
    containers: result.containers.map((c) => containerSummary(c, result)),
    split: { configured: result.split, effective: { A: result.effective.A, B: result.effective.B }, reason: result.effective.reason },
  };
  const failed = result.finished && op.status !== "succeeded";
  return failed ? { ...text(summary), isError: true } : text(summary);
}

const container = { type: "string", enum: ["A", "B"], description: 'Container to act on: "A" (container-a) or "B" (container-b)' };

const TOOLS = [
  {
    name: "get_status",
    title: "Get load balancer status",
    description:
      "Current state of the A/B system: each container's status (healthy, unhealthy, draining, stopped...), Docker state, " +
      "health check result, configured vs effective traffic split (and why they differ), request counts, " +
      "in-flight requests, active or last rollout, traffic generator stats, and current errors.",
    inputSchema: { type: "object", properties: {} },
    annotations: { readOnlyHint: true },
  },
  {
    name: "set_traffic_split",
    title: "Set traffic split",
    description:
      "Set the percentage of new requests sent to A and to B. Rules: whole numbers that add up to exactly 100; " +
      "any container given more than 0% must be running, healthy and not draining; rejected while a rollout runs.",
    inputSchema: {
      type: "object",
      properties: {
        a_percent: { type: "integer", minimum: 0, maximum: 100, description: "Percent of traffic to container A" },
        b_percent: { type: "integer", minimum: 0, maximum: 100, description: "Percent of traffic to container B" },
      },
      required: ["a_percent", "b_percent"],
    },
    annotations: { idempotentHint: true },
  },
  {
    name: "start_container",
    title: "Start container",
    description:
      "Start a stopped container and wait until it passes its health check or the timeout expires. " +
      "It joins with 0% of traffic unless it is the only container running. Rejected if already running or during a rollout.",
    inputSchema: {
      type: "object",
      properties: {
        container,
        timeout_seconds: { type: "integer", minimum: 5, maximum: 300, default: 60, description: "How long to wait for a healthy status" },
      },
      required: ["container"],
    },
  },
  {
    name: "stop_container",
    title: "Stop container",
    description:
      "Gracefully stop a container: remove it from routing (its traffic moves to the other container), wait for its " +
      "in-flight requests to finish, then stop it. Rejected if it is the last healthy container, already stopped, or during a rollout.",
    inputSchema: { type: "object", properties: { container }, required: ["container"] },
    annotations: { destructiveHint: true },
  },
  {
    name: "safe_rollout",
    title: "Safe rollout",
    description:
      "Move all traffic from one container to the other safely: start target -> wait until healthy -> move 100% of traffic " +
      "-> drain the old container -> stop it. Aborts if the target is not healthy in time; rolls back the split if the " +
      "target turns unhealthy or the drain times out. Only one rollout at a time; other changes are rejected while it runs.",
    inputSchema: {
      type: "object",
      properties: {
        target: { ...container, description: "Container that should receive all traffic" },
        from: { ...container, description: "Optional: container traffic moves away from (must be the other one)" },
        timeout_seconds: { type: "integer", minimum: 5, maximum: 300, default: 30, description: "Timeout for the health wait and for the drain" },
      },
      required: ["target"],
    },
    annotations: { destructiveHint: true },
  },
  {
    name: "get_recent_logs",
    title: "Get recent controller events",
    description: "Recent entries from the central Event Log (newest first): time, action, source (ui, api, mcp, router, health-check, docker...), result and detail.",
    inputSchema: {
      type: "object",
      properties: {
        limit: { type: "integer", minimum: 1, maximum: 300, default: 20, description: "How many events to return" },
        source: { type: "string", description: 'Optional: only events from this source, e.g. "mcp" or "health-check"' },
      },
    },
    annotations: { readOnlyHint: true },
  },
];

const HANDLERS = {
  async get_status() {
    const s = await controller("GET", "/api/state", { mcp: true });
    const r = s.rollout;
    return text({
      load_balancer_url: `http://localhost:${s.lbPort}`,
      containers: s.containers.map((c) => containerSummary(c, s)),
      split: { configured: s.split, effective: { A: s.effective.A, B: s.effective.B }, reason: s.effective.reason },
      load_balancer_counts: {
        total_requests: s.lb.total,
        no_healthy_backend_503: s.lb.unavailable,
        bad_gateway_502: s.lb.badGateway,
        retried_on_other_container: s.lb.retries,
      },
      rollout: r ? { from: r.from, target: r.target, status: r.status, detail: r.detail, started_at: r.startedAt, ended_at: r.endedAt, steps: r.steps } : null,
      traffic_generator: s.gen,
      errors: {
        docker: s.dockerError,
        recent: s.events
          .filter((e) => ["failed", "rejected", "aborted", "warning"].includes(e.result))
          .slice(0, 10),
      },
    });
  },

  async set_traffic_split(args) {
    const data = await controller("POST", "/api/split", { body: { A: args.a_percent, B: args.b_percent }, mcp: true });
    return text(data.message);
  },

  async start_container(args, progress) {
    const timeout = Number(args.timeout_seconds) || 60;
    // Allow extra time beyond the health timeout for `docker compose up` (it may need to build the image).
    return runOperation(`/api/containers/${encodeURIComponent(args.container ?? "")}/start`,
      { timeoutSec: args.timeout_seconds }, (timeout + 120) * 1000, progress);
  },

  async stop_container(args, progress) {
    return runOperation(`/api/containers/${encodeURIComponent(args.container ?? "")}/stop`, {}, 150000, progress);
  },

  async safe_rollout(args, progress) {
    const timeout = Number(args.timeout_seconds) || 30;
    return runOperation("/api/rollout",
      { target: args.target, from: args.from, timeoutSec: args.timeout_seconds }, (2 * timeout + 180) * 1000, progress);
  },

  async get_recent_logs(args) {
    const query = new URLSearchParams({ limit: String(args.limit ?? 20) });
    if (args.source) query.set("source", args.source);
    const data = await controller("GET", `/api/events?${query}`, { mcp: true });
    return text(data);
  },
};

const server = new Server(
  { name: "ab-controller", version: "1.0.0" },
  {
    capabilities: { tools: {} },
    instructions:
      "Controls the A/B load balancer in front of container-a and container-b. Call get_status first. " +
      "Every action is validated by the controller and recorded in its Event Log with source \"mcp\".",
  },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));

server.setRequestHandler(CallToolRequestSchema, async (request, extra) => {
  const { name, arguments: args = {} } = request.params;
  const handler = HANDLERS[name];
  if (!handler) return failure(`Unknown tool "${name}". Available: ${TOOLS.map((t) => t.name).join(", ")}`);

  const progressToken = request.params._meta?.progressToken;
  let step = 0;
  const progress = async (message) => {
    if (progressToken === undefined) return;
    await extra.sendNotification({ method: "notifications/progress", params: { progressToken, progress: ++step, message } });
  };

  try {
    return await handler(args, progress);
  } catch (err) {
    return failure(err instanceof ToolError ? err.message : `Unexpected error: ${err.message}`);
  }
});

await server.connect(new StdioServerTransport());
console.error(`ab-controller MCP server running (controller: ${CONTROLLER_URL.origin})`);
