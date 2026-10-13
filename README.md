# Docker A/B Load Balancer with MCP Controls

## Architecture

- `server.js`: Express backend image, run as containers A and B.
- `controller/controller.js`: HTTP load balancer on port 9080, dashboard and control API on port 9081. Uses the local Docker Compose CLI to manage backend containers.
- `controller/public/index.html`: live dashboard.
- `controller/mcp-server.mjs`: stdio MCP server forwarding requests to the same controller API.
- `docker-compose.yml`: defines backend A (port 3001), B (port 3002), and the controller (ports 9080/9081).

## Requirements

Docker Engine with `docker compose`; Node.js 20+ and npm are required on the host only for local MCP stdio. The controller runs in Compose and requires the Docker socket `/var/run/docker.sock` to manage backends. **Security:** Docker socket access effectively grants host-level Docker control; use only in a trusted local demo environment. Docker Desktop/host-gateway behavior and Linux socket permissions must be validated on your machine.

## Start locally

From this repository's root:

```bash
docker compose up --build -d
```

Open http://localhost:9081 for the dashboard. Send application traffic through http://localhost:9080. Backends are also available at http://localhost:3001 and http://localhost:3002.

To run MCP, open another terminal:

```bash
cd controller
npm ci
node mcp-server.mjs
```

## MCP client configuration

Configure an MCP stdio server with executable `node`, argument **absolute path** to `<repository>/controller/mcp-server.mjs`, and environment variable `CONTROLLER_URL=http://localhost:9081`. The client must start the MCP process on the same machine where the controller is accessible. Avoid committing machine-specific absolute paths.

MCP tools: `get_status`, `set_traffic_split`, `start_container`, `stop_container`, `safe_rollout`, `get_recent_logs`.

## HTTP API

- `GET /api/state`, `GET /api/events?limit=50`, `GET /api/stream`
- `POST /api/split` (JSON split percentages)
- `POST /api/containers/A/start`, `/stop`, `/fail`, `/recover` (also B)
- `POST /api/rollout`
- `POST /api/traffic/start`, `POST /api/traffic/stop`
- `GET /api/operations/<operation-id>`
- `POST /api/counters/reset`

API routes above are served by the controller on port 9081; the application load balancer uses port 9080.

## Timeouts and checks

Controller configuration currently includes a 1-second health check interval, 1-second health request timeout, 2 failed checks before marking unhealthy, 2-second Docker polling, 60-second container startup health timeout, and 60-second stop/drain timeout. Rollout timeouts are configurable between 5 and 300 seconds per step.

## Submission notes

- `docker compose up --build -d` starts the backends, controller, and dashboard. The MCP stdio process is started by your MCP client or separately using the commands above.
- Docker Compose behavior and host-gateway/socket permissions need validation on the target machine; this execution environment has no Docker Engine.
- Static Node.js syntax checks were run for backend, controller, and MCP server. Docker runtime and end-to-end testing require Docker Engine and were not performed in the preparation environment.
- Add a **real** demo video URL after recording the working application; none is included or fabricated.
- AI assistance: ChatGPT was used to inspect the supplied ZIP, isolate the assignment files, check JavaScript syntax, and prepare GitHub submission documentation. Existing application/controller code was supplied by the project owner.
