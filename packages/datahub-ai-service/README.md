# datahub-ai-service

DataHub's AI service. It serves DataHub's MCP tools (from `datahub-core-mcp-tools`) over streamable
HTTP at `/mcp`, and an unauthenticated `/health` probe.

Authentication follows GMS. When GMS auth is on (`METADATA_SERVICE_AUTH_ENABLED=true`), every
MCP request carries its caller's DataHub personal access token as `Authorization: Bearer <token>`;
the service checks it with GMS and calls DataHub as that caller. When GMS auth is off, no token
is needed and requests run as DataHub's system user, as direct calls to GMS do. The service
never holds a shared or system credential. While GMS can't be reached, or its auth mode can't be
told, `/mcp` answers 503.

While GMS auth is off, anyone who reaches `/mcp` acts as the system user, so `/mcp` answers only
requests addressed to an allowed host (`DATAHUB_AI_ALLOWED_HOSTS`) and, when they carry an
`Origin`, from an allowed origin (`DATAHUB_AI_ALLOWED_ORIGINS`). This stops a web page from
reaching `/mcp` through DNS rebinding; it is not authentication. Behind the DataHub frontend, set
`DATAHUB_AI_TRUSTED_PROXIES` to the frontend so its `X-Forwarded-Host` is believed.

`/health` reports the auth mode it last saw, for status pages and debugging.

## Run

```bash
export DATAHUB_GMS_URL=http://localhost:8080
datahub-ai-service serve
```

Then point an MCP client at `http://localhost:9005/mcp`, for example:

```bash
claude mcp add --transport http datahub http://localhost:9005/mcp \
  --header "Authorization: Bearer <personal access token>"
```

Or run the image (`docker/datahub-ai-service/Dockerfile`, built with `./gradlew :packages:datahub-ai-service:docker`):

```bash
docker run -p 9005:9005 -e DATAHUB_GMS_URL=http://host.docker.internal:8080 acryldata/datahub-ai-service:head
```

The image runs as a non-root user and reports health from `/health`. Its third-party
dependencies are locked in `docker/datahub-ai-service/requirements.lock`; regenerate it with
`./gradlew :packages:datahub-ai-service:lockfileUpdate`.

## Configuration

| Variable                              | Default                     | Meaning                                                                |
| ------------------------------------- | --------------------------- | ---------------------------------------------------------------------- |
| `DATAHUB_GMS_URL`                     | required                    | GMS base URL; may include a path prefix                                |
| `DATAHUB_AI_HOST`                     | `0.0.0.0`                   | Interface to listen on                                                 |
| `DATAHUB_AI_PORT`                     | `9005`                      | Port to listen on                                                      |
| `DATAHUB_AI_MCP_ENABLED`              | `true`                      | Serve MCP at `/mcp`                                                    |
| `DATAHUB_AI_MCP_RESPONSE_MODE`        | `sse`                       | `sse` streams responses; `json` returns one body                       |
| `DATAHUB_AI_METRICS_ENABLED`          | `true`                      | Serve Prometheus metrics at `/metrics`                                 |
| `DATAHUB_AI_AUTH_MODE`                | `auto`                      | `auto` follows GMS; `required` serves `/mcp` only while GMS auth is on |
| `DATAHUB_AI_ALLOWED_HOSTS`            | `localhost,127.0.0.1,[::1]` | Hosts `/mcp` answers while GMS auth is off (`host` or `host:port`)     |
| `DATAHUB_AI_ALLOWED_ORIGINS`          | `http(s)://` those hosts    | Origins `/mcp` accepts while GMS auth is off (`scheme://host[:port]`)  |
| `DATAHUB_AI_TRUSTED_PROXIES`          | none                        | IPs, networks or hostnames whose `X-Forwarded-Host` is believed        |
| `DATAHUB_MCP_TOKEN_CACHE_TTL_SECONDS` | `300`                       | How long a verified token is trusted before checking again             |
| `DATAHUB_AI_GMS_TIMEOUT_SECONDS`      | `30`                        | Connect and read timeout for each GMS request                          |
| `DATAHUB_AI_TOOL_TIME_LIMIT_SECONDS`  | `300`                       | How long one tool call may keep sending requests to GMS                |

`DATAHUB_GMS_TOKEN` is refused: each caller sends their own token.

Requests to GMS are retried only when GMS never ran them (a refused connection or a 429), so
a write is never applied twice.

Use `json` only behind proxies that can't stream responses, and give them idle timeouts longer
than the slowest tool call.

## Metrics

`/metrics` serves Prometheus metrics without authentication, like `/health`; the frontend
proxies only `/mcp`, so it is reachable only where the service's own port is. Besides the
process metrics:

| Metric                                      | Labels             | Meaning                                                       |
| ------------------------------------------- | ------------------ | ------------------------------------------------------------- |
| `datahub_ai_mcp_tool_calls_total`           | `tool`, `outcome`  | Tool calls; `outcome` is `ok`, `error` or `cancelled`         |
| `datahub_ai_mcp_tool_call_duration_seconds` | `tool`             | Tool call latency                                             |
| `datahub_ai_mcp_auth_failures_total`        | `reason`           | `/mcp` requests refused before reaching a tool                |
| `datahub_ai_gms_request_duration_seconds`   | `endpoint`         | Latency of requests to GMS made for callers, retries included |
| `datahub_ai_gms_request_errors_total`       | `endpoint`, `kind` | Failed GMS requests: `timeout`, `connection`, `rejected`, ... |
| `datahub_ai_usage_events_dropped_total`     |                    | Usage events dropped because the send queue was full          |

A call to a tool the server doesn't serve is counted under `tool="other"`.

## Telemetry

Like the DataHub CLI, the service sends anonymous usage telemetry: one event when it starts and
one per tool call, with the tool's name, the MCP client's name and version, timings, result
size, the error's class when a call fails, and the GMS version. Events never include tool
arguments, results, error messages or who called. Turn them off with
`DATAHUB_TELEMETRY_ENABLED=false`.

## Development

```bash
../../gradlew :packages:datahub-ai-service:lintFix
../../gradlew :packages:datahub-ai-service:lint
../../gradlew :packages:datahub-ai-service:testQuick
```
