# datahub-ai-service

DataHub's AI service. It serves DataHub's MCP tools (from `datahub-core-mcp-tools`) over streamable
HTTP at `/mcp`, and an unauthenticated `/health` probe.

Every MCP request carries its caller's DataHub personal access token as
`Authorization: Bearer <token>`. The service checks it with GMS and calls DataHub as that
caller. It never uses a shared or system credential.

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

## Configuration

| Variable                              | Default   | Meaning                                                    |
| ------------------------------------- | --------- | ---------------------------------------------------------- |
| `DATAHUB_GMS_URL`                     | required  | GMS base URL; may include a path prefix                    |
| `DATAHUB_AI_HOST`                     | `0.0.0.0` | Interface to listen on                                     |
| `DATAHUB_AI_PORT`                     | `9005`    | Port to listen on                                          |
| `DATAHUB_AI_MCP_ENABLED`              | `true`    | Serve MCP at `/mcp`                                        |
| `DATAHUB_AI_MCP_RESPONSE_MODE`        | `sse`     | `sse` streams responses; `json` returns one body           |
| `DATAHUB_MCP_TOKEN_CACHE_TTL_SECONDS` | `300`     | How long a verified token is trusted before checking again |
| `DATAHUB_AI_GMS_TIMEOUT_SECONDS`      | `30`      | Connect and read timeout for each GMS request              |

`DATAHUB_GMS_TOKEN` is refused: each caller sends their own token.

Use `json` only behind proxies that can't stream responses, and give them idle timeouts longer
than the slowest tool call.

## Development

```bash
../../gradlew :packages:datahub-ai-service:lintFix
../../gradlew :packages:datahub-ai-service:lint
../../gradlew :packages:datahub-ai-service:testQuick
```
