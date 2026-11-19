# datahub-ai-service

DataHub's AI service. It will serve DataHub's MCP tools (from `datahub-core-mcp-tools`) over
streamable HTTP at `/mcp`. Today it serves an unauthenticated `/health` probe.

## Run

```bash
export DATAHUB_GMS_URL=http://localhost:8080
datahub-ai-service serve
```

## Configuration

| Variable          | Default   | Meaning                                 |
| ----------------- | --------- | --------------------------------------- |
| `DATAHUB_GMS_URL` | required  | GMS base URL; may include a path prefix |
| `DATAHUB_AI_HOST` | `0.0.0.0` | Interface to listen on                  |
| `DATAHUB_AI_PORT` | `9005`    | Port to listen on                       |

`DATAHUB_GMS_TOKEN` is refused: each caller will send their own token.

## Development

```bash
../../gradlew :packages:datahub-ai-service:lintFix
../../gradlew :packages:datahub-ai-service:lint
../../gradlew :packages:datahub-ai-service:testQuick
```
