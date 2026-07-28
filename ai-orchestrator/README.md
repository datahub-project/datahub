# DataHub AI Orchestrator

Small Python service that powers the DataHub AI Assistant chat. It runs a
**model-agnostic agent loop**: takes a user question + page context, lets the LLM
(Claude) call **DataHub tools** (GraphQL) to fetch real metadata, and streams the
answer back to the UI over SSE.

> Hackathon build. Target is to port this loop into GMS (Java). See SHIPIT-64.

## Architecture

```
Browser (AIChatButton) --POST /api/ai/chat--> Orchestrator (this) --> Claude API
                        <----- SSE tokens -----            |
                                                           v
                                    DataHub MCP server (container) --> DataHub GMS
```

The MCP session is opened **once** at startup and reused for every request
(`PersistentMCP` in `mcp_tools.py`), so no server is spawned per chat message.
A background task owns the session and auto-reconnects if it drops.

Files:

- `main.py` — FastAPI server, SSE endpoint, config endpoints, MCP lifespan
- `agent.py` — model-agnostic agent loop (tool-use cycle)
- `mcp_tools.py` — persistent MCP session, tool discovery and execution
- `mcp-server/Dockerfile` — standalone MCP server image (pinned version)
- `docker-compose.mcp.yml` — runs that MCP server on `127.0.0.1:8001`
- `datahub_tools.py` — legacy direct-GraphQL tools, kept for reference only

## Run locally

Prerequisite: a running DataHub — `scripts/dev/datahub-dev.sh start`.

### 1. Secrets

Create `.env` (gitignored):

```bash
ANTHROPIC_API_KEY=sk-ant-...
DATAHUB_GMS_TOKEN=<pat>                     # required when GMS auth is enabled
DATAHUB_TELEMETRY_ENABLED=false             # avoids slow telemetry retries
DATAHUB_MCP_URL=http://localhost:8001/mcp   # use the containerised MCP server
```

Quickstart GMS runs with `METADATA_SERVICE_AUTH_ENABLED=true`, so a Personal Access
Token is needed. Generate one from the UI: Settings → Access Tokens.

### 2. MCP server

```bash
docker compose -f docker-compose.mcp.yml up -d --build
docker compose -f docker-compose.mcp.yml ps   # expect healthy
```

Published on loopback only — the endpoint carries a DataHub token and the OSS server
does not authenticate callers itself. Bump the pinned server version via
`MCP_SERVER_DATAHUB_VERSION` in `mcp-server/Dockerfile`.

### 3. Orchestrator

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

uvicorn main:app --port 8000 --reload
```

Startup logs a single `MCP connected (N tools).` line.

> ⚡ **Slow responses without the MCP container?** Add `DATAHUB_TELEMETRY_ENABLED=false`
> to your `.env`. The MCP server's telemetry retries (~30s) are the bottleneck.

## Test

```bash
curl -N -X POST http://localhost:8000/api/ai/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"What PII fields does the users dataset have?","context":{"pageType":"home"}}'
```

The UI (`AIChatButton.tsx`) points at `http://localhost:8000/api/ai/chat` and falls
back to a mock if the orchestrator is not running.

### Test in the browser

1. Open the DataHub UI — `http://localhost:9002` (quickstart) or `http://localhost:3000` (Vite dev)
2. Log in (`datahub` / `datahub`) and click the 🤖 button (bottom-right)
3. Run a multi-turn conversation to verify memory:

   | # | Ask | What it proves |
   |---|-----|----------------|
   | 1 | `What datasets are on Hive?` | tool call → real metadata |
   | 2 | `How many of those did you just list?` | remembers Turn 1 |
   | 3 | `Tell me more about the first one` | remembers list order |

Responses should stream back in ~2–6s (containerised MCP). If the button shows a
canned/mock reply, the orchestrator isn't reachable on port 8000.

## Testing the Session API

Create a new session:

```bash
curl -X POST "http://127.0.0.1:8000/sessions" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_42",
    "system_prompt": "You are a helpful DataHub assistant.",
    "metadata": {"env": "development", "app": "ai-orchestrator"}
  }'

curl -X  GET "http://127.0.0.1:8000/sessions/9cacfa60-7ad4-417a-acf4-44176d1ea7fe" \
     -H "Accept: application/json"
```

## Conversation Memory (multi-turn chat)

The `/api/ai/chat` endpoint accepts an optional `session_id`. When present, the
orchestrator:

1. Loads all prior messages for that session from MySQL
2. Passes them to the agent so Claude has full conversation context
3. Saves the user message + assistant response back to MySQL

The UI (`AIChatButton.tsx`) generates a `session_id` via `crypto.randomUUID()`
once per browser tab and sends it in every request — so each tab is one
continuous conversation.

Verify memory works across two separate requests:

```bash
SID="demo-$(date +%s)"

# Turn 1 — tell it something
curl -sN -X POST http://localhost:8000/api/ai/chat \
  -H "Content-Type: application/json" \
  -d "{\"message\":\"Remember the word PINEAPPLE\",\"session_id\":\"$SID\"}"

# Turn 2 — same session, ask it to recall
curl -sN -X POST http://localhost:8000/api/ai/chat \
  -H "Content-Type: application/json" \
  -d "{\"message\":\"What word did I ask you to remember?\",\"session_id\":\"$SID\"}"
# -> Claude replies "PINEAPPLE" (loaded from MySQL history)
```
