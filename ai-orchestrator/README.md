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
                                              DataHub tools (GraphQL) --> DataHub GMS
```

Files:

- `main.py` — FastAPI server, SSE endpoint, config endpoints
- `agent.py` — model-agnostic agent loop (tool-use cycle)
- `mcp_tools.py` — MCP client: connects to DataHub's MCP server via stdio (`uvx`) or HTTP
- `datahub_tools.py` — fallback GraphQL tool definitions (used before MCP integration)

## Run locally

```bash
cd ai-orchestrator
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export ANTHROPIC_API_KEY="sk-ant-..."     # required
export ANTHROPIC_MODEL="claude-sonnet-5"  # optional

# --- Tool source: DataHub's MCP server ---
# Option A (default): stdio — spawns `uvx mcp-server-datahub` against your GMS.
#   Requires `uv` installed (https://github.com/astral-sh/uv).
export DATAHUB_GMS_URL="http://localhost:8080"      # your DataHub GMS (local or remote)
export DATAHUB_GMS_TOKEN="<pat>"                    # optional (if GMS auth is on)

# IMPORTANT: disable DataHub telemetry, otherwise the MCP server tries to reach
# track.datahubproject.io on every request and times out (~30s retries) before
# Claude can respond. Disabling drops response time from ~35s to ~3s.
export DATAHUB_TELEMETRY_ENABLED=false


# Use the venv's uvicorn directly to avoid conflicts with system-installed versions
.venv/bin/uvicorn main:app --port 8000 --reload
```

> ⚡ **Slow responses?** If chat replies take ~30-40s, you almost certainly forgot
> `DATAHUB_TELEMETRY_ENABLED=false`. This is an env-var-only fix — no code changes.

Test:

```bash
curl -N -X POST http://localhost:8000/api/ai/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"What PII fields does the users dataset have?","context":{"pageType":"home"}}'
```

The UI (`AIChatButton.tsx`) points at `http://localhost:8000/api/ai/chat` and falls
back to a mock if the orchestrator is not running.

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
