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


# Use the venv's uvicorn directly to avoid conflicts with system-installed versions
.venv/bin/uvicorn main:app --port 8000 --reload
```

Test:

```bash
curl -N -X POST http://localhost:8000/api/ai/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"What PII fields does the users dataset have?","context":{"pageType":"home"}}'
```

The UI (`AIChatButton.tsx`) points at `http://localhost:8000/api/ai/chat` and falls
back to a mock if the orchestrator is not running.
