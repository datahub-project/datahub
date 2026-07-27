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
- `datahub_tools.py` — tool definitions + GraphQL executors

## Run locally

```bash
cd ai-orchestrator
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export ANTHROPIC_API_KEY="sk-ant-..."     # required
export ANTHROPIC_MODEL="claude-sonnet-5"  # optional
export DATAHUB_GMS_URL="http://localhost:8080"  # optional

uvicorn main:app --port 8000 --reload
```

Test:
```bash
curl -N -X POST http://localhost:8000/api/ai/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"What PII fields does the users dataset have?","context":{"pageType":"home"}}'
```

The UI (`AIChatButton.tsx`) points at `http://localhost:8000/api/ai/chat` and falls
back to a mock if the orchestrator is not running.
