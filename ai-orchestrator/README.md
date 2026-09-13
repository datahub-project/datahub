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

PII tagging (see [Column-level PII tagging](#column-level-pii-tagging)):

- `pii_taxonomy.py` — the label set and the provenance tag
- `pii_models.py` — `Column` / `Verdict`, and validation of model output
- `pii_rules.py` — deterministic recognisers, run before the model
- `pii_classifier.py` — model pass, for residual columns only
- `pii_writer.py` — single batched `editableSchemaMetadata` write
- `pii_tagger.py` — propose/apply orchestration and the proposal cache
- `local_tools.py` — what the model may call, and the confirmation gate
- `bootstrap/` — `seed_tags.py`, `tag_state.py`, `try_tagger.py`

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

   | #   | Ask                                    | What it proves            |
   | --- | -------------------------------------- | ------------------------- |
   | 1   | `What datasets are on Hive?`           | tool call → real metadata |
   | 2   | `How many of those did you just list?` | remembers Turn 1          |
   | 3   | `Tell me more about the first one`     | remembers list order      |

Responses should stream back in ~2–6s (containerised MCP). If the button shows a
canned/mock reply, the orchestrator isn't reachable on port 8000.

## Column-level PII tagging

The assistant can classify a dataset's columns and write `PII.*` tags to DataHub. It is
a **two-step protocol** — propose, then apply — and nothing is written until a human
confirms.

```
propose_pii_tags(dataset_urn)   read-only: reads the schema, classifies, caches a proposal
apply_pii_tags(dataset_urn)     writes the confirmed proposal, one request
```

### Rules first, model second

Most PII columns are named exactly what they are, and a lookup decides those faster and
more repeatably than a model can. `pii_rules.py` settles them with no inference; only
columns that genuinely depend on context reach `pii_classifier.py` — `name` on an
employee table is a person, `name` on a product table is not.

Across the seeded demo datasets that is **117 columns, 69 flagged by rules, 6 needing a
model call**; 8 of 11 tables need none at all. Measured on `appdb.hr_employees` (12
tagged columns):

| Step    | Rules only | With a residual column    |
| ------- | ---------- | ------------------------- |
| propose | ~35 ms     | ~1.7–2 s (one Haiku call) |
| apply   | ~40 ms     | ~40 ms                    |

The rule tables need tuning against real schemas — production columns are abbreviated
(`given_nm`, `reporter_eml`, `emp_no`, `nino`), and each abbreviation left unencoded is
the difference between a 35 ms proposal and a 4.4 s one. The model is the safety net for
whatever the rules miss, so a gap costs latency, not correctness.

Remaining turn latency is the **chat model**, not this pipeline: `claude-sonnet-5` costs
~2–4.5 s per agent-loop iteration regardless of output size, and a tagging turn takes
three or four. `ANTHROPIC_MODEL=claude-haiku-4-5` roughly halves it.

### One write, not one per column

Tags live in the `editableSchemaMetadata` aspect, which the OpenAPI v3 endpoint replaces
wholesale. `pii_writer.py` reads it, merges every column, and writes once. That is not
only faster — it is the only correct option. DataHub's per-column write paths each
read-modify-write the same aspect, so a batch of them races and the last write wins,
which is how an earlier run that reported seven tagged columns left tags on one.

The merge is additive: existing tags, descriptions, and glossary terms on those fields
are preserved, because a replace would otherwise drop a steward's work. Because writes
go through this path, **MCP needs no mutation tools** — `local_tools.py` filters out any
write tool the MCP server offers, so the model has no way around the review step.

### Why the guards exist

**The confirmation gate is enforced in code, not by the prompt.** `apply_pii_tags`
refuses if the proposal was produced in the current turn. This is not theoretical: the
logs show the model proposing and immediately attempting to apply in the same turn, which
would have the user confirming a table they were never shown.

**Pending proposals are named in the system prompt.** Persisted history keeps only
message text, so the `tool_use` record proving a proposal exists is gone by the next
request. Left to infer, the model concludes it has "no proposal on record" and
classifies again — and since every turn reaches the same conclusion, the reviewer never
gets past the review step. `pending_prompt_note()` lists the pending datasets and their
exact URNs, which also removes the retyped-URN problem. Two consequences: `propose` returns
a live proposal untouched rather than reclassifying, and a reused proposal does **not**
arm the gate, since blocking a confirmation the user already gave would restart the loop.

A proposal is invalidated if the schema changes under it (fingerprint mismatch) or after
`PII_PROPOSAL_TTL_SECONDS`.

### Setup

Tags must exist before anything is tagged — an aspect write does not validate tag
references, so a missing tag still attaches and renders as a bare URN:

```bash
python bootstrap/seed_tags.py          # idempotent
python bootstrap/tag_state.py          # inspect current tags
python bootstrap/tag_state.py --reset  # strip our tags, leave stewards' alone
python bootstrap/try_tagger.py appdb.hr_employees --apply   # timings, no chat loop
```

### Tuning

| Variable                   | Default            | Purpose                                                   |
| -------------------------- | ------------------ | --------------------------------------------------------- |
| `PII_CLASSIFIER_MODEL`     | `claude-haiku-4-5` | Model for the residual pass                               |
| `PII_CONFIDENCE_FLOOR`     | `0.6`              | Below this a row is reported as uncertain and not written |
| `PII_PROPOSAL_TTL_SECONDS` | `1800`             | How long a proposal stays confirmable                     |
| `MAX_TOOL_ITERATIONS`      | `10`               | Agent-loop ceiling                                        |

The floor doubles as a policy dial. Raising it to `0.75` drops the deliberately cautious
labels: pseudonymous IDs (`PII.UserID`, 0.7), city-level location, display names, and
bare account numbers.

Columns that already carry a `PII.*` tag are skipped, so re-running is cheap and never
contradicts a steward.

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
