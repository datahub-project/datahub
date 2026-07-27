"""
DataHub AI Orchestrator — FastAPI service.

Endpoints:
  POST /api/ai/chat      -> SSE stream of {"token": "..."} events (matches the UI)
  GET  /api/ai-config    -> { model, hasKey }
  POST /api/ai-config    -> save model + key (hackathon: in-memory / env)
  GET  /health           -> health check

Run:
  export ANTHROPIC_API_KEY="sk-ant-..."
  uvicorn main:app --port 8000 --reload
"""
from __future__ import annotations

import json
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from agent import run_agent

app = FastAPI(title="DataHub AI Orchestrator")

# Allow the Vite dev server (localhost:3000) and Docker frontend (9002) to call us.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory config store (hackathon only). Production -> DataHub secret manager.
_CONFIG = {"model": os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")}


class ChatRequest(BaseModel):
    message: str
    context: dict | None = None


class ConfigRequest(BaseModel):
    apiKey: str | None = None
    model: str | None = None


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "hasKey": bool(os.environ.get("ANTHROPIC_API_KEY"))}


@app.post("/api/ai/chat")
async def chat(req: ChatRequest) -> StreamingResponse:
    async def event_stream():
        try:
            async for token in run_agent(req.message, req.context):
                yield f"data: {json.dumps({'token': token})}\n\n"
        except Exception as exc:  # noqa: BLE001
            yield f"data: {json.dumps({'token': f'[error: {exc}]'})}\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/ai-config")
async def get_config() -> dict:
    return {"model": _CONFIG["model"], "hasKey": bool(os.environ.get("ANTHROPIC_API_KEY"))}


@app.post("/api/ai-config")
async def save_config(req: ConfigRequest) -> dict:
    if req.model:
        _CONFIG["model"] = req.model
    if req.apiKey:
        # Hackathon: set in-process env. Production: write to DataHub secret manager.
        os.environ["ANTHROPIC_API_KEY"] = req.apiKey
    return {"status": "saved", "model": _CONFIG["model"], "hasKey": bool(os.environ.get("ANTHROPIC_API_KEY"))}
