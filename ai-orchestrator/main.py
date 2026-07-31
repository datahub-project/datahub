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
import logging
import os
from datetime import datetime
from pathlib import Path

import anthropic
import httpx
from dotenv import load_dotenv

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List

# Must run before the local imports below: mcp_tools reads DATAHUB_MCP_URL at module
# scope, and without it the orchestrator silently spawns its own unauthenticated
# `uvx mcp-server-datahub` over stdio instead of using the HTTP server on :8001.
load_dotenv(Path(__file__).resolve().parent / ".env")

# uvicorn configures only its own loggers, so without this every logger.info in
# mcp_tools/pii_* is swallowed and the tool calls are invisible while debugging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("orchestrator")

from agent import CONFIRM_SENTINEL, DEFAULT_MODEL, run_agent  # noqa: E402
from mcp_tools import get_mcp, shutdown_mcp  # noqa: E402
import mysql.connector
import uuid


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize DB tables and warm the MCP session so the first chat isn't slow.
    init_db()
    await get_mcp()
    yield
    await shutdown_mcp()


app = FastAPI(title="DataHub AI Orchestrator", lifespan=lifespan)

# Allow the Vite dev server (localhost:3000) and Docker frontend (9002) to call us.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory model selection store. Provider keys are fetched from GMS at runtime.
_CONFIG = {"model": DEFAULT_MODEL}
DATAHUB_GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN", "")
INTERNAL_PROVIDER_KEY_URL = f"{DATAHUB_GMS_URL}/api/ai-config/internal/provider-key"


class ChatRequest(BaseModel):
    message: str
    context: dict | None = None
    session_id: Optional[str] = None
    model: Optional[str] = None


class ConfigRequest(BaseModel):
    apiKey: str | None = None
    model: str | None = None


def _gms_headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if DATAHUB_GMS_TOKEN:
        headers["Authorization"] = f"Bearer {DATAHUB_GMS_TOKEN}"
    return headers


def _env_fallback(selected_model: str) -> dict[str, Any] | None:
    """The local .env key, when GMS cannot supply one."""
    env_api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not env_api_key or not selected_model.startswith("claude-"):
        return None
    return {
        "model": selected_model,
        "provider": "claude",
        "apiKey": env_api_key,
        "hasKey": True,
        "source": "env-fallback",
    }


async def _get_runtime_ai_config(model: str | None = None) -> dict[str, Any]:
    selected_model = (model or _CONFIG["model"]).strip().lower()

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                INTERNAL_PROVIDER_KEY_URL,
                headers=_gms_headers(),
                params={"model": selected_model},
            )
    except Exception as exc:
        fallback = _env_fallback(selected_model)
        if fallback:
            return fallback
        raise HTTPException(status_code=503, detail=f"Failed to reach GMS AI config endpoint: {exc}") from exc

    # A 404 means GMS holds no key for this model, which is not different in practice
    # from GMS being unreachable — both leave us without one, so both fall back.
    if response.status_code == 404:
        fallback = _env_fallback(selected_model)
        if fallback:
            logger.info("GMS has no key for %s; using ANTHROPIC_API_KEY.", selected_model)
            return fallback
        return {"model": selected_model, "provider": None, "apiKey": None, "hasKey": False}
    if response.status_code >= 400:
        detail = response.text
        raise HTTPException(
            status_code=502,
            detail=f"GMS AI config endpoint returned {response.status_code}: {detail}",
        )

    payload = response.json()
    return {
        "model": payload.get("model", selected_model),
        "provider": payload.get("provider"),
        "apiKey": payload.get("apiKey"),
        "hasKey": bool(payload.get("apiKey")),
    }


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "hasKey": bool(os.environ.get("ANTHROPIC_API_KEY"))}


# Idle timeout: if a session has had no activity for this many minutes, its
# prior context is considered "dead" and the conversation starts fresh.
# (Mohit's session design: ~10 min idle → session expires.)
SESSION_IDLE_TIMEOUT_MINUTES = 10
# Keep this many recent messages verbatim; summarize anything older.
SUMMARY_KEEP_RECENT = 6
# Model used for generating the running summary (cheap + fast).
SUMMARY_MODEL = "claude-haiku-4-5"


async def _summarize_messages(messages: list[dict], api_key: str) -> str:
    """Call Claude Haiku to produce a concise summary of old conversation turns."""
    client = anthropic.AsyncAnthropic(api_key=api_key)
    text_turns = "\n".join(
        f"{m['role'].upper()}: {m['content']}" for m in messages
    )
    prompt = (
        "You are summarizing earlier turns in a DataHub AI assistant conversation. "
        "Be concise (3-5 sentences). Capture key entities, datasets, and facts mentioned.\n\n"
        f"CONVERSATION TO SUMMARIZE:\n{text_turns}"
    )
    response = await client.messages.create(
        model=SUMMARY_MODEL,
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


@app.post("/api/ai/chat")
async def chat(req: ChatRequest) -> StreamingResponse:
    selected_model = req.model.strip().lower() if req.model else _CONFIG["model"]
    runtime_config = await _get_runtime_ai_config(selected_model)
    if runtime_config["provider"] not in (None, "claude"):
        raise HTTPException(
            status_code=501,
            detail=f"Provider {runtime_config['provider']} is not yet supported by the orchestrator.",
        )

    # Load conversation history from MySQL if a session_id was provided
    history: list[dict] = []
    if req.session_id:
        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            # Only load history if the most recent message is within the idle
            # window. TIMESTAMPDIFF returns minutes since the last message; if
            # that exceeds the timeout the session is stale, so we skip its
            # history (Claude answers fresh) and mark the session completed.
            cursor.execute(
                """
                SELECT role, content,
                       TIMESTAMPDIFF(
                           MINUTE,
                           (SELECT MAX(created_at) FROM messages WHERE session_id = %s),
                           NOW()
                       ) AS idle_minutes
                FROM messages
                WHERE session_id = %s
                ORDER BY created_at ASC
                """,
                (req.session_id, req.session_id),
            )
            rows = cursor.fetchall()
            idle_minutes = rows[0]["idle_minutes"] if rows else 0
            if rows and idle_minutes is not None and idle_minutes >= SESSION_IDLE_TIMEOUT_MINUTES:
                # Session expired due to inactivity — drop context and mark it dead.
                history = []
                cursor.execute(
                    "UPDATE sessions SET status = 'completed' WHERE id = %s",
                    (req.session_id,),
                )
                conn.commit()
            else:
                all_messages = [{"role": r["role"], "content": r["content"]} for r in rows]

                if len(all_messages) > SUMMARY_KEEP_RECENT:
                    # Use a separate cursor to fetch session summary (avoid cursor reuse issues)
                    cursor2 = conn.cursor(dictionary=True)
                    cursor2.execute("SELECT summary FROM sessions WHERE id = %s", (req.session_id,))
                    session_row = cursor2.fetchone()
                    cursor2.close()
                    existing_summary = (session_row or {}).get("summary") or ""

                    # Summarize messages outside the keep-window
                    old_messages = all_messages[:-SUMMARY_KEEP_RECENT]
                    recent_messages = all_messages[-SUMMARY_KEEP_RECENT:]

                    # Prepend existing summary context so it's cumulative
                    to_summarize = (
                        [{"role": "user", "content": f"[Prior summary]: {existing_summary}"}]
                        if existing_summary else []
                    ) + old_messages

                    if runtime_config["apiKey"]:
                        new_summary = await _summarize_messages(
                            to_summarize, runtime_config["apiKey"]
                        )
                    else:
                        new_summary = existing_summary

                    # Persist the updated summary back to sessions table
                    if new_summary != existing_summary:
                        cursor3 = conn.cursor()
                        cursor3.execute(
                            "UPDATE sessions SET summary = %s WHERE id = %s",
                            (new_summary, req.session_id),
                        )
                        conn.commit()
                        cursor3.close()

                    # Build history: summary as a synthetic user note + recent turns
                    history = recent_messages
                    if new_summary:
                        history = [
                            {
                                "role": "user",
                                "content": f"[Conversation summary so far]: {new_summary}",
                            },
                            {
                                "role": "assistant",
                                "content": "Understood, I have the context from the summary.",
                            },
                        ] + recent_messages
                else:
                    history = all_messages

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"[history/summary] skipped due to error: {e}")
            history = []  # graceful fallback — don't block chat if DB is down

        # Save the user message before streaming
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT IGNORE INTO sessions (id, user_id) VALUES (%s, %s)",
                (req.session_id, "datahub"),
            )
            cursor.execute(
                "INSERT INTO messages (id, session_id, role, content) VALUES (%s, %s, %s, %s)",
                (str(uuid.uuid4()), req.session_id, "user", req.message),
            )
            conn.commit()
            cursor.close()
            conn.close()
        except Exception:
            pass  # non-fatal

    async def event_stream():
        accumulated = ""
        try:
            if not runtime_config["apiKey"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"No API key configured for model {runtime_config['model']}.",
                )

            async for token in run_agent(
                req.message,
                req.context,
                api_key=runtime_config["apiKey"],
                model=selected_model,
                history=history,
            ):
                # The agent emits this sentinel when a fresh PII proposal is awaiting
                # confirmation. Turn it into a distinct event the UI can render as
                # Apply/Cancel/Custom buttons, and keep it out of the saved transcript.
                if token == CONFIRM_SENTINEL:
                    yield f"data: {json.dumps({'confirm': True})}\n\n"
                    continue
                accumulated += token
                yield f"data: {json.dumps({'token': token})}\n\n"
        except HTTPException as exc:
            yield f"data: {json.dumps({'token': f'[error: {exc.detail}]'})}\n\n"
        except Exception as exc:  # noqa: BLE001
            yield f"data: {json.dumps({'token': f'[error: {exc}]'})}\n\n"
        finally:
            yield "data: [DONE]\n\n"
            # Save the assistant response after streaming completes
            if req.session_id and accumulated:
                try:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO messages (id, session_id, role, content) VALUES (%s, %s, %s, %s)",
                        (str(uuid.uuid4()), req.session_id, "assistant", accumulated),
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()
                except Exception:
                    pass  # non-fatal

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


def init_db():
    """Reads schema.sql and runs statements on FastAPI startup."""
    sql_file_path = os.path.join(os.path.dirname(__file__), "scripts/schema.sql")
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"Schema file not found at {sql_file_path}")

    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    # Connect without specifying database first to run CREATE DATABASE
    conn = get_db_connection(include_db=True)
    cursor = conn.cursor()

    try:
        # Split script by semicolon to execute commands individually
        statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]
        for statement in statements:
            cursor.execute(statement)
        conn.commit()
        print("Database and tables initialized successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


class SessionCreate(BaseModel):
    user_id: str
    system_prompt: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class SessionResponse(BaseModel):
    id: str
    user_id: str
    system_prompt: Optional[str]
    status: str
    metadata: Optional[Dict[str, Any]]

class MessageCreate(BaseModel):
    role: str = Field(..., pattern="^(user|assistant|system)$")
    content: str
    tokens_used: Optional[int] = 0

class MessageResponse(BaseModel):
    id: str
    session_id: str
    role: str
    content: str
    tokens_used: int
    # Timestamp of when the message was stored (serialized to ISO-8601 by FastAPI).
    # Used by the frontend to detect an idle session (>10 min since last message)
    # and start a fresh chat.
    created_at: Optional[datetime] = None


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "datahub")
DB_PASSWORD = os.getenv("DB_PASSWORD", "datahub")
DB_NAME = "datahub"


def get_db_connection(include_db: bool = True):
    """Establishes a connection to MySQL."""
    config = {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }
    if include_db:
        config["database"] = DB_NAME
    return mysql.connector.connect(**config)


@app.post("/sessions/{session_id}/messages", response_model=MessageResponse, status_code=status.HTTP_201_CREATED)
def add_message(session_id: str, message: MessageCreate):
    """Appends a user or assistant message to a session thread."""
    message_id = str(uuid.uuid4())

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Verify session exists
        cursor.execute("SELECT id FROM sessions WHERE id = %s", (session_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Session not found")

        query = """
            INSERT INTO messages (id, session_id, role, content, tokens_used)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (message_id, session_id, message.role, message.content, message.tokens_used))
        conn.commit()

        return {
            "id": message_id,
            "session_id": session_id,
            "role": message.role,
            "content": message.content,
            "tokens_used": message.tokens_used
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/sessions/{session_id}/messages", response_model=List[MessageResponse])
def get_session_messages(session_id: str):
    """Retrieves conversation history for an agent loop."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT id, session_id, role, content, tokens_used, created_at FROM messages WHERE session_id = %s ORDER BY created_at ASC",
            (session_id,)
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

# -------------------------------------------------------------------
# Session Endpoints
# -------------------------------------------------------------------

@app.post("/sessions", response_model=SessionResponse, status_code=status.HTTP_201_CREATED)
def create_session(session: SessionCreate):
    """Creates a new agent session."""
    session_id = str(uuid.uuid4())
    metadata_json = json.dumps(session.metadata) if session.metadata else None

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
            INSERT INTO sessions (id, user_id, system_prompt, metadata)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (session_id, session.user_id, session.system_prompt, metadata_json))
        conn.commit()

        return {
            "id": session_id,
            "user_id": session.user_id,
            "system_prompt": session.system_prompt,
            "status": "active",
            "metadata": session.metadata
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/sessions/{session_id}", response_model=SessionResponse)
def get_session(session_id: str):
    """Retrieves session details by session_id."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = "SELECT id, user_id, system_prompt, status, metadata FROM sessions WHERE id = %s"
        cursor.execute(query, (session_id,))
        record = cursor.fetchone()

        if not record:
            raise HTTPException(status_code=404, detail="Session not found")

        # Parse JSON metadata string back into a dictionary if present
        if record["metadata"] and isinstance(record["metadata"], str):
            record["metadata"] = json.loads(record["metadata"])

        return record
    finally:
        cursor.close()
        conn.close()