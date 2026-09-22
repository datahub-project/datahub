"""Seeds a deterministic, comprehensive dataset into a running Langfuse
instance for the connector's integration test.

Traces/Observations are seeded via raw OTLP (POST /api/public/otel/v1/traces)
rather than the deprecated legacy batch-ingestion endpoint or the official
`langfuse` SDK: OTLP is the actual production write path Langfuse v4 uses,
it requires no extra Python dependency, and - critically - it lets the
caller pin exact trace/span IDs, which is what makes this fixture
deterministic across repeated runs. Scores and Prompts use their own
first-class REST creation endpoints (POST /api/public/scores,
POST /api/public/v2/prompts), which are synchronous and need no such
workaround.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

OTEL_TRACES_PATH = "/api/public/otel/v1/traces"
SCORES_CREATE_PATH = "/api/public/scores"
SCORES_V3_PATH = "/api/public/v3/scores"
PROMPTS_PATH = "/api/public/v2/prompts"
OBSERVATIONS_PATH = "/api/public/v2/observations"

# Deterministic OTel IDs (hex-only, fixed length: 32 chars for trace IDs,
# 16 chars for span/observation IDs).
TRACE_SUPPORT_CHAT = "a" * 32
TRACE_BILLING_QUERY = "b" * 32
TRACE_MULTI_TURN = "c" * 32

SPAN_SUPPORT_ROOT = "1" * 16
SPAN_SUPPORT_GENERATION = "2" * 16
SPAN_SUPPORT_TOOL = "3" * 16

SPAN_BILLING_GENERATION = "4" * 16

SPAN_MULTI_ROOT = "5" * 16
SPAN_MULTI_GENERATION_1 = "6" * 16
SPAN_MULTI_GENERATION_2 = "7" * 16
SPAN_MULTI_RETRIEVAL = "8" * 16

PROMPT_GREETING = "integration-test-greeting"
PROMPT_SYSTEM = "integration-test-system-instructions"


@dataclass
class SeededDataSummary:
    """Ground truth for what was seeded, so the test can assert against it
    instead of hardcoding magic numbers in two places."""

    trace_count: int
    generation_count: int
    non_generation_observation_count: int
    attachable_score_count: int
    dropped_score_count: int
    prompt_names: List[str]
    prompt_version_count: int


class LangfuseSeeder:
    def __init__(self, base_url: str, public_key: str, secret_key: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (public_key, secret_key)

    def _post(self, path: str, body: Dict[str, Any]) -> requests.Response:
        response = self.session.post(f"{self.base_url}{path}", json=body, timeout=30)
        response.raise_for_status()
        return response

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        response = self.session.get(f"{self.base_url}{path}", params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def send_otlp_span(
        self,
        trace_id: str,
        span_id: str,
        name: str,
        parent_span_id: Optional[str] = None,
        genai_model: Optional[str] = None,
        genai_input_tokens: Optional[int] = None,
        genai_output_tokens: Optional[int] = None,
        trace_name: Optional[str] = None,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        start_offset_seconds: float = 0.0,
        duration_seconds: float = 1.0,
    ) -> None:
        now_ns = int(time.time() * 1e9) + int(start_offset_seconds * 1e9)
        end_ns = now_ns + int(duration_seconds * 1e9)

        attributes = []
        if genai_model:
            attributes.append(
                {"key": "gen_ai.system", "value": {"stringValue": "openai"}}
            )
            attributes.append(
                {
                    "key": "gen_ai.request.model",
                    "value": {"stringValue": genai_model},
                }
            )
        if genai_input_tokens is not None:
            attributes.append(
                {
                    "key": "gen_ai.usage.input_tokens",
                    "value": {"intValue": str(genai_input_tokens)},
                }
            )
        if genai_output_tokens is not None:
            attributes.append(
                {
                    "key": "gen_ai.usage.output_tokens",
                    "value": {"intValue": str(genai_output_tokens)},
                }
            )
        if trace_name:
            attributes.append(
                {"key": "langfuse.trace.name", "value": {"stringValue": trace_name}}
            )
        if session_id:
            attributes.append(
                {"key": "session.id", "value": {"stringValue": session_id}}
            )
        if user_id:
            attributes.append({"key": "user.id", "value": {"stringValue": user_id}})
        if tags:
            attributes.append(
                {
                    "key": "langfuse.trace.tags",
                    "value": {
                        "arrayValue": {"values": [{"stringValue": t} for t in tags]}
                    },
                }
            )

        span: Dict[str, Any] = {
            "traceId": trace_id,
            "spanId": span_id,
            "name": name,
            "kind": 3,
            "startTimeUnixNano": str(now_ns),
            "endTimeUnixNano": str(end_ns),
            "attributes": attributes,
            "status": {},
        }
        if parent_span_id:
            span["parentSpanId"] = parent_span_id

        payload = {
            "resourceSpans": [
                {
                    "resource": {"attributes": []},
                    "scopeSpans": [
                        {"scope": {"name": "integration-test"}, "spans": [span]}
                    ],
                }
            ]
        }
        self.session.post(
            f"{self.base_url}{OTEL_TRACES_PATH}",
            json=payload,
            headers={"x-langfuse-ingestion-version": "4"},
            timeout=30,
        ).raise_for_status()

    def create_score(
        self,
        score_id: str,
        name: str,
        value: Any,
        data_type: str,
        trace_id: Optional[str] = None,
        observation_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> None:
        body: Dict[str, Any] = {
            "id": score_id,
            "name": name,
            "value": value,
            "dataType": data_type,
        }
        if trace_id:
            body["traceId"] = trace_id
        if observation_id:
            body["observationId"] = observation_id
        if session_id:
            body["sessionId"] = session_id
        self._post(SCORES_CREATE_PATH, body)

    def create_prompt_version(
        self,
        name: str,
        prompt_type: str,
        prompt: Any,
        labels: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        self._post(
            PROMPTS_PATH,
            {
                "name": name,
                "type": prompt_type,
                "prompt": prompt,
                "labels": labels or [],
                "tags": tags or [],
            },
        )

    def wait_until_observation_visible(
        self, observation_id: str, timeout_seconds: float = 60.0
    ) -> None:
        """OTel ingestion is processed asynchronously; poll until the last
        seeded observation is queryable through the same v2 API the
        connector uses, so the connector never races ahead of the seed."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            data = self._get(
                OBSERVATIONS_PATH,
                {
                    "fromStartTime": "2020-01-01T00:00:00Z",
                    "toStartTime": "2030-01-01T00:00:00Z",
                    "limit": 100,
                    "fields": "core",
                },
            )
            if any(item["id"] == observation_id for item in data.get("data", [])):
                return
            time.sleep(2)
        raise TimeoutError(
            f"Observation {observation_id} did not become visible within "
            f"{timeout_seconds}s"
        )

    def wait_until_score_visible(
        self, score_id: str, timeout_seconds: float = 60.0
    ) -> None:
        """Score creation returns 200 immediately, but the row may not be
        queryable via the read API (a separate ClickHouse write path) until
        slightly after. Poll until the last seeded score is queryable
        through the same v3 API the connector uses."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            data = self._get(
                SCORES_V3_PATH,
                {
                    "fromTimestamp": "2020-01-01T00:00:00Z",
                    "toTimestamp": "2030-01-01T00:00:00Z",
                    "limit": 100,
                },
            )
            if any(item["id"] == score_id for item in data.get("data", [])):
                return
            time.sleep(2)
        raise TimeoutError(
            f"Score {score_id} did not become visible within {timeout_seconds}s"
        )


def seed(base_url: str, public_key: str, secret_key: str) -> SeededDataSummary:
    seeder = LangfuseSeeder(base_url, public_key, secret_key)

    # --- Trace 1: wrapper span -> one generation -> one tool-ish span ---
    seeder.send_otlp_span(
        trace_id=TRACE_SUPPORT_CHAT,
        span_id=SPAN_SUPPORT_ROOT,
        name="support-chat",
        trace_name="support-chat",
        session_id="session-support-1",
        user_id="user-1",
        tags=["support"],
        start_offset_seconds=0,
        duration_seconds=3,
    )
    seeder.send_otlp_span(
        trace_id=TRACE_SUPPORT_CHAT,
        span_id=SPAN_SUPPORT_GENERATION,
        name="llm-call",
        parent_span_id=SPAN_SUPPORT_ROOT,
        genai_model="gpt-4o-mini",
        genai_input_tokens=100,
        genai_output_tokens=50,
        start_offset_seconds=0.1,
        duration_seconds=1,
    )
    seeder.send_otlp_span(
        trace_id=TRACE_SUPPORT_CHAT,
        span_id=SPAN_SUPPORT_TOOL,
        name="lookup-order",
        parent_span_id=SPAN_SUPPORT_ROOT,
        start_offset_seconds=1.2,
        duration_seconds=0.5,
    )

    # --- Trace 2: root observation IS the generation itself ---
    seeder.send_otlp_span(
        trace_id=TRACE_BILLING_QUERY,
        span_id=SPAN_BILLING_GENERATION,
        name="billing-query",
        trace_name="billing-query",
        genai_model="claude-3-haiku",
        genai_input_tokens=200,
        genai_output_tokens=80,
        tags=["billing"],
        start_offset_seconds=0,
        duration_seconds=2,
    )

    # --- Trace 3: wrapper span -> two generations (multi-turn) + one retrieval span ---
    seeder.send_otlp_span(
        trace_id=TRACE_MULTI_TURN,
        span_id=SPAN_MULTI_ROOT,
        name="multi-turn-conversation",
        trace_name="multi-turn-conversation",
        session_id="session-multi-1",
        start_offset_seconds=0,
        duration_seconds=5,
    )
    seeder.send_otlp_span(
        trace_id=TRACE_MULTI_TURN,
        span_id=SPAN_MULTI_RETRIEVAL,
        name="retrieve-context",
        parent_span_id=SPAN_MULTI_ROOT,
        start_offset_seconds=0.1,
        duration_seconds=0.4,
    )
    seeder.send_otlp_span(
        trace_id=TRACE_MULTI_TURN,
        span_id=SPAN_MULTI_GENERATION_1,
        name="llm-call-turn-1",
        parent_span_id=SPAN_MULTI_ROOT,
        genai_model="gpt-4o-mini",
        genai_input_tokens=150,
        genai_output_tokens=60,
        start_offset_seconds=0.6,
        duration_seconds=1,
    )
    seeder.send_otlp_span(
        trace_id=TRACE_MULTI_TURN,
        span_id=SPAN_MULTI_GENERATION_2,
        name="llm-call-turn-2",
        parent_span_id=SPAN_MULTI_ROOT,
        genai_model="gpt-4o-mini",
        genai_input_tokens=180,
        genai_output_tokens=70,
        start_offset_seconds=1.8,
        duration_seconds=1,
    )

    seeder.wait_until_observation_visible(SPAN_MULTI_GENERATION_2)

    # --- Scores: two attachable (trace + observation level), one dropped (session level) ---
    seeder.create_score(
        score_id="it-score-helpfulness",
        name="helpfulness",
        value=0.9,
        data_type="NUMERIC",
        trace_id=TRACE_SUPPORT_CHAT,
    )
    seeder.create_score(
        score_id="it-score-is-correct",
        name="is_correct",
        value=1,
        data_type="BOOLEAN",
        trace_id=TRACE_SUPPORT_CHAT,
        observation_id=SPAN_SUPPORT_GENERATION,
    )
    seeder.create_score(
        score_id="it-score-session-level",
        name="session_engagement",
        value=0.5,
        data_type="NUMERIC",
        session_id="session-support-1",
    )
    seeder.wait_until_score_visible("it-score-session-level")

    # --- Prompts: one text prompt with two versions, one chat prompt with one version ---
    seeder.create_prompt_version(
        name=PROMPT_GREETING,
        prompt_type="text",
        prompt="Hello {{name}}, how can I help you today?",
        labels=["production"],
        tags=["customer-facing"],
    )
    seeder.create_prompt_version(
        name=PROMPT_GREETING,
        prompt_type="text",
        prompt="Hi {{name}}! What can I do for you?",
        labels=["latest"],
        tags=["customer-facing"],
    )
    seeder.create_prompt_version(
        name=PROMPT_SYSTEM,
        prompt_type="chat",
        prompt=[{"role": "system", "content": "You are a helpful assistant."}],
        labels=["production"],
        tags=["system"],
    )

    return SeededDataSummary(
        trace_count=3,
        generation_count=4,
        non_generation_observation_count=4,
        attachable_score_count=2,
        dropped_score_count=1,
        prompt_names=[PROMPT_GREETING, PROMPT_SYSTEM],
        prompt_version_count=3,
    )


if __name__ == "__main__":
    import sys

    base_url, public_key, secret_key = sys.argv[1], sys.argv[2], sys.argv[3]
    summary = seed(base_url, public_key, secret_key)
    print(summary)
