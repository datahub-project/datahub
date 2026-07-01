from __future__ import annotations

import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from lib.graphql_adapt import GraphqlQueryRegistry


def log(msg: str) -> None:
    print(f"[authz-perf] {msg}", file=sys.stderr)


def effective_status_code(result: GraphqlResult) -> int:
    """Map frontend GraphQL outcomes to semantic HTTP status.

    Denied authz on GraphQL often returns HTTP 200 with an ``errors`` payload;
    treat that as 403 for expectation matching.
    """
    if result.error_kind in ("timeout", "network"):
        return 0
    if result.status_code != 200:
        return result.status_code
    if result.data.get("errors"):
        return 403
    return 200


def meets_expectation(result: GraphqlResult, expected_status_code: int) -> bool:
    if result.error_kind in ("timeout", "network"):
        return False
    return effective_status_code(result) == expected_status_code


@dataclass(frozen=True)
class GraphqlResult:
    ok: bool
    status_code: int
    elapsed_ms: float
    data: dict
    operation_name: str
    response_bytes: int = 0
    error_kind: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def failure_label(self) -> Optional[str]:
        if self.error_kind in ("timeout", "network"):
            if self.error_kind == "timeout":
                return f"timeout after {self.elapsed_ms:.1f}ms"
            return self.error_message or "network error"
        if self.error_kind == "http":
            return f"HTTP {self.status_code}: {self.error_message}"
        if self.error_kind == "graphql":
            return f"GraphQL errors: {self.error_message}"
        return self.error_message or "request failed"


@dataclass
class RequestHealthTracker:
    """Accumulates per-request outcomes for a scenario or run phase."""

    events: List[tuple[GraphqlResult, int]] = field(default_factory=list)

    def record(
        self, result: GraphqlResult, *, expected_status_code: int = 200
    ) -> None:
        self.events.append((result, expected_status_code))

    def summary(self) -> dict:
        status_codes = dict(Counter(r.status_code for r, _ in self.events))
        failures: List[dict] = []
        seen: set[str] = set()
        success_count = 0
        for result, expected in self.events:
            if meets_expectation(result, expected):
                success_count += 1
                continue
            effective = effective_status_code(result)
            label = result.failure_label or (
                f"expected HTTP {expected}, got {effective} "
                f"(raw HTTP {result.status_code})"
            )
            key = f"{result.operation_name}|{expected}|{effective}|{label}"
            if key in seen:
                continue
            seen.add(key)
            failures.append(
                {
                    "operation": result.operation_name,
                    "expected_status_code": expected,
                    "effective_status_code": effective,
                    "raw_status_code": result.status_code,
                    "error_kind": result.error_kind,
                    "message": label,
                }
            )
        total = len(self.events)
        return {
            "total_requests": total,
            "success_count": success_count,
            "failure_count": total - success_count,
            "status_codes": status_codes,
            "failures": failures,
        }

    def ensure_all_success(self, context: str) -> None:
        summary = self.summary()
        if summary["failure_count"] == 0:
            return
        details = "; ".join(
            f"{f['operation']}: {f['message']}" for f in summary["failures"][:3]
        )
        if len(summary["failures"]) > 3:
            details += f" (+{len(summary['failures']) - 3} more)"
        codes = summary["status_codes"]
        raise RuntimeError(
            f"{context}: {summary['failure_count']}/{summary['total_requests']} "
            f"responses mismatched expected status (status_codes={codes}). {details}"
        )

    @staticmethod
    def _bytes_to_kb(size_bytes: int) -> float:
        return round(size_bytes / 1024.0, 2)

    @staticmethod
    def response_size_summary(results: List[GraphqlResult]) -> dict:
        sizes = [r.response_bytes for r in results if r.response_bytes > 0]
        if not sizes:
            return {
                "avg_kb": 0.0,
                "min_kb": 0.0,
                "max_kb": 0.0,
                "sample_count": 0,
            }
        return {
            "avg_kb": RequestHealthTracker._bytes_to_kb(round(sum(sizes) / len(sizes))),
            "min_kb": RequestHealthTracker._bytes_to_kb(min(sizes)),
            "max_kb": RequestHealthTracker._bytes_to_kb(max(sizes)),
            "sample_count": len(sizes),
        }


def resolve_operation_document(
    registry: "GraphqlQueryRegistry",
    operation: str,
) -> str:
    """Return a schema-generated GraphQL document for a benchmark operation."""
    return registry.document(operation)


def _response_body_bytes(resp: object) -> int:
    content = getattr(resp, "content", None)
    if content is not None:
        return len(content)
    text = getattr(resp, "text", "") or ""
    return len(text.encode("utf-8"))


def execute_graphql(
    session: object,
    frontend_url: str,
    document: str,
    operation_name: str,
    variables: Optional[dict] = None,
    *,
    timeout_sec: float = 120.0,
) -> GraphqlResult:
    """POST to frontend GraphQL and return structured outcome (never raises)."""
    import time

    import requests

    url = frontend_url.rstrip("/") + "/api/v2/graphql"
    payload = {
        "operationName": operation_name,
        "query": document,
        "variables": variables or {},
    }
    start = time.perf_counter()
    try:
        resp = session.post(url, json=payload, timeout=timeout_sec)  # type: ignore[attr-defined]
    except requests.Timeout as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return GraphqlResult(
            ok=False,
            status_code=0,
            elapsed_ms=elapsed_ms,
            data={},
            operation_name=operation_name,
            error_kind="timeout",
            error_message=str(exc),
        )
    except requests.RequestException as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return GraphqlResult(
            ok=False,
            status_code=0,
            elapsed_ms=elapsed_ms,
            data={},
            operation_name=operation_name,
            error_kind="network",
            error_message=str(exc),
        )

    elapsed_ms = (time.perf_counter() - start) * 1000.0
    status_code = resp.status_code
    response_bytes = _response_body_bytes(resp)
    if status_code != 200:
        body_preview = resp.text[:500] if resp.text else ""
        return GraphqlResult(
            ok=False,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            data={},
            operation_name=operation_name,
            response_bytes=response_bytes,
            error_kind="http",
            error_message=body_preview,
        )

    try:
        data = resp.json()
    except ValueError as exc:
        return GraphqlResult(
            ok=False,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            data={},
            operation_name=operation_name,
            response_bytes=response_bytes,
            error_kind="http",
            error_message=f"invalid JSON response: {exc}",
        )

    errors = data.get("errors")
    if errors:
        return GraphqlResult(
            ok=False,
            status_code=status_code,
            elapsed_ms=elapsed_ms,
            data=data,
            operation_name=operation_name,
            response_bytes=response_bytes,
            error_kind="graphql",
            error_message=str(errors),
        )

    return GraphqlResult(
        ok=True,
        status_code=status_code,
        elapsed_ms=elapsed_ms,
        data=data,
        operation_name=operation_name,
        response_bytes=response_bytes,
    )


def get_nested_value(obj: dict, dotted_path: str):
    cur = obj
    for part in dotted_path.split("."):
        if cur is None:
            return None
        cur = cur.get(part)
    return cur


def check_assertions(response: dict, assertions: dict) -> List[str]:
    failures: List[str] = []
    for path, expected in assertions.items():
        actual = get_nested_value(response, path)
        if actual != expected:
            failures.append(f"{path}: expected {expected!r}, got {actual!r}")
    return failures
