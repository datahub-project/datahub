import gzip
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

NodeIdMap = dict[int, dict]

_BUNDLE_PATH = Path(__file__).parent / "mquery_bridge" / "bundle.js.gz"


class MQueryBridgeError(RuntimeError):
    """V8 context error or malformed response from the M-Query bridge."""

    pass


class MQueryParseError(RuntimeError):
    """Parser returned a structured parse error for a specific expression."""

    def __init__(self, message: str, expression: str = "") -> None:
        super().__init__(message)
        self.expression = expression


class MQueryBridge:
    def __init__(self) -> None:
        if not _BUNDLE_PATH.exists():
            raise ImportError(
                f"M-Query bridge bundle not found at {_BUNDLE_PATH}. "
                "Re-installing acryl-datahub[powerbi] may fix this."
            )
        try:
            from py_mini_racer import MiniRacer
        except ImportError as e:
            raise ImportError(
                "PowerBI M-Query parsing requires 'mini-racer'. "
                "Install it with: pip install 'mini-racer>=0.12.0'"
            ) from e

        # Decompress bundle.js.gz in memory — fast (~50ms for 500KB) and happens once per process.
        bundle_js = gzip.decompress(_BUNDLE_PATH.read_bytes()).decode("utf-8")
        self._ctx = MiniRacer()
        self._ctx.eval(bundle_js)

    def parse(self, expression: str) -> NodeIdMap:
        """
        Parse an M-Query expression.
        Returns nodeIdMap as dict[int, dict].
        Not thread-safe — callers must be single-threaded.

        Raises:
            MQueryParseError: parser returned a structured error for this expression.
            MQueryBridgeError: V8 context error or malformed response.
        """
        try:
            # parseExpression is async, so ctx.call() returns an unresolved plain dict.
            # Use ctx.eval() instead, which returns a JSPromise; call .get() to await it.
            promise = self._ctx.eval(f"parseExpression({json.dumps(expression)})")
            raw = promise.get()
        except Exception as e:
            # Catches all py_mini_racer errors (JSEvalException, JSTimeoutException, etc.)
            # MiniRacerBaseException is not exported from the top-level namespace in mini-racer.
            raise MQueryBridgeError(f"M-Query bridge V8 error: {e}") from e

        try:
            result = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            raise MQueryBridgeError(
                f"M-Query bridge returned malformed JSON: {e}. Raw: {raw!r}"
            ) from e

        if not result.get("ok"):
            raise MQueryParseError(
                result.get("error", "unknown error"), expression=expression
            )

        return {int(node_id): node for node_id, node in result["nodeIdMap"]}


_bridge_instance: Optional[MQueryBridge] = None


def get_bridge() -> MQueryBridge:
    global _bridge_instance
    if _bridge_instance is None:
        _bridge_instance = MQueryBridge()
    return _bridge_instance


def _clear_bridge() -> None:
    global _bridge_instance
    _bridge_instance = None
