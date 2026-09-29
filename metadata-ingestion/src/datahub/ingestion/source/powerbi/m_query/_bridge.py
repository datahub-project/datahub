import gzip
import json
import logging
import threading
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
                "Install it with: pip install 'acryl-datahub[powerbi]'"
            ) from e

        # Decompress bundle.js.gz in memory — fast (~50ms for 500KB) and happens once per process.
        try:
            bundle_js = gzip.decompress(_BUNDLE_PATH.read_bytes()).decode("utf-8")
        except (gzip.BadGzipFile, OSError, EOFError) as e:
            raise ImportError(
                f"M-Query bridge bundle at {_BUNDLE_PATH} appears to be corrupt: {e}. "
                "Re-installing acryl-datahub[powerbi] may fix this."
            ) from e
        self._ctx = MiniRacer()
        self._ctx.eval(bundle_js)

    def parse(self, expression: str) -> NodeIdMap:
        """
        Parse an M-Query expression and return a flat node map.

        Each key is a node ID (int); each value is a node dict with at least
        ``kind`` (NodeKind string) and ``id``. Child nodes are embedded inline,
        not as ID references, so you can walk them directly or look up any
        node by ID via the returned map.

        Example — the LetExpression root for ``let x = 1 in x`` is at the
        root of the returned dict and looks roughly like::

            {1: {"kind": "LetExpression", "id": 1, "variableList": {...}, ...},
             2: {"kind": "ArrayWrapper", "id": 2, ...},
             ...}

        Not thread-safe — callers must be single-threaded.

        Raises:
            MQueryParseError: parser returned a structured error for this expression.
            MQueryBridgeError: V8 context error or malformed response.
        """
        # JSPromise is available: __init__ already guaranteed py_mini_racer is installed.
        from py_mini_racer import JSPromise

        try:
            # parseExpression is async, so ctx.call() returns an unresolved plain dict.
            # Use ctx.eval() instead, which returns a JSPromise; call .get() to await it.
            result = self._ctx.eval(f"parseExpression({json.dumps(expression)})")
            if not isinstance(result, JSPromise):
                raise MQueryBridgeError(
                    f"M-Query bridge: expected JSPromise from parseExpression, got {type(result).__name__}"
                )
            raw = result.get()
        except MQueryBridgeError:
            raise
        except Exception as e:
            # Catches all py_mini_racer errors (JSEvalException, JSTimeoutException, etc.)
            # MiniRacerBaseException is not exported from the top-level namespace in mini-racer.
            raise MQueryBridgeError(f"M-Query bridge V8 error: {e}") from e

        if not isinstance(raw, str):
            raise MQueryBridgeError(
                f"M-Query bridge returned non-string result: {type(raw).__name__}"
            )

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

        node_id_map = result.get("nodeIdMap")
        if node_id_map is None:
            raise MQueryBridgeError(
                "M-Query bridge returned ok=true but 'nodeIdMap' is missing from response"
            )

        return {int(node_id): node for node_id, node in node_id_map}


_bridge_instance: Optional[MQueryBridge] = None
_bridge_lock = threading.Lock()


def get_bridge() -> MQueryBridge:
    """Return the process-wide MQueryBridge, creating it on first call."""
    global _bridge_instance
    if _bridge_instance is None:
        with _bridge_lock:
            if _bridge_instance is None:
                _bridge_instance = MQueryBridge()
    return _bridge_instance


def _clear_bridge() -> None:
    """Drop the singleton so the next call to get_bridge() starts fresh.

    Called after a V8 crash to avoid reusing a broken context, and in tests
    to ensure each test module gets an isolated bridge.
    """
    global _bridge_instance
    with _bridge_lock:
        if _bridge_instance is not None:
            # Explicitly close the V8 context before dropping the reference.
            # If we just set _bridge_instance = None here, Python's GC decides
            # when to finalize the MiniRacer object. If that happens while other
            # threads are active (e.g. in a later, unrelated test), MiniRacer's
            # __del__ -> close() path segfaults. Closing synchronously here, while
            # the lock is held and no concurrent parse() calls are in flight,
            # shuts down V8 cleanly.
            try:
                _bridge_instance._ctx.close()
            except Exception:
                pass
        _bridge_instance = None
