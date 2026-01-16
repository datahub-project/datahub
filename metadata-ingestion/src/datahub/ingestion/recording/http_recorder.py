"""HTTP recording and replay using VCR.py."""

import json
import logging
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

logger = logging.getLogger(__name__)


def _normalize_json_body(body: Any) -> Any:
    """Normalize JSON body for comparison.

    Handles:
    - Comma-separated ID lists in filters (order-independent)
    - Sort keys for consistent comparison
    - Binary bodies (returns as-is for byte comparison)

    Args:
        body: Request body (bytes, str, or None)

    Returns:
        Normalized body for comparison
    """
    if not body:
        return body

    try:
        if isinstance(body, bytes):
            # Try to decode as UTF-8, but handle binary data gracefully
            try:
                body = body.decode("utf-8")
            except UnicodeDecodeError:
                # Binary body (protobuf, gRPC, etc.) - return as-is for byte comparison
                return body
        data = json.loads(body)

        # Normalize filters with comma-separated IDs
        if isinstance(data, dict) and "filters" in data:
            filters = data["filters"]
            for key, value in filters.items():
                if isinstance(value, str) and "," in value:
                    # Sort comma-separated values for consistent comparison
                    values = sorted(value.split(","))
                    filters[key] = ",".join(values)

        return json.dumps(data, sort_keys=True)
    except (json.JSONDecodeError, TypeError, AttributeError, UnicodeDecodeError):
        # Not JSON or binary data - return as-is
        return body


# Global lock for serializing HTTP requests during recording
# This is necessary because VCR has a race condition when recording
# concurrent requests - only one thread's request gets recorded properly
_recording_lock = threading.RLock()
_recording_active = False


def _patch_requests_for_thread_safety() -> Callable[[], None]:
    """Patch requests.Session.request to serialize calls during recording.

    VCR has a race condition when multiple threads make concurrent HTTP requests:
    only some requests get recorded properly. This patch ensures all requests
    are serialized during recording to capture everything.

    Also patches vendored requests libraries used by some connectors (e.g., Snowflake)
    since VCR only patches the standard requests library.

    Returns:
        A function to restore the original behavior.
    """
    import requests

    originals: list[tuple[Any, str, Any]] = []

    def create_thread_safe_wrapper(original: Any) -> Any:
        def thread_safe_request(self: Any, *args: Any, **kwargs: Any) -> Any:
            global _recording_active
            if _recording_active:
                with _recording_lock:
                    return original(self, *args, **kwargs)
            else:
                return original(self, *args, **kwargs)

        return thread_safe_request

    # Patch standard requests library
    original_request = requests.Session.request
    requests.Session.request = create_thread_safe_wrapper(original_request)  # type: ignore[method-assign]
    originals.append((requests.Session, "request", original_request))

    # Patch Snowflake's vendored requests if available
    try:
        from snowflake.connector.vendored.requests import sessions as sf_sessions

        sf_original = sf_sessions.Session.request
        sf_sessions.Session.request = create_thread_safe_wrapper(sf_original)  # type: ignore[method-assign]
        originals.append((sf_sessions.Session, "request", sf_original))
        logger.debug("Patched Snowflake vendored requests for thread-safe recording")
    except ImportError:
        pass  # Snowflake connector not installed

    def restore() -> None:
        """Restore original request methods, removing thread-safety patches."""
        for cls, attr, original in originals:
            setattr(cls, attr, original)

    return restore


class HTTPRecorder:
    """Records and replays HTTP traffic using VCR.py.

    VCR.py automatically intercepts all requests made via the `requests` library
    and stores them in a cassette file. During replay, it serves the recorded
    responses without making actual network calls.

    This captures:
    - External API calls (PowerBI, Superset, Snowflake REST, etc.)
    - GMS/DataHub API calls (sink emissions, stateful ingestion checks)
    """

    def __init__(self, cassette_path: Path) -> None:
        """Initialize HTTP recorder.

        Args:
            cassette_path: Path where the VCR cassette will be stored.
        """
        from datahub.ingestion.recording.config import check_recording_dependencies

        check_recording_dependencies()

        import vcr

        self.cassette_path = cassette_path
        self._request_count = 0
        self._total_requests_recorded = 0

        def _log_request(request: Any) -> Any:
            """Log each request as it's being recorded."""
            self._request_count += 1
            logger.debug(
                f"[VCR] Recording request #{self._request_count}: "
                f"{request.method} {request.uri}"
            )
            return request

        self.vcr = vcr.VCR(
            # Use YAML serializer to handle binary data (Arrow, gRPC, etc.)
            # JSON serializer fails on binary responses from Databricks, BigQuery, etc.
            serializer="yaml",
            record_mode="new_episodes",
            # Match on these attributes to identify unique requests
            match_on=["uri", "method", "body"],
            # Filter out sensitive headers from recordings
            filter_headers=[
                "Authorization",
                "X-DataHub-Auth",
                "Cookie",
                "Set-Cookie",
            ],
            # Decode compressed responses for readability
            decode_compressed_response=True,
            # Log each request as it's recorded
            before_record_request=_log_request,
        )
        self._cassette: Optional[Any] = None

    @contextmanager
    def recording(self) -> Iterator["HTTPRecorder"]:
        """Context manager for recording HTTP traffic.

        This method ensures thread-safe recording by serializing all HTTP requests.
        VCR has a race condition when recording concurrent requests, which can cause
        some requests to be missed. The thread-safe patch ensures all requests are
        captured properly.

        Usage:
            recorder = HTTPRecorder(Path("cassette.json"))
            with recorder.recording():
                # All HTTP calls are recorded
                requests.get("https://api.example.com/data")
        """
        global _recording_active

        logger.info(f"Starting HTTP recording to {self.cassette_path}")
        logger.info(
            "Thread-safe recording enabled: HTTP requests will be serialized "
            "to ensure all are captured. This may impact performance but "
            "guarantees complete recordings."
        )

        # Ensure parent directory exists
        self.cassette_path.parent.mkdir(parents=True, exist_ok=True)

        # Patch requests for thread-safe recording
        restore_patch = _patch_requests_for_thread_safety()
        _recording_active = True

        try:
            with self.vcr.use_cassette(
                str(self.cassette_path),
                record_mode="new_episodes",
            ) as cassette:
                self._cassette = cassette
                try:
                    yield self
                finally:
                    # Store request count before clearing cassette
                    self._total_requests_recorded = len(cassette)
                    self._cassette = None
                    # Log detailed recording stats
                    logger.info(
                        f"HTTP recording complete. "
                        f"Recorded {self._total_requests_recorded} request(s) to {self.cassette_path}"
                    )
        finally:
            # Always restore original behavior
            _recording_active = False
            restore_patch()
            logger.debug("Thread-safe HTTP recording patch removed")

    @contextmanager
    def replaying(
        self, use_responses_library: bool = False
    ) -> Iterator["HTTPRecorder"]:
        """Context manager for replaying HTTP traffic.

        In replay mode, recorded responses are served without making actual network calls.
        This ensures true air-gapped operation.

        Args:
            use_responses_library: If True, use the 'responses' library for replay.
                This provides better compatibility with SDKs that have custom transports
                (e.g., Looker SDK). If False (default), use VCR.py which matches the
                recording library. VCR.py may have issues with some SDKs that use super()
                calls in urllib3's connection classes.

        Matching strategy:
        - Authentication requests (login endpoints): Match on URI and method only,
          ignoring body differences (credentials differ between record/replay)
        - Data requests: Match on URI, method, AND body to ensure correct
          responses for queries with different parameters

        Usage:
            recorder = HTTPRecorder(Path("cassette.json"))
            with recorder.replaying():
                # All HTTP calls are served from recordings
                response = requests.get("https://api.example.com/data")
        """
        if not self.cassette_path.exists():
            raise FileNotFoundError(
                f"HTTP cassette not found: {self.cassette_path}. "
                "Cannot replay without a recorded cassette.\n\n"
                "This usually means the recording failed before any HTTP requests were made. "
                "Common causes:\n"
                "  - Connection failed during source initialization (check credentials/network)\n"
                "  - Recording captured an exception before HTTP traffic occurred\n"
                "  - Source uses a non-standard HTTP library not intercepted by VCR\n\n"
                "Use 'datahub recording info <archive>' to check if the recording includes an exception."
            )

        if use_responses_library:
            # Use responses library for better compatibility
            with self._replaying_with_responses() as recorder:
                yield recorder
        else:
            # Fall back to VCR.py
            with self._replaying_with_vcr() as recorder:
                yield recorder

    @contextmanager
    def _replaying_with_responses(self) -> Iterator["HTTPRecorder"]:
        """Replay HTTP traffic using the responses library.

        The responses library patches at the requests adapter level rather than
        urllib3's connection level, which avoids issues with SDKs that use
        super() calls in custom transport implementations (e.g., Looker SDK).
        """
        try:
            import responses
            import yaml
        except ImportError:
            logger.warning(
                "responses library not available, falling back to VCR.py for replay. "
                "Install with: pip install responses"
            )
            with self._replaying_with_vcr() as recorder:
                yield recorder
            return

        logger.info(
            f"Starting HTTP replay from {self.cassette_path} (using responses library)"
        )

        # Load cassette data
        with open(self.cassette_path, "r") as f:
            cassette_data = yaml.safe_load(f)

        interactions = cassette_data.get("interactions", [])

        # Build a lookup for responses based on (method, uri, body)
        # Store responses in order for each (method, uri) pair to handle
        # requests with different bodies or sequential requests
        response_map: dict[tuple[str, str], list[dict[str, Any]]] = {}

        for interaction in interactions:
            request = interaction.get("request", {})
            response = interaction.get("response", {})

            method = request.get("method", "GET").upper()
            uri = request.get("uri", "")
            body = request.get("body", "")

            key = (method, uri)
            if key not in response_map:
                response_map[key] = []

            response_map[key].append(
                {
                    "request_body": body,
                    "response": response,
                }
            )

        # Track which response index to use for each (method, uri) pair
        response_indices: dict[tuple[str, str], int] = {}

        def request_callback(request: Any) -> tuple[int, dict[str, str], bytes]:
            """Callback to match request and return appropriate response."""
            method = request.method.upper()
            url = request.url

            key = (method, url)
            responses_list = response_map.get(key, [])

            if not responses_list:
                # Try to find a partial match (ignoring query params for some cases)
                for (m, u), resps in response_map.items():
                    if m == method and u.split("?")[0] == url.split("?")[0]:
                        responses_list = resps
                        key = (m, u)
                        break

            if not responses_list:
                raise Exception(
                    f"No recorded response for {method} {url}. "
                    "This request was not captured during recording."
                )

            # Get the next response in sequence for this (method, uri)
            idx = response_indices.get(key, 0)
            if idx >= len(responses_list):
                # Cycle back to the first response if we've exhausted the list
                idx = 0

            resp_data = responses_list[idx]
            response_indices[key] = idx + 1

            response = resp_data["response"]
            status_code = response.get("status", {}).get("code", 200)
            headers = {}
            for header_name, header_values in response.get("headers", {}).items():
                # responses library expects headers as dict[str, str]
                if isinstance(header_values, list):
                    headers[header_name] = header_values[0] if header_values else ""
                else:
                    headers[header_name] = str(header_values)

            body = response.get("body", {}).get("string", b"")
            if isinstance(body, str):
                body = body.encode("utf-8")

            return (status_code, headers, body)

        import re

        # Use responses library to mock HTTP calls
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            # Add callback for all HTTP methods using regex to match any URL
            any_url = re.compile(r".*")
            for method in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]:
                rsps.add_callback(
                    getattr(responses, method),
                    any_url,
                    callback=request_callback,
                )

            self._cassette = None  # No VCR cassette in this mode
            try:
                yield self
            finally:
                logger.info("HTTP replay complete (responses library)")

    @contextmanager
    def _replaying_with_vcr(self) -> Iterator["HTTPRecorder"]:
        """Replay HTTP traffic using VCR.py.

        Note: This may have issues with some SDKs that use custom transport
        implementations with super() calls in urllib3's connection classes.
        """
        logger.info(f"Starting HTTP replay from {self.cassette_path} (using VCR.py)")

        import vcr

        # Authentication endpoints that should match without body comparison
        # These contain credentials that differ between recording (real) and replay (dummy)
        AUTH_ENDPOINTS = [
            "/login",
            "/oauth",
            "/token",
            "/auth",
        ]

        def is_auth_request(request: Any) -> bool:
            """Check if request is an authentication request."""
            uri = request.uri.lower()
            return any(endpoint in uri for endpoint in AUTH_ENDPOINTS)

        def custom_body_matcher(r1: Any, r2: Any) -> bool:
            """Custom body matcher with special handling for auth and JSON bodies."""
            # For auth requests, don't compare body (credentials differ)
            if is_auth_request(r1):
                return True

            # Normalize and compare JSON bodies
            body1 = _normalize_json_body(r1.body)
            body2 = _normalize_json_body(r2.body)
            return body1 == body2

        # Create VCR with custom matchers
        replay_vcr = vcr.VCR(
            # Use YAML serializer to match recording format (handles binary data)
            serializer="yaml",
            record_mode="none",
            decode_compressed_response=True,
        )

        # Register custom body matcher
        replay_vcr.register_matcher("custom_body", custom_body_matcher)

        with replay_vcr.use_cassette(
            str(self.cassette_path),
            record_mode="none",  # Don't record, only replay
            # Match on URI, method, and custom body matcher
            # Custom body matcher skips body comparison for auth requests
            match_on=["uri", "method", "custom_body"],
            # Allow requests to be matched in any order and multiple times
            # This is necessary because sources may make parallel requests
            # and the order can differ between recording and replay
            allow_playback_repeats=True,
        ) as cassette:
            self._cassette = cassette
            try:
                yield self
            finally:
                self._cassette = None
                logger.info("HTTP replay complete (VCR.py)")

    @property
    def request_count(self) -> int:
        """Number of requests recorded/replayed in current session."""
        if self._cassette is not None:
            return len(self._cassette)
        # If cassette is cleared (after recording context exits), return stored count
        return self._total_requests_recorded


class HTTPReplayerForLiveSink:
    """HTTP replayer that allows specific hosts to make real requests.

    Used when replaying with --live-sink flag, where we want to:
    - Replay source HTTP calls from recordings
    - Allow real HTTP calls to the GMS server for sink operations
    """

    def __init__(
        self,
        cassette_path: Path,
        live_hosts: list[str],
    ) -> None:
        """Initialize HTTP replayer with live sink support.

        Args:
            cassette_path: Path to the VCR cassette file.
            live_hosts: List of hostnames that should make real requests
                       (e.g., ["localhost:8080", "datahub.example.com"]).
        """
        from datahub.ingestion.recording.config import check_recording_dependencies

        check_recording_dependencies()

        import vcr

        self.cassette_path = cassette_path
        self.live_hosts = live_hosts

        # Authentication endpoints that should match without body comparison
        AUTH_ENDPOINTS = [
            "/login",
            "/oauth",
            "/token",
            "/auth",
        ]

        def is_auth_request(request: Any) -> bool:
            """Check if request is an authentication request."""
            uri = request.uri.lower()
            return any(endpoint in uri for endpoint in AUTH_ENDPOINTS)

        # Custom matcher that allows live hosts to bypass recording
        # and handles auth vs data request body matching
        def custom_matcher(r1: Any, r2: Any) -> bool:
            """Match requests with special handling for live hosts and auth."""
            from urllib.parse import urlparse

            parsed = urlparse(r1.uri)
            host = parsed.netloc

            # If this is a live host, don't match (allow real request)
            for live_host in live_hosts:
                if live_host in host:
                    return False

            # URI and method must always match
            if r1.uri != r2.uri or r1.method != r2.method:
                return False

            # For auth requests, don't compare body (credentials differ)
            if is_auth_request(r1):
                return True

            # Normalize and compare JSON bodies
            body1 = _normalize_json_body(r1.body)
            body2 = _normalize_json_body(r2.body)
            return body1 == body2

        self.vcr = vcr.VCR(
            # Use YAML serializer to match recording format (handles binary data)
            serializer="yaml",
            record_mode="none",
            before_record_request=self._filter_live_hosts,
        )
        self.vcr.register_matcher("custom", custom_matcher)
        self._cassette: Optional[Any] = None

    def _filter_live_hosts(self, request: Any) -> Optional[Any]:
        """Filter out requests to live hosts from being recorded."""
        from urllib.parse import urlparse

        parsed = urlparse(request.uri)
        host = parsed.netloc

        for live_host in self.live_hosts:
            if live_host in host:
                return None  # Don't record this request

        return request

    @contextmanager
    def replaying(self) -> Iterator["HTTPReplayerForLiveSink"]:
        """Context manager for replaying with live sink support."""
        if not self.cassette_path.exists():
            raise FileNotFoundError(
                f"HTTP cassette not found: {self.cassette_path}. "
                "Cannot replay without a recorded cassette.\n\n"
                "This usually means the recording failed before any HTTP requests were made. "
                "Common causes:\n"
                "  - Connection failed during source initialization (check credentials/network)\n"
                "  - Recording captured an exception before HTTP traffic occurred\n"
                "  - Source uses a non-standard HTTP library not intercepted by VCR\n\n"
                "Use 'datahub recording info <archive>' to check if the recording includes an exception."
            )

        logger.info(
            f"Starting HTTP replay from {self.cassette_path} "
            f"with live hosts: {self.live_hosts}"
        )

        with self.vcr.use_cassette(
            str(self.cassette_path),
            record_mode="new_episodes",  # Allow new requests to live hosts
            match_on=["custom"],  # Use custom matcher for body handling
            allow_playback_repeats=True,
        ) as cassette:
            self._cassette = cassette
            try:
                yield self
            finally:
                self._cassette = None
                logger.info("HTTP replay with live sink complete")


@contextmanager
def vcr_bypass_context() -> Iterator[None]:
    """Temporarily disable VCR to allow direct HTTP connections.

    This is useful when VCR interferes with authentication for sources
    using vendored or non-standard HTTP libraries (e.g., Snowflake, Databricks).

    During the bypass, VCR's patching is temporarily disabled, allowing
    the connection to proceed normally. After the context exits, VCR
    patching is restored.

    Usage:
        try:
            # Try with VCR active
            connection = snowflake.connector.connect(...)
        except Exception:
            # Retry with VCR bypassed
            with vcr_bypass_context():
                connection = snowflake.connector.connect(...)
    """
    try:
        import vcr as vcr_module
    except ImportError:
        # VCR not installed, nothing to bypass
        logger.debug("VCR not installed, bypass context is a no-op")
        yield
        return

    # Store current VCR cassette state if any
    # VCR stores active cassettes in a thread-local variable
    original_cassettes = getattr(vcr_module.cassette, "_current_cassettes", None)

    # Temporarily clear active cassettes to bypass VCR
    if hasattr(vcr_module.cassette, "_current_cassettes"):
        vcr_module.cassette._current_cassettes = []

    try:
        logger.debug("VCR temporarily bypassed for connection")
        yield
    finally:
        # Restore VCR cassettes
        if original_cassettes is not None and hasattr(
            vcr_module.cassette, "_current_cassettes"
        ):
            vcr_module.cassette._current_cassettes = original_cassettes
        logger.debug("VCR patching restored")
