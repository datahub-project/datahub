import logging
import time
from datetime import datetime, timedelta
from typing import Any, List, Optional, Protocol, TypeVar

from requests import Response

from datahub.configuration.common import TraceTimeoutError, TraceValidationError
from datahub.emitter.response_helper import TraceData

logger = logging.getLogger(__name__)

PENDING_STATUS = "PENDING"
INITIAL_BACKOFF = 1.0  # Start with 1 second
MAX_BACKOFF = 300.0  # Cap at 5 minutes
BACKOFF_FACTOR = 2.0  # Double the wait time each attempt


class TracerProtocol(Protocol):
    _gms_server: str

    def _emit_generic_payload(self, url: str, payload: Any) -> Response: ...


T = TypeVar("T", bound=TracerProtocol)


class OpenAPITrace:
    def await_status(
        self: T,
        trace_data: List[TraceData],
        trace_timeout: Optional[timedelta] = timedelta(seconds=3600),
    ) -> None:
        """Verify the status of asynchronous write operations.

        Args:
            trace_data: List of trace data to verify
            trace_timeout: Maximum time to wait for verification.

        Raises:
            TraceTimeoutError: If verification fails or times out
            TraceValidationError: Expected write was not completed successfully
        """
        if trace_timeout is None:
            raise ValueError("trace_timeout cannot be None")

        try:
            if not trace_data:
                logger.debug("No trace data to verify")
                return

            start_time = datetime.now()

            for trace in trace_data:
                current_backoff = INITIAL_BACKOFF

                while trace.data:
                    if datetime.now() - start_time > trace_timeout:
                        raise TraceTimeoutError(
                            f"Timeout waiting for async write completion after {trace_timeout.total_seconds()} seconds"
                        )

                    base_url = f"{self._gms_server}/openapi/v1/trace/write"
                    url = f"{base_url}/{trace.trace_id}?onlyIncludeErrors=false&detailed=true"

                    response = self._emit_generic_payload(url, payload=trace.data)
                    json_data = response.json()

                    for urn, aspects in json_data.items():
                        for aspect_name, aspect_status in aspects.items():
                            if not aspect_status["success"]:
                                error_msg = (
                                    f"Unable to validate async write to DataHub GMS: "
                                    f"Persistence failure for URN '{urn}' aspect '{aspect_name}'. "
                                    f"Status: {aspect_status}"
                                )
                                raise TraceValidationError(error_msg, aspect_status)

                            primary_storage = aspect_status["primaryStorage"][
                                "writeStatus"
                            ]
                            search_storage = aspect_status["searchStorage"][
                                "writeStatus"
                            ]

                            # Remove resolved statuses
                            if (
                                primary_storage != PENDING_STATUS
                                and search_storage != PENDING_STATUS
                            ):
                                trace.data[urn].remove(aspect_name)

                        # Remove urns with all statuses resolved
                        if not trace.data[urn]:
                            trace.data.pop(urn)

                    # Adjust backoff based on response
                    if trace.data:
                        # If we still have pending items, increase backoff
                        current_backoff = min(
                            current_backoff * BACKOFF_FACTOR, MAX_BACKOFF
                        )
                        logger.debug(
                            f"Waiting {current_backoff} seconds before next check"
                        )
                        time.sleep(current_backoff)

        except Exception as e:
            logger.error(f"Error during status verification: {str(e)}")
            raise
