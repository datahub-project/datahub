from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional, Protocol, Tuple

from google.oauth2 import service_account

logger = logging.getLogger(__name__)

TOPIC_PATH_SEGMENTS = 4
PROJECTS_SEGMENT = "projects"
TOPICS_SEGMENT = "topics"


class PubSubResolutionReporter(Protocol):
    def report_pubsub_subscription_resolved(self) -> None: ...

    def report_pubsub_subscription_unresolved(self, context: str) -> None: ...

    def report_pubsub_subscription_cache_hit(self) -> None: ...


class PubSubSubscriptionResolver:
    """Subscription -> backing-topic resolution; failures are cached as None.

    One ``get_subscription`` per distinct subscription per run, so no retries.
    """

    def __init__(
        self,
        report: PubSubResolutionReporter,
        credentials: Optional[service_account.Credentials] = None,
        subscriber_client: Optional[Any] = None,
    ) -> None:
        self.report = report
        self._credentials = credentials
        self._client = subscriber_client
        self._disabled = False
        self._lock = threading.Lock()
        # (project_id, subscription_id) -> "pubsub:topic:{p}.{t}" or None.
        self._cache: Dict[Tuple[str, str], Optional[str]] = {}
        self._inflight: Dict[Tuple[str, str], threading.Event] = {}

    def _get_client(self) -> Optional[Any]:
        """Must be called with ``self._lock`` held."""
        if self._disabled:
            return None
        if self._client is None:
            try:
                # Lazy: this optional feature must not break the import.
                from google.cloud import pubsub_v1  # type: ignore[attr-defined]

                self._client = pubsub_v1.SubscriberClient(credentials=self._credentials)
            except Exception as exc:
                logger.warning(
                    "Pub/Sub client unavailable; subscription->topic resolution "
                    "disabled for this run: %s",
                    exc,
                )
                self._disabled = True
                return None
        return self._client

    def close(self) -> None:
        """Release the gRPC channel; the resolver is unusable afterwards."""
        with self._lock:
            client, self._client = self._client, None
            self._disabled = True
        close_client = getattr(client, "close", None)
        if close_client is not None:
            try:
                close_client()
            except Exception as exc:
                logger.debug("Pub/Sub client close failed: %s", exc)

    def resolve_topic_fqn(self, project_id: str, subscription_id: str) -> Optional[str]:
        """``pubsub:topic:{p}.{t}`` for a subscription, or None. Single-flight per key."""
        key = (project_id, subscription_id)
        while True:
            with self._lock:
                if key in self._cache:
                    self.report.report_pubsub_subscription_cache_hit()
                    return self._cache[key]
                fetch_event = self._inflight.get(key)
                if fetch_event is None:
                    fetch_event = threading.Event()
                    self._inflight[key] = fetch_event
                    client = self._get_client()
                    break
            fetch_event.wait()

        topic_fqn: Optional[str] = None
        subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"
        try:
            if client is None:
                self.report.report_pubsub_subscription_unresolved(
                    f"{subscription_path}: pubsub client unavailable"
                )
            else:
                try:
                    subscription = client.get_subscription(
                        request={"subscription": subscription_path}
                    )
                    # "projects/{p}/topics/{t}", or "_deleted-topic_".
                    topic_path = subscription.topic
                    parts = topic_path.split("/")
                    if (
                        len(parts) == TOPIC_PATH_SEGMENTS
                        and parts[0] == PROJECTS_SEGMENT
                        and parts[2] == TOPICS_SEGMENT
                    ):
                        topic_fqn = f"pubsub:topic:{parts[1]}.{parts[3]}"
                        self.report.report_pubsub_subscription_resolved()
                    else:
                        self.report.report_pubsub_subscription_unresolved(
                            f"{subscription_path} -> topic={topic_path!r}"
                        )
                except Exception as exc:
                    self.report.report_pubsub_subscription_unresolved(
                        f"{subscription_path}: {exc}"
                    )
                    logger.debug(
                        "Pub/Sub get_subscription failed for %s: %s",
                        subscription_path,
                        exc,
                    )
        finally:
            with self._lock:
                self._cache[key] = topic_fqn
                self._inflight.pop(key, None)
            fetch_event.set()
        return topic_fqn
