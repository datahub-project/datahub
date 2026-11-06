from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Protocol, Tuple

from google.oauth2 import service_account

from datahub.ingestion.api.source import SourceReport

logger = logging.getLogger(__name__)

TOPIC_PATH_SEGMENTS = 4
PROJECTS_SEGMENT = "projects"
TOPICS_SEGMENT = "topics"
SUBSCRIPTION_REQUEST_KEY = "subscription"


@dataclass(frozen=True)
class PubSubTopicRef:
    project_id: str
    topic_id: str


class Subscription(Protocol):
    @property
    def topic(self) -> str: ...


class SubscriberClient(Protocol):
    def get_subscription(self, request: Dict[str, str]) -> Subscription: ...

    def close(self) -> None: ...


class PubSubResolutionReporter(Protocol):
    def report_pubsub_subscription_unresolved(self, context: str) -> None: ...

    def report_pubsub_subscription_cache_hit(self) -> None: ...


class PubSubSubscriptionResolver:
    """Subscription -> backing-topic resolution; failures are cached as None.

    One ``get_subscription`` per distinct subscription per run, so no retries.
    """

    def __init__(
        self,
        report: PubSubResolutionReporter,
        source_report: SourceReport,
        credentials: Optional[service_account.Credentials] = None,
        subscriber_client: Optional[SubscriberClient] = None,
    ) -> None:
        self.report = report
        self.source_report = source_report
        self._credentials = credentials
        self._client = subscriber_client
        self._disabled = False
        self._lock = threading.Lock()
        # (project_id, subscription_id) -> backing topic, or None.
        self._cache: Dict[Tuple[str, str], Optional[PubSubTopicRef]] = {}
        self._inflight: Dict[Tuple[str, str], threading.Event] = {}

    def _get_client(self) -> Optional[SubscriberClient]:
        """Must be called with ``self._lock`` held."""
        if self._disabled:
            return None
        if self._client is None:
            try:
                # Lazy: this optional feature must not break the import.
                from google.cloud import pubsub_v1  # type: ignore[attr-defined]

                self._client = pubsub_v1.SubscriberClient(credentials=self._credentials)
            except Exception as exc:
                self.source_report.warning(
                    "The Pub/Sub client could not be created, so Pub/Sub "
                    "subscription upstreams are not resolved this run.",
                    title="Dataplex Pub/Sub client unavailable",
                    exc=exc,
                )
                self._disabled = True
                return None
        return self._client

    def close(self) -> None:
        """Release the gRPC channel; the resolver is unusable afterwards."""
        with self._lock:
            client, self._client = self._client, None
            self._disabled = True
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                logger.debug("Pub/Sub client close failed: %s", exc)

    def resolve_topic(
        self, project_id: str, subscription_id: str
    ) -> Optional[PubSubTopicRef]:
        """Backing topic of a subscription, or None. Single-flight per key."""
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

        topic: Optional[PubSubTopicRef] = None
        subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"
        try:
            if client is None:
                self.report.report_pubsub_subscription_unresolved(
                    f"{subscription_path}: pubsub client unavailable"
                )
            else:
                try:
                    subscription = client.get_subscription(
                        request={SUBSCRIPTION_REQUEST_KEY: subscription_path}
                    )
                    # "projects/{p}/topics/{t}", or "_deleted-topic_".
                    topic_path = subscription.topic
                    parts = topic_path.split("/")
                    if (
                        len(parts) == TOPIC_PATH_SEGMENTS
                        and parts[0] == PROJECTS_SEGMENT
                        and parts[2] == TOPICS_SEGMENT
                    ):
                        topic = PubSubTopicRef(project_id=parts[1], topic_id=parts[3])
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
                self._cache[key] = topic
                self._inflight.pop(key, None)
            fetch_event.set()
        return topic
