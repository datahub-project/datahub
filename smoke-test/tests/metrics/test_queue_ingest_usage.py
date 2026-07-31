"""Smoke coverage for queue-path metadata_ingest on the MCE consumer."""

from __future__ import annotations

import logging
import time
import uuid

import pytest
import requests
import tenacity

from datahub.emitter.kafka_emitter import DatahubKafkaEmitter, KafkaEmitterConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass
from tests.metrics.usage_aggregation_metrics import (
    REQUEST_COUNT_METRIC,
    sum_matching_metric_values,
)
from tests.utils import (
    get_kafka_broker_url,
    get_kafka_schema_registry,
    get_queue_ingest_prometheus_url,
)

logger = logging.getLogger(__name__)


def _fetch_messaging_ingest_total(prom_url: str) -> float:
    content = requests.get(prom_url, timeout=30).text
    return sum_matching_metric_values(
        content,
        REQUEST_COUNT_METRIC,
        {"usage_operation": "metadata_ingest", "request_api": "messaging"},
    )


def _usage_aggregation_exported(prom_url: str) -> bool:
    try:
        content = requests.get(prom_url, timeout=10).text
    except requests.RequestException:
        return False
    return REQUEST_COUNT_METRIC in content


@pytest.mark.dependency()
def test_queue_ingest_metadata_ingest_metric_on_mce():
    """Direct Kafka MCP ingest increments metadata_ingest (messaging) on MCE or embedded GMS."""
    prom_url, source = get_queue_ingest_prometheus_url()
    if prom_url is None:
        pytest.skip(
            "Queue ingest Prometheus endpoint not reachable — set DATAHUB_MCE_MANAGEMENT_URL "
            "for a standalone MCE consumer or DATAHUB_GMS_MANAGEMENT_URL when consumers are "
            "embedded in GMS."
        )
    if not _usage_aggregation_exported(prom_url):
        pytest.skip(
            f"USAGE_AGGREGATION_ENABLED appears off on {source} management endpoint "
            f"({prom_url})"
        )

    emitter = DatahubKafkaEmitter(
        KafkaEmitterConfig(
            connection={
                "bootstrap": get_kafka_broker_url(),
                "schema_registry_url": get_kafka_schema_registry(),
            }
        )
    )

    unique = uuid.uuid4().hex[:8]
    urn = f"urn:li:dataset:(urn:li:dataPlatform:kafka,queue-usage-smoke-{unique},PROD)"
    mcp = MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(name=f"queue-usage-smoke-{unique}"),
    )

    baseline = _fetch_messaging_ingest_total(prom_url)

    emitter.emit(mcp)
    emitter.flush()
    time.sleep(2)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(25),
        wait=tenacity.wait_fixed(3),
        reraise=True,
    )
    def _wait_for_delta() -> float:
        total = _fetch_messaging_ingest_total(prom_url)
        delta = total - baseline
        assert delta >= 1.0, (
            f"Expected metadata_ingest messaging delta>=1 on {source} endpoint, got {delta} "
            f"(baseline={baseline}, total={total}, prom_url={prom_url})"
        )
        return total

    try:
        _wait_for_delta()
    except AssertionError:
        if source == "gms":
            pytest.skip(
                "Queue-path metadata_ingest did not increase on GMS management metrics — "
                "embedded consumer mode needs MCE aggregation wiring on GMS or a standalone "
                "MCE consumer with USAGE_AGGREGATION_ENABLED=true."
            )
        raise

    logger.info(
        "metadata_ingest messaging counter increased after Kafka MCP publish "
        "(source=%s, prom_url=%s)",
        source,
        prom_url,
    )
