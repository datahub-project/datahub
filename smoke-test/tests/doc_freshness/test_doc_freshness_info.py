import logging
from random import randint

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DocFreshnessInfoClass
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns

logger = logging.getLogger(__name__)


def test_doc_freshness_info_round_trips(graph_client):
    dataset_urn = make_dataset_urn(
        "hive", f"doc_freshness_smoke_test_{randint(10000, 99999)}"
    )

    try:
        aspect = DocFreshnessInfoClass(
            verifiedAgainstUrns=[dataset_urn],
            verifiedAtVersion="abc123fingerprint",
            verifiedAtTime=1785700000000,
            actor="urn:li:corpuser:datahub",
            staleReason=None,
        )
        graph_client.emit(
            MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=aspect)
        )
        wait_for_writes_to_sync()

        fresh = graph_client.get_aspect(dataset_urn, DocFreshnessInfoClass)
        assert fresh is not None
        assert fresh.verifiedAgainstUrns == [dataset_urn]
        assert fresh.verifiedAtVersion == "abc123fingerprint"
        assert fresh.staleReason is None

        stale_aspect = DocFreshnessInfoClass(
            verifiedAgainstUrns=[dataset_urn],
            verifiedAtVersion="abc123fingerprint",
            verifiedAtTime=1785700000000,
            actor="urn:li:corpuser:datahub",
            staleReason="upstream schema changed",
        )
        graph_client.emit(
            MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=stale_aspect)
        )
        wait_for_writes_to_sync()

        stale = graph_client.get_aspect(dataset_urn, DocFreshnessInfoClass)
        assert stale is not None
        assert stale.staleReason == "upstream schema changed"
    finally:
        delete_urns(graph_client, [dataset_urn])
        wait_for_writes_to_sync()
