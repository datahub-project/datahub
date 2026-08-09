import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DocFreshnessInfoClass
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns, unique_dataset_urn

logger = logging.getLogger(__name__)

VERIFIED_AT_TIME = 1785700000000
ACTOR = "urn:li:corpuser:datahub"


def test_doc_freshness_info_round_trips(graph_client):
    dataset_urn = unique_dataset_urn("doc_freshness_smoke_test", platform="hive")

    try:
        aspect = DocFreshnessInfoClass(
            verifiedAgainstUrns=[dataset_urn],
            verifiedAtVersion="abc123fingerprint",
            verifiedAtTime=VERIFIED_AT_TIME,
            actor=ACTOR,
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
        assert fresh.verifiedAtTime == VERIFIED_AT_TIME
        assert fresh.actor == ACTOR
        assert fresh.staleReason is None

        stale_aspect = DocFreshnessInfoClass(
            verifiedAgainstUrns=[dataset_urn],
            verifiedAtVersion="abc123fingerprint",
            verifiedAtTime=VERIFIED_AT_TIME,
            actor=ACTOR,
            staleReason="upstream schema changed",
        )
        graph_client.emit(
            MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=stale_aspect)
        )
        wait_for_writes_to_sync()

        stale = graph_client.get_aspect(dataset_urn, DocFreshnessInfoClass)
        assert stale is not None
        assert stale.verifiedAgainstUrns == [dataset_urn]
        assert stale.verifiedAtVersion == "abc123fingerprint"
        assert stale.verifiedAtTime == VERIFIED_AT_TIME
        assert stale.actor == ACTOR
        assert stale.staleReason == "upstream schema changed"
    finally:
        delete_urns(graph_client, [dataset_urn])
        wait_for_writes_to_sync()
