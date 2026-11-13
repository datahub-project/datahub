from unittest.mock import MagicMock, call

from datahub.cli.delete_cli import _delete_one_urn
from datahub.ingestion.graph.client import DataHubGraph

ROLE_URN = "urn:li:role:my_role"
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)"


def _graph() -> MagicMock:
    graph = MagicMock(spec=DataHubGraph)
    graph.hard_delete_entity.return_value = (1, 0)
    graph.delete_references_to_urn.return_value = (2, [])
    return graph


def test_hard_delete_removes_references_before_entity() -> None:
    # The server finds references through incoming graph edges, which the hard
    # delete's MCL removes asynchronously, so references must go first.
    graph = _graph()

    result = _delete_one_urn(graph=graph, urn=ROLE_URN)

    assert graph.mock_calls == [
        call.delete_references_to_urn(urn=ROLE_URN, dry_run=False),
        call.hard_delete_entity(urn=ROLE_URN),
    ]
    assert result.num_referenced_entities == 2


def test_hard_delete_skips_references_for_other_types() -> None:
    graph = _graph()

    _delete_one_urn(graph=graph, urn=DATASET_URN)

    assert graph.mock_calls == [call.hard_delete_entity(urn=DATASET_URN)]
