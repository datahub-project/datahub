from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.metadata.urns import DataFlowUrn

OWNERS = {"zoe", "alice", "mike"}
GROUP_OWNERS = {"data-eng", "analytics"}
EXPECTED_OWNER_URNS = [
    "urn:li:corpGroup:analytics",
    "urn:li:corpGroup:data-eng",
    "urn:li:corpuser:alice",
    "urn:li:corpuser:mike",
    "urn:li:corpuser:zoe",
]


def test_dataflow_ownership_owners_are_sorted() -> None:
    flow = DataFlow(
        orchestrator="airflow",
        id="my_dag",
        env="PROD",
        owners=set(OWNERS),
        group_owners=set(GROUP_OWNERS),
    )

    [ownership] = flow.generate_ownership_aspect()

    assert [o.owner for o in ownership.owners] == EXPECTED_OWNER_URNS


def test_datajob_ownership_owners_are_sorted() -> None:
    job = DataJob(
        id="my_task",
        flow_urn=DataFlowUrn.create_from_ids(
            orchestrator="airflow", flow_id="my_dag", env="PROD"
        ),
        owners=set(OWNERS),
        group_owners=set(GROUP_OWNERS),
    )

    [ownership] = job.generate_ownership_aspect()

    assert [o.owner for o in ownership.owners] == EXPECTED_OWNER_URNS
