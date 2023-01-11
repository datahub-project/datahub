import pytest

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_dataset_urn,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState


@pytest.mark.parametrize(
    "entity_type,urn",
    [
        ("dataset", "d0150d70-b667-4efb-ad84-db3e49f00000"),
        ("chart", "charts.d0150d70-b667-4efb-ad84-db3e49f00000"),
        ("dashboard", "reports.add75db6-a256-49c3-a6a3-99db46ad0000"),
    ],
)
def test_powerbi_common_state(entity_type: str, urn: str) -> None:
    state1 = GenericCheckpointState()
    if entity_type == "dataset":
        test_urn = make_dataset_urn("powerbi", urn, "test")
    elif entity_type == "chart":
        test_urn = make_chart_urn("powerbi", urn)
    elif entity_type == "dashboard":
        test_urn = make_dashboard_urn("powerbi", urn)

    state1.add_checkpoint_urn(type=entity_type, urn=test_urn)

    state2 = GenericCheckpointState()

    urns_diff = list(
        state1.get_urns_not_in(type=entity_type, other_checkpoint_state=state2)
    )
    assert len(urns_diff) == 1 and urns_diff[0] == test_urn


def test_powerbi_state_migration() -> None:
    state = GenericCheckpointState.parse_obj(
        {
            "encoded_chart_urns": [
                "powerbi||charts.d0150d70-b667-4efb-ad84-db3e49f00000",
            ],
            "encoded_dataset_urns": [
                "powerbi||d0150d70-b667-4efb-ad84-db3e49f00000||test",
            ],
            "encoded_dashboard_urns": [
                "powerbi||reports.810a7176-7187-4c8d-ba08-36019c0600000",
                "powerbi||dashboards.810a7176-7187-4c8d-ba08-36019c06ffff",
            ],
            "encoded_container_urns": ["6abac8e2a5c58ed81951bc3320ddddd"],
        }
    )
    assert state.urns == [
        "urn:li:container:6abac8e2a5c58ed81951bc3320ddddd",
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,d0150d70-b667-4efb-ad84-db3e49f00000,test)",
        "urn:li:chart:(powerbi,charts.d0150d70-b667-4efb-ad84-db3e49f00000)",
        "urn:li:dashboard:(powerbi,reports.810a7176-7187-4c8d-ba08-36019c0600000)",
        "urn:li:dashboard:(powerbi,dashboards.810a7176-7187-4c8d-ba08-36019c06ffff)",
    ]
