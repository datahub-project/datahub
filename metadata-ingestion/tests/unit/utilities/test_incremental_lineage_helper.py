from typing import Optional

from datahub.ingestion.api.incremental_lineage_helper import convert_chart_info_to_patch
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeAuditStampsClass,
    ChartInfoClass,
    MetadataChangeProposalClass,
)


def test_convert_chart_info_to_patch():
    chart_info_class: ChartInfoClass = ChartInfoClass(
        title="foo",
        description="Checking patch",
        inputs=[
            "urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.pet_details,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:dbt,calm-pagoda-323403.jaffle_shop.customers,PROD)",
        ],
        lastModified=ChangeAuditStampsClass(),
    )

    mw: Optional[MetadataWorkUnit] = convert_chart_info_to_patch(
        urn="urn:li:chart:(looker,dashboard_elements.1)",
        aspect=chart_info_class,
        system_metadata=None,
    )

    assert mw

    assert mw.id == "urn:li:chart:(looker,dashboard_elements.1)-chartInfo"

    assert isinstance(mw.metadata, MetadataChangeProposalClass)

    assert mw.metadata.aspect

    assert (
        mw.metadata.aspect.value
        == b'[{"op": "add", "path": "/title", "value": "foo"}, {"op": "add", "path": "/lastModified", "value": {"created": {"time": 0, "actor": "urn:li:corpuser:unknown"}, "lastModified": {"time": 0, "actor": "urn:li:corpuser:unknown"}}}, {"op": "add", "path": "/description", "value": "Checking patch"}, {"op": "add", "path": "/inputs/urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.pet_details,PROD)", "value": "urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.pet_details,PROD)"}, {"op": "add", "path": "/inputs/urn:li:dataset:(urn:li:dataPlatform:dbt,calm-pagoda-323403.jaffle_shop.customers,PROD)", "value": "urn:li:dataset:(urn:li:dataPlatform:dbt,calm-pagoda-323403.jaffle_shop.customers,PROD)"}]'
    )
