from typing import Dict, List
from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
)
from datahub.ingestion.source.tibco_bw.config import TibcoBwSourceConfig
from datahub.ingestion.source.tibco_bw.models import TibcoApplication, TibcoScope
from datahub.ingestion.source.tibco_bw.source import TibcoBwSource
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    SubTypesClass,
)


def _source(**overrides: object) -> TibcoBwSource:
    config: Dict[str, object] = {
        "deployment": "on_prem",
        "base_url": "http://bw:8079",
        "username": "u",
        "password": "p",
    }
    config.update(overrides)
    return TibcoBwSource(
        TibcoBwSourceConfig.model_validate(config), PipelineContext(run_id="test")
    )


def _scope(name: str, apps: List[str], domain: str = "D1") -> TibcoScope:
    return TibcoScope(
        id=f"{domain}/{name}",
        name=name,
        properties={"domain": domain},
        applications=[TibcoApplication(name=app) for app in apps],
    )


def _run(source: TibcoBwSource, scopes: List[TibcoScope]) -> list:
    source.client = MagicMock()
    source.client.fetch_scopes.return_value = scopes
    return list(source.get_workunits_internal())


def _urns_with_subtype(workunits: list, subtype: str) -> List[str]:
    urns = []
    for wu in workunits:
        aspect = wu.metadata.aspect
        if isinstance(aspect, SubTypesClass) and aspect.typeNames == [subtype]:
            urns.append(wu.metadata.entityUrn)
    return urns


def _urns_with_aspect(workunits: list, aspect_type: type) -> List[str]:
    return [
        wu.metadata.entityUrn
        for wu in workunits
        if isinstance(wu.metadata.aspect, aspect_type)
    ]


def test_emits_flow_and_job_with_subtypes() -> None:
    source = _source()
    workunits = _run(source, [_scope("AS1", ["orders"])])

    flow_urns = _urns_with_aspect(workunits, DataFlowInfoClass)
    job_urns = _urns_with_aspect(workunits, DataJobInfoClass)
    assert len(flow_urns) == 1
    assert len(job_urns) == 1
    assert "tibco-bw" in flow_urns[0]
    assert flow_urns[0].endswith("D1/AS1,PROD)")

    assert (
        _urns_with_subtype(workunits, DataFlowSubTypes.TIBCO_BW_APPSPACE) == flow_urns
    )
    assert _urns_with_subtype(workunits, DataJobSubTypes.TIBCO_APPLICATION) == job_urns
    assert source.report.flows_emitted == 1
    assert source.report.jobs_emitted == 1


def test_scope_and_application_filtering() -> None:
    source = _source(
        appspace_pattern={"deny": ["AS_SKIP"]},
        application_pattern={"deny": ["test_.*"]},
    )
    workunits = _run(
        source,
        [
            _scope("AS_KEEP", ["orders", "test_tmp"]),
            _scope("AS_SKIP", ["ignored"]),
        ],
    )

    flow_urns = _urns_with_aspect(workunits, DataFlowInfoClass)
    job_urns = _urns_with_aspect(workunits, DataJobInfoClass)
    assert len(flow_urns) == 1 and flow_urns[0].endswith("D1/AS_KEEP,PROD)")
    assert len(job_urns) == 1 and job_urns[0].endswith("orders)")
    assert source.report.jobs_emitted == 1


def test_cloud_scope_uses_subscription_subtype() -> None:
    source = _source(
        deployment="cloud", base_url=None, username=None, password=None, token="abc"
    )
    workunits = _run(source, [_scope("sub1", ["appA"])])

    assert (
        len(_urns_with_subtype(workunits, DataFlowSubTypes.TIBCO_TCI_SUBSCRIPTION)) == 1
    )


def test_no_dataset_lineage_emitted_without_config() -> None:
    from datahub.metadata.schema_classes import DataJobInputOutputClass

    source = _source()
    workunits = _run(source, [_scope("AS1", ["orders"])])
    assert _urns_with_aspect(workunits, DataJobInputOutputClass) == []


_UPSTREAM_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,orders_in,PROD)"
_DOWNSTREAM_URN = "urn:li:dataset:(urn:li:dataPlatform:hana,sales.orders,PROD)"


def test_application_lineage_emits_iolets() -> None:
    from datahub.metadata.schema_classes import DataJobInputOutputClass

    source = _source(
        application_lineage={
            "orders": {
                "upstreams": [_UPSTREAM_URN],
                "downstreams": [_DOWNSTREAM_URN],
            }
        }
    )
    workunits = _run(source, [_scope("AS1", ["orders", "unmapped"])])

    io_aspects = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata.aspect, DataJobInputOutputClass)
    ]
    # Only the mapped application ("orders") gets an input/output aspect.
    assert len(io_aspects) == 1
    assert io_aspects[0].inputDatasets == [_UPSTREAM_URN]
    assert io_aspects[0].outputDatasets == [_DOWNSTREAM_URN]
    assert source.report.jobs_with_lineage == 1
    assert source.report.lineage_iolets_emitted == 2


def test_application_lineage_does_not_materialize_iolet_datasets() -> None:
    from datahub.metadata.schema_classes import DatasetKeyClass

    source = _source(application_lineage={"orders": {"upstreams": [_UPSTREAM_URN]}})
    workunits = _run(source, [_scope("AS1", ["orders"])])
    # The upstream dataset belongs to another connector; we link to it without
    # emitting a stub dataset entity for it.
    assert _urns_with_aspect(workunits, DatasetKeyClass) == []


def test_invalid_lineage_urn_is_rejected() -> None:
    import pytest

    with pytest.raises(ValueError):
        _source(application_lineage={"orders": {"upstreams": ["not-a-urn"]}})
