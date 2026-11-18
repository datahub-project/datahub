"""Sensitivity label parsing, Graph resolution, and tag or glossary emission."""

from unittest.mock import MagicMock

import pytest
import requests
from pydantic import ValidationError

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    PowerBiEnvironment,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Workspace,
    new_powerbi_dashboards,
    new_powerbi_dataset,
    new_powerbi_reports,
    sensitivity_label_id,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    DataResolverBase,
)
from datahub.ingestion.source.powerbi.sensitivity import (
    SensitivityLabel,
    SensitivityLabelEmitter,
    SensitivityLabelResolver,
    sensitivity_label_filter,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    TagPropertiesClass,
)
from datahub.sdk.glossary_term import GlossaryTerm


def _json_response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def _workspace() -> Workspace:
    return Workspace(
        id="ws",
        name="Workspace",
        type="Workspace",
        dashboards={},
        reports={},
        datasets={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
        webUrl="https://app.powerbi.com/groups/ws",
    )


def _emitter(output: str, label: SensitivityLabel | None) -> SensitivityLabelEmitter:
    resolver = MagicMock()
    resolver.resolve.return_value = label
    return SensitivityLabelEmitter(
        enabled=True,
        output=output,  # type: ignore[arg-type]
        resolver=resolver,
    )


def test_sensitivity_label_id_reads_scan_guid() -> None:
    assert (
        sensitivity_label_id({"sensitivityLabel": {"labelId": "label-guid"}})
        == "label-guid"
    )
    assert sensitivity_label_id({"sensitivityLabel": {"id": "label-guid"}}) == (
        "label-guid"
    )
    assert sensitivity_label_id({}) is None
    assert sensitivity_label_id({"sensitivityLabel": {}}) is None
    assert sensitivity_label_id({"sensitivityLabel": "Confidential"}) is None


def test_dataset_report_and_dashboard_keep_label_id() -> None:
    workspace = _workspace()
    dataset = new_powerbi_dataset(
        workspace,
        {"id": "ds", "name": "Sales", "sensitivityLabel": {"labelId": "dataset-label"}},
    )
    reports = new_powerbi_reports(
        workspace,
        [
            {
                "id": "report",
                "name": "Sales report",
                "reportType": "PowerBIReport",
                "sensitivityLabel": {"labelId": "report-label"},
            }
        ],
    )
    dashboards = new_powerbi_dashboards(
        workspace,
        [
            {
                "id": "dashboard",
                "displayName": "Sales dashboard",
                "sensitivityLabel": {"id": "dashboard-label"},
            }
        ],
    )

    assert dataset.sensitivity_label_id == "dataset-label"
    assert reports[0].sensitivity_label_id == "report-label"
    assert dashboards[0].sensitivity_label_id == "dashboard-label"


def test_sensitivity_label_filter_escapes_quotes() -> None:
    assert sensitivity_label_filter("label-guid") == "id in ('label-guid')"
    assert sensitivity_label_filter("o'brien") == "id in ('o''brien')"


def test_graph_label_url_follows_powerbi_environment() -> None:
    commercial = DataResolverBase.urls_for(PowerBiEnvironment.COMMERCIAL)
    government = DataResolverBase.urls_for(PowerBiEnvironment.GOVERNMENT)

    assert commercial["GRAPH_SCOPE"] == "https://graph.microsoft.com/.default"
    assert commercial["GRAPH_LABELS_URL"] == (
        "https://graph.microsoft.com/v1.0/security/"
        "dataSecurityAndGovernance/sensitivityLabels"
    )
    assert government["GRAPH_SCOPE"] == "https://graph.microsoft.us/.default"
    assert government["GRAPH_LABELS_URL"].startswith("https://graph.microsoft.us/")


def test_resolver_requests_graph_filter_and_caches() -> None:
    session = MagicMock()
    session.get.return_value = _json_response(
        {
            "value": [
                {
                    "id": "label-guid",
                    "name": "Confidential",
                    "description": "Secret data",
                }
            ]
        }
    )
    header = MagicMock(return_value={"Authorization": "Bearer token"})
    resolver = SensitivityLabelResolver(
        session=session,
        authorization_header=header,
        labels_url="https://graph.example/labels",
    )

    first = resolver.resolve("label-guid")
    second = resolver.resolve("label-guid")

    assert first == second
    assert first == SensitivityLabel(
        id="label-guid", name="Confidential", description="Secret data"
    )
    assert session.get.call_count == 1
    assert session.get.call_args.args[0] == "https://graph.example/labels"
    assert session.get.call_args.kwargs["params"] == {"$filter": "id in ('label-guid')"}
    assert session.get.call_args.kwargs["headers"]["Authorization"] == "Bearer token"
    header.assert_called_once_with()


def test_resolver_caches_a_failed_lookup() -> None:
    session = MagicMock()
    response = _json_response({})
    response.raise_for_status.side_effect = requests.HTTPError("403")
    session.get.return_value = response
    resolver = SensitivityLabelResolver(
        session=session,
        authorization_header=lambda: {"Authorization": "Bearer token"},
        labels_url="https://graph.example/labels",
    )

    assert resolver.resolve("label-guid") is None
    assert resolver.resolve("label-guid") is None
    assert session.get.call_count == 1


def test_emitter_tag_merges_with_endorsements_and_emits_tag_once() -> None:
    emitter = _emitter(
        "tag",
        SensitivityLabel(
            id="label-guid", name="Confidential", description="Secret data"
        ),
    )

    first = emitter.contributions(
        "urn:li:dataset:(urn:li:dataPlatform:powerbi,sales,PROD)",
        "label-guid",
        ["Certified"],
    )
    second = emitter.contributions(
        "urn:li:dashboard:(powerbi,reports.1)",
        "label-guid",
        [],
    )

    tag_mcps = [mcp for mcp in first if isinstance(mcp.aspect, TagPropertiesClass)]
    assert len(tag_mcps) == 1
    assert tag_mcps[0].aspect.name == "Confidential"
    assert tag_mcps[0].aspect.description == "Secret data"
    tags = next(mcp.aspect for mcp in first if isinstance(mcp.aspect, GlobalTagsClass))
    assert [association.tag for association in tags.tags] == [
        "urn:li:tag:Certified",
        "urn:li:tag:Confidential",
    ]
    assert not any(isinstance(mcp.aspect, TagPropertiesClass) for mcp in second)
    second_tags = next(
        mcp.aspect for mcp in second if isinstance(mcp.aspect, GlobalTagsClass)
    )
    assert [association.tag for association in second_tags.tags] == [
        "urn:li:tag:Confidential"
    ]


def test_emitter_glossary_term_uses_label_description() -> None:
    emitter = _emitter(
        "glossary_term",
        SensitivityLabel(
            id="label-guid", name="Confidential", description="Secret data"
        ),
    )

    created = emitter.contributions(
        "urn:li:dashboard:(powerbi,reports.1)", "label-guid", ["Certified"]
    )
    again = emitter.contributions(
        "urn:li:dashboard:(powerbi,reports.2)", "label-guid", []
    )

    info = next(mcp for mcp in created if isinstance(mcp.aspect, GlossaryTermInfoClass))
    assert info.aspect.name == "Confidential"
    assert info.aspect.definition == "Secret data"
    term = GlossaryTerm(
        id="label-guid", display_name="Confidential", definition="Secret data"
    )
    association = next(
        mcp.aspect for mcp in created if isinstance(mcp.aspect, GlossaryTermsClass)
    )
    assert association.terms[0].urn == str(term.urn)
    endorsements = next(
        mcp.aspect for mcp in created if isinstance(mcp.aspect, GlobalTagsClass)
    )
    assert [tag.tag for tag in endorsements.tags] == ["urn:li:tag:Certified"]
    assert not any(isinstance(mcp.aspect, GlossaryTermInfoClass) for mcp in again)
    assert any(isinstance(mcp.aspect, GlossaryTermsClass) for mcp in again)


def test_disabled_emitter_keeps_endorsement_tags() -> None:
    emitter = SensitivityLabelEmitter.disabled()

    mcps = emitter.contributions(
        "urn:li:dashboard:(powerbi,dashboards.1)", None, ["Promoted"]
    )

    assert len(mcps) == 1
    assert isinstance(mcps[0].aspect, GlobalTagsClass)
    assert mcps[0].aspect.tags[0].tag == "urn:li:tag:Promoted"


def test_mapper_writes_one_global_tags_aspect() -> None:
    config = PowerBiDashboardSourceConfig.model_validate(
        {
            "client_id": "foo",
            "client_secret": "bar",
            "tenant_id": "tenant",
            "extract_endorsements_to_tags": True,
            "extract_sensitivity_label": {"enabled": True, "output": "tag"},
        }
    )
    emitter = _emitter(
        "tag",
        SensitivityLabel(id="label-guid", name="Confidential", description=None),
    )
    mapper = Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
        labels=emitter,
    )
    mcps: list = []

    mapper.append_tag_mcp(
        mcps,
        "urn:li:dashboard:(powerbi,reports.1)",
        "dashboard",
        ["Certified"],
        sensitivity_label_id="label-guid",
    )

    global_tags = [mcp for mcp in mcps if isinstance(mcp.aspect, GlobalTagsClass)]
    assert len(global_tags) == 1
    assert [association.tag for association in global_tags[0].aspect.tags] == [
        "urn:li:tag:Certified",
        "urn:li:tag:Confidential",
    ]


def test_config_rejects_unknown_output() -> None:
    with pytest.raises(ValidationError):
        PowerBiDashboardSourceConfig.model_validate(
            {
                "client_id": "foo",
                "client_secret": "bar",
                "tenant_id": "tenant",
                "extract_sensitivity_label": {"output": "property"},
            }
        )
