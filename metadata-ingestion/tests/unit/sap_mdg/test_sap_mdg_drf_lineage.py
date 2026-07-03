from typing import Dict, List, Optional

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sap_mdg.config import SapMdgSourceConfig
from datahub.ingestion.source.sap_mdg.models import DrfDistribution
from datahub.ingestion.source.sap_mdg.odata_client import SapMdgODataClient
from datahub.ingestion.source.sap_mdg.report import SapMdgSourceReport
from datahub.ingestion.source.sap_mdg.source import SapMdgSource
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

_SERVICE = "/sap/opu/odata/sap/ZMDG_DEMO_SRV"

_METADATA = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">
  <edmx:DataServices>
    <Schema Namespace="demo" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
      <EntityType Name="Partner">
        <Key><PropertyRef Name="PartnerId"/></Key>
        <Property Name="PartnerId" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <EntityType Name="Contact">
        <Key><PropertyRef Name="ContactId"/></Key>
        <Property Name="ContactId" Type="Edm.String" Nullable="false"/>
        <Property Name="PartnerId" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Partners" EntityType="demo.Partner"/>
        <EntitySet Name="Contacts" EntityType="demo.Contact"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


class _FakeGraph:
    # Minimal stand-in for the DataHub graph exposing only get_schema_metadata,
    # which is all the column-lineage matcher reads.
    def __init__(self, field_names: List[str]) -> None:
        self._field_names = field_names

    def get_schema_metadata(self, entity_urn: str) -> Optional[SchemaMetadataClass]:
        return SchemaMetadataClass(
            schemaName="t",
            platform="urn:li:dataPlatform:hana",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath=name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="Edm.String",
                )
                for name in self._field_names
            ],
        )


def _source(**overrides: object) -> SapMdgSource:
    config = SapMdgSourceConfig.model_validate(
        {
            "base_url": "https://sap-gw.example.com:44300",
            "services": [_SERVICE],
            "token": "abc",
            "env": "PROD",
            **overrides,
        }
    )
    return SapMdgSource(config, PipelineContext(run_id="test"))


def _workunits(source: SapMdgSource) -> List[MetadataWorkUnit]:
    source.client.fetch_metadata = lambda service: _METADATA  # type: ignore[method-assign]
    return list(source.get_workunits_internal())


def _upstream_patches(workunits: List[MetadataWorkUnit]) -> List[MetadataWorkUnit]:
    return [
        wu
        for wu in workunits
        if getattr(wu.metadata, "aspectName", None) == "upstreamLineage"
    ]


def test_drf_distribution_pivots_active_models_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SapMdgODataClient(
        SapMdgSourceConfig.model_validate(
            {
                "base_url": "https://sap-gw.example.com:44300",
                "services": [_SERVICE],
                "token": "abc",
                "drf": {"enabled": True, "table_read_service": "/z/TABLE_SRV"},
            }
        ),
        SapMdgSourceReport(),
    )

    rows: Dict[str, List[Dict[str, object]]] = {
        "DRFC_APPL": [
            {"APPL": "BP_OUT", "USMD_MODEL": "BP", "ACTIVE": "X"},
            {"APPL": "MAT_OUT", "USMD_MODEL": "MM", "ACTIVE": ""},
        ],
        "DRFC_APPL_SYS": [
            {"APPL": "BP_OUT", "BUSINESS_SYSTEM": "SYS_A"},
            {"APPL": "BP_OUT", "BUSINESS_SYSTEM": "SYS_A"},
            {"APPL": "BP_OUT", "BUSINESS_SYSTEM": "SYS_B"},
            {"APPL": "MAT_OUT", "BUSINESS_SYSTEM": "SYS_C"},
        ],
    }
    monkeypatch.setattr(client, "_fetch_rows", lambda entity_set: rows[entity_set])

    distribution = client.fetch_drf_distribution()
    # Inactive model MM is excluded; SYS_A is de-duplicated and order preserved.
    assert distribution.targets_by_data_model == {"BP": ["SYS_A", "SYS_B"]}


def test_extract_rows_handles_v2_and_v4_envelopes() -> None:
    assert SapMdgODataClient._extract_rows({"value": [{"a": 1}]}) == [{"a": 1}]
    assert SapMdgODataClient._extract_rows({"d": {"results": [{"a": 1}]}}) == [{"a": 1}]
    assert SapMdgODataClient._extract_rows({"d": [{"a": 1}]}) == [{"a": 1}]
    assert SapMdgODataClient._extract_rows({"unexpected": 1}) == []


def test_emits_upstream_lineage_onto_downstream_target() -> None:
    source = _source(logical_system_to_platform={"SYS_A": {"platform": "hana"}})
    source.config.drf.service_to_data_model = {"ZMDG_DEMO_SRV": "BP"}
    source._drf_distribution = DrfDistribution(targets_by_data_model={"BP": ["SYS_A"]})

    patches = _upstream_patches(_workunits(source))

    # One edge per entity set (Partners, Contacts) to the single target system.
    assert source.report.lineage_edges_emitted == 2
    downstream_urns = {getattr(wu.metadata, "entityUrn", None) for wu in patches}
    assert (
        make_dataset_urn_with_platform_instance(
            platform="hana", name="demo.contacts", platform_instance=None, env="PROD"
        )
        in downstream_urns
    )


def test_unresolved_target_system_is_reported() -> None:
    source = _source()  # no logical_system_to_platform mapping for SYS_X
    source.config.drf.service_to_data_model = {"ZMDG_DEMO_SRV": "BP"}
    source._drf_distribution = DrfDistribution(targets_by_data_model={"BP": ["SYS_X"]})

    patches = _upstream_patches(_workunits(source))

    assert patches == []
    assert source.report.lineage_edges_emitted == 0
    assert "SYS_X" in list(source.report.unresolved_target_systems)


def test_no_lineage_when_service_has_no_data_model_mapping() -> None:
    source = _source(logical_system_to_platform={"SYS_A": {"platform": "hana"}})
    source._drf_distribution = DrfDistribution(targets_by_data_model={"BP": ["SYS_A"]})
    # service_to_data_model left empty: the service maps to no data model.

    assert _upstream_patches(_workunits(source)) == []
    assert source.report.lineage_edges_emitted == 0


def test_column_lineage_matches_downstream_fields_by_name() -> None:
    source = _source(logical_system_to_platform={"SYS_A": {"platform": "hana"}})
    source.config.drf.service_to_data_model = {"ZMDG_DEMO_SRV": "BP"}
    source.config.drf.emit_column_lineage = True
    source._drf_distribution = DrfDistribution(targets_by_data_model={"BP": ["SYS_A"]})
    # Downstream has only PartnerId in common with the MDG entities.
    source.ctx.graph = _FakeGraph(["PartnerId", "unrelated_col"])  # type: ignore[assignment]

    _workunits(source)

    # Partners.PartnerId and Contacts.PartnerId both match -> two field edges.
    assert source.report.column_lineage_edges_emitted == 2


def test_column_lineage_skipped_without_graph() -> None:
    source = _source(logical_system_to_platform={"SYS_A": {"platform": "hana"}})
    source.config.drf.service_to_data_model = {"ZMDG_DEMO_SRV": "BP"}
    source.config.drf.emit_column_lineage = True
    source._drf_distribution = DrfDistribution(targets_by_data_model={"BP": ["SYS_A"]})
    # ctx.graph is None -> no schema to match against, dataset lineage still emitted.

    assert source.report.lineage_edges_emitted == 0  # not run yet
    _workunits(source)
    assert source.report.lineage_edges_emitted == 2
    assert source.report.column_lineage_edges_emitted == 0


def test_config_requires_table_read_service_when_drf_enabled() -> None:
    with pytest.raises(ValueError):
        SapMdgSourceConfig.model_validate(
            {
                "base_url": "https://x",
                "services": [_SERVICE],
                "token": "abc",
                "drf": {"enabled": True},
            }
        )
