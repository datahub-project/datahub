from typing import List

import pytest
import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sap_mdg.config import SapMdgSourceConfig
from datahub.ingestion.source.sap_mdg.models import (
    ODataAssociation,
    ODataAssociationEnd,
    ODataNavigationProperty,
    ODataReferentialConstraint,
)
from datahub.ingestion.source.sap_mdg.odata_client import SapMdgODataClient
from datahub.ingestion.source.sap_mdg.source import SapMdgSource

_SERVICE = "/sap/opu/odata/sap/ZMDG_DEMO_SRV"

# V2 metadata: Contact -> Partner via an Association-backed navigation (foreign
# key), plus an Orphan entity type that no entity set exposes.
_V2_METADATA = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx"
           xmlns:sap="http://www.sap.com/Protocols/SAPData">
  <edmx:DataServices>
    <Schema Namespace="demo" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
      <EntityType Name="Partner">
        <Key><PropertyRef Name="PartnerId"/></Key>
        <Property Name="PartnerId" Type="Edm.String" Nullable="false" sap:label="Partner"/>
      </EntityType>
      <EntityType Name="Contact">
        <Key><PropertyRef Name="ContactId"/></Key>
        <Property Name="ContactId" Type="Edm.String" Nullable="false"/>
        <Property Name="PartnerId" Type="Edm.String"/>
        <NavigationProperty Name="Partner" Relationship="demo.Contact_Partner"
                            FromRole="ContactRole" ToRole="PartnerRole"/>
      </EntityType>
      <EntityType Name="Orphan">
        <Key><PropertyRef Name="OrphanId"/></Key>
        <Property Name="OrphanId" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <Association Name="Contact_Partner">
        <End Role="PartnerRole" Type="demo.Partner" Multiplicity="1"/>
        <End Role="ContactRole" Type="demo.Contact" Multiplicity="*"/>
        <ReferentialConstraint>
          <Principal Role="PartnerRole"><PropertyRef Name="PartnerId"/></Principal>
          <Dependent Role="ContactRole"><PropertyRef Name="PartnerId"/></Dependent>
        </ReferentialConstraint>
      </Association>
      <EntityContainer Name="Container">
        <EntitySet Name="Partners" EntityType="demo.Partner"/>
        <EntitySet Name="Contacts" EntityType="demo.Contact"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

# One entity set points at a type that is not declared in the schema.
_V2_UNKNOWN_TYPE = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">
  <edmx:DataServices>
    <Schema Namespace="demo" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
      <EntityType Name="Partner">
        <Key><PropertyRef Name="PartnerId"/></Key>
        <Property Name="PartnerId" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Partners" EntityType="demo.Partner"/>
        <EntitySet Name="Ghosts" EntityType="demo.Missing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


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


def _run(source: SapMdgSource, content: bytes = _V2_METADATA) -> None:
    source.client.fetch_metadata = lambda service: content  # type: ignore[method-assign]
    list(source.get_workunits_internal())


def test_emits_datasets_and_foreign_key_via_association():
    source = _source()
    _run(source)
    assert source.report.datasets_emitted == 2
    assert source.report.foreign_keys_emitted == 1
    assert source.report.foreign_keys_unresolved == 0


def test_include_foreign_keys_disabled_skips_constraints():
    source = _source(include_foreign_keys=False)
    _run(source)
    assert source.report.datasets_emitted == 2
    assert source.report.foreign_keys_emitted == 0


def test_entity_set_pattern_filters_sets():
    source = _source(entity_set_pattern={"deny": ["Contacts"]})
    _run(source)
    assert source.report.datasets_emitted == 1
    assert "Contacts" in list(source.report.filtered_entity_sets)


def test_orphan_entity_types_emitted_when_enabled():
    source = _source(emit_entity_types_without_sets=True)
    _run(source)
    # Partners + Contacts entity sets, plus the Orphan entity type.
    assert source.report.datasets_emitted == 3


def test_unknown_entity_type_is_skipped():
    source = _source()
    _run(source, content=_V2_UNKNOWN_TYPE)
    # Only the resolvable Partners set becomes a dataset; the ghost set is skipped.
    assert source.report.datasets_emitted == 1
    assert source.report.warnings


def test_service_fetch_failure_is_reported_and_non_fatal():
    source = _source()

    def _raise(service: str) -> bytes:
        raise requests.exceptions.ConnectionError("boom")

    source.client.fetch_metadata = _raise  # type: ignore[method-assign]
    list(source.get_workunits_internal())
    assert source.report.services_failed == 1
    assert source.report.datasets_emitted == 0


def test_service_id_strips_slashes_and_takes_last_segment():
    source = _source()
    assert source._service_id("/sap/opu/odata/sap/ZMDG_DEMO_SRV/") == "ZMDG_DEMO_SRV"


def test_entity_set_dataset_name_is_namespace_qualified():
    from datahub.ingestion.source.sap_mdg.models import ODataEntitySet

    entity_set = ODataEntitySet(
        name="Partners",
        entity_type_fqn="demo.Partner",
        container_namespace="demo",
    )
    assert _source()._entity_set_dataset_name(entity_set) == "demo.Partners"


def test_unknown_edm_type_falls_back_to_record():
    from datahub.metadata.schema_classes import RecordTypeClass, StringTypeClass

    source = _source()
    assert isinstance(source._resolve_field_type("Edm.String").type, StringTypeClass)
    assert isinstance(
        source._resolve_field_type("demo.ComplexType").type, RecordTypeClass
    )


def test_resolve_navigation_v4_inline_target():
    navigation = ODataNavigationProperty(
        name="Partner",
        target_type_fqn="demo.Partner",
        referential_constraints=[
            ODataReferentialConstraint(
                dependent_property="PartnerId", principal_property="PartnerId"
            )
        ],
    )
    target = _source()._resolve_navigation(navigation, {})
    assert target.target_type_fqn == "demo.Partner"
    assert len(target.referential_constraints) == 1


def test_resolve_navigation_v2_via_association():
    navigation = ODataNavigationProperty(
        name="Partner",
        relationship="demo.Contact_Partner",
        to_role="PartnerRole",
    )
    association = ODataAssociation(
        namespace="demo",
        name="Contact_Partner",
        ends=[
            ODataAssociationEnd(role="PartnerRole", type_fqn="demo.Partner"),
            ODataAssociationEnd(role="ContactRole", type_fqn="demo.Contact"),
        ],
        referential_constraints=[
            ODataReferentialConstraint(
                dependent_property="PartnerId", principal_property="PartnerId"
            )
        ],
    )
    target = _source()._resolve_navigation(
        navigation, {"demo.Contact_Partner": association}
    )
    assert target.target_type_fqn == "demo.Partner"
    assert target.referential_constraints[0].dependent_property == "PartnerId"


@pytest.mark.parametrize(
    "navigation",
    [
        ODataNavigationProperty(name="X"),
        ODataNavigationProperty(name="X", relationship="demo.Missing", to_role="R"),
    ],
)
def test_resolve_navigation_returns_empty_when_unresolvable(
    navigation: ODataNavigationProperty,
) -> None:
    target = _source()._resolve_navigation(navigation, {})
    assert target.target_type_fqn is None
    assert target.referential_constraints == []


def test_test_connection_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SapMdgODataClient, "test_connection", lambda self: None)
    report = SapMdgSource.test_connection(
        {
            "base_url": "https://sap-gw.example.com:44300",
            "services": [_SERVICE],
            "token": "abc",
        }
    )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True


def test_test_connection_request_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(self: SapMdgODataClient) -> None:
        raise requests.exceptions.ConnectionError("refused")

    monkeypatch.setattr(SapMdgODataClient, "test_connection", _raise)
    report = SapMdgSource.test_connection(
        {
            "base_url": "https://sap-gw.example.com:44300",
            "services": [_SERVICE],
            "token": "abc",
        }
    )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False


def test_test_connection_invalid_config_is_not_capable():
    # Missing credentials fails config validation before any network call.
    report = SapMdgSource.test_connection(
        {"base_url": "https://x", "services": [_SERVICE]}
    )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False


def test_build_foreign_keys_unresolved_without_constraints():
    from datahub.ingestion.source.sap_mdg.models import ODataEntityType

    source = _source()
    entity_type = ODataEntityType(
        namespace="demo",
        name="Contact",
        navigation_properties=[ODataNavigationProperty(name="Partner")],
    )
    keys: List = source._build_foreign_keys(
        entity_type,
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:sap-mdg,demo.Contacts,PROD)",
        associations_by_fqn={},
        sets_by_type_fqn={},
    )
    assert keys == []
    assert source.report.foreign_keys_unresolved == 1
