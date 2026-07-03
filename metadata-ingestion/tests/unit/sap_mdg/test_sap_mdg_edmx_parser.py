import pathlib

import pytest
from defusedxml.common import DefusedXmlException

from datahub.ingestion.source.sap_mdg.edmx_parser import parse_metadata
from datahub.ingestion.source.sap_mdg.models import ODataVersion

_V4_FIXTURE = (
    pathlib.Path(__file__).parents[2]
    / "integration"
    / "sap_mdg"
    / "setup"
    / "ZMDG_DEMO_SRV.xml"
)

# OData V2 metadata declares navigation targets and referential constraints via a
# separate Association element rather than inline on the navigation property.
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


def test_parse_v4_entity_types_and_keys():
    metadata = parse_metadata(_V4_FIXTURE.read_bytes())

    assert metadata.version == ODataVersion.V4
    types = metadata.entity_types_by_fqn()
    partner = types["com.example.mdg.BusinessPartner"]
    assert partner.key_property_names == ["BusinessPartnerId"]
    assert partner.label == "Business Partner"

    by_name = {prop.name: prop for prop in partner.properties}
    assert by_name["BusinessPartnerId"].nullable is False
    assert by_name["BusinessPartnerId"].max_length == 10
    assert by_name["Name"].description == "Name"
    assert by_name["CreatedAt"].type_name == "Edm.DateTimeOffset"


def test_parse_v4_navigation_referential_constraint():
    metadata = parse_metadata(_V4_FIXTURE.read_bytes())
    address = metadata.entity_types_by_fqn()["com.example.mdg.Address"]

    assert len(address.navigation_properties) == 1
    navigation = address.navigation_properties[0]
    assert navigation.target_type_fqn == "com.example.mdg.BusinessPartner"
    assert len(navigation.referential_constraints) == 1
    constraint = navigation.referential_constraints[0]
    assert constraint.dependent_property == "BusinessPartnerId"
    assert constraint.principal_property == "BusinessPartnerId"


def test_parse_v4_entity_sets():
    metadata = parse_metadata(_V4_FIXTURE.read_bytes())
    sets_by_type = metadata.entity_sets_by_type_fqn()

    address_set = sets_by_type["com.example.mdg.Address"]
    assert address_set.name == "Addresses"
    assert address_set.container_namespace == "com.example.mdg"


def test_parse_v2_association_resolves_navigation_target():
    metadata = parse_metadata(_V2_METADATA)

    assert metadata.version == ODataVersion.V2
    contact = metadata.entity_types_by_fqn()["demo.Contact"]
    navigation = contact.navigation_properties[0]
    # V2 targets are not inline; they resolve through the association.
    assert navigation.target_type_fqn is None
    assert navigation.relationship == "demo.Contact_Partner"

    association = metadata.associations_by_fqn()["demo.Contact_Partner"]
    partner_end = next(end for end in association.ends if end.role == "PartnerRole")
    assert partner_end.type_fqn == "demo.Partner"
    constraint = association.referential_constraints[0]
    assert constraint.dependent_property == "PartnerId"
    assert constraint.principal_property == "PartnerId"


def test_nullable_defaults_to_true_when_absent():
    contact = parse_metadata(_V2_METADATA).entity_types_by_fqn()["demo.Contact"]
    by_name = {prop.name: prop for prop in contact.properties}
    # PartnerId declares no Nullable attribute, so it defaults to nullable.
    assert by_name["PartnerId"].nullable is True


def test_version_unknown_without_edm_namespace():
    metadata = parse_metadata(
        b'<?xml version="1.0"?><root xmlns="urn:example:not-edm"><child/></root>'
    )
    assert metadata.version == ODataVersion.UNKNOWN


def test_collection_navigation_type_is_unwrapped():
    content = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="demo" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Partner">
        <NavigationProperty Name="Contacts" Type="Collection(demo.Contact)"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    partner = parse_metadata(content).entity_types_by_fqn()["demo.Partner"]
    assert partner.navigation_properties[0].target_type_fqn == "demo.Contact"


def test_non_numeric_max_length_is_ignored():
    content = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="demo" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Doc">
        <Property Name="Body" Type="Edm.String" MaxLength="Max"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    doc = parse_metadata(content).entity_types_by_fqn()["demo.Doc"]
    assert doc.properties[0].max_length is None


def test_quickinfo_used_as_description_when_label_absent():
    content = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx"
           xmlns:sap="http://www.sap.com/Protocols/SAPData">
  <edmx:DataServices>
    <Schema Namespace="demo" xmlns="http://schemas.microsoft.com/ado/2008/09/edm">
      <EntityType Name="Doc">
        <Property Name="Body" Type="Edm.String" sap:quickinfo="Long text"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    doc = parse_metadata(content).entity_types_by_fqn()["demo.Doc"]
    assert doc.properties[0].description == "Long text"


def test_defusedxml_rejects_entity_expansion():
    # Guards the security hardening: a billion-laughs style payload must not be
    # expanded (defusedxml raises instead of exhausting memory).
    malicious = b"""<?xml version="1.0"?>
<!DOCTYPE lolz [
  <!ENTITY lol "lol">
  <!ENTITY lol2 "&lol;&lol;&lol;&lol;">
  <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;">
]>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">&lol3;</edmx:Edmx>
"""
    with pytest.raises(DefusedXmlException):
        parse_metadata(malicious)
