import pytest
from defusedxml.common import DefusedXmlException
from pydantic import ValidationError

from datahub.ingestion.source.boomi.component_parser import parse_process_component
from datahub.ingestion.source.boomi.models import ComponentMetadata, QueryResponse

EXPORT_STYLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<bns:Component xmlns:bns="http://api.platform.boomi.com/" componentId="p1" folderFullPath="Acme/Production/Orders">
  <bns:encryptedValues/>
  <bns:description>Copies orders into the ERP.</bns:description>
  <bns:object>
    <process xmlns="">
      <description>shape-level text that must not be picked up</description>
    </process>
  </bns:object>
</bns:Component>
"""

DOCS_STYLE_XML = """<Component xmlns="http://api.platform.boomi.com/" componentId="p2" folderFullPath="Acme/Finance">
  <description>  Exports invoices.  </description>
  <object><process/></object>
</Component>
"""


def test_parse_export_with_empty_default_namespace_children() -> None:
    info = parse_process_component(EXPORT_STYLE_XML)
    assert info.description == "Copies orders into the ERP."
    assert info.folder_full_path == "Acme/Production/Orders"


def test_parse_default_namespace_variant() -> None:
    info = parse_process_component(DOCS_STYLE_XML)
    assert info.description == "Exports invoices."
    assert info.folder_full_path == "Acme/Finance"


def test_parse_without_description() -> None:
    xml = '<bns:Component xmlns:bns="http://api.platform.boomi.com/"><bns:description/></bns:Component>'
    info = parse_process_component(xml)
    assert info.description is None
    assert info.folder_full_path is None


def test_parse_rejects_entity_expansion() -> None:
    xml = (
        '<!DOCTYPE c [<!ENTITY e "boom">]>'
        '<bns:Component xmlns:bns="http://api.platform.boomi.com/">&e;</bns:Component>'
    )
    with pytest.raises(DefusedXmlException):
        parse_process_component(xml)


@pytest.mark.parametrize("raw", [["Long", 12], 12, "12"])
def test_long_values_accept_tagged_and_plain_forms(raw: object) -> None:
    component = ComponentMetadata.model_validate(
        {"componentId": "p1", "name": "Orders Sync", "type": "process", "version": raw}
    )
    assert component.version == 12


def test_long_rejects_other_tagged_lists() -> None:
    with pytest.raises(ValidationError):
        QueryResponse.model_validate({"numberOfResults": ["Int", 3]})
