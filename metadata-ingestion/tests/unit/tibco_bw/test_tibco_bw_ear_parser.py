import io
import zipfile
from pathlib import Path
from typing import Dict, Optional

from datahub.ingestion.source.tibco_bw.ear_parser import parse_archives
from datahub.ingestion.source.tibco_bw.models import JmsDestinationType
from datahub.ingestion.source.tibco_bw.report import TibcoBwSourceReport

# A JMS activity is found by its type id rather than by element path, because BW5,
# BW6 and BWCE wrap it in different extension elements. These fixtures use the BW6
# shape; a version that nests the config one level deeper is covered separately.
_SEND_ACTIVITY = """<?xml version="1.0" encoding="UTF-8"?>
<process xmlns="http://docs.oasis-open.org/wsbpel/2.0/process/executable"
         xmlns:tibex="http://www.tibco.com/bpel/2007/extensions"
         xmlns:ord="http://adidas.example/orders">
  <extensionActivity>
    <tibex:activityExtension activityTypeID="bw.jms.SendMessage"
                             destination="%%Orders.Queue%%"
                             destinationType="queue"
                             element="ord:OrderMessage"/>
  </extensionActivity>
</process>
"""

_ORDER_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="http://adidas.example/orders">
  <xs:element name="OrderMessage">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="orderId" type="xs:string"/>
        <xs:element name="grossAmount" type="xs:decimal"/>
        <xs:element name="placedAt" type="xs:dateTime" minOccurs="0"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""

_SUBSTVAR = """<?xml version="1.0" encoding="UTF-8"?>
<repository>
  <globalVariables>
    <globalVariable><name>Orders.Queue</name><value>orders.new</value></globalVariable>
  </globalVariables>
</repository>
"""


def _ear(
    tmp_path: Path,
    name: str = "OrderPublisher_1.2.0.ear",
    entries: Optional[Dict[str, str]] = None,
    nested: bool = True,
) -> str:
    """Build an EAR the way BW packages one: module bundles nested inside the ear."""
    contents = (
        entries
        if entries is not None
        else {
            "Processes/orders/Publish.bwp": _SEND_ACTIVITY,
            "Schemas/Order.xsd": _ORDER_XSD,
            "META-INF/default.substvar": _SUBSTVAR,
        }
    )
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as ear:
        if not nested:
            for entry_name, text in contents.items():
                ear.writestr(entry_name, text)
            return str(path)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as module:
            for entry_name, text in contents.items():
                module.writestr(entry_name, text)
        ear.writestr("OrderPublisher.module_1.0.0.jar", buffer.getvalue())
    return str(path)


def _parse(path: str) -> tuple:
    report = TibcoBwSourceReport()
    return parse_archives([path], report), report


def test_declared_schema_is_read_from_a_nested_module_bundle(tmp_path: Path) -> None:
    # An EAR is a zip of module jars, so the processes are a level down from the
    # archive root; a parser that only reads the top level finds nothing.
    schemas, report = _parse(_ear(tmp_path))
    assert len(schemas) == 1
    schema = schemas[0]
    assert [f.path for f in schema.fields] == ["orderId", "grossAmount", "placedAt"]
    assert report.archives_read == 1
    assert report.jms_activities_found == 1


def test_destination_property_reference_is_resolved(tmp_path: Path) -> None:
    # A destination is more often a %%module property%% than a literal.
    schemas, _ = _parse(_ear(tmp_path))
    assert schemas[0].destination_name == "orders.new"
    assert schemas[0].destination_type is JmsDestinationType.QUEUE


def test_unresolvable_destination_is_skipped_not_guessed(tmp_path: Path) -> None:
    # Emitting the schema against a name with a %%placeholder%% left in it would
    # attach the contract to a topic that does not exist.
    schemas, report = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Publish.bwp": _SEND_ACTIVITY,
                "Schemas/Order.xsd": _ORDER_XSD,
            },
        )
    )
    assert schemas == []
    assert report.unresolved_destinations


def test_field_types_and_optionality_come_from_the_xsd(tmp_path: Path) -> None:
    schemas, _ = _parse(_ear(tmp_path))
    by_path = {f.path: f for f in schemas[0].fields}
    assert by_path["grossAmount"].xsd_type == "decimal"
    assert by_path["orderId"].nullable is False
    assert by_path["placedAt"].nullable is True


def test_nested_elements_flatten_to_dotted_paths(tmp_path: Path) -> None:
    nested_xsd = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="OrderMessage">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="orderId" type="xs:string"/>
        <xs:element name="customer">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="email" type="xs:string"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""
    schemas, _ = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Publish.bwp": _SEND_ACTIVITY,
                "Schemas/Order.xsd": nested_xsd,
                "META-INF/default.substvar": _SUBSTVAR,
            },
        )
    )
    assert [f.path for f in schemas[0].fields] == ["orderId", "customer.email"]


def test_named_complex_type_is_resolved(tmp_path: Path) -> None:
    # An element declared by reference to a named type, rather than inline.
    named_xsd = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="OrderMessage" type="OrderType"/>
  <xs:complexType name="OrderType">
    <xs:sequence>
      <xs:element name="orderId" type="xs:string"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>
"""
    schemas, _ = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Publish.bwp": _SEND_ACTIVITY,
                "Schemas/Order.xsd": named_xsd,
                "META-INF/default.substvar": _SUBSTVAR,
            },
        )
    )
    assert [f.path for f in schemas[0].fields] == ["orderId"]


def test_recursive_type_terminates(tmp_path: Path) -> None:
    # An order line that contains order lines would otherwise flatten forever.
    recursive_xsd = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="OrderMessage" type="NodeType"/>
  <xs:complexType name="NodeType">
    <xs:sequence>
      <xs:element name="id" type="xs:string"/>
      <xs:element name="child" type="NodeType"/>
    </xs:sequence>
  </xs:complexType>
</xs:schema>
"""
    schemas, _ = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Publish.bwp": _SEND_ACTIVITY,
                "Schemas/Order.xsd": recursive_xsd,
                "META-INF/default.substvar": _SUBSTVAR,
            },
        )
    )
    assert schemas
    assert len(schemas[0].fields) < 40


def test_consume_activity_is_marked_as_reading(tmp_path: Path) -> None:
    # A consumer's declared message is lineage, not the destination's contract.
    consume = _SEND_ACTIVITY.replace("bw.jms.SendMessage", "bw.jms.GetMessage")
    schemas, _ = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Consume.bwp": consume,
                "Schemas/Order.xsd": _ORDER_XSD,
                "META-INF/default.substvar": _SUBSTVAR,
            },
        )
    )
    assert schemas[0].publishes is False


def test_topic_destination_type_is_detected(tmp_path: Path) -> None:
    topic = _SEND_ACTIVITY.replace('destinationType="queue"', 'destinationType="topic"')
    schemas, _ = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Publish.bwp": topic,
                "Schemas/Order.xsd": _ORDER_XSD,
                "META-INF/default.substvar": _SUBSTVAR,
            },
        )
    )
    assert schemas[0].destination_type is JmsDestinationType.TOPIC


def test_activity_config_on_a_child_element_is_found(tmp_path: Path) -> None:
    # BW versions differ on whether the JMS configuration sits on the activity or
    # on a child of it; both have to resolve.
    child_config = """<?xml version="1.0" encoding="UTF-8"?>
<process xmlns:tibex="http://www.tibco.com/bpel/2007/extensions">
  <tibex:activityExtension activityTypeID="bw.jms.SendMessage">
    <config destination="orders.new" element="ord:OrderMessage"/>
  </tibex:activityExtension>
</process>
"""
    schemas, _ = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Publish.bwp": child_config,
                "Schemas/Order.xsd": _ORDER_XSD,
            },
        )
    )
    assert schemas[0].destination_name == "orders.new"


def test_unparseable_process_is_reported_and_the_rest_still_parse(
    tmp_path: Path,
) -> None:
    # One malformed process in a large estate must not cost the whole run.
    schemas, report = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Broken.bwp": "<process><unclosed>",
                "Processes/Publish.bwp": _SEND_ACTIVITY,
                "Schemas/Order.xsd": _ORDER_XSD,
                "META-INF/default.substvar": _SUBSTVAR,
            },
        )
    )
    assert len(schemas) == 1
    assert report.warnings


def test_element_with_no_matching_xsd_is_reported(tmp_path: Path) -> None:
    # Typically a schema imported from a module that was not packaged here.
    schemas, report = _parse(
        _ear(
            tmp_path,
            entries={
                "Processes/Publish.bwp": _SEND_ACTIVITY,
                "META-INF/default.substvar": _SUBSTVAR,
            },
        )
    )
    assert schemas == []
    assert report.unresolved_elements


def test_unreadable_archive_is_reported_rather_than_raising(tmp_path: Path) -> None:
    path = tmp_path / "not-an-archive.ear"
    path.write_text("this is not a zip")
    report = TibcoBwSourceReport()

    assert parse_archives([str(path)], report) == []
    assert report.warnings


def test_path_matching_nothing_is_reported(tmp_path: Path) -> None:
    report = TibcoBwSourceReport()

    assert parse_archives([str(tmp_path / "*.ear")], report) == []
    assert report.warnings


def test_flat_archive_without_module_bundles_still_parses(tmp_path: Path) -> None:
    schemas, _ = _parse(_ear(tmp_path, nested=False))
    assert len(schemas) == 1
