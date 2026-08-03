import io
import zipfile
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.tibco_bw.config import TibcoBwSourceConfig
from datahub.ingestion.source.tibco_bw.constants import (
    PROPERTY_SCHEMA_DECLARED_BY,
    PROPERTY_SCHEMA_SOURCE,
    SCHEMA_SOURCE_EAR,
)
from datahub.ingestion.source.tibco_bw.models import TibcoApplication, TibcoScope
from datahub.ingestion.source.tibco_bw.source import TibcoBwSource
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
)

_ORDERS_QUEUE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:tibco-ems,default.queue.orders.new,PROD)"
)

_PUBLISH = """<?xml version="1.0" encoding="UTF-8"?>
<process xmlns:tibex="http://www.tibco.com/bpel/2007/extensions">
  <tibex:activityExtension activityTypeID="bw.jms.SendMessage"
                           destination="orders.new"
                           destinationType="queue"
                           element="ord:OrderMessage"/>
</process>
"""

_CONSUME = """<?xml version="1.0" encoding="UTF-8"?>
<process xmlns:tibex="http://www.tibco.com/bpel/2007/extensions">
  <tibex:activityExtension activityTypeID="bw.jms.GetMessage"
                           destination="orders.new"
                           destinationType="queue"
                           element="ord:OrderMessage"/>
</process>
"""

_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="OrderMessage">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="orderId" type="xs:string"/>
        <xs:element name="grossAmount" type="xs:decimal"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""


def _ear(tmp_path: Path, name: str, entries: Dict[str, str]) -> str:
    path = tmp_path / name
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as module:
        for entry_name, text in entries.items():
            module.writestr(entry_name, text)
    with zipfile.ZipFile(path, "w") as ear:
        ear.writestr("module_1.0.0.jar", buffer.getvalue())
    return str(path)


def _publisher_ear(tmp_path: Path, name: str = "OrderPublisher_1.2.0.ear") -> str:
    return _ear(
        tmp_path,
        name,
        {"Processes/Publish.bwp": _PUBLISH, "Schemas/Order.xsd": _XSD},
    )


def _source(
    archives: List[str],
    application_lineage: Optional[Dict[str, object]] = None,
    **overrides: object,
) -> TibcoBwSource:
    config: Dict[str, object] = {
        "deployment": "on_prem",
        "base_url": "http://bw:8079",
        "username": "u",
        "password": "p",
        "application_archives": {"paths": archives, **overrides},
        "application_lineage": application_lineage or {},
    }
    return TibcoBwSource(
        TibcoBwSourceConfig.model_validate(config), PipelineContext(run_id="test")
    )


def _run(source: TibcoBwSource, applications: List[str]) -> list:
    source.client = MagicMock()
    source.client.fetch_scopes.return_value = [
        TibcoScope(
            id="D1/AS1",
            name="AS1",
            properties={"domain": "D1"},
            applications=[TibcoApplication(name=app) for app in applications],
        )
    ]
    return list(source.get_workunits_internal())


def _aspect(workunits: list, aspect_type: type, urn: str) -> Optional[object]:
    for workunit in workunits:
        if workunit.metadata.entityUrn != urn:
            continue
        if isinstance(workunit.metadata.aspect, aspect_type):
            return workunit.metadata.aspect
    return None


def test_declared_schema_lands_on_the_ems_destination(tmp_path: Path) -> None:
    # The destination urn has to match the one the TIBCO EMS source builds, or the
    # two connectors describe two different entities for one queue.
    workunits = _run(_source([_publisher_ear(tmp_path)]), ["OrderPublisher"])

    schema = _aspect(workunits, SchemaMetadataClass, _ORDERS_QUEUE_URN)
    assert isinstance(schema, SchemaMetadataClass)
    assert [f.fieldPath for f in schema.fields] == ["orderId", "grossAmount"]
    assert schema.fields[1].nativeDataType == "decimal"


def test_declared_schema_carries_its_provenance(tmp_path: Path) -> None:
    # Marked as declared so it is distinguishable from a schema the EMS source
    # estimated from downstream consumers.
    workunits = _run(_source([_publisher_ear(tmp_path)]), ["OrderPublisher"])

    properties = _aspect(workunits, DatasetPropertiesClass, _ORDERS_QUEUE_URN)
    assert isinstance(properties, DatasetPropertiesClass)
    assert properties.customProperties[PROPERTY_SCHEMA_SOURCE] == SCHEMA_SOURCE_EAR
    assert (
        properties.customProperties[PROPERTY_SCHEMA_DECLARED_BY]
        == "Processes/Publish.bwp"
    )


def test_schema_is_written_non_primary(tmp_path: Path) -> None:
    # The destination belongs to the EMS source; this connector only contributes
    # the declaration, so it must not participate in stale-entity removal here.
    workunits = _run(_source([_publisher_ear(tmp_path)]), ["OrderPublisher"])

    emitted = [w for w in workunits if w.metadata.entityUrn == _ORDERS_QUEUE_URN]
    assert emitted
    assert all(not w.is_primary_source for w in emitted)


def test_published_destination_becomes_an_application_outlet(tmp_path: Path) -> None:
    # The archive is named for the application it deploys, which is the only link
    # back to the running application the bwagent reports.
    workunits = _run(_source([_publisher_ear(tmp_path)]), ["OrderPublisher"])

    job_urn = "urn:li:dataJob:(urn:li:dataFlow:(tibco-bw,D1/AS1,PROD),OrderPublisher)"
    io_aspect = _aspect(workunits, DataJobInputOutputClass, job_urn)
    assert isinstance(io_aspect, DataJobInputOutputClass)
    assert io_aspect.outputDatasets == [_ORDERS_QUEUE_URN]
    assert io_aspect.inputDatasets == []


def test_consumed_destination_becomes_an_application_inlet(tmp_path: Path) -> None:
    archive = _ear(
        tmp_path,
        "OrderConsumer_1.0.0.ear",
        {"Processes/Consume.bwp": _CONSUME, "Schemas/Order.xsd": _XSD},
    )
    workunits = _run(_source([archive]), ["OrderConsumer"])

    job_urn = "urn:li:dataJob:(urn:li:dataFlow:(tibco-bw,D1/AS1,PROD),OrderConsumer)"
    io_aspect = _aspect(workunits, DataJobInputOutputClass, job_urn)
    assert isinstance(io_aspect, DataJobInputOutputClass)
    assert io_aspect.inputDatasets == [_ORDERS_QUEUE_URN]


def test_a_consumers_message_is_not_published_as_the_contract(tmp_path: Path) -> None:
    # What a consumer reads is lineage; only the publisher declares the shape.
    archive = _ear(
        tmp_path,
        "OrderConsumer_1.0.0.ear",
        {"Processes/Consume.bwp": _CONSUME, "Schemas/Order.xsd": _XSD},
    )
    workunits = _run(_source([archive]), ["OrderConsumer"])

    assert _aspect(workunits, SchemaMetadataClass, _ORDERS_QUEUE_URN) is None


def test_archive_lineage_adds_to_the_configured_map(tmp_path: Path) -> None:
    # An archive only knows its own JMS destinations; a database endpoint can only
    # be declared in config. Dropping either side would lose real edges.
    hana = "urn:li:dataset:(urn:li:dataPlatform:hana,SALES.ORDER_LINE,PROD)"
    source = _source(
        [_publisher_ear(tmp_path)],
        application_lineage={"OrderPublisher": {"upstreams": [hana]}},
    )
    workunits = _run(source, ["OrderPublisher"])

    job_urn = "urn:li:dataJob:(urn:li:dataFlow:(tibco-bw,D1/AS1,PROD),OrderPublisher)"
    io_aspect = _aspect(workunits, DataJobInputOutputClass, job_urn)
    assert isinstance(io_aspect, DataJobInputOutputClass)
    assert io_aspect.inputDatasets == [hana]
    assert io_aspect.outputDatasets == [_ORDERS_QUEUE_URN]


def test_versionless_archive_name_still_matches_the_application(
    tmp_path: Path,
) -> None:
    workunits = _run(
        _source([_publisher_ear(tmp_path, "OrderPublisher.ear")]), ["OrderPublisher"]
    )

    job_urn = "urn:li:dataJob:(urn:li:dataFlow:(tibco-bw,D1/AS1,PROD),OrderPublisher)"
    io_aspect = _aspect(workunits, DataJobInputOutputClass, job_urn)
    assert isinstance(io_aspect, DataJobInputOutputClass)
    assert io_aspect.outputDatasets == [_ORDERS_QUEUE_URN]


def test_server_group_leads_the_destination_name(tmp_path: Path) -> None:
    # EMS server groups are independent namespaces, so the group leads the dataset
    # name on both connectors.
    source = _source([_publisher_ear(tmp_path)], ems_target={"server_group": "emsgrp1"})
    workunits = _run(source, ["OrderPublisher"])

    expected = (
        "urn:li:dataset:(urn:li:dataPlatform:tibco-ems,emsgrp1.queue.orders.new,PROD)"
    )
    assert _aspect(workunits, SchemaMetadataClass, expected) is not None


def test_schema_emission_can_be_disabled(tmp_path: Path) -> None:
    source = _source([_publisher_ear(tmp_path)], emit_destination_schemas=False)
    workunits = _run(source, ["OrderPublisher"])

    assert _aspect(workunits, SchemaMetadataClass, _ORDERS_QUEUE_URN) is None


def test_lineage_emission_can_be_disabled(tmp_path: Path) -> None:
    source = _source([_publisher_ear(tmp_path)], emit_destination_lineage=False)
    workunits = _run(source, ["OrderPublisher"])

    job_urn = "urn:li:dataJob:(urn:li:dataFlow:(tibco-bw,D1/AS1,PROD),OrderPublisher)"
    assert _aspect(workunits, DataJobInputOutputClass, job_urn) is None


def test_no_archives_configured_changes_nothing(tmp_path: Path) -> None:
    workunits = _run(_source([]), ["OrderPublisher"])

    assert _aspect(workunits, SchemaMetadataClass, _ORDERS_QUEUE_URN) is None
    assert workunits


def test_two_processes_publishing_one_destination_report_the_conflict(
    tmp_path: Path,
) -> None:
    # They should agree. Where they do not, the last writer must not silently win.
    other = _PUBLISH.replace("OrderMessage", "OtherMessage")
    other_xsd = _XSD.replace("OrderMessage", "OtherMessage")
    archive = _ear(
        tmp_path,
        "OrderPublisher_1.0.0.ear",
        {
            "Processes/A_Publish.bwp": _PUBLISH,
            "Processes/B_Publish.bwp": other,
            "Schemas/Order.xsd": _XSD,
            "Schemas/Other.xsd": other_xsd,
        },
    )
    source = _source([archive])
    _run(source, ["OrderPublisher"])

    assert source.report.duplicate_schema_elements
    assert source.report.destination_schemas_emitted == 1
