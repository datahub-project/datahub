import json
import sys
from typing import Any, Dict, List, Literal, Optional, Tuple
from unittest.mock import MagicMock

import boto3
import pytest

from datahub.configuration.common import GraphError
from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion import extractor
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging import schema_registry
from datahub.ingestion.source.aws_messaging.clients import SnsClient
from datahub.ingestion.source.aws_messaging.config import SqsSourceConfig
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetadataChangeProposalClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from tests.unit.aws_messaging.conftest import (
    ACCOUNT,
    REGION,
    base_config,
    sns_client,
    sqs_client,
)

ORDER_SCHEMA = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "Amount", "type": "double"},
        {"name": "note", "type": "string"},
    ],
}
QUEUE_URN = f"urn:li:dataset:(urn:li:dataPlatform:sqs,{ACCOUNT}.{REGION}.orders,PROD)"
DLQ_URN = f"urn:li:dataset:(urn:li:dataPlatform:sqs,{ACCOUNT}.{REGION}.orders-dlq,PROD)"
TOPIC_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:sns,{ACCOUNT}.{REGION}.order-events,PROD)"
)


PROTOBUF_ORDER = 'syntax = "proto3";\nmessage Order { string order_id = 1; }'


def register_schema(
    name: str,
    data_format: Literal["AVRO", "JSON", "PROTOBUF"] = "AVRO",
    definition: Optional[str] = None,
) -> None:
    glue = boto3.client("glue", region_name=REGION)
    try:
        glue.create_registry(RegistryName="default-registry")
    except glue.exceptions.AlreadyExistsException:
        pass
    glue.create_schema(
        RegistryId={"RegistryName": "default-registry"},
        SchemaName=name,
        DataFormat=data_format,
        Compatibility="NONE",
        SchemaDefinition=definition or json.dumps(ORDER_SCHEMA),
    )


def schemas(workunits: List[Any]) -> Dict[str, SchemaMetadataClass]:
    return {
        wu.get_urn(): wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, SchemaMetadataClass)
    }


def fine_grained(workunits: List[Any]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for wu in workunits:
        metadata = wu.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == "upstreamLineage"
        ):
            assert metadata.aspect is not None
            body = json.loads(metadata.aspect.value)
            ops = body["patch"] if isinstance(body, dict) else body
            result.extend(op for op in ops if "fineGrainedLineages" in op["path"])
    return result


def field_pairs(lineage: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    # Path: /fineGrainedLineages/<transform>/<downstream field>/<query>/<upstream field>
    return sorted(
        (
            op["path"].rsplit(",", 1)[-1].rstrip(")"),
            op["path"].split("/")[3].rsplit(",", 1)[-1].rstrip(")"),
        )
        for op in lineage
    )


def test_queue_schema_from_glue(aws: None) -> None:
    sqs_client().create_queue(QueueName="orders")
    register_schema("order-v1")
    source = SqsSource.create(
        base_config(
            glue_schema_registry={
                "enabled": True,
                "queue_schema_map": {"orders": "order-v1"},
            }
        ),
        PipelineContext(run_id="schemas-test"),
    )
    found = schemas(list(source.get_workunits()))
    assert [f.fieldPath for f in found[QUEUE_URN].fields] == [
        "[version=2.0].[type=Order].[type=string].order_id",
        "[version=2.0].[type=Order].[type=double].Amount",
        "[version=2.0].[type=Order].[type=string].note",
    ]
    assert source.get_report().schemas_attached == 1


def test_configured_but_disabled_registry_is_rejected() -> None:
    with pytest.raises(ValueError):
        SqsSourceConfig.model_validate(
            base_config(glue_schema_registry={"queue_schema_map": {"orders": "x"}})
        )


def _queue_schema_in_datahub() -> SchemaMetadataClass:
    def field(path: str) -> SchemaFieldClass:
        return SchemaFieldClass(
            fieldPath=path,
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType="number",
        )

    return SchemaMetadataClass(
        schemaName="orders",
        platform="urn:li:dataPlatform:sqs",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[field("order_id"), field("amount"), field("unrelated")],
    )


def _subscribe(raw: bool) -> SnsSource:
    url = sqs_client().create_queue(QueueName="orders")["QueueUrl"]
    queue_arn = sqs_client().get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    topic = sns_client().create_topic(Name="order-events")["TopicArn"]
    sns_client().subscribe(
        TopicArn=topic,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={"RawMessageDelivery": "true" if raw else "false"},
    )
    register_schema("order-v1")
    graph = MagicMock()
    graph.get_schema_metadata.side_effect = lambda urn: (
        _queue_schema_in_datahub() if urn == QUEUE_URN else None
    )
    return SnsSource.create(
        base_config(
            glue_schema_registry={
                "enabled": True,
                "topic_schema_map": {"order-events": "order-v1"},
            },
            include_subscription_attributes=True,
        ),
        PipelineContext(run_id="schemas-test", graph=graph),
    )


def test_raw_delivery_gets_field_lineage_for_shared_fields(aws: None) -> None:
    source = _subscribe(raw=True)
    lineage = fine_grained(list(source.get_workunits()))
    # Matched case-insensitively: the topic's `Amount` feeds the queue's `amount`.
    assert field_pairs(lineage) == [("Amount", "amount"), ("order_id", "order_id")]
    assert source.get_report().column_lineage_edges_emitted == 2


def test_raw_delivery_uses_a_topic_schema_already_in_datahub(aws: None) -> None:
    # No Glue: both schemas come from DataHub.
    url = sqs_client().create_queue(QueueName="orders")["QueueUrl"]
    queue_arn = sqs_client().get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    topic = sns_client().create_topic(Name="order-events")["TopicArn"]
    sns_client().subscribe(
        TopicArn=topic,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={"RawMessageDelivery": "true"},
    )
    graph = MagicMock()
    graph.get_schema_metadata.side_effect = lambda urn: (
        _queue_schema_in_datahub() if urn in (QUEUE_URN, TOPIC_URN) else None
    )
    source = SnsSource.create(
        base_config(include_column_lineage=True, include_subscription_attributes=True),
        PipelineContext(run_id="schemas-test", graph=graph),
    )
    lineage = fine_grained(list(source.get_workunits()))
    assert field_pairs(lineage) == [
        ("amount", "amount"),
        ("order_id", "order_id"),
        ("unrelated", "unrelated"),
    ]


def test_enveloped_delivery_gets_table_lineage_only(aws: None) -> None:
    source = _subscribe(raw=False)
    assert fine_grained(list(source.get_workunits())) == []
    assert source.get_report().column_lineage_skipped == {"not raw message delivery": 1}


def _dead_letter_source(include_column_lineage: bool = True) -> SqsSource:
    dlq_url = sqs_client().create_queue(QueueName="orders-dlq")["QueueUrl"]
    dlq_arn = sqs_client().get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    sqs_client().create_queue(
        QueueName="orders",
        Attributes={
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}
            )
        },
    )
    register_schema("order-v1")
    graph = MagicMock()
    graph.get_schema_metadata.side_effect = lambda urn: (
        _queue_schema_in_datahub() if urn == DLQ_URN else None
    )
    return SqsSource.create(
        base_config(
            include_column_lineage=include_column_lineage,
            glue_schema_registry={
                "enabled": True,
                "queue_schema_map": {"orders": "order-v1"},
            },
        ),
        PipelineContext(run_id="schemas-test", graph=graph),
    )


def test_dead_letter_queue_gets_identity_field_lineage(aws: None) -> None:
    source = _dead_letter_source()
    lineage = fine_grained(list(source.get_workunits()))
    assert all(f"({DLQ_URN}," in op["path"] for op in lineage)
    assert field_pairs(lineage) == [("Amount", "amount"), ("order_id", "order_id")]
    assert source.get_report().column_lineage_edges_emitted == 2


def test_schemas_with_no_shared_fields_get_table_lineage_only(aws: None) -> None:
    source = _dead_letter_source()
    graph = source.schemas.graph
    assert isinstance(graph, MagicMock)
    disjoint = _queue_schema_in_datahub()
    disjoint.fields = [f for f in disjoint.fields if f.fieldPath == "unrelated"]
    graph.get_schema_metadata.side_effect = lambda urn: (
        disjoint if urn == DLQ_URN else None
    )
    assert fine_grained(list(source.get_workunits())) == []
    assert source.get_report().column_lineage_skipped == {"no fields in common": 1}


def _lineage_in_datahub(
    source: SqsSource, field_edges: List[FineGrainedLineageClass]
) -> None:
    graph = source.lineage.graph
    assert isinstance(graph, MagicMock)
    graph.get_related_entities.side_effect = lambda urn, types, direction: (
        [MagicMock(urn=DLQ_URN)] if urn == QUEUE_URN and "DownstreamOf" in types else []
    )
    graph.get_aspect.return_value = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=QUEUE_URN,
                type=DatasetLineageTypeClass.TRANSFORMED,
                properties={"relationship": "dead_letter"},
            )
        ],
        fineGrainedLineages=field_edges,
    )


def _field_edge(upstream_field: str, downstream_field: str) -> FineGrainedLineageClass:
    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[upstream_field],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        downstreams=[downstream_field],
    )


def test_renamed_field_edge_is_retracted(aws: None) -> None:
    # An earlier run linked a field the schemas no longer share; this run's
    # field edges stay and the old one is removed.
    source = _dead_letter_source()
    _lineage_in_datahub(
        source,
        [
            _field_edge(
                make_schema_field_urn(QUEUE_URN, "old_id"),
                make_schema_field_urn(DLQ_URN, "old_id"),
            )
        ],
    )
    ops = fine_grained(list(source.get_workunits()))
    removed = [op["path"] for op in ops if op["op"] == "remove"]
    assert len(removed) == 1 and removed[0].endswith(
        make_schema_field_urn(QUEUE_URN, "old_id")
    )
    assert len([op for op in ops if op["op"] == "add"]) == 2
    assert source.get_report().column_lineage_edges_removed == 1


def test_field_edges_kept_when_field_lineage_was_skipped(aws: None) -> None:
    # The dead-letter queue's schema can't be read this run, so the hop's field
    # lineage is unknown rather than gone.
    source = _dead_letter_source()
    source.schemas.graph = None
    _lineage_in_datahub(
        source,
        [
            _field_edge(
                make_schema_field_urn(QUEUE_URN, "order_id"),
                make_schema_field_urn(DLQ_URN, "order_id"),
            )
        ],
    )
    assert fine_grained(list(source.get_workunits())) == []
    assert source.get_report().column_lineage_edges_removed == 0


def test_field_edge_with_several_downstreams_is_kept(aws: None) -> None:
    # Another writer's edge fans out to several fields; it isn't one this source
    # emits, so it is left alone.
    source = _dead_letter_source()
    shared = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=[make_schema_field_urn(QUEUE_URN, "old_id")],
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
        downstreams=[
            make_schema_field_urn(DLQ_URN, "old_id"),
            make_schema_field_urn(DLQ_URN, "note"),
        ],
    )
    _lineage_in_datahub(source, [shared])
    ops = fine_grained(list(source.get_workunits()))
    assert [op for op in ops if op["op"] == "remove"] == []
    assert source.get_report().column_lineage_edges_removed == 0


def test_failed_glue_lookup_does_not_fall_back_to_datahub(aws: None) -> None:
    # The mapped schema can't be read from Glue this run; DataHub's copy may be
    # stale, so the hop's field lineage is unknown and its field edges stay.
    source = _dead_letter_source()
    source.config.glue_schema_registry.queue_schema_map["orders"] = "not-there"
    graph = source.schemas.graph
    assert isinstance(graph, MagicMock)
    graph.get_schema_metadata.side_effect = lambda urn: _queue_schema_in_datahub()
    _lineage_in_datahub(
        source,
        [
            _field_edge(
                make_schema_field_urn(QUEUE_URN, "order_id"),
                make_schema_field_urn(DLQ_URN, "order_id"),
            )
        ],
    )
    assert fine_grained(list(source.get_workunits())) == []
    report = source.get_report()
    assert report.column_lineage_skipped == {"schema missing on one side": 1}
    assert report.column_lineage_edges_removed == 0


def test_bad_field_urn_does_not_block_table_edge_removal(aws: None) -> None:
    # The redrive policy was removed; a malformed field edge from another writer
    # stays, while the table edge and the valid field edge go.
    sqs_client().create_queue(QueueName="orders")
    sqs_client().create_queue(QueueName="orders-dlq")
    source = SqsSource.create(
        base_config(include_column_lineage=True),
        PipelineContext(run_id="schemas-test", graph=MagicMock()),
    )
    _lineage_in_datahub(
        source,
        [
            _field_edge("not-a-field-urn", make_schema_field_urn(DLQ_URN, "note")),
            _field_edge(
                make_schema_field_urn(QUEUE_URN, "order_id"),
                make_schema_field_urn(DLQ_URN, "order_id"),
            ),
        ],
    )
    workunits = list(source.get_workunits())
    removed_fields = [
        op["path"] for op in fine_grained(workunits) if op["op"] == "remove"
    ]
    assert len(removed_fields) == 1 and removed_fields[0].endswith(
        make_schema_field_urn(QUEUE_URN, "order_id")
    )
    report = source.get_report()
    assert report.lineage_edges_removed == 1
    assert report.warnings


def test_include_column_lineage_defaults_to_the_glue_setting() -> None:
    assert not SqsSourceConfig.model_validate(base_config()).include_column_lineage
    enabled = SqsSourceConfig.model_validate(
        base_config(glue_schema_registry={"enabled": True})
    )
    assert enabled.include_column_lineage


def test_dead_letter_column_lineage_can_be_turned_off(aws: None) -> None:
    source = _dead_letter_source(include_column_lineage=False)
    assert fine_grained(list(source.get_workunits())) == []
    # The table-level edge is still written.
    assert source.get_report().lineage_edges_emitted == 1


def test_unreadable_schema_in_datahub_keeps_field_edges(aws: None) -> None:
    source = _dead_letter_source()
    graph = source.schemas.graph
    assert isinstance(graph, MagicMock)
    graph.get_schema_metadata.side_effect = GraphError("timed out")
    assert fine_grained(list(source.get_workunits())) == []
    report = source.get_report()
    assert report.column_lineage_skipped == {"schema missing on one side": 1}
    assert "Schema not read from DataHub" in [w.title for w in report.warnings]


def test_dead_letter_without_dlq_schema_is_counted(aws: None) -> None:
    source = _dead_letter_source()
    source.schemas.graph = None
    assert fine_grained(list(source.get_workunits())) == []
    assert source.get_report().column_lineage_skipped == {
        "schema missing on one side": 1
    }


def _queue_with_registry(**registry: Any) -> SqsSource:
    sqs_client().create_queue(QueueName="orders")
    boto3.client("glue", region_name=REGION).create_registry(
        RegistryName="default-registry"
    )
    return SqsSource.create(
        base_config(glue_schema_registry={"enabled": True, **registry}),
        PipelineContext(run_id="schemas-test"),
    )


def test_missing_mapped_schema_is_a_warning(aws: None) -> None:
    source = _queue_with_registry(queue_schema_map={"orders": "not-there"})
    assert schemas(list(source.get_workunits())) == {}
    report = source.get_report()
    assert [entry.split(":")[0] for entry in report.schema_resolution_failures] == [
        "orders"
    ]
    assert report.warnings


def test_naming_convention_miss_is_not_a_warning(aws: None) -> None:
    source = _queue_with_registry(use_naming_convention=True)
    assert schemas(list(source.get_workunits())) == {}
    report = source.get_report()
    assert list(report.schema_naming_convention_misses) == ["orders"]
    assert not report.schema_resolution_failures


def test_schema_that_parses_to_no_fields_is_not_attached(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # protobuf_util returns [] when compilation fails instead of raising.
    monkeypatch.setattr(schema_registry, "parse_glue_schema_fields", lambda *args: [])
    source = _queue_with_registry(queue_schema_map={"orders": "order-v1"})
    register_schema("order-v1", "PROTOBUF", PROTOBUF_ORDER)
    assert schemas(list(source.get_workunits())) == {}
    report = source.get_report()
    assert [entry.split(":")[0] for entry in report.schema_resolution_failures] == [
        "orders"
    ]
    assert report.warnings


def test_unparseable_schema_is_a_warning(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Glue rejects invalid definitions on create, so the parser error is faked.
    def fail(*args: Any) -> None:
        raise ValueError("bad schema")

    monkeypatch.setattr(schema_registry, "parse_glue_schema_fields", fail)
    source = _queue_with_registry(queue_schema_map={"orders": "order-v1"})
    register_schema("order-v1")
    assert schemas(list(source.get_workunits())) == {}
    report = source.get_report()
    assert list(report.schema_resolution_failures) == ["orders: parse failed (AVRO)"]
    assert [w.title for w in report.warnings] == ["Glue schema parse failed"]


def test_unsupported_schema_format_is_a_warning(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Glue only accepts AVRO, JSON and PROTOBUF today, so a new format is faked.
    monkeypatch.setattr(schema_registry, "parse_glue_schema_fields", lambda *args: None)
    source = _queue_with_registry(queue_schema_map={"orders": "order-v1"})
    register_schema("order-v1")
    assert schemas(list(source.get_workunits())) == {}
    report = source.get_report()
    assert [entry.split(":")[0] for entry in report.schema_resolution_failures] == [
        "orders"
    ]
    assert [w.title for w in report.warnings] == ["Unsupported Glue schema format"]


def test_missing_protobuf_extra_names_the_extra(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Without the kafka extra, importing the protobuf converter fails.
    monkeypatch.setitem(sys.modules, "datahub.ingestion.extractor.protobuf_util", None)
    monkeypatch.delattr(extractor, "protobuf_util", raising=False)
    source = _queue_with_registry(queue_schema_map={"orders": "order-v1"})
    register_schema("order-v1", "PROTOBUF", PROTOBUF_ORDER)
    assert schemas(list(source.get_workunits())) == {}
    report = source.get_report()
    assert [w.title for w in report.warnings] == ["Glue schema parser not installed"]


def test_repeated_raw_subscriptions_add_field_lineage_once(aws: None) -> None:
    source = _subscribe(raw=True)
    queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT}:orders"
    topic_arn = f"arn:aws:sns:{REGION}:{ACCOUNT}:order-events"
    # A second raw subscription of the same queue, e.g. with another filter policy.
    sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={
            "RawMessageDelivery": "true",
            "FilterPolicy": '{"kind": ["refund"]}',
        },
    )
    lineage = fine_grained(list(source.get_workunits()))
    assert len(lineage) == 2
    assert source.get_report().column_lineage_edges_emitted == 2


def test_empty_schema_map_entry_does_not_fall_back_to_the_convention(
    aws: None,
) -> None:
    source = _queue_with_registry(
        queue_schema_map={"orders": ""}, use_naming_convention=True
    )
    register_schema("orders")
    assert schemas(list(source.get_workunits())) == {}
    assert "Empty Glue schema name" in [w.title for w in source.get_report().warnings]


def test_failed_subscription_attribute_read_keeps_field_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A failed read says nothing about raw delivery, so the hop is unknown rather
    # than "not raw", which would remove its field edges.
    monkeypatch.setattr(
        SnsClient, "get_subscription_attributes", lambda self, arn: None
    )
    source = _subscribe(raw=True)
    assert fine_grained(list(source.get_workunits())) == []
    assert source.get_report().column_lineage_skipped == {
        "subscription attributes not read": 1
    }
