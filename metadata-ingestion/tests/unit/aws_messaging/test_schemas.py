import json
from typing import Any, Dict, List
from unittest.mock import MagicMock

import boto3
import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging.config import SqsSourceConfig
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.metadata.schema_classes import (
    MetadataChangeProposalClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
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


def register_schema(name: str) -> None:
    glue = boto3.client("glue", region_name=REGION)
    try:
        glue.create_registry(RegistryName="default-registry")
    except glue.exceptions.AlreadyExistsException:
        pass
    glue.create_schema(
        RegistryId={"RegistryName": "default-registry"},
        SchemaName=name,
        DataFormat="AVRO",
        Compatibility="NONE",
        SchemaDefinition=json.dumps(ORDER_SCHEMA),
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
            }
        ),
        PipelineContext(run_id="schemas-test", graph=graph),
    )


def test_raw_delivery_gets_field_lineage_for_shared_fields(aws: None) -> None:
    source = _subscribe(raw=True)
    lineage = fine_grained(list(source.get_workunits()))
    # Path: /fineGrainedLineages/<transform>/<downstream field>/<query>/<upstream field>
    pairs = sorted(
        (
            op["path"].rsplit(",", 1)[-1].rstrip(")"),
            op["path"].split("/")[3].rsplit(",", 1)[-1].rstrip(")"),
        )
        for op in lineage
    )
    # Matched case-insensitively: the topic's `Amount` feeds the queue's `amount`.
    assert pairs == [("Amount", "amount"), ("order_id", "order_id")]
    assert source.get_report().column_lineage_edges_emitted == 2


def test_enveloped_delivery_gets_table_lineage_only(aws: None) -> None:
    source = _subscribe(raw=False)
    assert fine_grained(list(source.get_workunits())) == []
    assert source.get_report().column_lineage_skipped == {"not raw message delivery": 1}


def _dead_letter_source(column_lineage: bool = True) -> SqsSource:
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
            column_lineage=column_lineage,
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
    assert len(lineage) == 2
    assert all(f"({DLQ_URN}," in op["path"] for op in lineage)
    assert source.get_report().column_lineage_edges_emitted == 2


def test_dead_letter_column_lineage_can_be_turned_off(aws: None) -> None:
    source = _dead_letter_source(column_lineage=False)
    assert fine_grained(list(source.get_workunits())) == []


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
