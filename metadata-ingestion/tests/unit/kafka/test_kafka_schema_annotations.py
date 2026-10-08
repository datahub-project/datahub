import json
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
    SchemaReference,
)

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.ingestion.extractor.protobuf_util import (
    ProtobufSchema,
    get_protobuf_annotations,
)
from datahub.ingestion.source.kafka.kafka import KafkaSource
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StructuredPropertiesClass,
    TagAssociationClass,
)
from datahub.sdk._attribution import KnownAttribution, change_default_attribution

TOPIC_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,{},PROD)"
PII = "urn:li:structuredProperty:io.example.classification"

ANNOTATIONS_PROTO = """
syntax = "proto3";
package acme;
import "google/protobuf/descriptor.proto";
message EventAnnotations {
  string owner = 1;
  repeated string tags = 2;
  string classification = 3;
}
message FieldAnnotations {
  repeated string glossary_terms = 1;
  string classification = 2;
}
extend google.protobuf.MessageOptions { EventAnnotations event = 50001; }
extend google.protobuf.FieldOptions { FieldAnnotations field = 50002; }
"""

EVENT_PROTO = """
syntax = "proto3";
package acme.events;
import "acme/annotations.proto";

// A customer placed an order.
message OrderPlaced {
  option (acme.event) = {owner: "orders-team" tags: ["alpha", "beta"] classification: "PII"};

  // Identifier of the customer.
  string customer_id = 1 [(acme.field) = {glossary_terms: ["Customer ID"], classification: "PII"}];

  message Detail {
    // Seconds the checkout took.
    int64 seconds = 1;
  }
  Detail detail = 2;
}
"""

EVENT_JSON = {
    "title": "AccountCreated",
    "description": "A customer created an account.",
    "type": "object",
    "acme": {
        "event": {
            "owner": "accounts-team",
            "tags": ["alpha"],
            "classification": "PII",
        }
    },
    "properties": {
        "email": {
            "type": "string",
            "description": "Account email.",
            "acme": {"field": {"glossary_terms": ["Email"], "classification": "PII"}},
        },
        "sku": {"type": "string", "description": "Product SKU."},
    },
}

RECIPE = {
    "connection": {"bootstrap": "localhost:9092"},
    "schema_tags_field": "acme.event.tags",
    "meta_mapping": {
        "acme.event.owner": {
            "match": ".+",
            "operation": "add_owner",
            "config": {"owner_type": "group", "owner_category": "TECHNICAL_OWNER"},
        },
        "acme.event.classification": {
            "match": ".+",
            "operation": "add_structured_property",
            "config": {"structured_property_urn": PII},
        },
    },
    "field_meta_mapping": {
        "acme.field.glossary_terms": {
            "match": ".+",
            "operation": "add_terms",
            "config": {"separator": ","},
        },
        "acme.field.classification": {
            "match": ".+",
            "operation": "add_structured_property",
            "config": {"structured_property_urn": PII},
        },
    },
}


def _registered(
    subject: str,
    schema_str: str,
    schema_type: str,
    references: Optional[List[SchemaReference]] = None,
) -> RegisteredSchema:
    return RegisteredSchema(
        schema_id=1,
        guid=None,
        schema=Schema(
            schema_str=schema_str, schema_type=schema_type, references=references or []
        ),
        subject=subject,
        version=1,
    )


SUBJECTS: Dict[str, RegisteredSchema] = {
    "acme.annotations": _registered("acme.annotations", ANNOTATIONS_PROTO, "PROTOBUF"),
    "orders-value": _registered(
        "orders-value",
        EVENT_PROTO,
        "PROTOBUF",
        references=[
            SchemaReference(
                name="acme/annotations.proto", subject="acme.annotations", version=1
            )
        ],
    ),
    "accounts-value": _registered("accounts-value", json.dumps(EVENT_JSON), "JSON"),
}


def _workunits(recipe: Dict, graph: Optional[MagicMock] = None) -> List:
    with (
        patch(
            "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient",
            autospec=True,
        ) as registry,
        patch(
            "datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer",
            autospec=True,
        ) as consumer,
        patch("datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True),
    ):
        metadata = MagicMock()
        metadata.topics = {"orders": None, "accounts": None}
        consumer.return_value.list_topics.return_value = metadata
        registry.return_value.get_subjects.return_value = list(SUBJECTS)
        registry.return_value.get_latest_version = lambda subject_name: SUBJECTS.get(
            subject_name
        )
        registry.return_value.get_version = lambda subject_name, version: SUBJECTS[
            subject_name
        ]
        ctx = PipelineContext(run_id="test", graph=graph)
        # As inside a pipeline run: ingestion writes the non-editable aspects.
        with change_default_attribution(KnownAttribution.INGESTION):
            return list(KafkaSource.create(recipe, ctx).get_workunits())


def _aspects(workunits: List, entity_urn: str) -> Dict[str, object]:
    found: Dict[str, object] = {}
    for wu in workunits:
        mcp = wu.metadata
        if (
            isinstance(mcp, MetadataChangeProposalWrapper)
            and mcp.entityUrn == entity_urn
        ):
            assert mcp.aspectName is not None
            found[mcp.aspectName] = mcp.aspect
    return found


def _fields(aspects: Dict[str, object]) -> Dict[str, SchemaFieldClass]:
    schema = aspects["schemaMetadata"]
    assert isinstance(schema, SchemaMetadataClass)
    return {f.fieldPath.rsplit(".", 1)[-1]: f for f in schema.fields}


@pytest.mark.parametrize(
    "topic, description, owner, record_name, field, field_description, term",
    [
        (
            "orders",
            "A customer placed an order.",
            "urn:li:corpGroup:orders-team",
            "acme.events.OrderPlaced",
            "customer_id",
            "Identifier of the customer.",
            "urn:li:glossaryTerm:Customer ID",
        ),
        (
            "accounts",
            "A customer created an account.",
            "urn:li:corpGroup:accounts-team",
            "AccountCreated",
            "email",
            "Account email.",
            "urn:li:glossaryTerm:Email",
        ),
    ],
)
def test_json_and_protobuf_annotations_map_like_avro(
    topic: str,
    description: str,
    owner: str,
    record_name: str,
    field: str,
    field_description: str,
    term: str,
) -> None:
    workunits = _workunits({**RECIPE, "write_semantics": "OVERRIDE"})
    urn = TOPIC_URN.format(topic)
    aspects = _aspects(workunits, urn)

    properties = aspects["datasetProperties"]
    assert isinstance(properties, DatasetPropertiesClass)
    assert properties.description == description
    assert properties.customProperties["Schema Record Name"] == record_name

    ownership = aspects["ownership"]
    assert isinstance(ownership, OwnershipClass)
    assert [(o.owner, o.type) for o in ownership.owners] == [
        (owner, OwnershipTypeClass.TECHNICAL_OWNER)
    ]
    tags = aspects["globalTags"]
    assert isinstance(tags, GlobalTagsClass)
    assert "urn:li:tag:alpha" in [t.tag for t in tags.tags]
    structured = aspects["structuredProperties"]
    assert isinstance(structured, StructuredPropertiesClass)
    assert [(p.propertyUrn, p.values) for p in structured.properties] == [
        (PII, ["PII"])
    ]

    annotated = _fields(aspects)[field]
    assert annotated.description == field_description
    assert annotated.glossaryTerms is not None
    assert [t.urn for t in annotated.glossaryTerms.terms] == [term]
    field_aspects = _aspects(workunits, make_schema_field_urn(urn, annotated.fieldPath))
    field_structured = field_aspects["structuredProperties"]
    assert isinstance(field_structured, StructuredPropertiesClass)
    assert field_structured.properties[0].values == ["PII"]


def test_protobuf_nested_message_comments_become_field_descriptions() -> None:
    aspects = _aspects(
        _workunits({**RECIPE, "write_semantics": "OVERRIDE"}),
        TOPIC_URN.format("orders"),
    )
    assert _fields(aspects)["seconds"].description == "Seconds the checkout took."


def test_patch_keeps_edits_made_outside_the_source() -> None:
    graph = MagicMock()
    graph.get_tags.return_value = GlobalTagsClass(
        tags=[TagAssociationClass(tag="urn:li:tag:steward-reviewed")]
    )
    graph.get_ownership.return_value = OwnershipClass(
        owners=[
            # Added in the UI: no source, kept.
            OwnerClass(
                owner="urn:li:corpuser:steward", type=OwnershipTypeClass.BUSINESS_OWNER
            ),
            # Added by an earlier run of this source: replaced by the schema's owner.
            OwnerClass(
                owner="urn:li:corpGroup:old-team",
                type=OwnershipTypeClass.TECHNICAL_OWNER,
                source=OwnershipSourceClass(type="SERVICE"),
            ),
        ]
    )
    graph.get_glossary_terms.return_value = GlossaryTermsClass(
        terms=[GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:Reviewed")],
        auditStamp=MagicMock(),
    )

    workunits = _workunits({**RECIPE, "write_semantics": "PATCH"}, graph=graph)
    aspects = _aspects(workunits, TOPIC_URN.format("orders"))

    tags = aspects["globalTags"]
    assert isinstance(tags, GlobalTagsClass)
    assert {t.tag for t in tags.tags} == {
        "urn:li:tag:alpha",
        "urn:li:tag:beta",
        "urn:li:tag:steward-reviewed",
    }
    ownership = aspects["ownership"]
    assert isinstance(ownership, OwnershipClass)
    assert {o.owner for o in ownership.owners} == {
        "urn:li:corpGroup:orders-team",
        "urn:li:corpuser:steward",
    }
    # Structured properties go out as a patch, so other properties on the topic survive.
    patches = [
        wu.metadata
        for wu in workunits
        if getattr(wu.metadata, "entityUrn", None) == TOPIC_URN.format("orders")
        and getattr(wu.metadata, "aspectName", None) == "structuredProperties"
    ]
    assert patches and all(p.changeType == "PATCH" for p in patches)


def test_patch_without_graph_falls_back_to_override() -> None:
    workunits = _workunits({**RECIPE, "write_semantics": "PATCH"})
    aspects = _aspects(workunits, TOPIC_URN.format("accounts"))
    assert isinstance(aspects["structuredProperties"], StructuredPropertiesClass)


def test_protobuf_annotations_include_nested_messages() -> None:
    annotations = get_protobuf_annotations(
        ProtobufSchema("orders-value.proto", EVENT_PROTO),
        [ProtobufSchema("acme/annotations.proto", ANNOTATIONS_PROTO)],
    )
    assert annotations.main_message == "acme.events.OrderPlaced"
    main = annotations.messages["acme.events.OrderPlaced"]
    assert main.description == "A customer placed an order."
    assert main.props == {
        "acme": {
            "event": {
                "owner": "orders-team",
                "tags": ["alpha", "beta"],
                "classification": "PII",
            }
        }
    }
    nested = annotations.fields[("acme.events.OrderPlaced.Detail", "seconds")]
    assert nested.description == "Seconds the checkout took."


def test_protobuf_annotations_of_an_uncompilable_schema_are_empty() -> None:
    annotations = get_protobuf_annotations(
        ProtobufSchema("broken.proto", 'syntax = "proto3"; message {')
    )
    assert annotations.main_message is None
    assert not annotations.messages and not annotations.fields


def test_json_schema_custom_keywords_are_kept_in_json_props() -> None:
    fields = list(JsonSchemaTranslator.get_fields_from_schema(EVENT_JSON))
    email = next(f for f in fields if f.fieldPath.endswith(".email"))
    assert email.jsonProps is not None
    props = json.loads(email.jsonProps)
    assert props["acme"] == {
        "field": {"glossary_terms": ["Email"], "classification": "PII"}
    }
    # Standard JSON Schema keywords stay out.
    assert "description" not in props and "type" not in props
