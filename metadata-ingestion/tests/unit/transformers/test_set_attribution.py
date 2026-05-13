import json
from typing import cast
from unittest.mock import ANY

import pytest
from pydantic import ValidationError

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import UNIT_SEPARATOR, _Patch
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.set_attribution import (
    SetAttributionConfig,
    SetAttributionTransformer,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)

SAMPLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.my-table,PROD)"
)
ATTRIBUTION_SOURCE = "urn:li:platformResource:ingestion"


@pytest.fixture
def pipeline_context():
    return PipelineContext(run_id="test")


@pytest.fixture
def transformer(pipeline_context):
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        actor="urn:li:corpuser:datahub",
        patch_mode=False,
    )
    return SetAttributionTransformer(config, pipeline_context)


@pytest.fixture
def transformer_patch_mode(pipeline_context):
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        actor="urn:li:corpuser:datahub",
        patch_mode=True,
    )
    return SetAttributionTransformer(config, pipeline_context)


def transform_one(transformer, record):
    """Run transformer on a single record and return the single output record."""
    envelope = RecordEnvelope(record=record, metadata={})
    results = list(transformer.transform([envelope]))
    assert len(results) == 1
    return results[0].record


# Test SetAttributionConfig URN validation
@pytest.mark.parametrize(
    "kwargs,match",
    [
        (
            {"attribution_source": "not-a-urn", "actor": "urn:li:corpuser:datahub"},
            "urn:li:",
        ),
        (
            {"attribution_source": "", "actor": "urn:li:corpuser:datahub"},
            "non-empty",
        ),
        ({"attribution_source": ATTRIBUTION_SOURCE, "actor": "not-a-urn"}, "urn:li:"),
    ],
)
def test_config_invalid_raises(kwargs, match):
    """Invalid URN or empty required field raises ValidationError."""
    with pytest.raises(ValidationError, match=match):
        SetAttributionConfig(**kwargs)


@pytest.mark.parametrize(
    "kwargs,expected_actor",
    [
        (
            {
                "attribution_source": ATTRIBUTION_SOURCE,
                "actor": "urn:li:corpuser:datahub",
            },
            "urn:li:corpuser:datahub",
        ),
        ({"attribution_source": ATTRIBUTION_SOURCE}, "urn:li:corpuser:datahub"),
        ({"attribution_source": ATTRIBUTION_SOURCE, "actor": None}, None),
    ],
)
def test_config_valid(kwargs, expected_actor):
    """Valid config is accepted; actor default or explicit."""
    config = SetAttributionConfig(**kwargs)
    assert config.attribution_source == ATTRIBUTION_SOURCE
    assert config.actor == expected_actor


# Test _Patch serialization
def test_patch_to_obj():
    """Test _Patch serialization."""
    op = _Patch(op="add", path=("tags", "test"), value={"tag": "urn:li:tag:test"})
    result = op.to_obj()
    assert result == {
        "op": "add",
        "path": "/tags/test",
        "value": {"tag": "urn:li:tag:test"},
    }

    op_no_value = _Patch(op="remove", path=("tags", "test"), value={})
    result = op_no_value.to_obj()
    assert result == {"op": "remove", "path": "/tags/test", "value": {}}


# Test GlobalTags transformer
def test_global_tags_upsert_mode(transformer):
    """Test GlobalTags transformation in upsert mode."""
    tags = GlobalTagsClass(
        tags=[
            TagAssociationClass(tag="urn:li:tag:tagA"),
            TagAssociationClass(tag="urn:li:tag:tagB"),
        ]
    )
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer, mcpw)
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspectName == "globalTags"
    assert result.aspect is not None

    # Verify GenericJsonPatch structure
    patch_dict = json.loads(result.aspect.value.decode())

    expected_patch_dict = {
        "arrayPrimaryKeys": {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]},
        "patch": [
            {
                "op": "add",
                "path": f"/tags/{ATTRIBUTION_SOURCE}",
                "value": {
                    "urn:li:tag:tagA": {
                        "tag": "urn:li:tag:tagA",
                        "attribution": {
                            "time": ANY,
                            "actor": "urn:li:corpuser:datahub",
                            "source": ATTRIBUTION_SOURCE,
                        },
                    },
                    "urn:li:tag:tagB": {
                        "tag": "urn:li:tag:tagB",
                        "attribution": {
                            "time": ANY,
                            "actor": "urn:li:corpuser:datahub",
                            "source": ATTRIBUTION_SOURCE,
                        },
                    },
                },
            }
        ],
        "forceGenericPatch": True,
    }

    assert patch_dict == expected_patch_dict


def test_global_tags_patch_mode(transformer_patch_mode):
    """Test GlobalTags transformation in patch mode."""
    tags = GlobalTagsClass(
        tags=[
            TagAssociationClass(tag="urn:li:tag:tagA"),
            TagAssociationClass(tag="urn:li:tag:tagB"),
        ]
    )
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer_patch_mode, mcpw)
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspect is not None

    # Verify patch mode creates individual operations
    patch_dict = json.loads(result.aspect.value.decode())

    expected_patch_dict = {
        "arrayPrimaryKeys": {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]},
        "patch": [
            {
                "op": "add",
                "path": f"/tags/{ATTRIBUTION_SOURCE}/urn:li:tag:tagA",
                "value": {
                    "tag": "urn:li:tag:tagA",
                    "attribution": {
                        "time": ANY,
                        "actor": "urn:li:corpuser:datahub",
                        "source": ATTRIBUTION_SOURCE,
                    },
                },
            },
            {
                "op": "add",
                "path": f"/tags/{ATTRIBUTION_SOURCE}/urn:li:tag:tagB",
                "value": {
                    "tag": "urn:li:tag:tagB",
                    "attribution": {
                        "time": ANY,
                        "actor": "urn:li:corpuser:datahub",
                        "source": ATTRIBUTION_SOURCE,
                    },
                },
            },
        ],
        "forceGenericPatch": True,
    }

    assert patch_dict == expected_patch_dict


def test_global_tags_with_source_detail(pipeline_context):
    """Test GlobalTags with sourceDetail map."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        actor="urn:li:corpuser:datahub",
        source_detail={"pipeline_id": "my-pipeline", "version": "1.0"},
        patch_mode=False,
    )
    transformer = SetAttributionTransformer(config, pipeline_context)

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:tagA")])
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer, mcpw)
    assert result.aspect is not None
    patch_dict = json.loads(result.aspect.value.decode())
    tag_value = patch_dict["patch"][0]["value"]["urn:li:tag:tagA"]
    assert "attribution" in tag_value
    assert tag_value["attribution"]["sourceDetail"] == {
        "pipeline_id": "my-pipeline",
        "version": "1.0",
    }


# Test Ownership transformer
def test_ownership_upsert_mode(transformer):
    """Test Ownership transformation in upsert mode."""
    owners = OwnershipClass(
        owners=[
            OwnerClass(
                owner="urn:li:corpuser:user1",
                type=OwnershipTypeClass.DATAOWNER,
            ),
            OwnerClass(
                owner="urn:li:corpuser:user2",
                type=OwnershipTypeClass.DATAOWNER,
            ),
        ]
    )
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="ownership",
        aspect=owners,
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer, mcpw)
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspectName == "ownership"
    assert result.aspect is not None

    patch_dict = json.loads(result.aspect.value.decode())

    expected_patch_dict = {
        "arrayPrimaryKeys": {"owners": [f"attribution{UNIT_SEPARATOR}source", "owner"]},
        "patch": [
            {
                "op": "add",
                "path": f"/owners/{ATTRIBUTION_SOURCE}",
                "value": {
                    "urn:li:corpuser:user1": {
                        "owner": "urn:li:corpuser:user1",
                        "type": "DATAOWNER",
                        "attribution": {
                            "time": ANY,
                            "actor": "urn:li:corpuser:datahub",
                            "source": ATTRIBUTION_SOURCE,
                        },
                    },
                    "urn:li:corpuser:user2": {
                        "owner": "urn:li:corpuser:user2",
                        "type": "DATAOWNER",
                        "attribution": {
                            "time": ANY,
                            "actor": "urn:li:corpuser:datahub",
                            "source": ATTRIBUTION_SOURCE,
                        },
                    },
                },
            }
        ],
        "forceGenericPatch": True,
    }

    assert patch_dict == expected_patch_dict


# Test GlossaryTerms transformer
def test_glossary_terms_upsert_mode(transformer):
    """Test GlossaryTerms transformation in upsert mode."""
    from datahub.metadata.schema_classes import AuditStampClass

    terms = GlossaryTermsClass(
        terms=[
            GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:term1"),
            GlossaryTermAssociationClass(urn="urn:li:glossaryTerm:term2"),
        ],
        auditStamp=AuditStampClass._construct_with_defaults(),
    )
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="glossaryTerms",
        aspect=terms,
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer, mcpw)
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspectName == "glossaryTerms"
    assert result.aspect is not None

    patch_dict = json.loads(result.aspect.value.decode())
    assert "arrayPrimaryKeys" in patch_dict
    assert patch_dict["arrayPrimaryKeys"]["terms"] == [
        f"attribution{UNIT_SEPARATOR}source",
        "urn",
    ]


# Test PATCH input handling
def test_patch_input_with_attribution(transformer):
    """Test that PATCH inputs have attribution added while preserving semantics."""
    # Create a PATCH MCP with existing patch operations
    existing_patch = {
        "arrayPrimaryKeys": {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]},
        "patch": [
            {
                "op": "add",
                "path": f"/tags/{ATTRIBUTION_SOURCE}/urn:li:tag:tagC",
                "value": {
                    "tag": "urn:li:tag:tagC",
                    "context": "existing context",
                },
            }
        ],
        "forceGenericPatch": True,
    }
    patch_aspect = GenericAspectClass(
        value=json.dumps(existing_patch).encode(),
        contentType="application/json-patch+json",
    )
    # Use MetadataChangeProposalClass directly for PATCH with GenericAspectClass
    mcp = MetadataChangeProposalClass(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=patch_aspect,
        changeType=ChangeTypeClass.PATCH,
    )

    result = transform_one(transformer, mcp)
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspect is not None

    patch_dict = json.loads(result.aspect.value.decode())
    expected_patch_dict = {
        "arrayPrimaryKeys": existing_patch["arrayPrimaryKeys"],
        "patch": [
            {
                "op": "add",
                "path": f"/tags/{ATTRIBUTION_SOURCE}/urn:li:tag:tagC",
                "value": {
                    "context": "existing context",
                    "tag": "urn:li:tag:tagC",
                    "attribution": {
                        "time": ANY,
                        "actor": "urn:li:corpuser:datahub",
                        "source": ATTRIBUTION_SOURCE,
                    },
                },
            }
        ],
        "forceGenericPatch": True,
    }
    assert patch_dict == expected_patch_dict


def test_patch_input_different_attribution_source(transformer):
    """Test that PATCH inputs with different attribution source pass through unchanged."""
    # Create a PATCH MCP with existing patch operations using a different attribution source
    different_source = "urn:li:platformResource:different-source"
    existing_patch = {
        "arrayPrimaryKeys": {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]},
        "patch": [
            {
                "op": "add",
                "path": f"/tags/{different_source}/urn:li:tag:tagC",
                "value": {
                    "tag": "urn:li:tag:tagC",
                    "context": "existing context",
                    "attribution": {
                        "time": 1234567890,
                        "actor": "urn:li:corpuser:other",
                        "source": different_source,
                    },
                },
            }
        ],
        "forceGenericPatch": True,
    }
    patch_aspect = GenericAspectClass(
        value=json.dumps(existing_patch).encode(),
        contentType="application/json-patch+json",
    )
    mcp = MetadataChangeProposalClass(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=patch_aspect,
        changeType=ChangeTypeClass.PATCH,
    )

    result = transform_one(transformer, mcp)
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspect is not None

    patch_dict = json.loads(result.aspect.value.decode())
    assert patch_dict == existing_patch


def test_patch_input_unsupported_content_type(transformer):
    """Test that PATCH inputs with unsupported content type pass through."""
    # Create a PATCH MCP with wrong content type
    patch_aspect = GenericAspectClass(
        value=json.dumps({"patch": []}).encode(),
        contentType="application/json",  # Wrong content type
    )
    mcp = MetadataChangeProposalClass(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=patch_aspect,
        changeType=ChangeTypeClass.PATCH,
    )

    result = transform_one(transformer, mcp)
    assert result == mcp


# Test MCE input handling
def test_mce_input_conversion(transformer):
    """Test MCE input conversion to PATCH MCP."""
    from datahub.metadata.schema_classes import DatasetSnapshotClass

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:tagA")])
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=SAMPLE_URN,
            aspects=[tags],
        )
    )

    result = transform_one(transformer, mce)
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspectName == "globalTags"


def test_mce_input_multiple_aspects(transformer):
    """MCE with multiple supported aspects yields one PATCH MCP per aspect."""
    from datahub.metadata.schema_classes import DatasetSnapshotClass

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:tagA")])
    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner="urn:li:corpuser:datahub",
                type=OwnershipTypeClass.TECHNICAL_OWNER,
            )
        ]
    )
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=SAMPLE_URN,
            aspects=[tags, ownership],
        )
    )

    envelope = RecordEnvelope(record=mce, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 2
    aspect_names = {r.record.aspectName for r in results}
    assert aspect_names == {"globalTags", "ownership"}
    for r in results:
        assert isinstance(r.record, MetadataChangeProposalClass)
        assert r.record.changeType == ChangeTypeClass.PATCH
        assert r.record.entityUrn == SAMPLE_URN


# Test edge cases
def test_empty_aspect(transformer):
    """Test handling of empty aspect."""
    tags = GlobalTagsClass(tags=[])
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer, mcpw)
    assert result.aspect is not None
    patch_dict = json.loads(result.aspect.value.decode())

    expected_patch_dict = {
        "arrayPrimaryKeys": {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]},
        "patch": [
            {
                "op": "add",
                "path": f"/tags/{ATTRIBUTION_SOURCE}",
                "value": {},
            }
        ],
        "forceGenericPatch": True,
    }

    assert patch_dict == expected_patch_dict


def test_unsupported_aspect_passthrough(transformer):
    """Test that unsupported aspects in MCPW pass through unchanged."""
    from datahub.metadata.schema_classes import StatusClass

    status = StatusClass(removed=False)
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer, mcpw)
    assert result == mcpw


def test_unsupported_aspect_passthrough_mcp(transformer):
    """Test that unsupported aspects in MCP pass through unchanged."""
    from datahub.metadata.schema_classes import StatusClass

    status = StatusClass(removed=False)
    mcp = MetadataChangeProposalClass(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="status",
        aspect=cast(GenericAspectClass, status),
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer, mcp)
    assert result == mcp


def test_mixed_supported_and_unsupported_aspects(transformer):
    """Test that a mix of supported and unsupported aspects: supported are transformed, unsupported pass through."""
    from datahub.metadata.schema_classes import StatusClass

    status = StatusClass(removed=False)
    mcpw_unsupported1 = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT,
    )
    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:tagA")])
    mcpw_supported = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )
    mcpw_unsupported2 = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT,
    )

    eos = EndOfStream()
    envelopes = [
        RecordEnvelope(record=mcpw_unsupported1, metadata={}),
        RecordEnvelope(record=mcpw_supported, metadata={}),
        RecordEnvelope(record=mcpw_unsupported2, metadata={}),
        RecordEnvelope(record=eos, metadata={}),
    ]
    results = list(transformer.transform(envelopes))

    assert len(results) == 4
    assert results[0].record == mcpw_unsupported1
    mcp_result = results[1].record
    assert isinstance(mcp_result, MetadataChangeProposalClass)
    assert mcp_result.entityUrn == SAMPLE_URN
    assert mcp_result.changeType == ChangeTypeClass.PATCH
    assert mcp_result.aspectName == "globalTags"
    assert mcp_result.aspect is not None
    patch_dict = json.loads(mcp_result.aspect.value.decode())
    expected_patch_dict = {
        "arrayPrimaryKeys": {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]},
        "patch": [
            {
                "op": "add",
                "path": f"/tags/{ATTRIBUTION_SOURCE}",
                "value": {
                    "urn:li:tag:tagA": {
                        "tag": "urn:li:tag:tagA",
                        "attribution": {
                            "time": ANY,
                            "actor": "urn:li:corpuser:datahub",
                            "source": ATTRIBUTION_SOURCE,
                        },
                    },
                },
            }
        ],
        "forceGenericPatch": True,
    }
    assert patch_dict == expected_patch_dict
    assert results[2].record == mcpw_unsupported2
    assert results[3].record is eos


def test_end_of_stream_passthrough(transformer):
    """Test that EndOfStream passes through."""
    eos = EndOfStream()
    result = transform_one(transformer, eos)
    assert result == eos


def test_attribution_overwrite_warning(transformer):
    """Test that overwriting existing attribution logs a warning."""
    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:tagA")])
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )

    result = transform_one(transformer, mcpw)
    assert result.aspect is not None
    patch_dict = json.loads(result.aspect.value.decode())

    expected_patch_dict = {
        "arrayPrimaryKeys": {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]},
        "patch": [
            {
                "op": "add",
                "path": f"/tags/{ATTRIBUTION_SOURCE}",
                "value": {
                    "urn:li:tag:tagA": {
                        "tag": "urn:li:tag:tagA",
                        "attribution": {
                            "time": ANY,
                            "actor": "urn:li:corpuser:datahub",
                            "source": ATTRIBUTION_SOURCE,
                        },
                    },
                },
            }
        ],
        "forceGenericPatch": True,
    }

    assert patch_dict == expected_patch_dict
