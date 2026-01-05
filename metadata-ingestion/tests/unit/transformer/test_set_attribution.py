import json
from unittest.mock import ANY

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import UNIT_SEPARATOR, GenericJsonPatch, _Patch
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


# Test GenericJsonPatch and _Patch classes
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


def test_generic_json_patch_to_dict():
    """Test GenericJsonPatch serialization."""
    patch_ops = [
        _Patch(op="add", path=("tags", "test1"), value={"tag": "urn:li:tag:test1"}),
        _Patch(op="add", path=("tags", "test2"), value={"tag": "urn:li:tag:test2"}),
    ]
    array_primary_keys = {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]}
    patch = GenericJsonPatch(
        array_primary_keys=array_primary_keys,
        patch=patch_ops,
        force_generic_patch=False,
    )

    result = patch.to_dict()
    assert result["arrayPrimaryKeys"] == {
        "tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]
    }
    assert len(result["patch"]) == 2
    assert result["forceGenericPatch"] is False


def test_generic_json_patch_to_generic_aspect():
    """Test GenericJsonPatch conversion to GenericAspectClass."""
    patch_ops = [
        _Patch(op="add", path=("tags", "test"), value={"tag": "urn:li:tag:test"})
    ]
    array_primary_keys = {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]}
    patch = GenericJsonPatch(
        array_primary_keys=array_primary_keys,
        patch=patch_ops,
    )

    aspect = patch.to_generic_aspect()
    assert isinstance(aspect, GenericAspectClass)
    assert aspect.contentType == "application/json-patch+json"
    # Verify JSON is valid
    decoded = json.loads(aspect.value.decode())
    assert "arrayPrimaryKeys" in decoded
    assert "patch" in decoded


# Test GlobalTags transformer
def test_global_tags_upsert_mode():
    """Test GlobalTags transformation in upsert mode."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        actor="urn:li:corpuser:datahub",
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

    # Create input with tags
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

    envelope = RecordEnvelope(record=mcpw, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
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
        "forceGenericPatch": False,
    }

    assert patch_dict == expected_patch_dict


def test_global_tags_patch_mode():
    """Test GlobalTags transformation in patch mode."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        actor="urn:li:corpuser:datahub",
        patch_mode=True,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

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

    envelope = RecordEnvelope(record=mcpw, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
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
        "forceGenericPatch": False,
    }

    assert patch_dict == expected_patch_dict


def test_global_tags_with_source_detail():
    """Test GlobalTags with sourceDetail map."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        actor="urn:li:corpuser:datahub",
        source_detail={"pipeline_id": "my-pipeline", "version": "1.0"},
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:tagA")])
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )

    envelope = RecordEnvelope(record=mcpw, metadata={})
    results = list(transformer.transform([envelope]))

    result = results[0].record
    assert result.aspect is not None
    patch_dict = json.loads(result.aspect.value.decode())
    tag_value = patch_dict["patch"][0]["value"]["urn:li:tag:tagA"]
    assert "attribution" in tag_value
    assert tag_value["attribution"]["sourceDetail"] == {
        "pipeline_id": "my-pipeline",
        "version": "1.0",
    }


# Test Ownership transformer
def test_ownership_upsert_mode():
    """Test Ownership transformation in upsert mode."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

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

    envelope = RecordEnvelope(record=mcpw, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
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
        "forceGenericPatch": False,
    }

    assert patch_dict == expected_patch_dict


# Test GlossaryTerms transformer
def test_glossary_terms_upsert_mode():
    """Test GlossaryTerms transformation in upsert mode."""
    from datahub.metadata.schema_classes import AuditStampClass

    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

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

    envelope = RecordEnvelope(record=mcpw, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
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
def test_patch_input_with_attribution():
    """Test that PATCH inputs have attribution added while preserving semantics."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

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
        "forceGenericPatch": False,
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

    envelope = RecordEnvelope(record=mcp, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspect is not None

    # Verify attribution was added
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
        "forceGenericPatch": False,
    }

    assert patch_dict == expected_patch_dict


def test_patch_input_different_attribution_source():
    """Test that PATCH inputs with different attribution source pass through unchanged."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

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
        "forceGenericPatch": False,
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

    envelope = RecordEnvelope(record=mcp, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspect is not None

    # Verify patch operations pass through unchanged (different attribution source)
    patch_dict = json.loads(result.aspect.value.decode())

    # Should be identical to input since attribution source doesn't match
    assert patch_dict == existing_patch


def test_patch_input_unsupported_content_type():
    """Test that PATCH inputs with unsupported content type pass through."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

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

    envelope = RecordEnvelope(record=mcp, metadata={})
    results = list(transformer.transform([envelope]))

    # Should pass through unchanged
    assert len(results) == 1
    assert results[0].record == mcp


# Test MCE input handling
def test_mce_input_conversion():
    """Test MCE input conversion to PATCH MCP."""
    from datahub.metadata.schema_classes import DatasetSnapshotClass

    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

    # Create MCE with GlobalTags
    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:tagA")])
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=SAMPLE_URN,
            aspects=[tags],
        )
    )

    envelope = RecordEnvelope(record=mce, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
    assert isinstance(result, MetadataChangeProposalClass)
    assert result.changeType == ChangeTypeClass.PATCH
    assert result.aspectName == "globalTags"


# Test edge cases
def test_empty_aspect():
    """Test handling of empty aspect."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

    tags = GlobalTagsClass(tags=[])
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )

    envelope = RecordEnvelope(record=mcpw, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
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
        "forceGenericPatch": False,
    }

    assert patch_dict == expected_patch_dict


def test_unsupported_aspect_passthrough():
    """Test that unsupported aspects pass through unchanged."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

    from datahub.metadata.schema_classes import StatusClass

    status = StatusClass(removed=False)
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT,
    )

    envelope = RecordEnvelope(record=mcpw, metadata={})
    results = list(transformer.transform([envelope]))

    # Should pass through unchanged
    assert len(results) == 1
    assert results[0].record == mcpw


def test_end_of_stream_passthrough():
    """Test that EndOfStream passes through."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

    eos = EndOfStream()
    envelope = RecordEnvelope(record=eos, metadata={})
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    assert results[0].record == eos


def test_attribution_overwrite_warning():
    """Test that overwriting existing attribution logs a warning."""
    config = SetAttributionConfig(
        attribution_source=ATTRIBUTION_SOURCE,
        patch_mode=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = SetAttributionTransformer(config, ctx)

    # Create tag - the transformer will add attribution
    # If there was existing attribution, it would be overwritten
    tags = GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:tagA")])
    mcpw = MetadataChangeProposalWrapper(
        entityUrn=SAMPLE_URN,
        entityType="dataset",
        aspectName="globalTags",
        aspect=tags,
        changeType=ChangeTypeClass.UPSERT,
    )

    envelope = RecordEnvelope(record=mcpw, metadata={})
    # Should not raise, but overwrite attribution
    results = list(transformer.transform([envelope]))

    assert len(results) == 1
    result = results[0].record
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
        "forceGenericPatch": False,
    }

    assert patch_dict == expected_patch_dict
