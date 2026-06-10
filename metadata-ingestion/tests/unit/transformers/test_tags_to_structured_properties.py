from datahub.emitter.mce_builder import make_tag_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.tags_to_structured_properties import (
    StructuredPropertyMappingConfig,
    TagsToStructuredPropertiesConfig,
    TagsToStructuredPropertiesTransformer,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    StructuredPropertiesClass,
    TagAssociationClass,
)


def test_key_value_tag_parsing_default_separator():
    """Test parsing key-value tags with default : separator."""
    config = TagsToStructuredPropertiesConfig(
        process_key_value_tags=True,
        key_value_separator=":",
        key_value_property_prefix="io.company.",
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    # Create tags aspect with key-value formatted tags
    tags = GlobalTagsClass(
        tags=[
            TagAssociationClass(tag=make_tag_urn("dept:Finance")),
            TagAssociationClass(tag=make_tag_urn("tier:gold")),
        ]
    )

    # Transform
    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    # Should have 2 aspects: globalTags + structuredProperties
    assert len(output) == 2
    assert output[0][0] == "globalTags"
    assert output[1][0] == "structuredProperties"

    # Check structured properties
    struct_props = output[1][1]
    assert isinstance(struct_props, StructuredPropertiesClass)
    assert len(struct_props.properties) == 2

    # Verify property URNs and values
    props_dict = {p.propertyUrn: p.values for p in struct_props.properties}
    assert "urn:li:structuredProperty:io.company.dept" in props_dict
    assert props_dict["urn:li:structuredProperty:io.company.dept"] == ["Finance"]
    assert "urn:li:structuredProperty:io.company.tier" in props_dict
    assert props_dict["urn:li:structuredProperty:io.company.tier"] == ["gold"]


def test_key_value_tag_parsing_custom_separator():
    """Test parsing key-value tags with custom separator."""
    config = TagsToStructuredPropertiesConfig(
        process_key_value_tags=True,
        key_value_separator="=",
        key_value_property_prefix="",
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    tags = GlobalTagsClass(
        tags=[TagAssociationClass(tag=make_tag_urn("status=active"))]
    )

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    struct_props = output[1][1]
    assert isinstance(struct_props, StructuredPropertiesClass)
    props_dict = {p.propertyUrn: p.values for p in struct_props.properties}
    assert "urn:li:structuredProperty:status" in props_dict
    assert props_dict["urn:li:structuredProperty:status"] == ["active"]


def test_keyword_tag_mapping():
    """Test mapping keyword tags via configuration."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(
                values=["Finance", "Sales", "Marketing"]
            ),
            "io.company.dataClassification": StructuredPropertyMappingConfig(
                values=["PII", "Confidential"]
            ),
        }
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    tags = GlobalTagsClass(
        tags=[
            TagAssociationClass(tag=make_tag_urn("Finance")),
            TagAssociationClass(tag=make_tag_urn("PII")),
            TagAssociationClass(tag=make_tag_urn("Sales")),
        ]
    )

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    struct_props = output[1][1]
    assert isinstance(struct_props, StructuredPropertiesClass)
    props_dict = {p.propertyUrn: p.values for p in struct_props.properties}

    # Finance and Sales should map to department
    assert "urn:li:structuredProperty:io.company.department" in props_dict
    assert set(props_dict["urn:li:structuredProperty:io.company.department"]) == {
        "Finance",
        "Sales",
    }

    # PII should map to dataClassification
    assert "urn:li:structuredProperty:io.company.dataClassification" in props_dict
    assert props_dict["urn:li:structuredProperty:io.company.dataClassification"] == [
        "PII"
    ]


def test_remove_original_tags():
    """Test that original tags are removed when configured."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(values=["Finance"])
        },
        remove_original_tags=True,
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag=make_tag_urn("Finance"))])

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    # First aspect should be None (removing tags)
    assert output[0][0] == "globalTags"
    assert output[0][1] is None

    # Second aspect should be structured properties
    assert output[1][0] == "structuredProperties"
    assert output[1][1] is not None


def test_keep_original_tags():
    """Test that original tags are kept when configured."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(values=["Finance"])
        },
        remove_original_tags=False,
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    original_tags = GlobalTagsClass(
        tags=[TagAssociationClass(tag=make_tag_urn("Finance"))]
    )

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=original_tags,
        )
    )

    # First aspect should be the original tags
    assert output[0][0] == "globalTags"
    assert output[0][1] == original_tags


def test_unmatched_tags_warning(caplog):
    """Test that unmatched tags generate warnings."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(values=["Finance"])
        }
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    tags = GlobalTagsClass(
        tags=[
            TagAssociationClass(tag=make_tag_urn("UnknownTag")),
            TagAssociationClass(tag=make_tag_urn("AnotherUnknown")),
        ]
    )

    with caplog.at_level("WARNING"):
        output = list(
            transformer.transform_aspects(
                entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
                aspect_name="globalTags",
                aspect=tags,
            )
        )

    # Should have warnings for unmatched tags
    assert "UnknownTag" in caplog.text
    assert "AnotherUnknown" in caplog.text

    # Should only return globalTags, no structuredProperties (no matches)
    assert len(output) == 1
    assert output[0][0] == "globalTags"


def test_multiple_tags_same_property():
    """Test multiple tags mapping to same property."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(
                values=["Finance", "Sales", "Marketing"]
            )
        }
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    tags = GlobalTagsClass(
        tags=[
            TagAssociationClass(tag=make_tag_urn("Finance")),
            TagAssociationClass(tag=make_tag_urn("Marketing")),
        ]
    )

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    struct_props = output[1][1]
    assert isinstance(struct_props, StructuredPropertiesClass)
    assert len(struct_props.properties) == 1

    # Both tags should be values of the same property
    prop = struct_props.properties[0]
    assert prop.propertyUrn == "urn:li:structuredProperty:io.company.department"
    assert set(prop.values) == {"Finance", "Marketing"}


def test_empty_tags():
    """Test handling of empty tags."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(values=["Finance"])
        }
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    tags = GlobalTagsClass(tags=[])

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    # Should return empty list for empty tags
    assert len(output) == 0


def test_none_aspect():
    """Test handling of None aspect."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(values=["Finance"])
        }
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=None,
        )
    )

    # Should return empty list for None aspect
    assert len(output) == 0


def test_combined_key_value_and_keyword():
    """Test combination of key-value and keyword mapping."""
    config = TagsToStructuredPropertiesConfig(
        process_key_value_tags=True,
        key_value_separator=":",
        key_value_property_prefix="io.kv.",
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(
                values=["Finance", "Sales"]
            )
        },
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    tags = GlobalTagsClass(
        tags=[
            TagAssociationClass(tag=make_tag_urn("dept:Engineering")),  # key-value
            TagAssociationClass(tag=make_tag_urn("Finance")),  # keyword
            TagAssociationClass(tag=make_tag_urn("tier:gold")),  # key-value
        ]
    )

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    struct_props = output[1][1]
    assert isinstance(struct_props, StructuredPropertiesClass)
    props_dict = {p.propertyUrn: p.values for p in struct_props.properties}

    # Key-value tags
    assert "urn:li:structuredProperty:io.kv.dept" in props_dict
    assert props_dict["urn:li:structuredProperty:io.kv.dept"] == ["Engineering"]
    assert "urn:li:structuredProperty:io.kv.tier" in props_dict
    assert props_dict["urn:li:structuredProperty:io.kv.tier"] == ["gold"]

    # Keyword tag
    assert "urn:li:structuredProperty:io.company.department" in props_dict
    assert props_dict["urn:li:structuredProperty:io.company.department"] == ["Finance"]


def test_entity_types():
    """Test that transformer applies to correct entity types."""
    config = TagsToStructuredPropertiesConfig()
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    entity_types = transformer.entity_types()

    # Should support multiple entity types
    assert "dataset" in entity_types
    assert "chart" in entity_types
    assert "dashboard" in entity_types
    assert "dataJob" in entity_types
    assert "dataFlow" in entity_types
    assert "schemaField" in entity_types


def test_aspect_name():
    """Test that transformer subscribes to globalTags."""
    config = TagsToStructuredPropertiesConfig()
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    assert transformer.aspect_name() == "globalTags"


def test_extract_tag_name():
    """Test tag name extraction from URN."""
    config = TagsToStructuredPropertiesConfig()
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    assert transformer._extract_tag_name("urn:li:tag:Finance") == "Finance"
    assert transformer._extract_tag_name("urn:li:tag:dept:Sales") == "dept:Sales"


def test_parse_key_value_tag_helper():
    """Test the key-value parsing helper method."""
    config = TagsToStructuredPropertiesConfig(
        process_key_value_tags=True, key_value_separator=":"
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    # Valid key-value
    assert transformer._parse_key_value_tag("dept:Finance") == ("dept", "Finance")
    assert transformer._parse_key_value_tag("a:b:c") == ("a", "b:c")  # Only split once

    # Invalid key-value
    assert transformer._parse_key_value_tag("Finance") is None
    assert transformer._parse_key_value_tag("") is None


def test_find_property_for_keyword_helper():
    """Test the keyword lookup helper method."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(
                values=["Finance", "Sales"]
            ),
            "io.company.sensitivity": StructuredPropertyMappingConfig(values=["PII"]),
        }
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    assert transformer._find_property_for_keyword("Finance") == "io.company.department"
    assert transformer._find_property_for_keyword("Sales") == "io.company.department"
    assert transformer._find_property_for_keyword("PII") == "io.company.sensitivity"
    assert transformer._find_property_for_keyword("Unknown") is None


def test_key_value_takes_precedence():
    """Test that key-value parsing takes precedence over keyword mapping."""
    config = TagsToStructuredPropertiesConfig(
        process_key_value_tags=True,
        key_value_separator=":",
        key_value_property_prefix="io.kv.",
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(
                values=["dept:Finance"]
            )
        },
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    # Tag that could match both patterns
    tags = GlobalTagsClass(tags=[TagAssociationClass(tag=make_tag_urn("dept:Finance"))])

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    struct_props = output[1][1]
    assert isinstance(struct_props, StructuredPropertiesClass)
    props_dict = {p.propertyUrn: p.values for p in struct_props.properties}

    # Should use key-value parsing, not keyword mapping
    assert "urn:li:structuredProperty:io.kv.dept" in props_dict
    assert props_dict["urn:li:structuredProperty:io.kv.dept"] == ["Finance"]
    assert "urn:li:structuredProperty:io.company.department" not in props_dict


def test_no_properties_mapped():
    """Test when no tags match any configuration."""
    config = TagsToStructuredPropertiesConfig(
        tag_structured_property_map={
            "io.company.department": StructuredPropertyMappingConfig(values=["Finance"])
        }
    )
    ctx = PipelineContext(run_id="test")
    transformer = TagsToStructuredPropertiesTransformer(config, ctx)

    tags = GlobalTagsClass(tags=[TagAssociationClass(tag=make_tag_urn("UnknownTag"))])

    output = list(
        transformer.transform_aspects(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)",
            aspect_name="globalTags",
            aspect=tags,
        )
    )

    # Should only return globalTags, no structuredProperties
    assert len(output) == 1
    assert output[0][0] == "globalTags"
    assert output[0][1] == tags
