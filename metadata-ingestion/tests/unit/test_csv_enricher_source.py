import io
import pathlib
from typing import Dict, List, Optional, Tuple, Type, Union, cast
from unittest import mock

import boto3
import pytest
from moto import mock_aws

from datahub.configuration.common import ConfigurationError
from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.csv_enricher import CSVEnricherConfig, CSVEnricherSource
from datahub.metadata.schema_classes import (
    EditableDatasetPropertiesClass,
    EditableSchemaMetadataClass,
    GlossaryTermAssociationClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipSourceClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
)

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset.test.Test,PROD)"
)


def create_owners_list_from_urn_list(
    owner_urns: List[str], source_type: str
) -> List[OwnerClass]:
    ownership_source_type: Union[None, OwnershipSourceClass] = None
    if source_type:
        ownership_source_type = OwnershipSourceClass(type=source_type)
    owners_list = [
        OwnerClass(
            owner=owner_urn,
            type=OwnershipTypeClass.DATAOWNER,
            source=ownership_source_type,
        )
        for owner_urn in owner_urns
    ]
    return owners_list


def create_mocked_csv_enricher_source() -> CSVEnricherSource:
    ctx = PipelineContext("test-run-id")
    graph = mock.MagicMock()
    graph.get_ownership.return_value = mce_builder.make_ownership_aspect_from_urn_list(
        ["urn:li:corpuser:olduser1"], "AUDIT"
    )
    graph.get_glossary_terms.return_value = (
        mce_builder.make_glossary_terms_aspect_from_urn_list(
            ["urn:li:glossaryTerm:oldterm1", "urn:li:glossaryTerm:oldterm2"]
        )
    )
    graph.get_tags.return_value = mce_builder.make_global_tag_aspect_with_tag_list(
        ["oldtag1", "oldtag2"]
    )
    graph.get_aspect.return_value = None
    graph.get_aspect_v2.return_value = None
    graph.get_domain.return_value = None
    ctx.graph = graph
    return CSVEnricherSource(
        CSVEnricherConfig(**create_base_csv_enricher_config()), ctx
    )


def create_base_csv_enricher_config() -> Dict:
    return dict(
        {
            "filename": "../integration/csv_enricher/csv_enricher_test_data.csv",
            "write_semantics": "PATCH",
            "delimiter": ",",
            "array_delimiter": "|",
        },
    )


def test_get_resource_glossary_terms_work_unit_no_terms():
    source = create_mocked_csv_enricher_source()
    maybe_terms_wu = source.get_resource_glossary_terms_work_unit(DATASET_URN, [])
    assert not maybe_terms_wu


def test_get_resource_glossary_terms_no_new_glossary_terms():
    source = create_mocked_csv_enricher_source()
    new_glossary_terms = [
        "urn:li:glossaryTerm:oldterm1",
        "urn:li:glossaryTerm:oldterm2",
    ]
    term_associations: List[GlossaryTermAssociationClass] = [
        GlossaryTermAssociationClass(term) for term in new_glossary_terms
    ]
    maybe_terms_wu = source.get_resource_glossary_terms_work_unit(
        DATASET_URN, term_associations
    )
    assert not maybe_terms_wu


def test_get_resource_glossary_terms_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_glossary_terms = [
        "urn:li:glossaryTerm:newterm1",
        "urn:li:glossaryTerm:newterm2",
    ]
    term_associations: List[GlossaryTermAssociationClass] = [
        GlossaryTermAssociationClass(term) for term in new_glossary_terms
    ]
    maybe_terms_wu = source.get_resource_glossary_terms_work_unit(
        DATASET_URN, term_associations
    )
    assert maybe_terms_wu


def test_get_resource_tags_work_unit_no_tags():
    source = create_mocked_csv_enricher_source()
    maybe_tags_wu = source.get_resource_tags_work_unit(DATASET_URN, [])
    assert not maybe_tags_wu


def test_get_resource_tags_no_new_tags():
    source = create_mocked_csv_enricher_source()
    new_tags = ["urn:li:tag:oldtag1", "urn:li:tag:oldtag2"]
    tag_associations: List[TagAssociationClass] = [
        TagAssociationClass(tag) for tag in new_tags
    ]
    maybe_tags_wu = source.get_resource_tags_work_unit(DATASET_URN, tag_associations)
    assert not maybe_tags_wu


def test_get_resource_tags_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_tags = ["urn:li:tag:newtag1", "urn:li:tag:newtag2"]
    tag_associations: List[TagAssociationClass] = [
        TagAssociationClass(tag) for tag in new_tags
    ]
    maybe_tags_wu = source.get_resource_tags_work_unit(DATASET_URN, tag_associations)
    assert maybe_tags_wu


def test_get_resource_owners_work_unit_no_terms():
    source = create_mocked_csv_enricher_source()
    maybe_owners_wu = source.get_resource_owners_work_unit(DATASET_URN, [])
    assert not maybe_owners_wu


def test_get_resource_owners_no_new_owners():
    source = create_mocked_csv_enricher_source()
    new_owners = ["urn:li:corpuser:owner1", "urn:li:corpuser:owner2"]
    owners: List[OwnerClass] = [
        OwnerClass(owner, type=OwnershipTypeClass.NONE) for owner in new_owners
    ]
    maybe_owners_wu = source.get_resource_owners_work_unit(DATASET_URN, owners)
    assert maybe_owners_wu


def test_maybe_extract_owners_ownership_type_urn():
    source = create_mocked_csv_enricher_source()
    row = {
        "resource": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "owners": "urn:li:corpuser:datahub",
        "ownership_type": "TECHNICAL_OWNER",
    }
    assert source.maybe_extract_owners(row=row, is_resource_row=True) == [
        OwnerClass(
            owner="urn:li:corpuser:datahub", type=OwnershipTypeClass.TECHNICAL_OWNER
        )
    ]

    row2 = {
        "resource": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "owners": "urn:li:corpuser:datahub",
        "ownership_type": "TECHNICAL_OWNER",
        "ownership_type_urn": "urn:li:ownershipType:technical_owner",
    }
    assert source.maybe_extract_owners(row=row2, is_resource_row=True) == [
        OwnerClass(
            owner="urn:li:corpuser:datahub",
            type=OwnershipTypeClass.CUSTOM,
            typeUrn="urn:li:ownershipType:technical_owner",
        )
    ]


def test_get_resource_owners_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_owners = ["urn:li:corpuser:owner1", "urn:li:corpuser:owner2"]
    owners: List[OwnerClass] = [
        OwnerClass(owner, type=OwnershipTypeClass.NONE) for owner in new_owners
    ]
    maybe_owners_wu = source.get_resource_owners_work_unit(DATASET_URN, owners)
    assert maybe_owners_wu


def test_get_resource_description_no_description():
    source = create_mocked_csv_enricher_source()
    new_description = None
    maybe_description_wu = source.get_resource_description_work_unit(
        DATASET_URN, new_description
    )
    assert not maybe_description_wu


def test_get_resource_description_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_description = "description"
    maybe_description_wu = source.get_resource_description_work_unit(
        DATASET_URN, new_description
    )
    assert maybe_description_wu


def test_get_resource_domain_no_domain():
    source = create_mocked_csv_enricher_source()
    new_domain = None
    maybe_domain_wu = source.get_resource_domain_work_unit(DATASET_URN, new_domain)
    assert not maybe_domain_wu


def test_get_resource_domain_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    new_domain = "domain"
    maybe_domain_wu = source.get_resource_domain_work_unit(DATASET_URN, new_domain)
    assert maybe_domain_wu


def test_maybe_extract_structured_properties_requires_mapping():
    source = create_mocked_csv_enricher_source()
    row = {
        "resource": DATASET_URN,
        "subresource": "",
        "classification": "Sensitive",
        "owner_team": "Finance",
    }

    structured_properties = source.maybe_extract_structured_properties(
        row=row,
    )
    assert structured_properties == []


def test_maybe_extract_structured_properties_from_config_mapping():
    source = CSVEnricherSource(
        CSVEnricherConfig(
            **{
                **create_base_csv_enricher_config(),
                "write_semantics": "OVERRIDE",
                "structured_properties": {
                    "tier": "io.acryl.metadata.tier",
                    "classification": "urn:li:structuredProperty:io.acryl.privacy.classification",
                },
            }
        ),
        PipelineContext("test-run-id"),
    )

    row = {
        "resource": DATASET_URN,
        "subresource": "",
        "tier": "Gold",
        "classification": "Confidential",
    }

    structured_properties = source.maybe_extract_structured_properties(
        row=row,
    )

    assert structured_properties == [
        StructuredPropertyValueAssignmentClass(
            propertyUrn="urn:li:structuredProperty:io.acryl.metadata.tier",
            values=["Gold"],
        ),
        StructuredPropertyValueAssignmentClass(
            propertyUrn="urn:li:structuredProperty:io.acryl.privacy.classification",
            values=["Confidential"],
        ),
    ]


def test_get_resource_structured_properties_work_unit_produced():
    source = create_mocked_csv_enricher_source()
    structured_properties = [
        StructuredPropertyValueAssignmentClass(
            propertyUrn="urn:li:structuredProperty:io.acryl.test.classification",
            values=["Sensitive"],
        )
    ]

    maybe_wu = source.get_resource_structured_properties_work_unit(
        DATASET_URN,
        structured_properties,
    )
    assert maybe_wu


def test_get_resource_structured_properties_no_new_values():
    source = create_mocked_csv_enricher_source()
    graph = cast(mock.MagicMock, source.ctx.graph)
    graph.get_aspect.return_value = StructuredPropertiesClass(
        properties=[
            StructuredPropertyValueAssignmentClass(
                propertyUrn="urn:li:structuredProperty:io.acryl.test.classification",
                values=["Sensitive"],
            )
        ]
    )

    structured_properties = [
        StructuredPropertyValueAssignmentClass(
            propertyUrn="urn:li:structuredProperty:io.acryl.test.classification",
            values=["Sensitive"],
        )
    ]
    maybe_wu = source.get_resource_structured_properties_work_unit(
        DATASET_URN,
        structured_properties,
    )
    assert not maybe_wu


def test_get_workunits_internal_emits_structured_properties(tmp_path):
    csv_content = """resource,subresource,glossary_terms,tags,owners,ownership_type,description,domain,classification,owner_team
\"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)\",,,,,,,,Sensitive,Finance
"""
    csv_file = tmp_path / "structured_properties.csv"
    csv_file.write_text(csv_content)

    source = CSVEnricherSource(
        CSVEnricherConfig(
            filename=str(csv_file),
            write_semantics="OVERRIDE",
            delimiter=",",
            array_delimiter="|",
            structured_properties={
                "classification": "io.acryl.test.classification",
                "owner_team": "ownerTeam",
            },
        ),
        PipelineContext("test-run-id"),
    )

    workunits = list(source.get_workunits_internal())
    structured_properties_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, StructuredPropertiesClass)
    ]

    assert len(structured_properties_mcps) == 1
    aspect = structured_properties_mcps[0].aspect
    assert isinstance(aspect, StructuredPropertiesClass)
    assert {(prop.propertyUrn, tuple(prop.values)) for prop in aspect.properties} == {
        ("urn:li:structuredProperty:io.acryl.test.classification", ("Sensitive",)),
        ("urn:li:structuredProperty:ownerTeam", ("Finance",)),
    }


HIVE_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"
CLASSIFICATION_URN = "urn:li:structuredProperty:io.acryl.test.classification"
CSV_HEADER = "resource,subresource,glossary_terms,tags,owners,ownership_type,description,domain,classification"


def make_schema_metadata(field_paths: List[str]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:hive",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=field_path,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            )
            for field_path in field_paths
        ],
    )


def schema_field_urn(field_path: str) -> str:
    return mce_builder.make_schema_field_urn(HIVE_DATASET_URN, field_path)


def make_graph(
    schema: Optional[SchemaMetadataClass],
    existing: Optional[Dict[str, StructuredPropertiesClass]] = None,
) -> mock.MagicMock:
    """Mock graph whose aspects are keyed on (entity urn, aspect type)."""
    aspects: Dict[Tuple[str, Type], object] = {
        (HIVE_DATASET_URN, SchemaMetadataClass): schema,
        **{
            (urn, StructuredPropertiesClass): aspect
            for urn, aspect in (existing or {}).items()
        },
    }
    graph = mock.MagicMock()
    graph.get_aspect.side_effect = lambda entity_urn, aspect_type: aspects.get(
        (entity_urn, aspect_type)
    )
    graph.get_schema_metadata.side_effect = lambda entity_urn: aspects.get(
        (entity_urn, SchemaMetadataClass)
    )
    return graph


def run_csv(
    tmp_path: pathlib.Path,
    rows: List[str],
    graph: Optional[mock.MagicMock],
    write_semantics: str = "PATCH",
    header: str = CSV_HEADER,
) -> Tuple[CSVEnricherSource, List[MetadataChangeProposalWrapper]]:
    csv_file = tmp_path / "enricher.csv"
    csv_file.write_text("\n".join([header, *rows]) + "\n")
    ctx = PipelineContext("test-run-id")
    ctx.graph = graph
    source = CSVEnricherSource(
        CSVEnricherConfig(
            filename=str(csv_file),
            write_semantics=write_semantics,
            delimiter=",",
            array_delimiter="|",
            structured_properties={
                "classification": "io.acryl.test.classification",
                "tier": "io.acryl.test.tier",
            },
        ),
        ctx,
    )
    mcps = [
        wu.metadata
        for wu in source.get_workunits_internal()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]
    return source, mcps


def column_row(subresource: str, value: str = "Sensitive", tags: str = "") -> str:
    return f'"{HIVE_DATASET_URN}",{subresource},,{tags},,,,,{value}'


def structured_properties_by_urn(
    mcps: List[MetadataChangeProposalWrapper],
) -> Dict[str, List[Tuple[str, List[Union[str, float]]]]]:
    result: Dict[str, List[Tuple[str, List[Union[str, float]]]]] = {}
    for mcp in mcps:
        if isinstance(mcp.aspect, StructuredPropertiesClass):
            assert mcp.entityUrn and mcp.entityUrn not in result, "one write per entity"
            result[mcp.entityUrn] = [
                (p.propertyUrn, p.values) for p in mcp.aspect.properties
            ]
    return result


def warning_contexts(source: CSVEnricherSource) -> List[str]:
    return [context for entry in source.report.warnings for context in entry.context]


def test_column_row_emits_structured_properties_on_schema_field(tmp_path):
    source, mcps = run_csv(
        tmp_path, [column_row("col_a")], graph=None, write_semantics="OVERRIDE"
    )

    assert structured_properties_by_urn(mcps) == {
        schema_field_urn("col_a"): [(CLASSIFICATION_URN, ["Sensitive"])]
    }
    assert source.report.num_schema_field_structured_properties_workunits_produced == 1


def test_column_structured_properties_only_row_does_not_override_column_metadata(
    tmp_path,
):
    # An OVERRIDE run must not replace editableSchemaMetadata (column tags,
    # terms, descriptions) for a row that only sets structured properties.
    _, mcps = run_csv(
        tmp_path, [column_row("col_a")], graph=None, write_semantics="OVERRIDE"
    )

    assert not [m for m in mcps if isinstance(m.aspect, EditableSchemaMetadataClass)]


def test_column_structured_properties_patch_merges_existing_values(tmp_path):
    graph = make_graph(
        schema=make_schema_metadata(["col_a"]),
        existing={
            schema_field_urn("col_a"): StructuredPropertiesClass(
                properties=[
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn=CLASSIFICATION_URN, values=["Internal"]
                    )
                ]
            )
        },
    )
    _, mcps = run_csv(tmp_path, [column_row("col_a")], graph=graph)

    assert structured_properties_by_urn(mcps) == {
        schema_field_urn("col_a"): [(CLASSIFICATION_URN, ["Internal", "Sensitive"])]
    }


def test_column_structured_properties_override_with_graph_replaces_values(tmp_path):
    v2_path = "[version=2.0].[type=struct].address.[type=string].zip"
    graph = make_graph(
        schema=make_schema_metadata([v2_path]),
        existing={
            schema_field_urn(v2_path): StructuredPropertiesClass(
                properties=[
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn=CLASSIFICATION_URN, values=["Internal"]
                    )
                ]
            )
        },
    )
    _, mcps = run_csv(
        tmp_path, [column_row("address.zip")], graph=graph, write_semantics="OVERRIDE"
    )

    assert structured_properties_by_urn(mcps) == {
        schema_field_urn(v2_path): [(CLASSIFICATION_URN, ["Sensitive"])]
    }


def test_column_structured_properties_resolves_v2_field_path(tmp_path):
    v2_path = "[version=2.0].[type=struct].address.[type=string].zip"
    graph = make_graph(schema=make_schema_metadata([v2_path]))
    source, mcps = run_csv(tmp_path, [column_row("address.zip")], graph=graph)

    assert list(structured_properties_by_urn(mcps)) == [schema_field_urn(v2_path)]
    assert not source.report.warnings


def test_column_structured_properties_prefers_value_field_over_key_field(tmp_path):
    key_path = "[version=2.0].[key=True].[type=string].id"
    value_path = "[version=2.0].[type=string].id"
    graph = make_graph(schema=make_schema_metadata([key_path, value_path]))
    _, mcps = run_csv(tmp_path, [column_row("id")], graph=graph)

    assert list(structured_properties_by_urn(mcps)) == [schema_field_urn(value_path)]


def test_column_structured_properties_skips_field_missing_from_schema(tmp_path):
    graph = make_graph(schema=make_schema_metadata(["col_a"]))
    source, mcps = run_csv(tmp_path, [column_row("col_missing")], graph=graph)

    assert structured_properties_by_urn(mcps) == {}
    assert len(warning_contexts(source)) == 1


def test_column_structured_properties_without_schema_uses_csv_path(tmp_path):
    graph = make_graph(schema=None)
    source, mcps = run_csv(
        tmp_path, [column_row("col_a"), column_row("col_b")], graph=graph
    )

    assert list(structured_properties_by_urn(mcps)) == [
        schema_field_urn("col_a"),
        schema_field_urn("col_b"),
    ]
    # One schema lookup and one warning per dataset, not per row.
    assert graph.get_schema_metadata.call_count == 1
    assert len(warning_contexts(source)) == 1


def test_column_structured_properties_schema_lookup_error_falls_back(tmp_path):
    graph = make_graph(schema=None)
    graph.get_schema_metadata.side_effect = RuntimeError("503 Service Unavailable")
    source, mcps = run_csv(
        tmp_path,
        [column_row("col_a"), column_row("col_b")],
        graph=graph,
        write_semantics="OVERRIDE",
    )

    assert list(structured_properties_by_urn(mcps)) == [
        schema_field_urn("col_a"),
        schema_field_urn("col_b"),
    ]
    assert graph.get_schema_metadata.call_count == 1
    assert len(warning_contexts(source)) == 1


def test_column_structured_properties_duplicate_rows_merge_into_one_write(tmp_path):
    graph = make_graph(
        schema=make_schema_metadata(["col_a"]),
        existing={
            schema_field_urn("col_a"): StructuredPropertiesClass(
                properties=[
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn=CLASSIFICATION_URN, values=["Internal"]
                    )
                ]
            )
        },
    )
    _, mcps = run_csv(
        tmp_path,
        [column_row("col_a", "Sensitive"), column_row("col_a", "Restricted")],
        graph=graph,
    )

    assert structured_properties_by_urn(mcps) == {
        schema_field_urn("col_a"): [
            (CLASSIFICATION_URN, ["Internal", "Sensitive", "Restricted"])
        ]
    }


def test_non_dataset_subresource_row_warns(tmp_path):
    source, mcps = run_csv(
        tmp_path,
        ['"urn:li:chart:(looker,baz1)",col_a,,,,,,,Sensitive'],
        graph=None,
        write_semantics="OVERRIDE",
    )

    assert mcps == []
    assert len(warning_contexts(source)) == 1


def test_column_structured_properties_short_row(tmp_path):
    # csv.DictReader fills columns missing from a short row; the mapped but
    # missing "tier" column must be treated as empty, not crash.
    source, mcps = run_csv(
        tmp_path,
        [column_row("col_a")],
        graph=None,
        write_semantics="OVERRIDE",
        header=f"{CSV_HEADER},tier",
    )

    assert structured_properties_by_urn(mcps) == {
        schema_field_urn("col_a"): [(CLASSIFICATION_URN, ["Sensitive"])]
    }


def test_column_structured_properties_duplicate_rows_same_value_dedupe(tmp_path):
    _, mcps = run_csv(
        tmp_path,
        [column_row("col_a"), column_row("col_a")],
        graph=None,
        write_semantics="OVERRIDE",
    )

    assert structured_properties_by_urn(mcps) == {
        schema_field_urn("col_a"): [(CLASSIFICATION_URN, ["Sensitive"])]
    }


def test_column_structured_properties_accepts_exact_v2_field_path(tmp_path):
    v2_path = "[version=2.0].[type=struct].address.[type=string].zip"
    graph = make_graph(schema=make_schema_metadata([v2_path]))
    source, mcps = run_csv(tmp_path, [column_row(f'"{v2_path}"')], graph=graph)

    assert list(structured_properties_by_urn(mcps)) == [schema_field_urn(v2_path)]
    assert not source.report.warnings


CSV_WITH_DESCRIPTION = (
    "resource,subresource,glossary_terms,tags,owners,ownership_type,description,domain\n"
    f'"{DATASET_URN}",,,,,,Remote description,\n'
)


def _read_descriptions(filename: str) -> List[Optional[str]]:
    source = CSVEnricherSource(
        CSVEnricherConfig(
            filename=filename,
            write_semantics="OVERRIDE",
            delimiter=",",
            array_delimiter="|",
        ),
        PipelineContext("test-run-id"),
    )
    descriptions = []
    for wu in source.get_workunits_internal():
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        if isinstance(wu.metadata.aspect, EditableDatasetPropertiesClass):
            descriptions.append(wu.metadata.aspect.description)
    return descriptions


def test_get_workunits_internal_reads_local_file_with_bom(tmp_path):
    csv_file = tmp_path / "bom.csv"
    csv_file.write_bytes(CSV_WITH_DESCRIPTION.encode("utf-8-sig"))

    assert _read_descriptions(str(csv_file)) == ["Remote description"]


@pytest.mark.parametrize(
    "url", ["http://example.com/file.csv", "https://example.com/file.csv?sig=abc"]
)
def test_get_workunits_internal_reads_http_via_fs(url):
    with mock.patch("datahub.ingestion.fs.http_fs.smart_open") as mock_smart_open:
        mock_smart_open.open.return_value = io.BytesIO(
            CSV_WITH_DESCRIPTION.encode("utf-8-sig")
        )
        assert _read_descriptions(url) == ["Remote description"]
    mock_smart_open.open.assert_called_once_with(url, mode="rb", transport_params={})


def test_get_workunits_internal_reads_s3_via_fs():
    with (
        mock.patch("datahub.ingestion.fs.s3_fs.boto3") as mock_boto3,
        mock.patch("datahub.ingestion.fs.s3_fs.smart_open") as mock_smart_open,
    ):
        mock_smart_open.open.return_value = io.BytesIO(
            CSV_WITH_DESCRIPTION.encode("utf-8")
        )
        assert _read_descriptions("s3://my-bucket/enrich.csv") == ["Remote description"]
    mock_smart_open.open.assert_called_once_with(
        "s3://my-bucket/enrich.csv",
        mode="rb",
        transport_params={"client": mock_boto3.client.return_value},
    )


def test_get_workunits_internal_remote_read_error_is_configuration_error():
    read_error = OSError("404 Not Found")
    with mock.patch("datahub.ingestion.fs.http_fs.smart_open") as mock_smart_open:
        mock_smart_open.open.side_effect = read_error
        with pytest.raises(
            ConfigurationError, match="Cannot read remote file"
        ) as exc_info:
            _read_descriptions("https://example.com/missing.csv")
    mock_smart_open.open.assert_called_once()
    assert exc_info.value.__cause__ is read_error


def test_get_workunits_internal_missing_local_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        _read_descriptions(str(tmp_path / "missing.csv"))
    # A Windows drive letter parses as a one-letter URL scheme; it must still be
    # treated as a local path.
    with pytest.raises(FileNotFoundError):
        _read_descriptions(r"C:\no\such\dir\enrich.csv")


def test_config_rejects_unsupported_scheme():
    with pytest.raises(ValueError, match="file, http, https, s3"):
        CSVEnricherConfig(filename="gs://my-bucket/enrich.csv")


def test_missing_fs_dependency_suggests_csv_enricher_extra():
    with (
        mock.patch(
            "datahub.ingestion.source.csv_enricher.fs_registry.get",
            side_effect=ConfigurationError(
                "s3 is disabled due to a missing dependency"
            ),
        ),
        pytest.raises(ConfigurationError, match=r"acryl-datahub\[csv-enricher\]"),
    ):
        _read_descriptions("s3://my-bucket/enrich.csv")


def test_remote_csv_decode_error_is_not_reported_as_read_error():
    with mock.patch("datahub.ingestion.fs.http_fs.smart_open") as mock_smart_open:
        mock_smart_open.open.return_value = io.BytesIO(b"resource\n\xff\xfe\n")
        with pytest.raises(UnicodeDecodeError):
            _read_descriptions("https://example.com/latin1.csv")


@mock_aws
def test_get_workunits_internal_reads_s3_with_moto(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="my-bucket")
    s3.put_object(
        Bucket="my-bucket",
        Key="enrich.csv",
        Body=CSV_WITH_DESCRIPTION.encode("utf-8-sig"),
    )

    assert _read_descriptions("s3://my-bucket/enrich.csv") == ["Remote description"]
