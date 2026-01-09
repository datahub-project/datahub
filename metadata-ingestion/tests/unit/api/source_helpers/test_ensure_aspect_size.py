import json
import time
from unittest.mock import patch

import pytest
from freezegun.api import freeze_time

from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import INGEST_MAX_PAYLOAD_BYTES
from datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size import (
    EnsureAspectSizeProcessor,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DatasetFieldProfileClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetSnapshotClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    NumberTypeClass,
    OtherSchemaClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    QuerySubjectClass,
    QuerySubjectsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)


@pytest.fixture
def processor():
    return EnsureAspectSizeProcessor(SourceReport())


def too_big_schema_metadata() -> SchemaMetadataClass:
    fields = [
        SchemaFieldClass(
            "aaaa",
            nativeDataType="int",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        ),
        SchemaFieldClass(
            "bbbb",
            nativeDataType="string",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        ),
        SchemaFieldClass(
            "cccc",
            nativeDataType="int",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        ),
    ]
    # simple int type field takes ~160 bytes in JSON representation, below is to assure we exceed the threshold
    for f_no in range(1000):
        fields.append(
            SchemaFieldClass(
                fieldPath=f"t{f_no}",
                nativeDataType="int",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                description=20000 * "a",
            )
        )

    # adding small field to check whether it will still be present in the output
    fields.append(
        SchemaFieldClass(
            "dddd",
            nativeDataType="int",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        )
    )
    return SchemaMetadataClass(
        schemaName="abcdef",
        version=1,
        platform="s3",
        hash="ABCDE1234567890",
        platformSchema=OtherSchemaClass(rawSchema="aaa"),
        fields=fields,
    )


def proper_schema_metadata() -> SchemaMetadataClass:
    fields = [
        SchemaFieldClass(
            "aaaa",
            nativeDataType="int",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        ),
        SchemaFieldClass(
            "bbbb",
            nativeDataType="string",
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        ),
        SchemaFieldClass(
            "cccc",
            nativeDataType="int",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        ),
    ]
    return SchemaMetadataClass(
        schemaName="abcdef",
        version=1,
        platform="s3",
        hash="ABCDE1234567890",
        platformSchema=OtherSchemaClass(rawSchema="aaa"),
        fields=fields,
    )


def proper_query_subjects() -> QuerySubjectsClass:
    subjects = [
        QuerySubjectClass(
            entity="urn:li:dataset:(urn:li:dataPlatform:hive,db1.table1,PROD)"
        ),
        QuerySubjectClass(
            entity="urn:li:dataset:(urn:li:dataPlatform:hive,db1.table2,PROD)"
        ),
        QuerySubjectClass(
            entity="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db1.table1,PROD),col1)"
        ),
        QuerySubjectClass(
            entity="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db1.table2,PROD),col2)"
        ),
    ]
    return QuerySubjectsClass(subjects=subjects)


def too_big_query_subjects() -> QuerySubjectsClass:
    subjects = []

    # Add a few table-level subjects
    for i in range(5):
        subjects.append(
            QuerySubjectClass(
                entity=f"urn:li:dataset:(urn:li:dataPlatform:hive,db.table{i},PROD)"
            )
        )

    # Add many column-level subjects with very large entity URNs to exceed the 15MB constraint
    # Each URN will be about 40KB, so 500 subjects should create ~20MB of data
    for i in range(500):
        large_table_name = "a" * 20000  # Very large table name
        large_column_name = "b" * 20000  # Very large column name
        subjects.append(
            QuerySubjectClass(
                entity=f"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,{large_table_name}_{i},PROD),{large_column_name}_{i})"
            )
        )

    return QuerySubjectsClass(subjects=subjects)


def proper_upstream_lineage() -> UpstreamLineageClass:
    upstreams = [
        UpstreamClass(
            dataset="urn:li:dataset:(urn:li:dataPlatform:hive,db1.table1,PROD)",
            type=DatasetLineageTypeClass.TRANSFORMED,
        ),
        UpstreamClass(
            dataset="urn:li:dataset:(urn:li:dataPlatform:hive,db1.table2,PROD)",
            type=DatasetLineageTypeClass.TRANSFORMED,
        ),
    ]
    fine_grained_lineages = [
        FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.DATASET,
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            upstreams=["urn:li:dataset:(urn:li:dataPlatform:hive,db1.table3,PROD)"],
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db1.target,PROD),col1)"
            ],
        ),
        FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            upstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db1.table4,PROD),col2)"
            ],
            downstreams=[
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,db1.target,PROD),col2)"
            ],
        ),
    ]
    return UpstreamLineageClass(
        upstreams=upstreams, fineGrainedLineages=fine_grained_lineages
    )


def too_big_upstream_lineage() -> UpstreamLineageClass:
    upstreams = []
    fine_grained_lineages = []

    # Add upstreams (highest priority)
    for i in range(5):
        upstreams.append(
            UpstreamClass(
                dataset=f"urn:li:dataset:(urn:li:dataPlatform:hive,upstream_table_{i},PROD)",
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
        )

    # Add DATASET fine-grained lineages with large URNs
    for i in range(200):
        large_dataset_name = "a" * 20000
        large_downstream_name = "b" * 20000
        fine_grained_lineages.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.DATASET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    f"urn:li:dataset:(urn:li:dataPlatform:hive,{large_dataset_name}_{i},PROD)"
                ],
                downstreams=[
                    f"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,target,PROD),{large_downstream_name}_{i})"
                ],
            )
        )

    # Add FIELD_SET fine-grained lineages with large URNs
    for i in range(200):
        large_upstream_name = "c" * 20000
        large_downstream_name = "d" * 20000
        fine_grained_lineages.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[
                    f"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,source,PROD),{large_upstream_name}_{i})"
                ],
                downstreams=[
                    f"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,target,PROD),{large_downstream_name}_{i})"
                ],
            )
        )

    # Add NONE fine-grained lineages with large URNs (lowest priority)
    for i in range(200):
        large_downstream_name = "e" * 20000
        fine_grained_lineages.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.NONE,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[
                    f"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,target,PROD),{large_downstream_name}_{i})"
                ],
            )
        )

    return UpstreamLineageClass(
        upstreams=upstreams, fineGrainedLineages=fine_grained_lineages
    )


def proper_query_properties() -> QueryPropertiesClass:
    # Create a query properties with a reasonably sized statement (~1KB)
    query_statement = (
        "SELECT * FROM table1 WHERE column1 = 'value' AND column2 > 100;" * 20
    )

    return QueryPropertiesClass(
        statement=QueryStatementClass(
            value=query_statement,
            language=QueryLanguageClass.SQL,
        ),
        source=QuerySourceClass.SYSTEM,
        created=AuditStampClass(time=1000000000000, actor="urn:li:corpuser:test"),
        lastModified=AuditStampClass(time=1000000000000, actor="urn:li:corpuser:test"),
    )


def too_big_query_properties() -> QueryPropertiesClass:
    # Create a query properties with a very large statement (~6MB, exceeding the 5MB default limit)
    # This is larger than the QUERY_PROPERTIES_STATEMENT_MAX_PAYLOAD_BYTES default (5MB)
    large_query_statement = (
        "SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, "
        "col11, col12, col13, col14, col15, col16, col17, col18, col19, col20 "
        "FROM very_long_table_name_with_lots_of_characters_to_make_it_big "
        "WHERE condition1 = 'some_very_long_value_with_lots_of_text' "
        "AND condition2 IN ('value1', 'value2', 'value3', 'value4') "
        "ORDER BY col1, col2, col3, col4, col5 LIMIT 1000;"
    ) * 15000  # ~6MB

    return QueryPropertiesClass(
        statement=QueryStatementClass(
            value=large_query_statement,
            language=QueryLanguageClass.SQL,
        ),
        source=QuerySourceClass.SYSTEM,
        created=AuditStampClass(time=1000000000000, actor="urn:li:corpuser:test"),
        lastModified=AuditStampClass(time=1000000000000, actor="urn:li:corpuser:test"),
        name="Large Test Query",
        description="A test query with a very large statement",
    )


def proper_dataset_profile() -> DatasetProfileClass:
    sample_values = [
        "23483295",
        "234234",
        "324234",
        "12123",
        "3150314",
        "19231",
        "211",
        "93498",
        "12837",
        "73847",
        "12434",
        "33466",
        "98785",
        "4546",
        "4547",
        "342",
        "11",
        "34",
        "444",
        "38576",
    ]
    field_profiles = [
        DatasetFieldProfileClass(fieldPath="a", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="b", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="c", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="d", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="e", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="f", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="g", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="h", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="i", sampleValues=sample_values),
        DatasetFieldProfileClass(fieldPath="j", sampleValues=sample_values),
    ]
    return DatasetProfileClass(
        timestampMillis=int(time.time()) * 1000, fieldProfiles=field_profiles
    )


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_proper_dataset_profile(processor):
    profile = proper_dataset_profile()
    orig_repr = json.dumps(profile.to_obj())
    processor.ensure_dataset_profile_size(
        "urn:li:dataset:(s3, dummy_dataset, DEV)", profile
    )
    assert orig_repr == json.dumps(profile.to_obj()), (
        "Aspect was modified in case where workunit processor should have been no-op"
    )


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_too_big_schema_metadata(processor):
    schema = too_big_schema_metadata()
    assert len(schema.fields) == 1004

    processor.ensure_schema_metadata_size(
        "urn:li:dataset:(s3, dummy_dataset, DEV)", schema
    )
    assert len(schema.fields) < 1004, "Schema has not been properly truncated"
    assert schema.fields[-1].fieldPath == "dddd", "Small field was not added at the end"
    # +100kb is completely arbitrary, but we are truncating the aspect based on schema fields size only, not total taken
    # by other parameters of the aspect - it is reasonable approach though - schema fields is the only field in schema
    # metadata which can be expected to grow out of control
    assert len(json.dumps(schema.to_obj())) < INGEST_MAX_PAYLOAD_BYTES + 100000, (
        "Aspect exceeded acceptable size"
    )


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_proper_schema_metadata(processor):
    schema = proper_schema_metadata()
    orig_repr = json.dumps(schema.to_obj())
    processor.ensure_schema_metadata_size(
        "urn:li:dataset:(s3, dummy_dataset, DEV)", schema
    )
    assert orig_repr == json.dumps(schema.to_obj()), (
        "Aspect was modified in case where workunit processor should have been no-op"
    )


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_too_big_dataset_profile(processor):
    profile = proper_dataset_profile()
    big_field = DatasetFieldProfileClass(
        fieldPath="big",
        sampleValues=20 * [(int(INGEST_MAX_PAYLOAD_BYTES / 20) - 10) * "a"],
    )
    assert profile.fieldProfiles
    profile.fieldProfiles.insert(4, big_field)
    processor.ensure_dataset_profile_size(
        "urn:li:dataset:(s3, dummy_dataset, DEV)", profile
    )

    expected_profile = proper_dataset_profile()
    reduced_field = DatasetFieldProfileClass(
        fieldPath="big",
        sampleValues=[],
    )
    assert expected_profile.fieldProfiles
    expected_profile.fieldProfiles.insert(4, reduced_field)
    assert json.dumps(profile.to_obj()) == json.dumps(expected_profile.to_obj()), (
        "Field 'big' was not properly removed from aspect due to its size"
    )


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_dataset_profile_size"
)
def test_wu_processor_triggered_by_data_profile_aspect(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock, processor
):
    ret = [  # noqa: F841
        *processor.ensure_aspect_size(
            [
                MetadataChangeProposalWrapper(
                    entityUrn="urn:li:dataset:(urn:li:dataPlatform:s3, dummy_name, DEV)",
                    aspect=proper_dataset_profile(),
                ).as_workunit()
            ]
        )
    ]
    ensure_dataset_profile_size_mock.assert_called_once()
    ensure_schema_metadata_size_mock.assert_not_called()


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_dataset_profile_size"
)
def test_wu_processor_triggered_by_data_profile_aspect_mcpc(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock, processor
):
    profile_aspect = proper_dataset_profile()
    mcpc = MetadataWorkUnit(
        id="test",
        mcp_raw=MetadataChangeProposalClass(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:s3, dummy_name, DEV)",
            aspectName=DatasetProfileClass.ASPECT_NAME,
            aspect=GenericAspectClass(
                value=json.dumps(profile_aspect.to_obj()).encode(),
                contentType=JSON_CONTENT_TYPE,
            ),
        ),
    )
    ret = [*processor.ensure_aspect_size([mcpc])]  # noqa: F841
    ensure_dataset_profile_size_mock.assert_called_once()
    ensure_schema_metadata_size_mock.assert_not_called()


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_dataset_profile_size"
)
def test_wu_processor_triggered_by_data_profile_aspect_mce(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock, processor
):
    snapshot = DatasetSnapshotClass(
        urn="urn:li:dataset:(urn:li:dataPlatform:s3, dummy_name, DEV)",
        aspects=[proper_schema_metadata()],
    )
    mce = MetadataWorkUnit(
        id="test", mce=MetadataChangeEvent(proposedSnapshot=snapshot)
    )
    ret = [*processor.ensure_aspect_size([mce])]  # noqa: F841
    ensure_schema_metadata_size_mock.assert_called_once()
    ensure_dataset_profile_size_mock.assert_not_called()


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_dataset_profile_size"
)
def test_wu_processor_triggered_by_schema_metadata_aspect(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock, processor
):
    ret = [  # noqa: F841
        *processor.ensure_aspect_size(
            [
                MetadataChangeProposalWrapper(
                    entityUrn="urn:li:dataset:(urn:li:dataPlatform:s3, dummy_name, DEV)",
                    aspect=proper_schema_metadata(),
                ).as_workunit()
            ]
        )
    ]
    ensure_schema_metadata_size_mock.assert_called_once()
    ensure_dataset_profile_size_mock.assert_not_called()


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_dataset_profile_size"
)
def test_wu_processor_not_triggered_by_unhandled_aspects(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock, processor
):
    ret = [  # noqa: F841
        *processor.ensure_aspect_size(
            [
                MetadataChangeProposalWrapper(
                    entityUrn="urn:li:dataset:(urn:li:dataPlatform:s3, dummy_name, DEV)",
                    aspect=StatusClass(removed=False),
                ).as_workunit(),
                MetadataChangeProposalWrapper(
                    entityUrn="urn:li:dataset:(urn:li:dataPlatform:s3, dummy_name, DEV)",
                    aspect=SubTypesClass(typeNames=["table"]),
                ).as_workunit(),
            ]
        )
    ]
    ensure_schema_metadata_size_mock.assert_not_called()
    ensure_dataset_profile_size_mock.assert_not_called()


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_proper_query_subjects(processor):
    query_subjects = proper_query_subjects()
    orig_repr = json.dumps(query_subjects.to_obj())
    processor.ensure_query_subjects_size(
        "urn:li:query:(urn:li:dataPlatform:hive, dummy_query, DEV)", query_subjects
    )
    assert orig_repr == json.dumps(query_subjects.to_obj()), (
        "Aspect was modified in case where workunit processor should have been no-op"
    )


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_too_big_query_subjects(processor):
    query_subjects = too_big_query_subjects()
    assert len(query_subjects.subjects) == 505  # 5 table + 500 column subjects

    # Verify that the initial size exceeds the default payload constraint
    initial_size = len(json.dumps(query_subjects.to_obj()))
    expected_size = 20 * 1024 * 1024  # 20MB
    assert initial_size == pytest.approx(expected_size, rel=0.05), (
        f"Initial size {initial_size} should be around 20MB (±5%), got {initial_size / (1024 * 1024):.1f}MB"
    )
    assert initial_size > INGEST_MAX_PAYLOAD_BYTES, (
        f"Initial size {initial_size} should exceed payload constraint {INGEST_MAX_PAYLOAD_BYTES}"
    )

    processor.ensure_query_subjects_size(
        "urn:li:query:(urn:li:dataPlatform:hive, dummy_query, DEV)", query_subjects
    )

    # Should be significantly reduced due to size constraints
    # With ~20MB of data needing to be reduced to ~15MB, we expect ~25% reduction (125 subjects)
    # So final count should be around 380, using 400 as upper bound with buffer
    assert len(query_subjects.subjects) < 400, (
        "Query subjects has not been properly truncated"
    )

    # Check that table-level subjects are prioritized (should still be present)
    table_subjects = [
        s
        for s in query_subjects.subjects
        if not s.entity.startswith("urn:li:schemaField:")
    ]
    assert len(table_subjects) > 0, (
        "Table-level subjects should be prioritized and present"
    )

    # The aspect should not exceed acceptable size
    assert len(json.dumps(query_subjects.to_obj())) < INGEST_MAX_PAYLOAD_BYTES, (
        "Aspect exceeded acceptable size"
    )


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_query_subjects_size"
)
def test_wu_processor_triggered_by_query_subjects_aspect(
    ensure_query_subjects_size_mock, processor
):
    ret = [  # noqa: F841
        *processor.ensure_aspect_size(
            [
                MetadataChangeProposalWrapper(
                    entityUrn="urn:li:query:(urn:li:dataPlatform:hive, dummy_query, DEV)",
                    aspect=proper_query_subjects(),
                ).as_workunit()
            ]
        )
    ]
    ensure_query_subjects_size_mock.assert_called_once()


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_proper_upstream_lineage(processor):
    upstream_lineage = proper_upstream_lineage()
    orig_repr = json.dumps(upstream_lineage.to_obj())
    processor.ensure_upstream_lineage_size(
        "urn:li:dataset:(urn:li:dataPlatform:hive, dummy_dataset, DEV)",
        upstream_lineage,
    )
    assert orig_repr == json.dumps(upstream_lineage.to_obj()), (
        "Aspect was modified in case where workunit processor should have been no-op"
    )


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_too_big_upstream_lineage(processor):
    upstream_lineage = too_big_upstream_lineage()
    assert len(upstream_lineage.upstreams) == 5  # 5 upstreams
    assert upstream_lineage.fineGrainedLineages is not None
    assert (
        len(upstream_lineage.fineGrainedLineages) == 600
    )  # 200 DATASET + 200 FIELD_SET + 200 NONE

    # Verify that the initial size exceeds the default payload constraint
    initial_size = len(json.dumps(upstream_lineage.to_obj()))
    expected_size = 20 * 1024 * 1024  # 20MB
    assert initial_size == pytest.approx(expected_size, rel=0.05), (
        f"Initial size {initial_size} should be around 20MB (±5%), got {initial_size / (1024 * 1024):.1f}MB"
    )
    assert initial_size > INGEST_MAX_PAYLOAD_BYTES, (
        f"Initial size {initial_size} should exceed payload constraint {INGEST_MAX_PAYLOAD_BYTES}"
    )

    processor.ensure_upstream_lineage_size(
        "urn:li:dataset:(urn:li:dataPlatform:hive, dummy_dataset, DEV)",
        upstream_lineage,
    )

    # Should be significantly reduced due to size constraints
    # With ~20MB of data needing to be reduced to ~15MB, we expect ~25% reduction
    # Total items: 5 upstreams + 600 fine-grained = 605, expect around ~450 after 25% reduction
    total_items = len(upstream_lineage.upstreams) + (
        len(upstream_lineage.fineGrainedLineages)
        if upstream_lineage.fineGrainedLineages
        else 0
    )
    assert total_items < 500, "Upstream lineage has not been properly truncated"

    # Check that upstreams are prioritized (should still be present)
    assert len(upstream_lineage.upstreams) > 0, (
        "Upstreams should be prioritized and present"
    )

    # Check that DATASET fine-grained lineages are prioritized over FIELD_SET and NONE
    if upstream_lineage.fineGrainedLineages:
        dataset_count = sum(
            1
            for fg in upstream_lineage.fineGrainedLineages
            if str(fg.upstreamType) == "DATASET"
        )
        field_set_count = sum(
            1
            for fg in upstream_lineage.fineGrainedLineages
            if str(fg.upstreamType) == "FIELD_SET"
        )
        none_count = sum(
            1
            for fg in upstream_lineage.fineGrainedLineages
            if str(fg.upstreamType) == "NONE"
        )

        # DATASET should be prioritized over FIELD_SET and NONE
        assert dataset_count >= field_set_count, (
            "DATASET fine-grained lineages should be prioritized"
        )
        assert dataset_count >= none_count, (
            "DATASET fine-grained lineages should be prioritized over NONE"
        )

    # The aspect should not exceed acceptable size
    assert len(json.dumps(upstream_lineage.to_obj())) < INGEST_MAX_PAYLOAD_BYTES, (
        "Aspect exceeded acceptable size"
    )


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_upstream_lineage_size"
)
def test_wu_processor_triggered_by_upstream_lineage_aspect(
    ensure_upstream_lineage_size_mock, processor
):
    ret = [  # noqa: F841
        *processor.ensure_aspect_size(
            [
                MetadataChangeProposalWrapper(
                    entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive, dummy_dataset, DEV)",
                    aspect=proper_upstream_lineage(),
                ).as_workunit()
            ]
        )
    ]
    ensure_upstream_lineage_size_mock.assert_called_once()


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_proper_query_properties(processor):
    query_properties = proper_query_properties()
    original_statement = query_properties.statement.value

    # Verify initial size is reasonable (under 5MB)
    initial_size = len(json.dumps(query_properties.to_obj()))
    assert initial_size < 5 * 1024 * 1024, "Test query properties should be under 5MB"

    processor.ensure_query_properties_size("urn:li:query:test", query_properties)

    # Statement should remain unchanged for properly sized query properties
    assert query_properties.statement.value == original_statement
    assert len(processor.report.warnings) == 0


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_too_big_query_properties(processor):
    query_properties = too_big_query_properties()
    original_statement_size = len(query_properties.statement.value)

    # Verify initial size exceeds it's about 5.5MB limit and definitely larger than 5MB
    initial_size = len(json.dumps(query_properties.to_obj()))
    expected_initial_size = 5.5 * 1024 * 1024  # ~5.5MB
    assert initial_size == pytest.approx(expected_initial_size, rel=0.1), (
        f"Expected initial size ~{expected_initial_size}, got {initial_size}"
    )
    assert initial_size > 5 * 1024 * 1024, "Test data should exceed 5MB limit"

    processor.ensure_query_properties_size("urn:li:query:test", query_properties)

    # Statement should be truncated
    assert len(query_properties.statement.value) < original_statement_size

    # Should contain truncation message
    assert "... [original value was" in query_properties.statement.value
    assert (
        f"{original_statement_size} bytes and truncated to"
        in query_properties.statement.value
    )
    assert query_properties.statement.value.endswith(" bytes]")

    # Final size should be within constraints, ie <= 5MB + buffer
    final_size = len(json.dumps(query_properties.to_obj()))
    expected_final_size = 5 * 1024 * 1024 + 100  # 5MB + buffer
    assert final_size <= expected_final_size, (
        f"Final size {final_size} should be <= {expected_final_size}"
    )

    # Should have logged a warning
    assert len(processor.report.warnings) == 1


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.EnsureAspectSizeProcessor.ensure_query_properties_size"
)
def test_wu_processor_triggered_by_query_properties_aspect(
    ensure_query_properties_size_mock, processor
):
    list(
        processor.ensure_aspect_size(
            [
                MetadataChangeProposalWrapper(
                    entityUrn="urn:li:query:test",
                    aspect=proper_query_properties(),
                ).as_workunit()
            ]
        )
    )
    ensure_query_properties_size_mock.assert_called_once()
