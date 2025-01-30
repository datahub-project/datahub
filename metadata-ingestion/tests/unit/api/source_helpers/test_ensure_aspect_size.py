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
    ChangeTypeClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
    DatasetSnapshotClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
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
