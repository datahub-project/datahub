import json
import time
from unittest.mock import patch

from freezegun.api import freeze_time

from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import _MAX_BATCH_INGEST_PAYLOAD_SIZE
from datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size import (
    ensure_aspect_size,
    ensure_dataset_profile_size,
)
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
    SubTypesClass,
)


def proper_schema_metadata() -> SchemaMetadataClass:
    fields = [
        SchemaFieldClass(
            "a",
            nativeDataType="int",
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        )
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
def test_ensure_size_of_proper_dataset_profile():
    profile = proper_dataset_profile()
    orig_repr = json.dumps(profile.to_obj())
    ensure_dataset_profile_size(profile)
    assert orig_repr == json.dumps(profile.to_obj()), (
        "Aspect was modified in case where workunit processor should " "have been no-op"
    )


@freeze_time("2023-01-02 00:00:00")
def test_ensure_size_of_too_big_dataset_profile():
    profile = proper_dataset_profile()
    big_field = DatasetFieldProfileClass(
        fieldPath="big",
        sampleValues=20 * [(int(_MAX_BATCH_INGEST_PAYLOAD_SIZE / 20) - 10) * "a"],
    )
    profile.fieldProfiles.insert(4, big_field)
    ensure_dataset_profile_size(profile)

    expected_profile = proper_dataset_profile()
    reduced_field = DatasetFieldProfileClass(
        fieldPath="big",
        sampleValues=[],
    )
    expected_profile.fieldProfiles.insert(4, reduced_field)
    assert json.dumps(profile.to_obj()) == json.dumps(
        expected_profile.to_obj()
    ), "Field 'big' was not properly removed from aspect due to its size"


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_dataset_profile_size"
)
def test_wu_processor_triggered_by_data_profile_aspect(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock
):
    ret = [  # noqa: F841
        *ensure_aspect_size(
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
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_dataset_profile_size"
)
def test_wu_processor_triggered_by_data_profile_aspect_mcpc(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock
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
    ret = [*ensure_aspect_size([mcpc])]  # noqa: F841
    ensure_dataset_profile_size_mock.assert_called_once()
    ensure_schema_metadata_size_mock.assert_not_called()


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_dataset_profile_size"
)
def test_wu_processor_triggered_by_data_profile_aspect_mce(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock
):
    snapshot = DatasetSnapshotClass(
        urn="urn:li:dataset:(urn:li:dataPlatform:s3, dummy_name, DEV)",
        aspects=[proper_schema_metadata()],
    )
    mce = MetadataWorkUnit(
        id="test", mce=MetadataChangeEvent(proposedSnapshot=snapshot)
    )
    ret = [*ensure_aspect_size([mce])]  # noqa: F841
    ensure_schema_metadata_size_mock.assert_called_once()
    ensure_dataset_profile_size_mock.assert_not_called()


@freeze_time("2023-01-02 00:00:00")
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_dataset_profile_size"
)
def test_wu_processor_triggered_by_schema_metadata_aspect(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock
):
    ret = [  # noqa: F841
        *ensure_aspect_size(
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
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_schema_metadata_size"
)
@patch(
    "datahub.ingestion.api.auto_work_units.auto_ensure_aspect_size.ensure_dataset_profile_size"
)
def test_wu_processor_not_triggered_by_unhandled_aspects(
    ensure_dataset_profile_size_mock, ensure_schema_metadata_size_mock
):
    ret = [  # noqa: F841
        *ensure_aspect_size(
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
