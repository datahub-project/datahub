import unittest
from datetime import datetime

from botocore.stub import Stubber
from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.glue import GlueSource, GlueSourceConfig, get_column_type
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp, Status
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    MapTypeClass,
    MySqlDDL,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

FROZEN_TIME = "2020-04-14 07:00:00"


class GlueSourceTest(unittest.TestCase):
    glue_source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-east-1"),
    )

    def test_get_column_type_contains_key(self):

        field_type = "char"
        data_type = get_column_type(self.glue_source, field_type, "a_table", "a_field")
        self.assertEqual(
            data_type.to_obj(), SchemaFieldDataType(type=StringTypeClass()).to_obj()
        )

    def test_get_column_type_contains_array(self):

        field_type = "array_lol"
        data_type = get_column_type(self.glue_source, field_type, "a_table", "a_field")
        self.assertEqual(
            data_type.to_obj(), SchemaFieldDataType(type=ArrayTypeClass()).to_obj()
        )

    def test_get_column_type_contains_map(self):

        field_type = "map_hehe"
        data_type = get_column_type(self.glue_source, field_type, "a_table", "a_field")
        self.assertEqual(
            data_type.to_obj(), SchemaFieldDataType(type=MapTypeClass()).to_obj()
        )

    def test_get_column_type_contains_set(self):

        field_type = "set_yolo"
        data_type = get_column_type(self.glue_source, field_type, "a_table", "a_field")
        self.assertEqual(
            data_type.to_obj(), SchemaFieldDataType(type=ArrayTypeClass()).to_obj()
        )

    def test_get_column_type_not_contained(self):

        field_type = "bad_column_type"
        data_type = get_column_type(self.glue_source, field_type, "a_table", "a_field")
        self.assertEqual(
            data_type.to_obj(), SchemaFieldDataType(type=StringTypeClass()).to_obj()
        )
        self.assertEqual(
            self.glue_source.report.warnings["bad_column_type"],
            [
                "The type 'bad_column_type' is not recognised for field 'a_field' in table 'a_table', "
                "setting as StringTypeClass."
            ],
        )

    @freeze_time(FROZEN_TIME)
    def test_turn_boto_glue_data_to_metadata_event(self):
        stringy_timestamp = datetime.strptime(FROZEN_TIME, "%Y-%m-%d %H:%M:%S")
        timestamp = int(datetime.timestamp(stringy_timestamp) * 1000)

        get_databases_response = {
            "DatabaseList": [
                {
                    "Name": "datalake_grilled",
                    "Description": "irrelevant",
                    "LocationUri": "irrelevant",
                    "Parameters": {},
                    "CreateTime": datetime(2015, 1, 1),
                    "CreateTableDefaultPermissions": [],
                    "CatalogId": "irrelevant",
                },
            ],
        }
        get_tables_response = {
            "TableList": [
                {
                    "Name": "Barbeque",
                    "Owner": "Susan",
                    "DatabaseName": "datalake_grilled",
                    "Description": "Grilled Food",
                    "StorageDescriptor": {
                        "Columns": [
                            {
                                "Name": "Size",
                                "Type": "int",
                                "Comment": "Maximum attendees permitted",
                            }
                        ]
                    },
                }
            ]
        }

        with Stubber(self.glue_source.glue_client) as stubber:
            stubber.add_response("get_databases", get_databases_response, {})
            stubber.add_response(
                "get_tables", get_tables_response, {"DatabaseName": "datalake_grilled"}
            )
            actual_work_unit = list(self.glue_source.get_workunits())[0]

        expected_metadata_work_unit = create_metadata_work_unit(timestamp)

        self.assertEqual(expected_metadata_work_unit, actual_work_unit)


def create_metadata_work_unit(timestamp):
    dataset_snapshot = DatasetSnapshot(
        urn="urn:li:dataset:(urn:li:dataPlatform:glue,datalake_grilled.Barbeque,PROD)",
        aspects=[],
    )

    dataset_snapshot.aspects.append(Status(removed=False))

    dataset_snapshot.aspects.append(
        OwnershipClass(
            owners=[
                OwnerClass(
                    owner="urn:li:corpuser:Susan", type=OwnershipTypeClass.DATAOWNER
                )
            ],
            lastModified=AuditStampClass(
                time=timestamp, actor="urn:li:corpuser:datahub"
            ),
        )
    )

    dataset_snapshot.aspects.append(
        DatasetPropertiesClass(
            description="Grilled Food",
        )
    )

    fields = [
        SchemaField(
            fieldPath="Size",
            nativeDataType="int",
            type=SchemaFieldDataType(type=NumberTypeClass()),
            description="Maximum attendees permitted",
            nullable=True,
            recursive=False,
        )
    ]

    schema_metadata = SchemaMetadata(
        schemaName="datalake_grilled.Barbeque",
        version=0,
        fields=fields,
        platform="urn:li:dataPlatform:glue",
        created=AuditStamp(time=timestamp, actor="urn:li:corpuser:etl"),
        lastModified=AuditStamp(time=timestamp, actor="urn:li:corpuser:etl"),
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
    )
    dataset_snapshot.aspects.append(schema_metadata)

    mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
    return MetadataWorkUnit(id="glue-datalake_grilled.Barbeque", mce=mce)
