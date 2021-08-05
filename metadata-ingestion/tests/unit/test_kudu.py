import logging
import unittest
from unittest.mock import Mock

from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kudu import KuduConfig, KuduSource
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    NumberTypeClass,
    SchemaField,
    SchemalessClass,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
)

logger = logging.getLogger(__name__)
# from mock_alchemy.mocking import AlchemyMagicMock


class KuduSourceTest(unittest.TestCase):
    def test_kudu_source_configuration(self):
        """
        Test if the config is accepted without throwing an exception
        """
        ctx = PipelineContext(run_id="test")
        kudu_source = KuduSource.create(KuduConfig(), ctx)
        url, _ = kudu_source.config.get_url()
        assert url == "jdbc:impala://localhost:21050;"

    @freeze_time("2012-01-14 12:00:01.000")
    def test_kudu_source_workunits(self):
        """
        test that the ingestion is able to pull out a workunit
        """
        mock_engine = Mock(
            description=[
                ("# Rows", ""),
                ("Start Key", ""),
                ("Stop Key", ""),
                ("Leader Replica", ""),
                ("# Replicas", ""),
            ]
        )
        mock_execute = Mock()
        mock_execute.side_effect = [
            "table",
            "stats",
            "describe",
            "describe_formatted",
        ]  # show tables, show
        mock_fetch = Mock()
        all_tables_in_schema = [("my_first_table",)]
        describe_value = [
            (
                "id",
                "bigint",
                "",
                "true",
                "false",
                "",
                "AUTO_ENCODING",
                "DEFAULT_COMPRESSION",
                "0",
            ),
            (
                "name",
                "string",
                "",
                "false",
                "true",
                "",
                "AUTO_ENCODING",
                "DEFAULT_COMPRESSION",
                "0",
            ),
        ]
        table_formatted_stats = [
            ("# col_name            ", "data_type           ", "comment             "),
            ("", None, None),
            ("id", "bigint", None),
            ("name", "string", None),
            ("", None, None),
            ("# Detailed Table Information", None, None),
            ("Database:           ", "default             ", None),
            ("OwnerType:          ", "USER                ", None),
            ("Owner:              ", "impala              ", None),
            ("CreateTime:         ", "Sun Aug 01 09:27:13 UTC 2021", None),
            ("LastAccessTime:     ", "UNKNOWN             ", None),
            ("Retention:          ", "0                   ", None),
            (
                "Location:           ",
                "file:/var/lib/impala/warehouse/my_first_table",
                None,
            ),
            ("Table Type:         ", "EXTERNAL_TABLE      ", None),
            ("Table Parameters:", None, None),
            ("", "EXTERNAL            ", "TRUE                "),
            (
                "",
                "kudu.master_addresses",
                "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251",
            ),
            ("", "kudu.table_name     ", "impala::default.my_first_table"),
            (
                "",
                "storage_handler     ",
                "org.apache.hadoop.hive.kudu.KuduStorageHandler",
            ),
            ("", "transient_lastDdlTime", "1627810033          "),
            ("", None, None),
            ("# Storage Information", None, None),
            ("SerDe Library:      ", "org.apache.hadoop.hive.kudu.KuduSerDe", None),
            (
                "InputFormat:        ",
                "org.apache.hadoop.hive.kudu.KuduInputFormat",
                None,
            ),
            (
                "OutputFormat:       ",
                "org.apache.hadoop.hive.kudu.KuduOutputFormat",
                None,
            ),
            ("Compressed:         ", "No                  ", None),
            ("Num Buckets:        ", "0                   ", None),
            ("Bucket Columns:     ", "[]                  ", None),
            ("Sort Columns:       ", "[]                  ", None),
        ]
        mock_fetch.side_effect = [
            all_tables_in_schema,
            describe_value,
            table_formatted_stats,
        ]
        mock_engine.execute = mock_execute
        mock_engine.fetchall = mock_fetch

        ctx = PipelineContext(run_id="test")
        src = KuduSource(KuduConfig(), ctx)
        yield_gen = src.loop_tables(mock_engine, "default", KuduConfig())
        expected_return = MetadataWorkUnit(
            id="default.my_first_table",
            mce=MetadataChangeEvent(
                proposedSnapshot=DatasetSnapshot(
                    urn="urn:li:dataset:(urn:li:dataPlatform:kudu,default.my_first_table,PROD)",
                    aspects=[
                        OwnershipClass(
                            owners=[
                                OwnerClass(
                                    owner="urn:li:corpuser:impala",
                                    type="DATAOWNER",
                                )
                            ],
                        ),
                        DatasetPropertiesClass(
                            customProperties={
                                "table_location": "file:/var/lib/impala/warehouse/my_first_table",
                                "table_type": "EXTERNAL_TABLE",
                                "kudu_master": "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251",
                            },
                            description="",
                        ),
                        SchemaMetadataClass(
                            schemaName="default.my_first_table",
                            version=0,
                            hash="",
                            platform="urn:li:dataPlatform:kudu",
                            platformSchema=SchemalessClass(),
                            created=AuditStampClass(
                                time=0, actor="urn:li:corpuser:etl"
                            ),
                            lastModified=AuditStampClass(
                                time=0, actor="urn:li:corpuser:etl"
                            ),
                            fields=[
                                SchemaField(
                                    fieldPath="id",
                                    nativeDataType="'bigint'",
                                    type=SchemaFieldDataTypeClass(
                                        type=NumberTypeClass()
                                    ),
                                    description="",
                                    nullable=False,
                                    recursive=False,
                                ),
                                SchemaField(
                                    fieldPath="name",
                                    nativeDataType="'string'",
                                    type=SchemaFieldDataTypeClass(
                                        type=StringTypeClass()
                                    ),
                                    description="",
                                    nullable=True,
                                    recursive=False,
                                ),
                            ],
                            primaryKeys=["id"],
                            foreignKeysSpecs=None,
                        ),
                    ],
                )
            ),
        )
        for (
            item
        ) in (
            yield_gen
        ):  # there shd be a more elegant way to pull the first item off a generator...
            generated = item
            break
        self.assertEqual(generated, expected_return)
