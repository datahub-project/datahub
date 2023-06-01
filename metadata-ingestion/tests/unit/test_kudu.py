import logging
import unittest
from unittest.mock import Mock, patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    ContainerClass,
    DatasetPropertiesClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
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
            "table",
            "select * from table",
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
        dataframe = [
            (1, "mary"),
            (1, "mary"),
            (1, "mary"),
            (1, "mary"),
        ]
        mock_fetch.side_effect = [
            all_tables_in_schema,
            describe_value,
            table_formatted_stats,
            all_tables_in_schema,
            dataframe,
        ]

        mock_engine.execute = mock_execute
        mock_engine.fetchall = mock_fetch

        ctx = PipelineContext(run_id="test")
        src = KuduSource(KuduConfig(), ctx)
        yield_dataset = src.loop_tables(mock_engine, "default", KuduConfig())
        generated_output = []
        for item in yield_dataset:
            generated_output.append(item)
        logger.error("generated output is")
        logger.error(generated_output[0])

        with patch.object(mock_engine, "description", [("id",), ("name",)]):
            yield_profile = src.loop_profiler(mock_engine, "default", KuduConfig())
            generated_profile = next(yield_profile)  # type: ignore
        expected_mcp = MetadataWorkUnit(
            id="urn:li:dataset:(urn:li:dataPlatform:kudu,default.my_first_table,PROD)",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType="UPSERT",
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:kudu,default.my_first_table,PROD)",
                entityKeyAspect=None,
                auditHeader=None,
                aspectName="container",
                aspect=ContainerClass(
                    container="urn:li:container:49bf859a5c09dfbdb4cbbdeb96daf518"
                ),
            ),
        )
        expected_mce = MetadataWorkUnit(
            id="default.my_first_table",
            mce=MetadataChangeEvent(
                proposedSnapshot=DatasetSnapshot(
                    urn="urn:li:dataset:(urn:li:dataPlatform:kudu,default.my_first_table,PROD)",
                    aspects=[
                        StatusClass(removed=False),
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
        assert generated_output[0] == expected_mcp
        assert generated_output[1] == expected_mce
        gen_mcp = generated_profile.metadata.aspect
        self.assertTrue(gen_mcp.validate())
