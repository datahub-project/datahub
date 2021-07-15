import logging
import unittest
from unittest.mock import Mock

import sqlalchemy.sql.sqltypes as types
from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kudu import KuduConfig, KuduSource
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
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
        kudu_source = KuduSource.create(KuduConfig(use_ssl=False), ctx)

        assert (
            kudu_source.config.get_sql_alchemy_url()
            == "impala://localhost:21050/default"
        )

    @freeze_time("2012-01-14 12:00:01.000")
    def test_kudu_source_workunits(self):
        """
        test that the ingestion is able to pull out a workunit
        """
        mock_engine = Mock()
        mock_engine.execute.return_value.fetchone.return_value = {
            "# Rows": "",
            "Start Key": "",
            "Stop Key": "",
            "Leader Replica": "",
            "# Replicas": "",
        }
        mock_inspect = Mock()
        mock_inspect.get_table_names.return_value = ["my_first_table"]
        mock_inspect.get_table_comment.return_value = {"text": None}
        mock_inspect.get_columns.return_value = [
            {
                "name": "id",
                "type": types.BIGINT(),
                "nullable": True,
                "autoincrement": False,
            },
            {
                "name": "name",
                "type": types.String(),
                "nullable": True,
                "autoincrement": False,
            },
        ]

        ctx = PipelineContext(run_id="test")
        src = KuduSource(KuduConfig(use_ssl=False), ctx)
        yield_gen = src.loop_tables(
            mock_inspect, "default", KuduConfig(use_ssl=False), mock_engine
        )
        expected_return = MetadataWorkUnit(
            id="default.my_first_table",
            mce=MetadataChangeEvent(
                proposedSnapshot=DatasetSnapshot(
                    urn="urn:li:dataset:(urn:li:dataPlatform:kudu,default.my_first_table,PROD)",
                    aspects=[
                        SchemaMetadataClass(
                            schemaName="default.my_first_table",
                            version=0,
                            hash="",
                            platform="urn:li:dataPlatform:kudu",
                            platformSchema=SchemalessClass(),
                            created=AuditStampClass(
                                time=1326542401000, actor="urn:li:corpuser:etl"
                            ),
                            lastModified=AuditStampClass(
                                time=1326542401000, actor="urn:li:corpuser:etl"
                            ),
                            fields=[
                                SchemaField(
                                    fieldPath="id",
                                    nativeDataType="BIGINT()",
                                    type=SchemaFieldDataTypeClass(
                                        type=NumberTypeClass()
                                    ),
                                    description=None,
                                    nullable=True,
                                    recursive=False,
                                ),
                                SchemaField(
                                    fieldPath="name",
                                    nativeDataType="String()",
                                    type=SchemaFieldDataTypeClass(
                                        type=StringTypeClass()
                                    ),
                                    description=None,
                                    nullable=True,
                                    recursive=False,
                                ),
                            ],
                        )
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
