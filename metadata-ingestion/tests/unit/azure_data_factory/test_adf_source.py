"""Unit tests for Azure Data Factory source - business logic only.

Following the accelerator guidelines, we test:
- Platform mapping logic (linked service type -> DataHub platform)
- Activity subtype mapping
- Table name extraction from dataset properties
- Run status mapping
- Lineage extraction logic patterns

We do NOT test:
- Trivial getters/setters
- Third-party library behavior
- Pydantic validation (covered by test_adf_config.py)
"""

from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from azure.mgmt.datafactory.models import Activity

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure.constants import ADF_LINKED_SERVICE_PLATFORM_MAP
from datahub.ingestion.source.azure_data_factory.adf_column_lineage import (
    CopyActivityColumnLineageExtractor,
    DatasetSchemaInfo,
    SinkColumnCasing,
    get_activity_translator_config,
)
from datahub.ingestion.source.azure_data_factory.adf_config import (
    AzureDataFactoryConfig,
    DatabricksCatalogMapping,
)
from datahub.ingestion.source.azure_data_factory.adf_report import (
    AzureDataFactorySourceReport,
)
from datahub.ingestion.source.azure_data_factory.adf_source import (
    ACTIVITY_SUBTYPE_MAP,
    AzureDataFactorySource,
    ColumnMapping,
    LineagePair,
)
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.metadata.urns import DataJobUrn
from datahub.sdk.dataflow import DataFlow


def make_source() -> AzureDataFactorySource:
    with patch(
        "datahub.ingestion.source.azure_data_factory.adf_source.AzureDataFactoryClient"
    ):
        return AzureDataFactorySource(
            AzureDataFactoryConfig(subscription_id="test-sub"),
            PipelineContext(run_id="test"),
        )


class TestLinkedServicePlatformMapping:
    """Tests for linked service to DataHub platform mapping.

    This is critical business logic - incorrect mapping would create
    lineage to wrong platform URNs.
    """

    def test_azure_sql_variants_map_to_mssql(self) -> None:
        """All Azure SQL variants should map to mssql platform."""
        azure_sql_types = ["AzureSqlDatabase", "AzureSqlMI", "SqlServer"]
        for sql_type in azure_sql_types:
            assert ADF_LINKED_SERVICE_PLATFORM_MAP.get(sql_type) == "mssql", (
                f"{sql_type} should map to 'mssql'"
            )

    def test_synapse_variants_map_to_mssql(self) -> None:
        """Azure Synapse variants should map to mssql platform (same protocol)."""
        synapse_types = ["AzureSynapseAnalytics", "AzureSqlDW"]
        for synapse_type in synapse_types:
            assert ADF_LINKED_SERVICE_PLATFORM_MAP.get(synapse_type) == "mssql", (
                f"{synapse_type} should map to 'mssql'"
            )

    def test_databricks_variants_map_correctly(self) -> None:
        """Databricks services should all map to databricks platform."""
        databricks_types = ["AzureDatabricks", "AzureDatabricksDeltaLake"]
        for db_type in databricks_types:
            assert ADF_LINKED_SERVICE_PLATFORM_MAP.get(db_type) == "databricks", (
                f"{db_type} should map to 'databricks'"
            )

    def test_azure_storage_types_map_to_abs_platform(self) -> None:
        """All Azure storage types should map to abs (Azure Blob Storage) platform."""
        assert ADF_LINKED_SERVICE_PLATFORM_MAP["AzureBlobStorage"] == "abs"
        assert ADF_LINKED_SERVICE_PLATFORM_MAP["AzureBlobFS"] == "abs"
        assert ADF_LINKED_SERVICE_PLATFORM_MAP["AzureDataLakeStore"] == "abs"

    def test_major_cloud_databases_covered(self) -> None:
        """Major cloud databases should be mapped."""
        major_databases = {
            "Snowflake": "snowflake",
            "GoogleBigQuery": "bigquery",
            "AmazonRedshift": "redshift",
        }
        for service_type, expected_platform in major_databases.items():
            assert (
                ADF_LINKED_SERVICE_PLATFORM_MAP.get(service_type) == expected_platform
            )

    def test_common_open_source_databases_covered(self) -> None:
        """Common OSS databases should be mapped."""
        oss_databases = {
            "PostgreSql": "postgres",
            "MySql": "mysql",
            "Oracle": "oracle",
        }
        for service_type, expected_platform in oss_databases.items():
            assert (
                ADF_LINKED_SERVICE_PLATFORM_MAP.get(service_type) == expected_platform
            )

    def test_unknown_service_type_returns_none(self) -> None:
        """Unknown service types should return None (not raise)."""
        assert ADF_LINKED_SERVICE_PLATFORM_MAP.get("UnknownServiceType") is None
        assert ADF_LINKED_SERVICE_PLATFORM_MAP.get("CustomConnector") is None

    def test_v2_and_rds_variants_map_to_the_same_platform_as_their_base(self) -> None:
        """Regression test: several ADF linked service types were missing
        from the map even though a real DataHub platform for them exists -
        "V2" (newer connector version) and Amazon RDS variants speak the
        same wire protocol as their already-mapped counterparts, and
        should map identically."""
        variants = {
            "PostgreSqlV2": "postgres",
            "SnowflakeV2": "snowflake",
            "GoogleBigQueryV2": "bigquery",
            "SalesforceV2": "salesforce",
            "SalesforceServiceCloudV2": "salesforce",
            "AmazonRdsForOracle": "oracle",
            "AmazonRdsForSqlServer": "mssql",
        }
        for service_type, expected_platform in variants.items():
            assert (
                ADF_LINKED_SERVICE_PLATFORM_MAP.get(service_type) == expected_platform
            ), f"{service_type} should map to '{expected_platform}'"

    def test_additional_database_platforms_covered(self) -> None:
        """Regression test: these ADF-supported database linked service
        types have a real, existing DataHub platform but were previously
        missing from the map entirely, silently skipping lineage for any
        activity using them."""
        additional_databases = {
            "MariaDB": "mariadb",
            "AzureMariaDB": "mariadb",
            "Cassandra": "cassandra",
            "Couchbase": "couchbase",
            "Informix": "informix",
            "Presto": "presto",
            "SapHana": "hana",
            "MongoDb": "mongodb",
            "MongoDbV2": "mongodb",
            "MongoDbAtlas": "mongodb",
            "CosmosDbMongoDbApi": "mongodb",
            "GoogleSheets": "google_sheets",
        }
        for service_type, expected_platform in additional_databases.items():
            assert (
                ADF_LINKED_SERVICE_PLATFORM_MAP.get(service_type) == expected_platform
            ), f"{service_type} should map to '{expected_platform}'"


class TestNestedActivityTraversal:
    """Tests for BFS traversal over nested container activities
    (ForEach/Until/IfCondition/Switch), used by both pipeline processing
    and _find_activity_by_name.
    """

    def _make_source(self) -> AzureDataFactorySource:
        return make_source()

    @pytest.mark.timeout(5)
    def test_find_activity_by_name_terminates_on_self_referencing_container(
        self,
    ) -> None:
        """Regression test: a malformed pipeline where a ForEach
        container's own activity list includes itself must not hang the
        BFS traversal forever - the visited-set guard should let it
        terminate and correctly report the activity as not found."""
        source = self._make_source()
        cyclic_foreach = SimpleNamespace(name="LoopForever", type="ForEach")
        cyclic_foreach.activities = [cyclic_foreach]  # self-reference
        pipeline = SimpleNamespace(activities=[cyclic_foreach])

        result = source._find_activity_by_name(pipeline, "DoesNotExist")

        assert result is None

    @pytest.mark.timeout(5)
    def test_find_activity_by_name_terminates_on_mutually_referencing_containers(
        self,
    ) -> None:
        """Same as above, but for a two-container cycle (A contains B,
        B contains A) rather than direct self-reference."""
        source = self._make_source()
        container_a = SimpleNamespace(name="ContainerA", type="ForEach")
        container_b = SimpleNamespace(name="ContainerB", type="ForEach")
        container_a.activities = [container_b]
        container_b.activities = [container_a]
        pipeline = SimpleNamespace(activities=[container_a])

        result = source._find_activity_by_name(pipeline, "DoesNotExist")

        assert result is None

    def test_find_activity_by_name_finds_nested_activity(self) -> None:
        """Sanity check alongside the cycle tests: a genuinely nested
        (non-cyclic) activity must still be found."""
        source = self._make_source()
        inner_copy = SimpleNamespace(name="InnerCopy", type="Copy")
        foreach = SimpleNamespace(name="OuterLoop", type="ForEach")
        foreach.activities = [inner_copy]
        pipeline = SimpleNamespace(activities=[foreach])

        result = source._find_activity_by_name(pipeline, "InnerCopy")

        assert result is inner_copy


class TestActivitySubtypeMapping:
    """Tests for activity type to subtype mapping.

    Subtypes affect how activities appear in the UI and their grouping.
    """

    def test_copy_activity_subtype(self) -> None:
        """Copy activity should have descriptive subtype."""
        assert ACTIVITY_SUBTYPE_MAP["Copy"] == "Copy Activity"

    def test_dataflow_activities_grouped_together(self) -> None:
        """Both DataFlow and ExecuteDataFlow should have same subtype."""
        assert ACTIVITY_SUBTYPE_MAP["DataFlow"] == "Data Flow Activity"
        assert ACTIVITY_SUBTYPE_MAP["ExecuteDataFlow"] == "Data Flow Activity"

    def test_control_flow_activities_have_descriptive_names(self) -> None:
        """Control flow activities should have user-friendly subtypes."""
        control_flow_map = {
            "IfCondition": "If Condition",
            "ForEach": "ForEach Loop",
            "Until": "Until Loop",
            "Switch": "Switch Activity",
            "Wait": "Wait Activity",
        }
        for activity_type, expected_subtype in control_flow_map.items():
            assert ACTIVITY_SUBTYPE_MAP.get(activity_type) == expected_subtype

    def test_databricks_activities_identifiable(self) -> None:
        """Databricks activities should be clearly identified."""
        databricks_activities = [
            "DatabricksNotebook",
            "DatabricksSparkJar",
            "DatabricksSparkPython",
        ]
        for activity in databricks_activities:
            subtype = ACTIVITY_SUBTYPE_MAP.get(activity)
            assert subtype is not None
            assert "Databricks" in subtype


class TestTableNameExtractionLogic:
    """Tests for the logic patterns used in table name extraction.

    These tests verify the extraction logic that would be used in
    _extract_table_name without needing a full source instance.
    """

    def test_extract_simple_table_name(self) -> None:
        """Should extract tableName property directly."""
        type_props = {"tableName": "dbo.customers"}
        # Logic pattern from _extract_table_name
        table_name = type_props.get("tableName")
        assert table_name == "dbo.customers"

    def test_combine_schema_and_table(self) -> None:
        """Should combine separate schema and table fields."""
        type_props = {"schema": "sales", "table": "orders"}
        # Logic pattern from _extract_table_name
        schema = type_props.get("schema", "")
        table = type_props.get("table", "")
        result = f"{schema}.{table}" if schema and table else table or schema
        assert result == "sales.orders"

    def test_schema_only_returns_schema(self) -> None:
        """Should return schema when table is missing."""
        type_props = {"schema": "dbo"}
        schema = type_props.get("schema", "")
        table = type_props.get("table", "")
        result = f"{schema}.{table}" if schema and table else table or schema
        assert result == "dbo"

    def test_table_only_returns_table(self) -> None:
        """Should return table when schema is missing."""
        type_props = {"table": "orders"}
        schema = type_props.get("schema", "")
        table = type_props.get("table", "")
        result = f"{schema}.{table}" if schema and table else table or schema
        assert result == "orders"


class TestExpressionParameterResolution:
    """Tests for resolving ADF dynamic-content dataset typeProperties
    (e.g. "@dataset().table_name") via activity/dataset parameters.

    Unlike TestTableNameExtractionLogic above, these call the real
    _extract_table_name/_resolve_dataset_urn methods directly instead of
    re-implementing the logic inline - a hand-copied "logic pattern" test is
    exactly how the schema+table dead-code bug and the Expression-dict
    garbage-URN bug both shipped undetected.
    """

    def _make_source(self) -> AzureDataFactorySource:
        config = AzureDataFactoryConfig(subscription_id="test-sub")
        source = make_source()
        source.config = config
        source.report = AzureDataFactorySourceReport()
        source._datasets_cache = {}
        source._linked_services_cache = {}
        source._global_parameters_cache = {}
        return source

    def _make_dataset(
        self,
        table_name: Any = None,
        table: Any = None,
        schema: Any = None,
        dataset_parameters: Optional[dict] = None,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            properties=SimpleNamespace(
                table_name=table_name,
                table=table,
                schema_type_properties_schema=schema,
                file_name=None,
                folder_path=None,
                location=None,
                parameters=dataset_parameters or {},
            )
        )

    def test_schema_and_table_combine_when_both_literal(self) -> None:
        """Regression test for a dead-code bug: schema+table previously
        never combined because `table` short-circuited before `schema` was
        checked."""
        source = self._make_source()
        dataset = self._make_dataset(table="Customers", schema="dbo")
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
        )
        assert result == "dbo.Customers"

    def test_expression_resolved_via_activity_parameter_override(self) -> None:
        """ "@dataset().table_name" resolves using the activity's literal
        DatasetReference.parameters override."""
        source = self._make_source()
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
            schema="dbo",
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
            activity_dataset_parameters={"table_name": "Orders"},
        )
        assert result == "dbo.Orders"

    def test_expression_resolved_via_expression_wrapped_activity_override(
        self,
    ) -> None:
        """Regression test: in real ADF data, an activity's
        DatasetReference.parameters value can itself be an
        Expression-wrapped dict ({"value": "Orders", "type": "Expression"})
        rather than a plain string - ADF's "Add dynamic content" UI wraps
        even literal values this way. This must resolve exactly like a
        bare string override."""
        source = self._make_source()
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
            schema="dbo",
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
            activity_dataset_parameters={
                "table_name": {"value": "Orders", "type": "Expression"}
            },
        )
        assert result == "dbo.Orders"

    def test_expression_resolved_via_dataset_parameter_default(self) -> None:
        """Falls back to the dataset's own declared parameter default when
        the activity doesn't override it."""
        source = self._make_source()
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
            dataset_parameters={
                "table_name": SimpleNamespace(default_value="DefaultOrders")
            },
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
        )
        assert result == "DefaultOrders"

    def test_expression_wrapped_dataset_parameter_default_resolves(self) -> None:
        """Regression test: ADF's "Add dynamic content" UI can wrap even a
        dataset's own declared parameter default as an Expression dict,
        same as it does for an activity override - this must be unwrapped
        the same way, not treated as unresolvable just because it isn't a
        bare string."""
        source = self._make_source()
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
            dataset_parameters={
                "table_name": SimpleNamespace(
                    default_value={"value": "DefaultOrders", "type": "Expression"}
                )
            },
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
        )
        assert result == "DefaultOrders"

    def test_global_parameter_dataset_parameter_default_resolves(self) -> None:
        """Regression test: a dataset's own declared parameter default can
        itself be a global-parameter reference (a tenant-wide literal
        constant), not just a literal or an Expression-wrapped literal -
        it must resolve from _global_parameters_cache the same way an
        activity override referencing a global parameter does."""
        source = self._make_source()
        source._global_parameters_cache = {"rg/factory": {"DefaultTable": "RealOrders"}}
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
            dataset_parameters={
                "table_name": SimpleNamespace(
                    default_value="@pipeline().globalParameters.DefaultTable"
                )
            },
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
        )
        assert result == "RealOrders"

    def test_expression_unresolvable_falls_back_and_warns(self) -> None:
        """A value driven by a ForEach loop variable (@item()) has no
        run-history field that can resolve it either - should warn and
        return None (never the raw dict repr) so the caller falls back to
        the ADF dataset name."""
        source = self._make_source()
        source.config.include_execution_history = True
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
            activity_dataset_parameters={"table_name": "@item().TableName"},
        )
        assert result is None
        assert source.report.unresolved_dynamic_properties == 1

    def test_unresolvable_override_does_not_mask_with_dataset_default(self) -> None:
        """Regression test: when the activity supplies an override that is
        itself an unresolvable dynamic expression (e.g. "@item().TableName"
        from a ForEach loop), the dataset's own static declared default must
        NOT be silently substituted in its place - that would misrepresent
        a genuinely per-iteration dynamic value as a fixed placeholder."""
        source = self._make_source()
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
            dataset_parameters={
                "table_name": SimpleNamespace(default_value="NA"),
            },
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
            activity_dataset_parameters={"table_name": "@item().TableName"},
        )
        assert result is None
        assert result != "NA"
        assert source.report.unresolved_dynamic_properties == 1

    def test_global_parameter_override_resolves_without_execution_history(
        self,
    ) -> None:
        """Regression test: unlike a pipeline parameter reference (below),
        "@pipeline().globalParameters.X" is a tenant-wide literal
        constant - it must resolve from _global_parameters_cache even
        when execution history is disabled, not be deferred/treated as
        unresolvable."""
        source = self._make_source()
        source.config.include_execution_history = False
        source._global_parameters_cache = {"rg/factory": {"TargetTable": "RealOrders"}}
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
            activity_dataset_parameters={
                "table_name": "@pipeline().globalParameters.TargetTable"
            },
        )
        assert result == "RealOrders"
        assert source.report.unresolved_dynamic_properties == 0

    def test_pipeline_parameter_reference_deferred_without_warning(self) -> None:
        """When execution history is enabled, a pipeline-parameter-driven
        override defers to Layer 2 (run-history resolution) silently -
        it isn't an error at static-resolution time."""
        source = self._make_source()
        source.config.include_execution_history = True
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
            activity_dataset_parameters={
                "table_name": "@pipeline().parameters.TargetTable"
            },
        )
        assert result is None
        assert source.report.unresolved_dynamic_properties == 0

    def test_pipeline_parameter_reference_warns_when_history_disabled(self) -> None:
        """The same deferred case should warn when execution history is
        disabled, since Layer 2 will never run to resolve it."""
        source = self._make_source()
        source.config.include_execution_history = False
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
            activity_dataset_parameters={
                "table_name": "@pipeline().parameters.TargetTable"
            },
        )
        assert result is None
        assert source.report.unresolved_dynamic_properties == 1

    def test_never_produces_dict_repr_garbage(self) -> None:
        """Regression test for the reported bug: an Expression value must
        never be blindly stringified into the URN."""
        source = self._make_source()
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
        )
        assert result is None
        assert source.report.unresolved_dynamic_properties == 1

    def test_resolve_dataset_urn_end_to_end_never_garbage(self) -> None:
        """Full _resolve_dataset_urn path (dataset cache + linked service
        cache + platform mapping) with a literal activity override
        produces a clean URN, never dict-repr garbage."""
        source = self._make_source()
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
            schema="dbo",
        )
        dataset.properties.linked_service_name = SimpleNamespace(
            reference_name="MySqlLS"
        )
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(type="AzureSqlDatabase")
        )
        source._datasets_cache = {"rg/factory": {"MyDataset": dataset}}
        source._linked_services_cache = {"rg/factory": {"MySqlLS": linked_service}}
        source.config.platform_instance_map = {}

        urn = source._resolve_dataset_urn(
            "MyDataset",
            "rg/factory",
            activity_dataset_parameters={"table_name": "Orders"},
        )

        assert urn is not None
        assert "dbo.Orders" in str(urn)
        assert "{'value'" not in str(urn)
        assert "Expression" not in str(urn)

    def test_resolve_dataset_urn_applies_default_database_qualification(
        self,
    ) -> None:
        """Regression test: the static (no-execution-history) path must
        fully qualify a 2-part schema.table reference using the linked
        service's own connection-string database, the same qualification
        already applied on the per-run query-based path. Without this,
        static lineage - what most pipelines actually go through - stays
        disconnected from fully-qualified dataset entities even when the
        linked service declares a database."""
        source = self._make_source()
        dataset = self._make_dataset(table="Customers", schema="dbo")
        dataset.properties.linked_service_name = SimpleNamespace(
            reference_name="MySqlLS"
        )
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                type="AzureSqlDatabase",
                connection_string="Server=tcp:myserver;Initial Catalog=SourceDB;",
            )
        )
        source._datasets_cache = {"rg/factory": {"MyDataset": dataset}}
        source._linked_services_cache = {"rg/factory": {"MySqlLS": linked_service}}
        source.config.platform_instance_map = {}

        urn = source._resolve_dataset_urn("MyDataset", "rg/factory")

        assert urn is not None
        assert "SourceDB.dbo.Customers" in str(urn)

    def test_resolve_dataset_urn_never_double_qualifies(self) -> None:
        """A table reference that's already 3-part must not get a second
        database prefix jammed onto the front of it."""
        source = self._make_source()
        dataset = self._make_dataset(table="SourceDB.dbo.Customers")
        dataset.properties.linked_service_name = SimpleNamespace(
            reference_name="MySqlLS"
        )
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                type="AzureSqlDatabase",
                connection_string="Server=tcp:myserver;Initial Catalog=SourceDB;",
            )
        )
        source._datasets_cache = {"rg/factory": {"MyDataset": dataset}}
        source._linked_services_cache = {"rg/factory": {"MySqlLS": linked_service}}
        source.config.platform_instance_map = {}

        urn = source._resolve_dataset_urn("MyDataset", "rg/factory")

        assert urn is not None
        assert "SourceDB.SourceDB.dbo.Customers" not in str(urn)
        assert "SourceDB.dbo.Customers" in str(urn)

    def test_extract_table_name_reads_mongodb_collection(self) -> None:
        """MongoDB-family datasets identify their target via "collection",
        not table/table_name - without this, MongoDB/CosmosDB Mongo API
        lineage falls back to the generic ADF dataset name and never
        joins to DataHub's actual MongoDB source."""
        source = self._make_source()
        dataset = SimpleNamespace(
            properties=SimpleNamespace(
                table_name=None,
                table=None,
                collection="orders",
                schema_type_properties_schema=None,
                file_name=None,
                folder_path=None,
                location=None,
                parameters={},
            )
        )
        result = source._extract_table_name(
            dataset,
            linked_service=None,
            dataset_name="MyDataset",
            factory_key="rg/factory",
        )
        assert result == "orders"

    def test_data_flow_source_forwards_activity_dataset_parameters(self) -> None:
        """Regression test: Copy/Lookup activities already forward their
        DatasetReference.parameters into dataset resolution, but Data Flow
        sources/sinks didn't - parameterized Data Flow lineage stayed on
        the dataset's unresolved/default name."""
        source = self._make_source()
        dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
            schema="dbo",
        )
        dataset.properties.linked_service_name = SimpleNamespace(
            reference_name="MySqlLS"
        )
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(type="AzureSqlDatabase")
        )
        source._datasets_cache = {"rg/factory": {"MyDataset": dataset}}
        source._linked_services_cache = {"rg/factory": {"MySqlLS": linked_service}}
        source._data_flows_cache = {
            "rg/factory": {
                "MyDataFlow": SimpleNamespace(
                    properties=SimpleNamespace(
                        sources=[
                            SimpleNamespace(
                                name="src1",
                                dataset=SimpleNamespace(
                                    reference_name="MyDataset",
                                    parameters={"table_name": "Orders"},
                                ),
                            )
                        ]
                    )
                )
            }
        }
        source.config.platform_instance_map = {}
        activity = SimpleNamespace(
            name="RunFlow",
            data_flow=SimpleNamespace(reference_name="MyDataFlow"),
        )

        urns = source._extract_data_flow_sources(activity, "rg/factory")

        assert len(urns) == 1
        assert "dbo.Orders" in urns[0]

    def test_resolve_default_database_reads_mongodb_database(self) -> None:
        """MongoDB v2/Atlas/CosmosDB Mongo API linked services expose the
        database directly as "database" (already covered generically),
        but the legacy MongoDB linked service names the same field
        "database_name" instead."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(database_name="analytics")
        )
        result = source._resolve_default_database(
            linked_service, "mongodb", "rg/factory"
        )
        assert result == "analytics"

    def test_substitute_pipeline_run_parameters_rejects_unresolved_expression(
        self,
    ) -> None:
        """Regression test: Azure sometimes records a pipeline run's
        parameter value as literal, still-unevaluated ADF templating text
        (e.g. "@{linkedService().someField}", observed on a live tenant)
        rather than a resolved string - the same quirk seen on
        preCopyScript. Substituting it blindly would reproduce the exact
        class of garbage-URN bug this whole feature exists to fix."""
        source = self._make_source()
        static_params = {"table_name": "@pipeline().parameters.TargetDb"}
        pipeline_run_parameters = {
            "TargetDb": "@{linkedService().target_database_param}"
        }

        result = source._substitute_pipeline_run_parameters(
            static_params, pipeline_run_parameters
        )

        assert result is None
        assert source.report.unresolved_dynamic_properties == 1

    def test_resolve_default_database_rejects_parameterized_linked_service_without_default(
        self,
    ) -> None:
        """A "parameterized" linked service references its own parameter
        directly inside the connection string (e.g. "Initial
        Catalog=@{linkedService().someParam}", observed on a live
        tenant), to be substituted by the integration runtime at actual
        connection time. When that parameter has no declared default
        value to fall back on, the literal unevaluated text must never
        be used as a real database name."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string="Server=tcp:my-server;Initial Catalog=@{linkedService().target_database_param};"
            )
        )
        result = source._resolve_default_database(linked_service, "mssql", "rg/factory")
        assert result is None

    def test_resolve_default_database_uses_linked_service_parameter_default(
        self,
    ) -> None:
        """Regression test: when a "parameterized" linked service's
        referenced parameter DOES declare a default value, that's a real
        value from the API (mirroring how dataset parameter defaults are
        already resolved) - it should be used to fully qualify the table
        name, with no config-file setting required."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string="Server=tcp:my-server;Initial Catalog=@{linkedService().target_database_param};",
                parameters={
                    "target_database_param": SimpleNamespace(
                        default_value="WarehouseDB"
                    )
                },
            )
        )
        result = source._resolve_default_database(linked_service, "mssql", "rg/factory")
        assert result == "WarehouseDB"

    def test_resolve_default_database_accepts_literal_connection_string(
        self,
    ) -> None:
        """Sanity check alongside the rejection test above: a genuinely
        literal "Initial Catalog=" value must still resolve normally."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string="Server=tcp:my-server;Initial Catalog=MyRealDatabase;"
            )
        )
        result = source._resolve_default_database(linked_service, "mssql", "rg/factory")
        assert result == "MyRealDatabase"

    def test_resolve_default_database_accepts_expression_wrapped_connection_string(
        self,
    ) -> None:
        """Regression test: ADF's "Add dynamic content" UI can wrap even a
        linked service's connection_string as an Expression dict, same as
        it does for dataset/activity values - this must be unwrapped
        before the "Initial Catalog=" regex scan, not skipped outright
        just because it isn't already a bare string."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string={
                    "value": "Server=tcp:my-server;Initial Catalog=MyRealDatabase;",
                    "type": "Expression",
                }
            )
        )
        result = source._resolve_default_database(linked_service, "mssql", "rg/factory")
        assert result == "MyRealDatabase"

    def test_resolve_default_database_resolves_global_parameter_database(self) -> None:
        """Regression test: a MySQL/PostgreSQL-style linked service's
        standalone "database" property can itself be a global-parameter
        reference (a tenant-wide literal constant) rather than a literal
        or an Expression-wrapped literal - it must resolve from
        _global_parameters_cache, the same as the Databricks domain
        property already does."""
        source = self._make_source()
        source._global_parameters_cache = {"rg/factory": {"DbName": "RealDatabase"}}
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(database="@pipeline().globalParameters.DbName")
        )
        result = source._resolve_default_database(linked_service, "mysql", "rg/factory")
        assert result == "RealDatabase"

    def test_resolve_default_database_databricks_unset_by_default(self) -> None:
        """ADF exposes no catalog field anywhere for Databricks datasets
        or linked services, and there's no way to tell a legacy
        hive_metastore workspace from a Unity Catalog workspace with an
        arbitrary catalog name - guessing "hive_metastore" would often be
        wrong, so without explicit config nothing should be assumed."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                domain="https://my-workspace.azuredatabricks.net"
            )
        )
        result = source._resolve_default_database(
            linked_service, "databricks", "rg/factory"
        )
        assert result is None

    def test_resolve_default_database_databricks_uses_configured_catalog(self) -> None:
        """When the operator knows their workspace's catalog, the
        explicit config override should still be honored."""
        source = self._make_source()
        source.config.databricks_default_catalog = "hive_metastore"
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                domain="https://my-workspace.azuredatabricks.net"
            )
        )
        result = source._resolve_default_database(
            linked_service, "databricks", "rg/factory"
        )
        assert result == "hive_metastore"

    def test_resolve_default_database_databricks_catalog_map_by_linked_service(
        self,
    ) -> None:
        """A tenant with multiple Databricks workspaces that don't share
        a catalog can map each linked service to its own catalog."""
        source = self._make_source()
        source.config.databricks_catalog_map = {
            "MyDatabricksLS": DatabricksCatalogMapping(catalog="prod_catalog")
        }
        linked_service = SimpleNamespace(properties=SimpleNamespace(domain=None))
        result = source._resolve_default_database(
            linked_service, "databricks", "rg/factory", ls_ref_name="MyDatabricksLS"
        )
        assert result == "prod_catalog"

    def test_resolve_default_database_databricks_catalog_map_with_metastore(
        self,
    ) -> None:
        """When a metastore is also configured for the mapping, it's
        folded ahead of the catalog as "metastore.catalog" - a single
        opaque segment that DataHub's own URN join treats no differently
        than a plain catalog, producing
        metastore.catalog.schema.table overall (matching the shape
        DataHub's own Unity Catalog source uses with
        include_metastore enabled)."""
        source = self._make_source()
        source.config.databricks_catalog_map = {
            "MyDatabricksLS": DatabricksCatalogMapping(
                catalog="prod_catalog", metastore="prod_metastore"
            )
        }
        linked_service = SimpleNamespace(properties=SimpleNamespace(domain=None))
        result = source._resolve_default_database(
            linked_service, "databricks", "rg/factory", ls_ref_name="MyDatabricksLS"
        )
        assert result == "prod_metastore.prod_catalog"

    def test_resolve_default_database_databricks_catalog_map_falls_back_to_default(
        self,
    ) -> None:
        """A linked service not present in the map still falls back to
        the simpler global databricks_default_catalog, if set."""
        source = self._make_source()
        source.config.databricks_default_catalog = "hive_metastore"
        source.config.databricks_catalog_map = {
            "SomeOtherLS": DatabricksCatalogMapping(catalog="prod_catalog")
        }
        linked_service = SimpleNamespace(properties=SimpleNamespace(domain=None))
        result = source._resolve_default_database(
            linked_service, "databricks", "rg/factory", ls_ref_name="MyDatabricksLS"
        )
        assert result == "hive_metastore"

    def test_resolve_default_database_activity_override_wins_over_linked_service_default(
        self,
    ) -> None:
        """Regression test: a dataset shared across many pipelines can
        forward its own "database_name" parameter to the linked
        service's parameterized connection string. When the calling
        activity supplies a literal override for that dataset parameter,
        it must win over the linked service's own (unrelated, generic)
        declared default - observed on a live tenant where the same
        dataset/linked-service pair is reused with a different literal
        database name per pipeline."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string="Initial Catalog=@{linkedService().db_param};",
                parameters={
                    "db_param": SimpleNamespace(default_value="GenericDefaultDb")
                },
            )
        )
        dataset = SimpleNamespace(
            properties=SimpleNamespace(
                linked_service_name=SimpleNamespace(
                    parameters={
                        "db_param": {
                            "value": "@dataset().database_name",
                            "type": "Expression",
                        }
                    }
                ),
                parameters={"database_name": SimpleNamespace(default_value=None)},
            )
        )
        result = source._resolve_default_database(
            linked_service,
            "mssql",
            "rg/factory",
            dataset=dataset,
            activity_dataset_parameters={"database_name": "RealTargetDb"},
        )
        assert result == "RealTargetDb"

    def test_resolve_default_database_dataset_default_wins_when_no_activity_override(
        self,
    ) -> None:
        """When the calling activity doesn't override the forwarded
        parameter, the dataset's own declared default applies - not the
        unrelated linked service default."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string="Initial Catalog=@{linkedService().db_param};",
                parameters={
                    "db_param": SimpleNamespace(default_value="GenericDefaultDb")
                },
            )
        )
        dataset = SimpleNamespace(
            properties=SimpleNamespace(
                linked_service_name=SimpleNamespace(
                    parameters={
                        "db_param": {
                            "value": "@dataset().database_name",
                            "type": "Expression",
                        }
                    }
                ),
                parameters={
                    "database_name": SimpleNamespace(
                        default_value="DatasetOwnDefaultDb"
                    )
                },
            )
        )
        result = source._resolve_default_database(
            linked_service,
            "mssql",
            "rg/factory",
            dataset=dataset,
            activity_dataset_parameters=None,
        )
        assert result == "DatasetOwnDefaultDb"

    def test_resolve_default_database_falls_back_to_linked_service_default_when_dataset_does_not_forward(
        self,
    ) -> None:
        """When the dataset doesn't override the linked service's
        parameter at all, the linked service's own default is what
        actually applies at connection time - the pre-existing
        behavior."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string="Initial Catalog=@{linkedService().db_param};",
                parameters={
                    "db_param": SimpleNamespace(default_value="GenericDefaultDb")
                },
            )
        )
        dataset = SimpleNamespace(
            properties=SimpleNamespace(
                linked_service_name=SimpleNamespace(parameters={}),
                parameters={},
            )
        )
        result = source._resolve_default_database(
            linked_service, "mssql", "rg/factory", dataset=dataset
        )
        assert result == "GenericDefaultDb"

    def test_resolve_default_database_unresolved_does_not_fall_back_once_dataset_overrides(
        self,
    ) -> None:
        """When the dataset DOES forward the parameter but neither the
        activity nor the dataset's own default resolves it, the linked
        service's separate default must not be used as a guess - it may
        not even apply once the dataset explicitly overrides it."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string="Initial Catalog=@{linkedService().db_param};",
                parameters={
                    "db_param": SimpleNamespace(default_value="GenericDefaultDb")
                },
            )
        )
        dataset = SimpleNamespace(
            properties=SimpleNamespace(
                linked_service_name=SimpleNamespace(
                    parameters={
                        "db_param": {
                            "value": "@dataset().database_name",
                            "type": "Expression",
                        }
                    }
                ),
                parameters={"database_name": SimpleNamespace(default_value=None)},
            )
        )
        result = source._resolve_default_database(
            linked_service,
            "mssql",
            "rg/factory",
            dataset=dataset,
            activity_dataset_parameters=None,
        )
        assert result is None

    def test_resolve_default_database_standalone_database_field(self) -> None:
        """MySQL/PostgreSQL expose a standalone "database" typeProperty
        rather than embedding it in a connection string - a literal
        value there should resolve directly."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(connection_string=None, database="MyRealDb")
        )
        result = source._resolve_default_database(linked_service, "mysql", "rg/factory")
        assert result == "MyRealDb"

    def test_resolve_default_database_oracle_service_name_heuristic(self) -> None:
        """Oracle exposes no database/catalog field at all - its identity
        is baked into a free-form "server" connect string, conventionally
        built from a linked service parameter with "service" in its
        name."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string=None,
                database=None,
                server="@{concat(linkedService().host_name,':',linkedService().port,'/',linkedService().service_name)}",
                parameters={
                    "service_name": SimpleNamespace(default_value="MyOracleService")
                },
            )
        )
        result = source._resolve_default_database(
            linked_service, "oracle", "rg/factory"
        )
        assert result == "MyOracleService"

    def test_resolve_default_database_oracle_expression_wrapped_server(self) -> None:
        """The "server" connect string can be Expression-wrapped like any
        other linked-service property; the service-name hint must still
        be found inside it."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                connection_string=None,
                database=None,
                server={
                    "value": "@{concat(linkedService().host_name,'/',linkedService().service_name)}",
                    "type": "Expression",
                },
                parameters={
                    "service_name": SimpleNamespace(default_value="MyOracleService")
                },
            )
        )
        result = source._resolve_default_database(
            linked_service, "oracle", "rg/factory"
        )
        assert result == "MyOracleService"

    @pytest.mark.parametrize(
        ("table_name", "platform", "expected"),
        [
            # Two-tier: database.table is already complete.
            ("my_db.orders", "mysql", "my_db.orders"),
            ("orders", "mysql", "my_db.orders"),
            ("my_db.orders", "mongodb", "my_db.orders"),
            # Three-tier: schema.table still needs the database.
            ("dbo.orders", "mssql", "my_db.dbo.orders"),
        ],
    )
    def test_qualify_table_name_respects_platform_depth(
        self, table_name: str, platform: str, expected: str
    ) -> None:
        source = self._make_source()
        assert source._qualify_table_name(table_name, "my_db", platform) == expected

    def test_ddl_target_table_not_double_qualified_on_two_tier_platform(
        self,
    ) -> None:
        source = self._make_source()
        assert (
            source._extract_ddl_target_table(
                "TRUNCATE TABLE my_db.orders", "mysql", "my_db"
            )
            == "my_db.orders"
        )

    def test_resolve_literal_or_global_parameter_resolves_global_reference(
        self,
    ) -> None:
        """A factory-level global parameter (e.g.
        "@pipeline().globalParameters.databricks_workspace_url") is a
        tenant-wide literal constant - always resolvable without any
        pipeline run history, unlike a pipeline parameter."""
        source = self._make_source()
        source._global_parameters_cache["rg/factory"] = {
            "workspace_url": "https://adb-1234567890123456.4.azuredatabricks.net/"
        }
        result = source._resolve_literal_or_global_parameter(
            "@pipeline().globalParameters.workspace_url", "rg/factory"
        )
        assert result == "https://adb-1234567890123456.4.azuredatabricks.net/"

    def test_resolve_literal_or_global_parameter_unknown_global_returns_none(
        self,
    ) -> None:
        """A global parameter reference to a name the factory doesn't
        actually declare must not be treated as resolved."""
        source = self._make_source()
        source._global_parameters_cache["rg/factory"] = {}
        result = source._resolve_literal_or_global_parameter(
            "@pipeline().globalParameters.unknown_param", "rg/factory"
        )
        assert result is None

    def test_derive_databricks_platform_instance_from_literal_domain(self) -> None:
        """The workspace instance ID (e.g. "adb-1234567890123456") is the
        first dot-separated label of the workspace URL's host - matching
        how DataHub's Unity Catalog source derives its platform_instance
        from workspace_url under include_metastore."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                domain="https://adb-1234567890123456.4.azuredatabricks.net/"
            )
        )
        result = source._derive_databricks_platform_instance(
            linked_service, "rg/factory"
        )
        assert result == "adb-1234567890123456"

    def test_derive_databricks_platform_instance_via_linked_service_default(
        self,
    ) -> None:
        """A "parameterized" linked service's domain
        ("@{linkedService().dbx_domain}") resolves via its own declared
        parameter default, the same chain used for database names."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                domain="@{linkedService().dbx_domain}",
                parameters={
                    "dbx_domain": SimpleNamespace(
                        default_value="https://adb-1234567890123456.4.azuredatabricks.net/"
                    )
                },
            )
        )
        result = source._derive_databricks_platform_instance(
            linked_service, "rg/factory"
        )
        assert result == "adb-1234567890123456"

    def test_derive_databricks_platform_instance_via_activity_global_parameter(
        self,
    ) -> None:
        """Regression test: an activity commonly overrides a dataset's
        domain-forwarding parameter with a reference to a factory-level
        global parameter (e.g.
        "@pipeline().globalParameters.databricks_workspace_url") rather
        than a literal - this must resolve just as well as a literal
        override, since global parameters are tenant-wide constants
        available without any run history."""
        source = self._make_source()
        source._global_parameters_cache["rg/factory"] = {
            "databricks_workspace_url": "https://adb-1234567890123456.4.azuredatabricks.net/"
        }
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                domain="@{linkedService().dbx_domain}",
                parameters={
                    "dbx_domain": SimpleNamespace(
                        default_value="https://adb-9999999999999999.18.azuredatabricks.net/"
                    )
                },
            )
        )
        dataset = SimpleNamespace(
            properties=SimpleNamespace(
                linked_service_name=SimpleNamespace(
                    parameters={
                        "dbx_domain": {
                            "value": "@dataset().workspace_url",
                            "type": "Expression",
                        }
                    }
                ),
                parameters={"workspace_url": SimpleNamespace(default_value=None)},
            )
        )
        result = source._derive_databricks_platform_instance(
            linked_service,
            "rg/factory",
            dataset=dataset,
            activity_dataset_parameters={
                "workspace_url": {
                    "value": "@pipeline().globalParameters.databricks_workspace_url",
                    "type": "Expression",
                }
            },
        )
        assert result == "adb-1234567890123456"

    def test_derive_databricks_platform_instance_unresolvable_returns_none(
        self,
    ) -> None:
        """No default anywhere and no global parameter match - must not
        guess, just leave platform_instance unset."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                domain="@{linkedService().dbx_domain}",
                parameters={"dbx_domain": SimpleNamespace(default_value=None)},
            )
        )
        result = source._derive_databricks_platform_instance(
            linked_service, "rg/factory"
        )
        assert result is None

    @pytest.mark.parametrize(
        "domain",
        [
            "@linkedService().dbx_domain",
            {"value": "@{linkedService().dbx_domain}", "type": "Expression"},
            {"value": "@linkedService().dbx_domain", "type": "Expression"},
        ],
    )
    def test_derive_databricks_platform_instance_other_self_reference_shapes(
        self, domain: Any
    ) -> None:
        """The domain self-reference resolves whether it's interpolated,
        a whole-value expression, or Expression-wrapped."""
        source = self._make_source()
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(
                domain=domain,
                parameters={
                    "dbx_domain": SimpleNamespace(
                        default_value="https://adb-1234567890123456.4.azuredatabricks.net/"
                    )
                },
            )
        )
        result = source._derive_databricks_platform_instance(
            linked_service, "rg/factory"
        )
        assert result == "adb-1234567890123456"

    def test_substitute_pipeline_run_parameters_accepts_resolved_value(
        self,
    ) -> None:
        """Sanity check alongside the rejection test above: a genuinely
        resolved pipeline parameter value must still substitute normally."""
        source = self._make_source()
        static_params = {"table_name": "@pipeline().parameters.TargetDb"}
        pipeline_run_parameters = {"TargetDb": "Orders"}

        result = source._substitute_pipeline_run_parameters(
            static_params, pipeline_run_parameters
        )

        assert result == {"table_name": "Orders"}

    def test_get_activity_translator_config_from_sdk_object_attribute(self) -> None:
        """An SDK-typed translator object (exposing as_dict(), rather
        than already being a plain dict) counts as present too - the
        shape adf_source.py's column-lineage skip-check needs to
        recognize, on top of the dict/typeProperties shapes already
        covered by TestCopyActivityColumnLineageExtractor."""
        translator_obj = SimpleNamespace(
            as_dict=lambda: {"type": "TabularTranslator", "columnMappings": {}}
        )
        activity = SimpleNamespace(translator=translator_obj, type_properties={})
        result = get_activity_translator_config(activity)
        assert result == {"type": "TabularTranslator", "columnMappings": {}}

    def test_resolve_dynamic_dataset_refs_preserves_non_parameterized_sibling(
        self,
    ) -> None:
        """Regression test: when an activity has two inputs and only one
        of them is driven by "@pipeline().parameters.X", the resolved
        set must still include the other, non-parameterized input -
        previously, resolving even a single ref dropped every ref that
        had no static parameters to substitute at all."""
        source = self._make_source()
        static_dataset = self._make_dataset(table="StaticTable")
        dynamic_dataset = self._make_dataset(
            table={"value": "@dataset().table_name", "type": "Expression"},
        )
        for dataset in (static_dataset, dynamic_dataset):
            dataset.properties.linked_service_name = SimpleNamespace(
                reference_name="MySqlLS"
            )
        linked_service = SimpleNamespace(
            properties=SimpleNamespace(type="AzureSqlDatabase")
        )
        source._datasets_cache = {
            "rg/factory": {
                "StaticDataset": static_dataset,
                "DynamicDataset": dynamic_dataset,
            }
        }
        source._linked_services_cache = {"rg/factory": {"MySqlLS": linked_service}}
        source.config.platform_instance_map = {}

        static_ref = SimpleNamespace(reference_name="StaticDataset", parameters=None)
        dynamic_ref = SimpleNamespace(
            reference_name="DynamicDataset",
            parameters={"table_name": "@pipeline().parameters.TargetTable"},
        )

        resolved = source._resolve_dynamic_dataset_refs(
            [static_ref, dynamic_ref],
            "rg/factory",
            {"TargetTable": "RealOrders"},
        )

        assert any("StaticTable" in urn for urn in resolved)
        assert any("RealOrders" in urn for urn in resolved)
        assert len(resolved) == 2


class TestFilePathExtractionLogic:
    """Tests for file path extraction from dataset properties."""

    def test_combine_folder_and_filename(self) -> None:
        """Should combine folderPath and fileName."""
        type_props = {"folderPath": "raw/data", "fileName": "file.csv"}
        folder = type_props.get("folderPath", "")
        filename = type_props.get("fileName", "")
        result = f"{folder}/{filename}" if folder and filename else filename or folder
        assert result == "raw/data/file.csv"

    def test_folder_only_returns_folder(self) -> None:
        """Should return folder when filename is missing."""
        type_props = {"folderPath": "raw/data"}
        folder = type_props.get("folderPath", "")
        filename = type_props.get("fileName", "")
        result = f"{folder}/{filename}" if folder and filename else filename or folder
        assert result == "raw/data"

    def test_nested_location_extraction(self) -> None:
        """Should extract path components from nested location object."""
        type_props = {
            "location": {
                "container": "mycontainer",
                "folderPath": "data/raw",
                "fileName": "output.parquet",
            }
        }
        location = type_props.get("location", {})
        if isinstance(location, dict):
            container = location.get("container", "")
            folder = location.get("folderPath", "")
            filename = location.get("fileName", "")
            parts = [p for p in [container, folder, filename] if p]
            result = "/".join(parts) if parts else None
        else:
            result = None
        assert result == "mycontainer/data/raw/output.parquet"


class TestRunStatusMapping:
    """Tests for mapping ADF run status to DataHub InstanceRunResult."""

    def test_succeeded_maps_to_success(self) -> None:
        """Succeeded status should map to SUCCESS result."""
        status_map = {
            "Succeeded": InstanceRunResult.SUCCESS,
            "Failed": InstanceRunResult.FAILURE,
            "Cancelled": InstanceRunResult.SKIPPED,
        }
        assert status_map["Succeeded"] == InstanceRunResult.SUCCESS

    def test_failed_maps_to_failure(self) -> None:
        """Failed status should map to FAILURE result."""
        status_map = {
            "Succeeded": InstanceRunResult.SUCCESS,
            "Failed": InstanceRunResult.FAILURE,
            "Cancelled": InstanceRunResult.SKIPPED,
        }
        assert status_map["Failed"] == InstanceRunResult.FAILURE

    def test_cancelled_maps_to_skipped(self) -> None:
        """Cancelled status should map to SKIPPED result."""
        status_map = {
            "Cancelled": InstanceRunResult.SKIPPED,
        }
        assert status_map["Cancelled"] == InstanceRunResult.SKIPPED

    def test_in_progress_should_return_none(self) -> None:
        """In-progress statuses should not have a final result."""
        incomplete_statuses = ["InProgress", "Queued", "Cancelling"]
        status_map = {
            "InProgress": None,
            "Queued": None,
            "Cancelling": None,
        }
        for status in incomplete_statuses:
            assert status_map.get(status) is None


class TestResourceGroupExtractionLogic:
    """Tests for extracting resource group from Azure resource ID."""

    def test_extract_from_standard_resource_id(self) -> None:
        """Should extract resource group from standard Azure resource ID."""
        resource_id = (
            "/subscriptions/12345678-1234-1234-1234-123456789012"
            "/resourceGroups/my-resource-group"
            "/providers/Microsoft.DataFactory/factories/my-factory"
        )
        parts = resource_id.split("/")
        rg_index = parts.index("resourceGroups")
        resource_group = parts[rg_index + 1]
        assert resource_group == "my-resource-group"

    def test_extract_with_complex_resource_group_name(self) -> None:
        """Should handle resource groups with hyphens, underscores, and numbers."""
        test_cases = [
            ("prod-data-rg-001", "prod-data-rg-001"),
            ("RG_Production_123", "RG_Production_123"),
            ("simple", "simple"),
        ]
        for rg_name, expected in test_cases:
            resource_id = (
                f"/subscriptions/00000000-0000-0000-0000-000000000000"
                f"/resourceGroups/{rg_name}"
                f"/providers/Microsoft.DataFactory/factories/factory1"
            )
            parts = resource_id.split("/")
            rg_index = parts.index("resourceGroups")
            extracted = parts[rg_index + 1]
            assert extracted == expected


class TestActivityRunPropertyExtraction:
    """Tests for activity run property extraction logic.

    Activity runs create DataProcessInstance entities linked to DataJobs.
    These tests verify the property extraction patterns.
    """

    def test_activity_run_properties_extracted(self) -> None:
        """Verify essential activity run properties are extracted."""
        activity_run: dict[str, object] = {
            "activityRunId": "act-run-123",
            "activityName": "CopyData",
            "activityType": "Copy",
            "pipelineRunId": "pipe-run-456",
            "status": "Succeeded",
            "durationInMs": 45000,
        }

        # Logic pattern from _emit_activity_runs
        properties: dict[str, str] = {
            "activity_run_id": str(activity_run["activityRunId"]),
            "activity_type": str(activity_run["activityType"]),
            "pipeline_run_id": str(activity_run["pipelineRunId"]),
            "status": str(activity_run["status"]),
        }

        if activity_run.get("durationInMs") is not None:
            properties["duration_ms"] = str(activity_run["durationInMs"])

        assert properties["activity_run_id"] == "act-run-123"
        assert properties["activity_type"] == "Copy"
        assert properties["pipeline_run_id"] == "pipe-run-456"
        assert properties["status"] == "Succeeded"
        assert properties["duration_ms"] == "45000"

    def test_activity_run_error_truncated(self) -> None:
        """Verify error messages are truncated to prevent oversized properties."""
        MAX_RUN_MESSAGE_LENGTH = 500
        long_error = "E" * 1000  # 1000 character error

        activity_run: dict[str, object] = {
            "activityRunId": "act-run-err",
            "error": {"message": long_error},
        }

        # Logic pattern from _emit_activity_runs
        truncated = ""
        error = activity_run.get("error")
        if isinstance(error, dict):
            error_msg = str(error.get("message", ""))
            if error_msg:
                truncated = error_msg[:MAX_RUN_MESSAGE_LENGTH]

        assert len(truncated) == MAX_RUN_MESSAGE_LENGTH
        assert len(truncated) < len(long_error)

    def test_activity_run_missing_optional_fields(self) -> None:
        """Verify graceful handling of missing optional fields."""
        activity_run: dict[str, object] = {
            "activityRunId": "act-run-minimal",
            "activityName": "MinimalActivity",
            "activityType": "Copy",
            "pipelineRunId": "pipe-run-789",
            "status": "Succeeded",
            # No durationInMs, error, input, output
        }

        properties: dict[str, str] = {
            "activity_run_id": str(activity_run["activityRunId"]),
            "activity_type": str(activity_run["activityType"]),
            "pipeline_run_id": str(activity_run["pipelineRunId"]),
            "status": str(activity_run["status"]),
        }

        # Optional fields should not cause errors
        if activity_run.get("durationInMs") is not None:
            properties["duration_ms"] = str(activity_run["durationInMs"])

        error = activity_run.get("error")
        if isinstance(error, dict):
            error_msg = str(error.get("message", ""))
            if error_msg:
                properties["error"] = error_msg[:500]

        assert "duration_ms" not in properties
        assert "error" not in properties
        assert len(properties) == 4


class TestActivityRunToDataJobUrnMapping:
    """Tests for mapping activity runs to DataJob URNs.

    Activity runs must link to DataJob URNs (not DataFlow URNs) so the
    Runs tab appears on DataJob pages in the UI.
    """

    def test_datajob_urn_constructed_from_activity_run(self) -> None:
        """DataJob URN should use activity name as job_id."""
        from datahub.metadata.urns import DataFlowUrn, DataJobUrn

        factory_name = "my-factory"
        pipeline_name = "DataPipeline"
        activity_name = "CopyActivity"
        env = "PROD"
        platform = "azure-data-factory"

        # Logic pattern from _emit_activity_runs
        flow_name = f"{factory_name}.{pipeline_name}"
        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator=platform,
            flow_id=flow_name,
            env=env,
        )
        job_urn = DataJobUrn.create_from_ids(
            data_flow_urn=str(flow_urn),
            job_id=activity_name,
        )

        # Verify URN structure
        assert "dataJob" in str(job_urn)
        assert activity_name in str(job_urn)
        assert flow_name in str(job_urn)
        assert platform in str(job_urn)

    def test_activity_run_links_to_datajob_not_dataflow(self) -> None:
        """Verify activity runs link to DataJob, enabling the Runs tab in UI."""
        from datahub.metadata.urns import DataFlowUrn, DataJobUrn

        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator="azure-data-factory",
            flow_id="factory.pipeline",
            env="PROD",
        )
        job_urn = DataJobUrn.create_from_ids(
            data_flow_urn=str(flow_urn),
            job_id="MyActivity",
        )

        # The URN type should be dataJob, not dataFlow
        assert job_urn.entity_type == "dataJob"
        assert flow_urn.entity_type == "dataFlow"

        # The job URN should reference the flow URN
        assert str(flow_urn) in str(job_urn)

    def test_multiple_activities_get_unique_urns(self) -> None:
        """Each activity in a pipeline should have a unique DataJob URN."""
        from datahub.metadata.urns import DataFlowUrn, DataJobUrn

        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator="azure-data-factory",
            flow_id="factory.pipeline",
            env="PROD",
        )

        activities = ["CopyData", "TransformData", "LoadData"]
        job_urns = [
            DataJobUrn.create_from_ids(
                data_flow_urn=str(flow_urn),
                job_id=activity,
            )
            for activity in activities
        ]

        # All URNs should be unique
        assert len(set(str(u) for u in job_urns)) == len(activities)

        # Each URN should contain its activity name
        for activity, urn in zip(activities, job_urns, strict=False):
            assert activity in str(urn)


# =============================================================================
# Column-Level Lineage Tests
# =============================================================================


class MockActivity:
    """Mock activity object for testing column lineage extraction."""

    def __init__(
        self,
        activity_type: str = "Copy",
        translator: dict | None = None,
        type_properties: dict | None = None,
        name: str = "TestActivity",
    ):
        self.name = name
        self.type = activity_type
        self.translator = translator
        self.type_properties = type_properties


def make_schema_resolver(
    schema_map: Optional[dict[str, DatasetSchemaInfo]] = None,
) -> Callable[[str], Optional[DatasetSchemaInfo]]:
    """Create a schema resolver function for testing.

    Args:
        schema_map: Optional dict mapping dataset URNs to DatasetSchemaInfo.
                   If None, resolver always returns None.
    """

    def resolver(dataset_urn: str) -> Optional[DatasetSchemaInfo]:
        if schema_map is None:
            return None
        return schema_map.get(dataset_urn)

    return resolver


class TestCopyActivityColumnLineageExtractor:
    """Tests for CopyActivityColumnLineageExtractor.

    These tests verify the column mapping extraction logic for Copy activities.
    """

    @pytest.mark.parametrize(
        "activity_type,expected",
        [
            ("Copy", True),
            ("ExecuteDataFlow", False),
            ("Lookup", False),
            ("ForEach", False),
            ("If", False),
            ("Switch", False),
        ],
    )
    def test_supports_activity_types(self, activity_type: str, expected: bool) -> None:
        """Extractor should only support Copy activity type."""
        extractor = CopyActivityColumnLineageExtractor()
        assert extractor.supports_activity(activity_type) is expected

    def test_extract_dict_format_column_mappings(self) -> None:
        """Should parse legacy dictionary format: {source_col: sink_col}."""

        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={
                "type": "TabularTranslator",
                "columnMappings": {
                    "source_id": "target_id",
                    "source_name": "target_name",
                },
            }
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.source_table,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.sink_table,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 2
        # Extract column names from URNs (format: urn:li:schemaField:(...,column))
        mapping_dict = {}
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            upstream_col = fgl.upstreams[0].split(",")[-1].rstrip(")")
            downstream_col = fgl.downstreams[0].split(",")[-1].rstrip(")")
            mapping_dict[upstream_col] = downstream_col
        assert mapping_dict["source_id"] == "target_id"
        assert mapping_dict["source_name"] == "target_name"
        # Verify URNs contain the dataset URNs
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            assert source_urn in fgl.upstreams[0]
            assert sink_urn in fgl.downstreams[0]

    def test_extract_list_format_column_mappings(self) -> None:
        """Should parse current list format: [{source: {name}, sink: {name}}]."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={
                "type": "TabularTranslator",
                "mappings": [
                    {"source": {"name": "col_a"}, "sink": {"name": "col_x"}},
                    {"source": {"name": "col_b"}, "sink": {"name": "col_y"}},
                ],
            }
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 2
        # Extract column names from URNs
        mapping_dict = {}
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            upstream_col = fgl.upstreams[0].split(",")[-1].rstrip(")")
            downstream_col = fgl.downstreams[0].split(",")[-1].rstrip(")")
            mapping_dict[upstream_col] = downstream_col
        assert mapping_dict["col_a"] == "col_x"
        assert mapping_dict["col_b"] == "col_y"

    def test_empty_when_no_translator(self) -> None:
        """Should return empty list when no translator configuration."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(translator=None, type_properties=None)

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert lineages == []

    def test_empty_when_missing_inlets_or_outlets(self) -> None:
        """Should return empty list when inlets or outlets are empty."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"columnMappings": {"a": "b"}},
        )

        # Missing inlets
        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[],
            outlets=["urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"],
            schema_resolver=make_schema_resolver(),
        )
        assert lineages == []

        # Missing outlets
        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=["urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"],
            outlets=[],
            schema_resolver=make_schema_resolver(),
        )
        assert lineages == []

    def test_handles_empty_column_names_gracefully(self) -> None:
        """Should skip mappings with empty or None column names."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={
                "columnMappings": {
                    "valid_src": "valid_sink",
                    "": "empty_source",
                    "empty_sink": "",
                },
            }
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        # Only the valid mapping should be extracted
        assert len(lineages) == 1
        # Extract column names from URNs
        assert lineages[0].upstreams is not None and lineages[0].downstreams is not None
        upstream_col = lineages[0].upstreams[0].split(",")[-1].rstrip(")")
        downstream_col = lineages[0].downstreams[0].split(",")[-1].rstrip(")")
        assert upstream_col == "valid_src"
        assert downstream_col == "valid_sink"

    def test_extract_from_sdk_flattened_translator(self) -> None:
        """Should extract translator from activity-level attribute (SDK flattening)."""
        extractor = CopyActivityColumnLineageExtractor()
        # Translator at activity level (not in typeProperties)
        activity = MockActivity(
            translator={
                "type": "TabularTranslator",
                "columnMappings": {"id": "id"},
            },
            type_properties={},  # Empty typeProperties
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        assert lineages[0].upstreams is not None
        upstream_col = lineages[0].upstreams[0].split(",")[-1].rstrip(")")
        assert upstream_col == "id"

    def test_extract_from_type_properties_translator(self) -> None:
        """Should extract translator from typeProperties when not at activity level."""
        extractor = CopyActivityColumnLineageExtractor()
        # Translator in typeProperties (raw JSON format)
        activity = MockActivity(
            translator=None,
            type_properties={
                "translator": {
                    "type": "TabularTranslator",
                    "columnMappings": {"name": "full_name"},
                }
            },
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        assert lineages[0].upstreams is not None and lineages[0].downstreams is not None
        upstream_col = lineages[0].upstreams[0].split(",")[-1].rstrip(")")
        downstream_col = lineages[0].downstreams[0].split(",")[-1].rstrip(")")
        assert upstream_col == "name"
        assert downstream_col == "full_name"

    def test_infer_auto_mapped_columns_from_source_schema(self) -> None:
        """Should infer 1:1 mappings from source schema when no explicit mappings."""
        extractor = CopyActivityColumnLineageExtractor()
        # TabularTranslator without explicit mappings
        activity = MockActivity(
            translator={"type": "TabularTranslator"},  # No columnMappings or mappings
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"
        source_schema = DatasetSchemaInfo(columns=["id", "name", "email"])

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver({source_urn: source_schema}),
        )

        assert len(lineages) == 3
        # Auto-mapping means same column name on both sides
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            upstream_col = fgl.upstreams[0].split(",")[-1].rstrip(")")
            downstream_col = fgl.downstreams[0].split(",")[-1].rstrip(")")
            assert upstream_col == downstream_col
        column_names = set()
        for fgl in lineages:
            assert fgl.upstreams is not None
            column_names.add(fgl.upstreams[0].split(",")[-1].rstrip(")"))
        assert column_names == {"id", "name", "email"}

    def test_infer_auto_mapped_columns_uses_sink_schema_casing(self) -> None:
        """Regression test: when the sink's own schema is available, an
        auto-mapped column must use the sink's actual field-path casing,
        not the source's - a downstream schemaField URN with the wrong
        casing doesn't correspond to any real field DataHub knows about
        for that dataset, so it silently fails to render as a lineage
        edge even though the aspect data looks populated. ADF's own
        auto-mapping matches source and sink columns case-insensitively
        (matching a case-insensitive collation like SQL Server's), so a
        source column "Ship_Date" correctly maps onto a sink column
        physically named "ship_date"."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"type": "TabularTranslator"},
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"
        source_schema = DatasetSchemaInfo(columns=["Order_ID", "Ship_Date", "Region"])
        sink_schema = DatasetSchemaInfo(columns=["order_id", "ship_date", "region"])

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(
                {source_urn: source_schema, sink_urn: sink_schema}
            ),
        )

        assert len(lineages) == 3
        mapping = {}
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            upstream_col = fgl.upstreams[0].split(",")[-1].rstrip(")")
            downstream_col = fgl.downstreams[0].split(",")[-1].rstrip(")")
            mapping[upstream_col] = downstream_col
        assert mapping == {
            "Order_ID": "order_id",
            "Ship_Date": "ship_date",
            "Region": "region",
        }

    def test_infer_auto_mapped_columns_falls_back_to_source_casing_without_sink_schema(
        self,
    ) -> None:
        """When the sink's schema isn't available (e.g. not yet ingested
        by another connector), fall back to assuming identical casing -
        the prior behavior - rather than emitting nothing."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"type": "TabularTranslator"},
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:databricks,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"
        source_schema = DatasetSchemaInfo(columns=["Ship_Date"])

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver({source_urn: source_schema}),
        )

        assert len(lineages) == 1
        assert lineages[0].upstreams is not None and lineages[0].downstreams is not None
        downstream_col = lineages[0].downstreams[0].split(",")[-1].rstrip(")")
        assert downstream_col == "Ship_Date"

    def test_no_inference_without_source_schema(self) -> None:
        """Should not infer mappings when source schema is unavailable."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"type": "TabularTranslator"},  # No explicit mappings
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),  # Returns None
        )

        assert lineages == []


class TestSinkColumnCasing:
    def test_matches_case_insensitively(self) -> None:
        casing = SinkColumnCasing.from_schema(
            DatasetSchemaInfo(
                columns=["order_id", "ship_date", "region"], from_datahub=True
            )
        )
        assert casing.resolve("Ship_Date") == "ship_date"
        assert casing.resolve("Region") == "region"

    def test_keeps_given_name_without_sink_schema(self) -> None:
        assert SinkColumnCasing.from_schema(None).resolve("Ship_Date") == "Ship_Date"

    def test_no_match_in_datahub_schema_is_unresolved(self) -> None:
        casing = SinkColumnCasing.from_schema(
            DatasetSchemaInfo(columns=["order_id"], from_datahub=True)
        )
        assert casing.resolve("Ship_Date") is None

    def test_no_match_in_adf_declared_schema_keeps_given_name(self) -> None:
        # An ADF dataset's structure may list only some of the sink's columns.
        casing = SinkColumnCasing.from_schema(
            DatasetSchemaInfo(columns=["Order_ID"], from_datahub=False)
        )
        assert casing.resolve("order_id") == "Order_ID"
        assert casing.resolve("Ship_Date") == "Ship_Date"

    def test_columns_differing_only_by_case_are_unresolved(self) -> None:
        casing = SinkColumnCasing.from_schema(
            DatasetSchemaInfo(
                columns=["Region", "region", "order_id"], from_datahub=True
            )
        )
        assert casing.resolve("Region") is None
        assert casing.resolve("Order_ID") == "order_id"


class TestSourceDatasetSchemaExtraction:
    """Tests for dataset schema extraction logic.

    These tests verify the schema extraction patterns used by
    _get_source_dataset_schema and _extract_dataset_schema.
    """

    def test_extract_columns_from_schema_definition(self) -> None:
        """Should extract columns from dataset's schema property (newer format)."""
        schema = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "created_at", "type": "datetime"},
        ]

        # Logic pattern from _extract_dataset_schema
        columns = []
        for field in schema:
            if isinstance(field, dict):
                name = field.get("name")
                if name:
                    columns.append(str(name))

        assert columns == ["id", "name", "created_at"]

    def test_extract_columns_from_structure(self) -> None:
        """Should extract columns from dataset's structure property (legacy format)."""
        structure = [
            {"name": "column_a"},
            {"name": "column_b"},
        ]

        columns = []
        for field in structure:
            if isinstance(field, dict):
                name = field.get("name")
                if name:
                    columns.append(str(name))

        assert columns == ["column_a", "column_b"]

    def test_returns_empty_when_no_schema(self) -> None:
        """Should return empty list when no schema or structure."""
        schema = None
        structure = None

        columns: list[str] = []
        if schema and isinstance(schema, list):
            for field in schema:
                if isinstance(field, dict):
                    name = field.get("name")
                    if name:
                        columns.append(str(name))

        if not columns and structure and isinstance(structure, list):
            for field in structure:
                if isinstance(field, dict):
                    name = field.get("name")
                    if name:
                        columns.append(str(name))

        assert columns == []


class TestFineGrainedLineageOutput:
    """Tests for FineGrainedLineageClass output from column lineage extractor.

    These tests verify the extractor produces correct URN formats and types.
    """

    def test_produces_correct_schema_field_urns(self) -> None:
        """Should produce valid SchemaFieldUrn for upstreams and downstreams."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={
                "type": "TabularTranslator",
                "columnMappings": {"id": "target_id"},
            }
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        fgl = lineages[0]

        # Verify URN formats
        assert fgl.upstreams is not None and fgl.downstreams is not None
        assert "schemaField" in fgl.upstreams[0]
        assert "id" in fgl.upstreams[0]
        assert "schemaField" in fgl.downstreams[0]
        assert "target_id" in fgl.downstreams[0]

    def test_upstream_type_is_field_set(self) -> None:
        """Upstream type should be FIELD_SET (many-to-one possible)."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"columnMappings": {"col": "col"}},
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,t,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,t2,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        assert lineages[0].upstreamType == FineGrainedLineageUpstreamTypeClass.FIELD_SET

    def test_downstream_type_is_field(self) -> None:
        """Downstream type should be FIELD (single field target)."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"columnMappings": {"col": "col"}},
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,t,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,t2,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        assert lineages[0].downstreamType == FineGrainedLineageDownstreamTypeClass.FIELD


class _FreshChildActivity(Activity):
    """An activity whose children are rebuilt on every attribute access, as
    SDK-style wrappers that deserialize lazily do. Popped parents get freed,
    so CPython can hand their id() to a newly built child."""

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(name=spec["name"])
        self.type = spec["type"]
        self._spec = spec

    @property
    def activities(self) -> List["_FreshChildActivity"]:
        return [_FreshChildActivity(child) for child in self._spec.get("children", [])]


class TestWalkActivities:
    def test_nested_activities_are_not_dropped_when_wrappers_are_rebuilt(
        self,
    ) -> None:
        def loop(name: str, children: List[Dict[str, Any]]) -> Dict[str, Any]:
            return {"name": name, "type": "ForEach", "children": children}

        def copy(name: str) -> Dict[str, Any]:
            return {"name": name, "type": "Copy"}

        specs = [
            copy("Lookup"),
            loop("Outer", [copy("CopyA"), loop("Inner", [copy("CopyB")])]),
            loop("Second", [copy("CopyC")]),
        ]
        source = AzureDataFactorySource.__new__(AzureDataFactorySource)

        walked = source._walk_activities(_FreshChildActivity(s) for s in specs)

        assert [a.name for a in walked] == [
            "Lookup",
            "Outer",
            "Second",
            "CopyA",
            "Inner",
            "CopyC",
            "CopyB",
        ]


class TestSimpleSelectStarDetection:
    """Tests for _is_simple_select_star - the gate that decides whether
    a query is unambiguously "copy this whole table", the one shape
    where falling back to a DataHub-graph schema lookup is safe."""

    def _make_source(self) -> AzureDataFactorySource:
        source = make_source()
        source.report = AzureDataFactorySourceReport()
        return source

    def test_simple_select_star_detected(self) -> None:
        source = self._make_source()
        assert source._is_simple_select_star("SELECT * FROM my_table", "mssql")

    def test_select_star_with_join_rejected(self) -> None:
        """A join means the result set isn't just "all of one table's
        columns" - must not be treated as a safe whole-table copy."""
        source = self._make_source()
        query = "SELECT * FROM a JOIN b ON a.id = b.id"
        assert not source._is_simple_select_star(query, "mssql")

    def test_explicit_column_list_rejected(self) -> None:
        source = self._make_source()
        assert not source._is_simple_select_star(
            "SELECT id, name FROM my_table", "mssql"
        )

    def test_malformed_sql_rejected(self) -> None:
        source = self._make_source()
        assert not source._is_simple_select_star("NOT VALID SQL {{{", "mssql")

    def test_cross_apply_rejected(self) -> None:
        """A table-valued CROSS APPLY widens the result set beyond the
        single source table's own columns, even though it still only
        references one real Table node - must not be treated as a safe
        whole-table copy."""
        source = self._make_source()
        query = "SELECT * FROM t1 CROSS APPLY OPENJSON(t1.col) x"
        assert not source._is_simple_select_star(query, "mssql")


class TestGraphSchemaColumnLineageFallback:
    """Tests for resolving column lineage from DataHub's own knowledge of
    a source table's schema, when a dynamic "SELECT *" query gives the
    connector no column list of its own to work from."""

    def _make_source(self, graph: Optional[Any]) -> AzureDataFactorySource:
        source = make_source()
        source.report = AzureDataFactorySourceReport()
        source.ctx = PipelineContext(run_id="test", graph=graph)
        source._graph_schema_cache = {}
        return source

    def _schema_metadata(self, columns: list) -> SchemaMetadataClass:
        return SchemaMetadataClass(
            schemaName="test",
            platform="urn:li:dataPlatform:mssql",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=[
                SchemaFieldClass(
                    fieldPath=col,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar",
                )
                for col in columns
            ],
        )

    def test_resolves_schema_and_builds_same_name_mappings(self) -> None:
        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.orders,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,orders,PROD)"

        mock_graph = MagicMock()
        mock_graph.get_schema_metadata.return_value = self._schema_metadata(
            ["id", "customer_name"]
        )
        source = self._make_source(graph=mock_graph)

        mappings = source._infer_column_lineage_from_graph_schema(source_urn, sink_urn)

        assert set(mappings) == {
            (
                (f"urn:li:schemaField:({source_urn},id)",),
                f"urn:li:schemaField:({sink_urn},id)",
            ),
            (
                (f"urn:li:schemaField:({source_urn},customer_name)",),
                f"urn:li:schemaField:({sink_urn},customer_name)",
            ),
        }
        # Both the source's and the sink's own schema are resolved, so
        # that an auto-mapped column can use the sink's actual
        # field-path casing (see infer_auto_mappings_by_name) instead of
        # assuming it matches the source's.
        mock_graph.get_schema_metadata.assert_any_call(source_urn)
        mock_graph.get_schema_metadata.assert_any_call(sink_urn)
        assert mock_graph.get_schema_metadata.call_count == 2

    def test_uses_sink_schema_casing_when_it_differs_from_source(self) -> None:
        """Regression test: the source and sink schemas can genuinely
        differ in column casing (e.g. a Title_Case source vs a
        lowercase sink under a case-insensitive collation) - the mapped
        downstream field must use the sink's own casing, not the
        source's, or it won't match any real field in the sink's
        schema."""
        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.orders,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,orders,PROD)"

        mock_graph = MagicMock()
        schemas_by_urn = {
            source_urn: self._schema_metadata(["Customer_Name"]),
            sink_urn: self._schema_metadata(["customer_name"]),
        }
        mock_graph.get_schema_metadata.side_effect = lambda urn: schemas_by_urn.get(urn)
        source = self._make_source(graph=mock_graph)

        mappings = source._infer_column_lineage_from_graph_schema(source_urn, sink_urn)

        assert set(mappings) == {
            (
                (f"urn:li:schemaField:({source_urn},Customer_Name)",),
                f"urn:li:schemaField:({sink_urn},customer_name)",
            ),
        }

    def test_normalizes_v2_field_paths_before_matching(self) -> None:
        """Regression test: a schema fetched from the graph can use
        DataHub's v2 fieldPath encoding (e.g.
        "[version=2.0].[type=struct].id") rather than a plain column
        name. Matching must use the simple name, or a v2-pathed source
        schema would never line up with the plain names used by parsed
        SQL and ADF's own Copy activity column references."""
        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.orders,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,orders,PROD)"

        mock_graph = MagicMock()
        mock_graph.get_schema_metadata.return_value = self._schema_metadata(
            ["[version=2.0].[type=struct].id", "[version=2.0].[type=struct].name"]
        )
        source = self._make_source(graph=mock_graph)

        mappings = source._infer_column_lineage_from_graph_schema(source_urn, sink_urn)

        assert set(mappings) == {
            (
                (f"urn:li:schemaField:({source_urn},id)",),
                f"urn:li:schemaField:({sink_urn},id)",
            ),
            (
                (f"urn:li:schemaField:({source_urn},name)",),
                f"urn:li:schemaField:({sink_urn},name)",
            ),
        }

    def test_caches_schema_lookup_across_calls(self) -> None:
        """The same source table is often referenced by many activity
        runs - the graph should only be queried once per distinct URN."""
        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,my_db.orders,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,orders,PROD)"

        mock_graph = MagicMock()
        mock_graph.get_schema_metadata.return_value = self._schema_metadata(["id"])
        source = self._make_source(graph=mock_graph)

        source._infer_column_lineage_from_graph_schema(source_urn, sink_urn)
        source._infer_column_lineage_from_graph_schema(source_urn, sink_urn)

        # One call each for source_urn and sink_urn on the first
        # invocation; the second invocation hits the cache for both.
        assert mock_graph.get_schema_metadata.call_count == 2

    def test_no_graph_configured_returns_empty_without_error(self) -> None:
        """A file sink (or any recipe without datahub_api/datahub-rest)
        leaves ctx.graph unset - the fallback must degrade gracefully,
        not crash."""
        source = self._make_source(graph=None)

        mappings = source._infer_column_lineage_from_graph_schema(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,t,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,t,PROD)",
        )

        assert mappings == []

    def test_graph_lookup_failure_is_reported_and_not_retried(self) -> None:
        mock_graph = MagicMock()
        mock_graph.get_schema_metadata.side_effect = Exception("GMS unavailable")
        source = self._make_source(graph=mock_graph)

        for _ in range(2):
            mappings = source._infer_column_lineage_from_graph_schema(
                "urn:li:dataset:(urn:li:dataPlatform:mssql,t,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:postgres,t,PROD)",
            )
            assert mappings == []

        assert mock_graph.get_schema_metadata.call_count == 1
        assert {w.title for w in source.report.warnings} == {
            "Failed to Fetch Schema from DataHub"
        }

    def test_schema_with_no_fields_returns_empty(self) -> None:
        mock_graph = MagicMock()
        mock_graph.get_schema_metadata.return_value = self._schema_metadata([])
        source = self._make_source(graph=mock_graph)

        mappings = source._infer_column_lineage_from_graph_schema(
            "urn:li:dataset:(urn:li:dataPlatform:mssql,t,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:postgres,t,PROD)",
        )

        assert mappings == []


def _table_urn(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:mssql,db.{table},PROD)"


def _pair(source_table: str, sink_table: str) -> LineagePair:
    return LineagePair.of([_table_urn(source_table)], [_table_urn(sink_table)])


def _io_aspects(
    workunits: list[MetadataWorkUnit],
) -> dict[str, DataJobInputOutputClass]:
    aspects: dict[str, DataJobInputOutputClass] = {}
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, DataJobInputOutputClass
        ):
            assert wu.metadata.entityUrn is not None
            aspects[wu.metadata.entityUrn] = wu.metadata.aspect
    return aspects


def _warning_titles(source: AzureDataFactorySource) -> set[Optional[str]]:
    return {w.title for w in source.report.warnings}


class TestDynamicLineageFanOut:
    """_emit_dynamic_lineage_augmentation for an activity whose execution
    history has many distinct sources and many distinct sinks."""

    FACTORY_KEY = "test-rg/test-factory"
    PARENT_JOB_URN = (
        "urn:li:dataJob:(urn:li:dataFlow:(azure-data-factory,"
        "test-factory.TestPipeline,PROD),CopyActivity)"
    )

    def _run(
        self,
        pairs: dict[LineagePair, set[ColumnMapping]],
        static_pair: LineagePair = LineagePair(inputs=(), outputs=()),
        cap: int = 50,
        static_placeholders: Optional[set[str]] = None,
        unresolved_pairs: Optional[set[LineagePair]] = None,
        execute_pipeline_callers: Optional[list[str]] = None,
        stateful: bool = False,
        description: Optional[str] = None,
    ) -> tuple[AzureDataFactorySource, list[MetadataWorkUnit]]:
        source = make_source()
        config: dict[str, Any] = {
            "subscription_id": "test-sub",
            "include_column_lineage": False,
            "max_dynamic_lineage_pairs_per_activity": cap,
        }
        if stateful:
            config["stateful_ingestion"] = {"enabled": True}
        source.config = AzureDataFactoryConfig.model_validate(config)
        activity = SimpleNamespace(
            name="CopyActivity", type="Copy", description=description
        )
        source._pipelines_cache = {
            self.FACTORY_KEY: {"TestPipeline": SimpleNamespace(activities=[activity])}
        }
        source._dataflow_cache = {
            (self.FACTORY_KEY, "TestPipeline"): DataFlow(
                platform="azure-data-factory", name="test-factory.TestPipeline"
            )
        }
        source._dynamic_lineage_cache = {
            (self.FACTORY_KEY, "TestPipeline", "CopyActivity"): pairs
        }
        cache_key = (self.FACTORY_KEY, "TestPipeline", "CopyActivity")
        source._resolved_dynamic_pairs = {
            (cache_key, pair)
            for pair in pairs
            if pair not in (unresolved_pairs or set())
        }
        if execute_pipeline_callers:
            source._execute_pipeline_input_datajobs = {
                self.PARENT_JOB_URN: execute_pipeline_callers
            }

        def static_side(urns: tuple[str, ...]) -> Callable[..., list[str]]:
            def extract(
                activity: Any, factory_key: str, placeholders: Optional[set[str]] = None
            ) -> list[str]:
                if placeholders is not None:
                    placeholders.update(set(urns) & (static_placeholders or set()))
                return list(urns)

            return extract

        with (
            patch.object(
                source,
                "_extract_activity_inputs",
                side_effect=static_side(static_pair.inputs),
            ),
            patch.object(
                source,
                "_extract_activity_outputs",
                side_effect=static_side(static_pair.outputs),
            ),
        ):
            workunits = list(
                source._emit_dynamic_lineage_augmentation(
                    SimpleNamespace(name="test-factory"), "test-rg"
                )
            )
        return source, workunits

    def _siblings(
        self, workunits: list[MetadataWorkUnit]
    ) -> dict[str, DataJobInputOutputClass]:
        return {
            urn: aspect
            for urn, aspect in _io_aspects(workunits).items()
            if urn != self.PARENT_JOB_URN
        }

    def test_one_sibling_per_pair_with_only_that_pairs_tables(self) -> None:
        source, workunits = self._run(
            {_pair("orders", "stg_orders"): set(), _pair("users", "stg_users"): set()}
        )

        siblings = self._siblings(workunits)
        assert {
            (tuple(a.inputDatasets), tuple(a.outputDatasets)) for a in siblings.values()
        } == {
            ((_table_urn("orders"),), (_table_urn("stg_orders"),)),
            ((_table_urn("users"),), (_table_urn("stg_users"),)),
        }
        # The parent's static lineage is real here, so it is left alone.
        assert self.PARENT_JOB_URN not in _io_aspects(workunits)
        assert source.report.dynamic_lineage_sibling_jobs_emitted == 2

    def test_sibling_job_id_is_stable(self) -> None:
        _, workunits = self._run(
            {_pair("orders", "stg_orders"): set(), _pair("users", "stg_users"): set()}
        )

        assert (
            "urn:li:dataJob:(urn:li:dataFlow:(azure-data-factory,"
            "test-factory.TestPipeline,PROD),CopyActivity__f11cb8338a88)"
            in self._siblings(workunits)
        )

    def test_pairs_over_cap_are_dropped_by_urn_order(self) -> None:
        # Inserted out of order: which pairs make the cap must not depend on
        # the order runs came back from the API.
        pairs: dict[LineagePair, set[ColumnMapping]] = {
            _pair(name, f"stg_{name}"): set() for name in ["gamma", "alpha", "beta"]
        }

        source, workunits = self._run(pairs, cap=2)

        siblings = self._siblings(workunits)
        assert {a.inputDatasets[0] for a in siblings.values()} == {
            _table_urn("alpha"),
            _table_urn("beta"),
        }
        # Not unioned onto the parent either.
        assert self.PARENT_JOB_URN not in _io_aspects(workunits)
        assert source.report.dynamic_lineage_pairs_over_cap == 1
        assert "Dynamic Lineage Pair Cap Exceeded" in _warning_titles(source)

    def test_exactly_cap_pairs_is_not_over_cap(self) -> None:
        source, workunits = self._run(
            {_pair("a", "x"): set(), _pair("b", "y"): set()}, cap=2
        )

        assert len(self._siblings(workunits)) == 2
        assert source.report.dynamic_lineage_pairs_over_cap == 0

    def test_static_pair_does_not_use_a_cap_slot(self) -> None:
        static = _pair("a", "x")
        source, workunits = self._run(
            {static: set(), _pair("b", "y"): set(), _pair("c", "z"): set()},
            static_pair=static,
            cap=2,
        )

        siblings = self._siblings(workunits)
        assert {a.inputDatasets[0] for a in siblings.values()} == {
            _table_urn("b"),
            _table_urn("c"),
        }
        assert source.report.dynamic_lineage_pairs_over_cap == 0

    def test_static_pair_column_lineage_goes_to_parent(self) -> None:
        static = _pair("a", "x")
        column: ColumnMapping = (
            (f"urn:li:schemaField:({_table_urn('a')},id)",),
            f"urn:li:schemaField:({_table_urn('x')},id)",
        )

        _, workunits = self._run(
            {static: {column}, _pair("b", "y"): set()}, static_pair=static
        )

        parent = _io_aspects(workunits)[self.PARENT_JOB_URN]
        assert parent.inputDatasets == [_table_urn("a")]
        assert parent.fineGrainedLineages is not None
        assert parent.fineGrainedLineages[0].downstreams == [column[1]]

    def test_placeholder_removed_from_parent(self) -> None:
        placeholder_in = "urn:li:dataset:(urn:li:dataPlatform:mssql,DS_Source,PROD)"
        placeholder_out = "urn:li:dataset:(urn:li:dataPlatform:mssql,DS_Sink,PROD)"
        caller = (
            "urn:li:dataJob:(urn:li:dataFlow:(azure-data-factory,f.Parent,PROD),Run)"
        )

        _, workunits = self._run(
            {_pair("a", "x"): set(), _pair("b", "y"): set()},
            static_pair=LineagePair.of([placeholder_in], [placeholder_out]),
            static_placeholders={placeholder_in, placeholder_out},
            execute_pipeline_callers=[caller],
        )

        parent = _io_aspects(workunits)[self.PARENT_JOB_URN]
        assert parent.inputDatasets == []
        assert parent.outputDatasets == []
        assert parent.inputDatajobs == [caller]

    def test_parent_keeps_execute_pipeline_edge_and_static_lineage(self) -> None:
        caller = (
            "urn:li:dataJob:(urn:li:dataFlow:(azure-data-factory,f.Parent,PROD),Run)"
        )
        static = _pair("a", "x")

        _, workunits = self._run(
            {static: set(), _pair("b", "y"): set(), _pair("c", "z"): set()},
            static_pair=static,
            execute_pipeline_callers=[caller],
        )

        parent = _io_aspects(workunits)[self.PARENT_JOB_URN]
        assert parent.inputDatasets == [_table_urn("a")]
        assert parent.outputDatasets == [_table_urn("x")]
        assert parent.inputDatajobs == [caller]

    def test_unresolved_run_gets_no_sibling(self) -> None:
        placeholder = "urn:li:dataset:(urn:li:dataPlatform:mssql,DS_Sink,PROD)"
        unresolved = LineagePair.of([_table_urn("c")], [placeholder])
        source, workunits = self._run(
            {_pair("a", "x"): set(), _pair("b", "y"): set(), unresolved: set()},
            unresolved_pairs={unresolved},
        )

        siblings = self._siblings(workunits)
        assert all(placeholder not in a.outputDatasets for a in siblings.values())
        assert len(siblings) == 2
        assert source.report.dynamic_lineage_pairs_unresolved == 1

    def test_resolved_table_matching_a_placeholder_urn_keeps_its_sibling(
        self,
    ) -> None:
        # A run that parsed table "orders" from its query yields the same URN
        # as the activity's unresolved "orders" ADF dataset. It is a real
        # table for that run, so its pair still gets a sibling.
        placeholder = _table_urn("orders")
        static = LineagePair.of([placeholder], [_table_urn("DS_Sink")])

        _, workunits = self._run(
            {_pair("orders", "stg_orders"): set(), _pair("users", "stg_users"): set()},
            static_pair=static,
            static_placeholders={placeholder, _table_urn("DS_Sink")},
        )

        sibling_inputs = {
            a.inputDatasets[0] for a in self._siblings(workunits).values()
        }
        assert sibling_inputs == {_table_urn("orders"), _table_urn("users")}

    def test_unresolved_run_does_not_turn_union_into_fan_out(self) -> None:
        # Only the source varies; one run left the sink as a placeholder.
        placeholder = "urn:li:dataset:(urn:li:dataPlatform:mssql,DS_Sink,PROD)"
        unresolved = LineagePair.of([_table_urn("c")], [placeholder])
        source, workunits = self._run(
            {_pair("a", "x"): set(), _pair("b", "x"): set(), unresolved: set()},
            unresolved_pairs={unresolved},
        )

        assert self._siblings(workunits) == {}
        parent = _io_aspects(workunits)[self.PARENT_JOB_URN]
        assert parent.inputDatasets == [_table_urn("a"), _table_urn("b")]
        assert parent.outputDatasets == [_table_urn("x")]

    def test_one_varying_side_is_unioned_onto_parent(self) -> None:
        source, workunits = self._run({_pair("a", "x"): set(), _pair("b", "x"): set()})

        assert self._siblings(workunits) == {}
        parent = _io_aspects(workunits)[self.PARENT_JOB_URN]
        assert parent.inputDatasets == [_table_urn("a"), _table_urn("b")]
        assert source.report.dynamic_lineage_sibling_jobs_emitted == 0

    def test_sibling_matches_parent_job_shape(self) -> None:
        caller = (
            "urn:li:dataJob:(urn:li:dataFlow:(azure-data-factory,f.Parent,PROD),Run)"
        )
        _, workunits = self._run(
            {_pair("orders", "stg_orders"): set(), _pair("users", "stg_users"): set()},
            execute_pipeline_callers=[caller],
            description="Copies one table",
        )

        sibling_urn = next(
            urn for urn in self._siblings(workunits) if "f11cb8338a88" in urn
        )
        info = next(
            wu.metadata.aspect
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.entityUrn == sibling_urn
            and isinstance(wu.metadata.aspect, DataJobInfoClass)
        )
        assert info.name == "CopyActivity (orders -> stg_orders)"
        assert info.customProperties == {
            "activity_type": "Copy",
            "activity_description": "Copies one table",
            "parent_activity": "CopyActivity",
        }
        assert self._siblings(workunits)[sibling_urn].inputDatajobs == [caller]

    def test_warns_once_without_stale_metadata_removal(self) -> None:
        pairs: dict[LineagePair, set[ColumnMapping]] = {
            _pair("a", "x"): set(),
            _pair("b", "y"): set(),
        }

        source, _ = self._run(pairs)
        assert [w.title for w in source.report.warnings].count(
            "Per-Pair DataJobs Without Stale Metadata Removal"
        ) == 1

        source, _ = self._run(pairs, stateful=True)
        assert "Per-Pair DataJobs Without Stale Metadata Removal" not in (
            _warning_titles(source)
        )

    def test_missing_dataflow_warns_and_emits_nothing(self) -> None:
        source = make_source()
        source._dataflow_cache = {}
        activity = SimpleNamespace(name="CopyActivity", type="Copy", description=None)

        workunits = list(
            source._emit_fanout_dynamic_lineage(
                factory=SimpleNamespace(name="test-factory"),
                resource_group="test-rg",
                factory_key=self.FACTORY_KEY,
                pipeline_name="TestPipeline",
                activity=activity,
                job_urn=DataJobUrn.from_string(self.PARENT_JOB_URN),
                pairs={_pair("a", "x"): set(), _pair("b", "y"): set()},
                unresolved_pair_count=0,
                static_pair=LineagePair(inputs=(), outputs=()),
                static_placeholders=set(),
            )
        )

        assert workunits == []
        assert "Missing Cached DataFlow for Sibling DataJob" in _warning_titles(source)

    def test_display_name_truncates_more_than_three_names(self) -> None:
        pair = LineagePair.of([_table_urn(t) for t in "abcd"], [_table_urn("sink")])

        name = make_source()._pair_sibling_display_name("CopyActivity", pair)

        assert name == "CopyActivity (a, b, c and 1 more -> sink)"


class TestDynamicLineageColumnRecompute:
    """Column lineage recomputed from the translator for each observed pair,
    since extractors only read inlets[0]/outlets[0]."""

    def _make_source(self, include_column_lineage: bool) -> AzureDataFactorySource:
        source = make_source()
        source.config = AzureDataFactoryConfig(
            subscription_id="test-sub", include_column_lineage=include_column_lineage
        )
        return source

    def _activity(self) -> SimpleNamespace:
        return SimpleNamespace(
            name="CopyActivity",
            type="Copy",
            description=None,
            translator={"type": "TabularTranslator", "columnMappings": {"id": "id"}},
        )

    def test_union_recomputes_per_pair(self) -> None:
        source = self._make_source(include_column_lineage=True)
        job_urn = DataJobUrn.from_string(TestDynamicLineageFanOut.PARENT_JOB_URN)

        workunits = list(
            source._emit_unioned_dynamic_lineage(
                self._activity(),
                job_urn,
                "test-rg/test-factory",
                {_pair("customers", "stg"): set(), _pair("vendors", "stg"): set()},
                LineagePair(inputs=(), outputs=()),
            )
        )

        aspect = _io_aspects(workunits)[str(job_urn)]
        assert aspect.fineGrainedLineages is not None
        assert {
            fgl.upstreams[0] for fgl in aspect.fineGrainedLineages if fgl.upstreams
        } == {
            f"urn:li:schemaField:({_table_urn('customers')},id)",
            f"urn:li:schemaField:({_table_urn('vendors')},id)",
        }

    def test_union_skips_column_lineage_when_disabled(self) -> None:
        source = self._make_source(include_column_lineage=False)
        job_urn = DataJobUrn.from_string(TestDynamicLineageFanOut.PARENT_JOB_URN)

        workunits = list(
            source._emit_unioned_dynamic_lineage(
                self._activity(),
                job_urn,
                "test-rg/test-factory",
                {_pair("customers", "stg"): set(), _pair("vendors", "stg"): set()},
                LineagePair(inputs=(), outputs=()),
            )
        )

        aspect = _io_aspects(workunits)[str(job_urn)]
        assert aspect.fineGrainedLineages is None
        assert set(aspect.inputDatasets) == {
            _table_urn("customers"),
            _table_urn("vendors"),
        }

    def test_pair_with_empty_side_is_not_reported_unsupported(self) -> None:
        source = self._make_source(include_column_lineage=True)

        lineages = source._pair_fine_grained_lineages(
            self._activity(),
            "test-rg/test-factory",
            LineagePair.of([_table_urn("customers")], []),
            set(),
        )

        assert lineages is None
        assert source.report.column_lineage_activities_unsupported == {}

    def test_sibling_gets_only_its_pairs_columns(self) -> None:
        source = self._make_source(include_column_lineage=True)

        lineages = source._pair_fine_grained_lineages(
            self._activity(),
            "test-rg/test-factory",
            _pair("customers", "stg"),
            set(),
        )

        assert lineages is not None
        assert [(fgl.upstreams, fgl.downstreams) for fgl in lineages] == [
            (
                [f"urn:li:schemaField:({_table_urn('customers')},id)"],
                [f"urn:li:schemaField:({_table_urn('stg')},id)"],
            )
        ]


def _schema_metadata(columns: list[str]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:mssql",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=col,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="varchar",
            )
            for col in columns
        ],
    )


class TestSinkSchemaFromDataHub:
    """Auto-mapped column lineage uses the sink schema DataHub already has."""

    def _make_source(self, schemas: dict[str, list[str]]) -> AzureDataFactorySource:
        source = make_source()
        graph = MagicMock()
        graph.get_schema_metadata.side_effect = lambda urn: (
            _schema_metadata(schemas[urn]) if urn in schemas else None
        )
        source.ctx = PipelineContext(run_id="test", graph=graph)
        return source

    def test_static_copy_uses_datahub_sink_casing(self) -> None:
        source = self._make_source(
            {
                _table_urn("orders"): ["Ship_Date", "Region"],
                _table_urn("stg_orders"): ["ship_date", "region"],
            }
        )
        activity = SimpleNamespace(name="CopyActivity", type="Copy", translator=None)

        lineages = source._extract_column_lineage(
            activity=activity,
            activity_type="Copy",
            inlets=[_table_urn("orders")],
            outlets=[_table_urn("stg_orders")],
            factory_key="test-rg/test-factory",
        )

        assert lineages is not None
        assert sorted(d for fgl in lineages for d in fgl.downstreams or []) == [
            f"urn:li:schemaField:({_table_urn('stg_orders')},region)",
            f"urn:li:schemaField:({_table_urn('stg_orders')},ship_date)",
        ]

    def test_column_missing_from_sink_is_skipped_and_reported(self) -> None:
        source = self._make_source(
            {
                _table_urn("orders"): ["id", "legacy_flag"],
                _table_urn("stg_orders"): ["id"],
            }
        )
        activity = SimpleNamespace(name="CopyActivity", type="Copy", translator=None)

        lineages = source._extract_column_lineage(
            activity=activity,
            activity_type="Copy",
            inlets=[_table_urn("orders")],
            outlets=[_table_urn("stg_orders")],
            factory_key="test-rg/test-factory",
        )

        assert lineages is not None
        assert [fgl.downstreams for fgl in lineages] == [
            [f"urn:li:schemaField:({_table_urn('stg_orders')},id)"]
        ]
        assert source.report.column_lineage_sink_columns_unmatched == 1
        assert "Column Not Found in Sink Schema" in _warning_titles(source)

    def test_query_parsed_column_uses_sink_casing(self) -> None:
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"
        source = self._make_source({sink_urn: ["customer_name"]})
        activity = SimpleNamespace(
            type="Copy",
            inputs=[SimpleNamespace(reference_name="SrcDataset", parameters=None)],
        )
        activity_run = SimpleNamespace(
            input={"source": {"query": "SELECT Customer_Name FROM orders"}}
        )

        with patch.object(
            source, "_resolve_dataset_ref_context", return_value=("mssql", None, None)
        ):
            column_mappings = source._extract_query_column_lineage(
                activity, activity_run, "test-rg/test-factory", sink_urn
            )

        assert [downstream for _, downstream in column_mappings] == [
            f"urn:li:schemaField:({sink_urn},customer_name)"
        ]


class TestCopyExtractorSchemaLookups:
    def test_sink_not_looked_up_without_source_columns(self) -> None:
        resolver = MagicMock(return_value=None)

        lineages = CopyActivityColumnLineageExtractor().extract_column_lineage(
            activity=SimpleNamespace(name="CopyActivity", translator=None),
            inlets=[_table_urn("orders")],
            outlets=[_table_urn("stg_orders")],
            schema_resolver=resolver,
        )

        assert lineages == []
        resolver.assert_called_once_with(_table_urn("orders"))

    def test_explicit_translator_skips_schema_lookups(self) -> None:
        resolver = MagicMock(return_value=None)

        lineages = CopyActivityColumnLineageExtractor().extract_column_lineage(
            activity=SimpleNamespace(
                name="CopyActivity",
                translator={"type": "TabularTranslator", "columnMappings": {"a": "b"}},
            ),
            inlets=[_table_urn("orders")],
            outlets=[_table_urn("stg_orders")],
            schema_resolver=resolver,
        )

        assert len(lineages) == 1
        resolver.assert_not_called()
