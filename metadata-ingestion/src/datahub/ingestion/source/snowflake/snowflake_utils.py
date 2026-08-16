import abc
from functools import cached_property
from typing import ClassVar, Dict, List, Literal, Optional, Tuple, Type

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp_builder import DatabaseKey, DataProductKey, SchemaKey
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.snowflake.constants import (
    DEFAULT_SNOWFLAKE_DOMAIN,
    SNOWFLAKE_REGION_CLOUD_REGION_MAPPING,
    SnowflakeCloudProvider,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeFilterConfig,
    SnowflakeIdentifierConfig,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.sql.sql_utils import gen_database_key, gen_schema_key
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    DateType,
    NullType,
    NumberType,
    RecordType,
    StringType,
    TimeType,
)
from datahub.metadata.urns import MetricUrn, SemanticModelUrn

# Truncate definition strings (e.g. task / pipe SQL bodies) stored in
# customProperties to stay well within DataHub's aspect size limits.
MAX_DEFINITION_LENGTH = 4000

# https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
# TODO: Move to the standardized types in sql_types.py
SNOWFLAKE_FIELD_TYPE_MAPPINGS: Dict[str, Type] = {
    "DATE": DateType,
    "BIGINT": NumberType,
    "BINARY": BytesType,
    # 'BIT': BIT,
    "BOOLEAN": BooleanType,
    "CHAR": NullType,
    "CHARACTER": NullType,
    "DATETIME": TimeType,
    "DEC": NumberType,
    "DECIMAL": NumberType,
    "DOUBLE": NumberType,
    "FIXED": NumberType,
    "FLOAT": NumberType,
    "INT": NumberType,
    "INTEGER": NumberType,
    "NUMBER": NumberType,
    # 'OBJECT': ?
    "REAL": NumberType,
    "BYTEINT": NumberType,
    "SMALLINT": NumberType,
    "STRING": StringType,
    "TEXT": StringType,
    "TIME": TimeType,
    "TIMESTAMP": TimeType,
    "TIMESTAMP_TZ": TimeType,
    "TIMESTAMP_LTZ": TimeType,
    "TIMESTAMP_NTZ": TimeType,
    "TINYINT": NumberType,
    "VARBINARY": BytesType,
    "VARCHAR": StringType,
    "VARIANT": RecordType,
    "OBJECT": NullType,
    "ARRAY": ArrayType,
    "GEOGRAPHY": NullType,
}


def snowflake_identity_key(name: str, *, preserve_column_case: bool) -> str:
    """Internal identity of a column, metric or logical-table name. Never emitted.

    Answers "are these two spellings the same object?". Snowflake reports stored
    spellings, so with casing preserved they are distinct and with casing folded
    they are one -- and the fold has to be uppercase rather than lowercase,
    because it must not depend on convert_urns_to_lowercase (which decides how a
    name is *emitted*, not whether two names are the same thing).

    Module-level so the identifier builder and the data dictionary share one
    definition; the latter has no builder to call.
    """
    if preserve_column_case:
        return name
    return name.upper()


class SnowflakeStructuredReportMixin(abc.ABC):
    @property
    @abc.abstractmethod
    def structured_reporter(self) -> SourceReport: ...


class SnowsightUrlBuilder:
    CLOUD_REGION_IDS_WITHOUT_CLOUD_SUFFIX: ClassVar = [
        "us-west-2",
        "us-east-1",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-2",
    ]

    snowsight_base_url: str

    def __init__(
        self,
        account_locator: str,
        region: str,
        privatelink: bool = False,
        snowflake_domain: str = DEFAULT_SNOWFLAKE_DOMAIN,
        base_url_override: Optional[str] = None,
    ):
        if base_url_override:
            # Whether Snowsight is reachable via the public internet
            # (app.snowflake.com) or only via private link depends on the
            # customer's Snowflake configuration. When private link is required
            # for the UI, customers set `snowsight_base_url` in the ingestion
            # config to the value returned by `SYSTEM$GET_PRIVATELINK_CONFIG()`,
            # which lands here verbatim (with trailing slash normalisation).
            self.snowsight_base_url = (
                base_url_override
                if base_url_override.endswith("/")
                else f"{base_url_override}/"
            )
            return
        cloud, cloud_region_id = self.get_cloud_region_from_snowflake_region_id(region)
        self.snowsight_base_url = self.create_snowsight_base_url(
            account_locator, cloud_region_id, cloud, privatelink, snowflake_domain
        )

    @staticmethod
    def create_snowsight_base_url(
        account_locator: str,
        cloud_region_id: str,
        cloud: str,
        privatelink: bool = False,
        snowflake_domain: str = DEFAULT_SNOWFLAKE_DOMAIN,
    ) -> str:
        if cloud:
            url_cloud_provider_suffix = f".{cloud}"

        if cloud == SnowflakeCloudProvider.AWS:
            # Some AWS regions do not have cloud suffix. See below the list:
            # https://docs.snowflake.com/en/user-guide/admin-account-identifier#non-vps-account-locator-formats-by-cloud-platform-and-region
            if (
                cloud_region_id
                in SnowsightUrlBuilder.CLOUD_REGION_IDS_WITHOUT_CLOUD_SUFFIX
            ):
                url_cloud_provider_suffix = ""
            else:
                url_cloud_provider_suffix = f".{cloud}"
        # China region may use app.snowflake.cn instead of app.snowflake.com. This is not documented, just
        # guessing based on existence of snowflake.cn domain (https://domainindex.com/domains/snowflake.cn).
        # For private-link-only Snowsight, callers should pass `base_url_override` to `__init__`.
        if snowflake_domain == "snowflakecomputing.cn":
            url = f"https://app.snowflake.cn/{cloud_region_id}{url_cloud_provider_suffix}/{account_locator}/"
        else:
            url = f"https://app.snowflake.com/{cloud_region_id}{url_cloud_provider_suffix}/{account_locator}/"
        return url

    @staticmethod
    def get_cloud_region_from_snowflake_region_id(
        region: str,
    ) -> Tuple[str, str]:
        cloud: str
        if region in SNOWFLAKE_REGION_CLOUD_REGION_MAPPING:
            cloud, cloud_region_id = SNOWFLAKE_REGION_CLOUD_REGION_MAPPING[region]
        elif region.startswith(("aws_", "gcp_", "azure_")):
            # e.g. aws_us_west_2, gcp_us_central1, azure_northeurope
            cloud, cloud_region_id = region.split("_", 1)
            cloud_region_id = cloud_region_id.replace("_", "-")
        else:
            raise Exception(f"Unknown snowflake region {region}")
        return cloud, cloud_region_id

    # domain is either "view" or "table" or "semantic view"
    def get_external_url_for_table(
        self,
        table_name: str,
        schema_name: str,
        db_name: str,
        domain: Literal[
            SnowflakeObjectDomain.TABLE,
            SnowflakeObjectDomain.VIEW,
            SnowflakeObjectDomain.SEMANTIC_VIEW,
            SnowflakeObjectDomain.DYNAMIC_TABLE,
        ],
    ) -> Optional[str]:
        # For dynamic tables, use the dynamic-table domain in the URL path
        # Ensure only explicitly dynamic tables use dynamic-table URL path
        if domain == SnowflakeObjectDomain.DYNAMIC_TABLE:
            url_domain = "dynamic-table"
        elif domain == SnowflakeObjectDomain.SEMANTIC_VIEW:
            url_domain = "semantic-view"
        else:
            url_domain = str(domain)
        return f"{self.snowsight_base_url}#/data/databases/{db_name}/schemas/{schema_name}/{url_domain}/{table_name}/"

    def get_external_url_for_schema(
        self, schema_name: str, db_name: str
    ) -> Optional[str]:
        return f"{self.snowsight_base_url}#/data/databases/{db_name}/schemas/{schema_name}/"

    def get_external_url_for_database(self, db_name: str) -> Optional[str]:
        return f"{self.snowsight_base_url}#/data/databases/{db_name}/"

    def get_external_url_for_streamlit(
        self, app_name: str, schema_name: str, db_name: str
    ) -> Optional[str]:
        return f"{self.snowsight_base_url}#/streamlit-apps/{db_name}.{schema_name}.{app_name}"

    @staticmethod
    def marketplace_listing_url(listing_global_name: str) -> str:
        # Account-neutral URL — Snowflake redirects to the user's session automatically.
        return f"https://app.snowflake.com/marketplace/internal/listing/{listing_global_name}"

    def get_external_url_for_internal_marketplace_listing(
        self, listing_global_name: str
    ) -> str:
        return self.marketplace_listing_url(listing_global_name)


class SnowflakeFilter:
    def __init__(
        self, filter_config: SnowflakeFilterConfig, structured_reporter: SourceReport
    ) -> None:
        self.filter_config = filter_config
        self.structured_reporter = structured_reporter

    # TODO: Refactor remaining filtering logic into this class.

    def is_dataset_pattern_allowed(
        self,
        dataset_name: Optional[str],
        dataset_type: Optional[str],
    ) -> bool:
        if not dataset_type or not dataset_name:
            return True
        if dataset_type.lower() not in (
            SnowflakeObjectDomain.TABLE,
            SnowflakeObjectDomain.EXTERNAL_TABLE,
            SnowflakeObjectDomain.VIEW,
            SnowflakeObjectDomain.MATERIALIZED_VIEW,
            SnowflakeObjectDomain.SEMANTIC_VIEW,
            SnowflakeObjectDomain.ICEBERG_TABLE,
            SnowflakeObjectDomain.STREAM,
            SnowflakeObjectDomain.DYNAMIC_TABLE,
        ):
            return False
        if _is_sys_table(dataset_name):
            return False

        dataset_params = split_qualified_name(dataset_name)
        if len(dataset_params) != 3:
            self.structured_reporter.info(
                title="Unexpected dataset pattern",
                message=f"Found a {dataset_type} with an unexpected number of parts. Database and schema filtering will not work as expected, but table filtering will still work.",
                context=dataset_name,
            )
            # We fall-through here so table/view/stream filtering still works.

        if (
            len(dataset_params) >= 1
            and not self.filter_config.database_pattern.allowed(
                dataset_params[0].strip('"')
            )
        ) or (
            len(dataset_params) >= 2
            and not is_schema_allowed(
                self.filter_config.schema_pattern,
                dataset_params[1].strip('"'),
                dataset_params[0].strip('"'),
                self.filter_config.match_fully_qualified_names,
            )
        ):
            return False

        if dataset_type.lower() in {
            SnowflakeObjectDomain.TABLE,
            SnowflakeObjectDomain.DYNAMIC_TABLE,
        } and not self.filter_config.table_pattern.allowed(
            _cleanup_qualified_name(dataset_name, self.structured_reporter)
        ):
            return False

        if dataset_type.lower() in {
            SnowflakeObjectDomain.VIEW,
            SnowflakeObjectDomain.MATERIALIZED_VIEW,
        } and not self.filter_config.view_pattern.allowed(
            _cleanup_qualified_name(dataset_name, self.structured_reporter)
        ):
            return False

        if (
            dataset_type.lower() == SnowflakeObjectDomain.STREAM
            and not self.filter_config.stream_pattern.allowed(
                _cleanup_qualified_name(dataset_name, self.structured_reporter)
            )
        ):
            return False

        if (
            dataset_type.lower() == SnowflakeObjectDomain.SEMANTIC_VIEW
            and not self.filter_config.semantic_view_pattern.allowed(
                _cleanup_qualified_name(dataset_name, self.structured_reporter)
            )
        ):
            return False

        return True

    def is_procedure_allowed(self, procedure_name: str) -> bool:
        return self.filter_config.procedure_pattern.allowed(procedure_name)

    def is_streamlit_allowed(self, streamlit_name: str) -> bool:
        return self.filter_config.streamlit_pattern.allowed(streamlit_name)

    def is_semantic_view_allowed(self, semantic_view_name: str) -> bool:
        return self.filter_config.semantic_view_pattern.allowed(semantic_view_name)


def _combine_identifier_parts(
    *, table_name: str, schema_name: str, db_name: str
) -> str:
    return f"{db_name}.{schema_name}.{table_name}"


def _is_sys_table(table_name: str) -> bool:
    # Often will look like `SYS$_UNPIVOT_VIEW1737` or `sys$_pivot_view19`.
    return table_name.lower().startswith("sys$")


def split_qualified_name(qualified_name: str) -> List[str]:
    """
    Split a qualified name into its constituent parts.

    >>> split_qualified_name("db.my_schema.my_table")
    ['db', 'my_schema', 'my_table']
    >>> split_qualified_name('"db"."my_schema"."my_table"')
    ['db', 'my_schema', 'my_table']
    >>> split_qualified_name('TEST_DB.TEST_SCHEMA."TABLE.WITH.DOTS"')
    ['TEST_DB', 'TEST_SCHEMA', 'TABLE.WITH.DOTS']
    >>> split_qualified_name('TEST_DB."SCHEMA.WITH.DOTS".MY_TABLE')
    ['TEST_DB', 'SCHEMA.WITH.DOTS', 'MY_TABLE']
    """

    # Fast path - no quotes.
    if '"' not in qualified_name:
        return qualified_name.split(".")

    # First pass - split on dots that are not inside quotes.
    in_quote = False
    parts: List[List[str]] = [[]]
    for char in qualified_name:
        if char == '"':
            in_quote = not in_quote
        elif char == "." and not in_quote:
            parts.append([])
        else:
            parts[-1].append(char)

    # Second pass - remove outer pairs of quotes.
    result = []
    for part in parts:
        if len(part) > 2 and part[0] == '"' and part[-1] == '"':
            part = part[1:-1]

        result.append("".join(part))

    return result


# Qualified Object names from snowflake audit logs have quotes for for snowflake quoted identifiers,
# For example "test-database"."test-schema".test_table
# whereas we generate urns without quotes even for quoted identifiers for backward compatibility
# and also unavailability of utility function to identify whether current table/schema/database
# name should be quoted in above method get_dataset_identifier
def _cleanup_qualified_name(
    qualified_name: str, structured_reporter: SourceReport
) -> str:
    name_parts = split_qualified_name(qualified_name)
    if len(name_parts) != 3:
        if not _is_sys_table(qualified_name):
            structured_reporter.info(
                title="Unexpected dataset pattern",
                message="We failed to parse a Snowflake qualified name into its constituent parts. "
                "DB/schema/table filtering may not work as expected on these entities.",
                context=f"{qualified_name} has {len(name_parts)} parts",
            )
        return qualified_name.replace('"', "")
    return _combine_identifier_parts(
        db_name=name_parts[0],
        schema_name=name_parts[1],
        table_name=name_parts[2],
    )


class SnowflakeIdentifierBuilder:
    platform = "snowflake"

    def __init__(
        self,
        identifier_config: SnowflakeIdentifierConfig,
        structured_reporter: SourceReport,
    ) -> None:
        self.identifier_config = identifier_config
        self.structured_reporter = structured_reporter

    def snowflake_identifier(self, identifier: str) -> str:
        # to be in in sync with older connector, convert name to lowercase
        if self.identifier_config.convert_urns_to_lowercase:
            return identifier.lower()
        return identifier

    def snowflake_column_identifier(self, column_name: str) -> str:
        # Columns are folded separately from datasets: Snowflake's quoted identifiers
        # let `"col"` and `"COL"` coexist in one table, and lowercasing collapses them
        # into a single field path. Delegating on the default path keeps the emitted
        # output byte-identical to the pre-flag behaviour.
        if self.identifier_config.preserve_column_case:
            return column_name
        return self.snowflake_identifier(column_name)

    def column_identity_key(self, column_name: str) -> str:
        """Internal identity of a column/metric name. Never emitted.

        Answers "are these two spellings the same object?", which is what the
        semantic-model indices need. Distinct from snowflake_column_identifier,
        which answers "what does this object get called in a URN".

        With preserve_column_case off, case-only variants are one object, so
        they must fold together whatever convert_urns_to_lowercase says.
        Delegating to snowflake_column_identifier here makes the fold a no-op
        when convert_urns_to_lowercase is also off (both reduce to identity),
        which splits one metric into two entities and blinds the shadow check.
        """
        return snowflake_identity_key(
            column_name,
            preserve_column_case=self.identifier_config.preserve_column_case,
        )

    def logical_dataset_field_path(self, column_name: str) -> str:
        """Field path for a column on a semantic-model logical dataset.

        These datasets are built by the semantic-model mapper rather than
        gen_schema_metadata, and it has always uppercased their field paths.
        Emitting the stored name unconditionally would re-key every one of those
        URNs for deployments running with convert_urns_to_lowercase off, so the
        stored name is used only when preserve_column_case asks for it.
        """
        if not self.identifier_config.preserve_column_case:
            column_name = column_name.upper()
        return self.snowflake_column_identifier(column_name)

    def get_dataset_identifier(
        self, table_name: str, schema_name: str, db_name: str
    ) -> str:
        return self.snowflake_identifier(
            _combine_identifier_parts(
                table_name=table_name, schema_name=schema_name, db_name=db_name
            )
        )

    def gen_dataset_urn(self, dataset_identifier: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_identifier,
            platform_instance=self.identifier_config.platform_instance,
            env=self.identifier_config.env,
        )

    def gen_semantic_model_urn(
        self, view_name: str, schema_name: str, db_name: str
    ) -> str:
        # The semanticModel key has no env field and no separate platform_instance
        # field, so the instance is embedded into the path (mirroring how dataset
        # names are prefixed by make_dataset_urn_with_platform_instance).
        return str(
            SemanticModelUrn(
                platform=make_data_platform_urn(self.platform),
                path=self._semantic_path(schema_name, db_name),
                id=self.snowflake_identifier(view_name),
            )
        )

    def gen_metric_urn(
        self,
        metric_name: str,
        view_name: str,
        schema_name: str,
        db_name: str,
        logical_table: Optional[str] = None,
    ) -> str:
        # Metrics are scoped by their enclosing semantic view, so the view name is
        # part of the path. Snowflake allows the same metric name on different
        # logical tables (they are distinct, table-qualified metrics), so a
        # table-bound metric also carries its logical table in the path to stay
        # unique; view-scoped (derived) metrics omit it.
        path = f"{self._semantic_path(schema_name, db_name)}.{self.snowflake_identifier(view_name)}"
        if logical_table is not None:
            path = f"{path}.{self.snowflake_identifier(logical_table)}"
        return str(
            MetricUrn(
                platform=make_data_platform_urn(self.platform),
                path=path,
                # A metric name is a semantic-view identifier like a dimension's:
                # same DDL, same extraction, same folding. So it follows the
                # column rule, which delegates when the flag is off.
                id=self.snowflake_column_identifier(metric_name),
            )
        )

    def _semantic_path(self, schema_name: str, db_name: str) -> str:
        path = self.snowflake_identifier(f"{db_name}.{schema_name}")
        if self.identifier_config.platform_instance:
            return f"{self.identifier_config.platform_instance}.{path}"
        return path

    def gen_semantic_model_dataset_urn(
        self, view_name: str, logical_table: str, schema_name: str, db_name: str
    ) -> str:
        # Each logical table a semantic view exposes is its own dataset entity.
        # The identifier mirrors the semanticModel key shape (<db>.<schema>.<view>
        # with the logical-table name appended), so logical datasets stay unique
        # across semantic models on the same platform. The platform_instance
        # prefix is added by gen_dataset_urn (make_dataset_urn_with_platform_instance),
        # so it must NOT be baked into the identifier here (unlike gen_semantic_model_urn,
        # whose URN has no separate platform_instance field).
        identifier = self.snowflake_identifier(
            f"{self.snowflake_identifier(f'{db_name}.{schema_name}')}"
            f".{self.snowflake_identifier(view_name)}"
            f".{self.snowflake_identifier(logical_table)}"
        )
        return self.gen_dataset_urn(identifier)

    def gen_marketplace_data_product_key(
        self, listing_global_name: str
    ) -> DataProductKey:
        """Generate a data product key for marketplace listings"""
        return DataProductKey(
            platform="snowflake",  # Use 'snowflake' platform for proper UI integration
            name=self.snowflake_identifier(listing_global_name),
            instance=self.identifier_config.platform_instance,
            env=self.identifier_config.env,
        )

    def gen_marketplace_data_product_urn(self, listing_global_name: str) -> str:
        """Generate a data product URN for marketplace listings"""
        key = self.gen_marketplace_data_product_key(listing_global_name)
        return key.as_urn()

    def get_dataset_identifier_from_qualified_name(self, qualified_name: str) -> str:
        return self.snowflake_identifier(
            _cleanup_qualified_name(qualified_name, self.structured_reporter)
        )

    @staticmethod
    def _escape_identifier(name: str) -> str:
        """Escape embedded double-quotes in a Snowflake identifier by doubling them."""
        return name.replace('"', '""')

    @staticmethod
    def get_quoted_identifier_for_database(db_name):
        db_name = SnowflakeIdentifierBuilder._escape_identifier(db_name)
        return f'"{db_name}"'

    @staticmethod
    def get_quoted_identifier_for_schema(db_name, schema_name):
        db_name = SnowflakeIdentifierBuilder._escape_identifier(db_name)
        schema_name = SnowflakeIdentifierBuilder._escape_identifier(schema_name)
        return f'"{db_name}"."{schema_name}"'

    @staticmethod
    def get_quoted_identifier_for_table(
        db_name: Optional[str], schema_name: str, table_name: str
    ) -> str:
        schema_name = SnowflakeIdentifierBuilder._escape_identifier(schema_name)
        table_name = SnowflakeIdentifierBuilder._escape_identifier(table_name)
        if db_name is not None:
            db_name = SnowflakeIdentifierBuilder._escape_identifier(db_name)
            return f'"{db_name}"."{schema_name}"."{table_name}"'
        return f'"{schema_name}"."{table_name}"'

    # Note - decide how to construct user urns.
    # Historically urns were created using part before @ from user's email.
    # Users without email were skipped from both user entries as well as aggregates.
    # However email is not mandatory field in snowflake user, user_name is always present.
    def get_user_identifier(
        self,
        user_name: str,
        user_email: Optional[str],
    ) -> str:
        if user_email:
            return self.snowflake_identifier(user_email)
        return self.snowflake_identifier(
            f"{user_name}@{self.identifier_config.email_domain}"
            if self.identifier_config.email_domain is not None
            else user_name
        )

    def gen_schema_key(self, db_name: str, schema_name: str) -> SchemaKey:
        return gen_schema_key(
            db_name=self.snowflake_identifier(db_name),
            schema=self.snowflake_identifier(schema_name),
            platform=self.platform,
            platform_instance=self.identifier_config.platform_instance,
            env=self.identifier_config.env,
        )

    def gen_database_key(self, db_name: str) -> DatabaseKey:
        return gen_database_key(
            database=self.snowflake_identifier(db_name),
            platform=self.platform,
            platform_instance=self.identifier_config.platform_instance,
            env=self.identifier_config.env,
        )


class SnowflakeCommonMixin(SnowflakeStructuredReportMixin):
    platform = "snowflake"

    config: SnowflakeV2Config
    report: SnowflakeV2Report

    @property
    def structured_reporter(self) -> SourceReport:
        return self.report

    @cached_property
    def identifiers(self) -> SnowflakeIdentifierBuilder:
        return SnowflakeIdentifierBuilder(self.config, self.report)

    # TODO: Revisit this after stateful ingestion can commit checkpoint
    # for failures that do not affect the checkpoint
    # TODO: Add additional parameters to match the signature of the .warning and .failure methods
    def warn_if_stateful_else_error(self, key: str, reason: str) -> None:
        if (
            self.config.stateful_ingestion is not None
            and self.config.stateful_ingestion.enabled
        ):
            self.structured_reporter.warning(key, reason)
        else:
            self.structured_reporter.failure(key, reason)
