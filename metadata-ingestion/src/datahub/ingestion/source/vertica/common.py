from datahub.configuration.common import AllowDenyPattern
from dataclasses import dataclass, field
from pydantic.fields import Field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    SQLSourceReport,
    SQLAlchemyConfig
)

from datahub.emitter.mcp_builder import (
    DatabaseKey,
    SchemaKey
)

@dataclass
class SQLSourceReportVertica(SQLSourceReport):
    Projection_scanned: int = 0
    models_scanned: int = 0
    Outh_scanned: int = 0
    
    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table
        """
        
        if ent_type == "projection":
            self.Projection_scanned += 1
        elif ent_type == "models":
            self.models_scanned += 1
        elif ent_type == "OAuth":
            self.Outh_scanned += 1
        
        super().report_entity_scanned()
        
    
class SQLAlchemyConfigVertica(SQLAlchemyConfig):
    
    projection_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for projection to filter in ingestion. Specify regex to match the entire table name in database.schema.table format. e.g. to match all tables starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    models_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for ml models to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    oauth_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for OAuth to filter in ingestion. Note: Defaults to table_pattern if not specified. Specify regex to match the entire view name in database.schema.view format. e.g. to match all views starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    
    
    include_projections: Optional[bool] = Field(
        default=True, description="Whether projections should be ingested."
    )
    include_models: Optional[bool] = Field(
        default=True, description="Whether Models should be ingested."
    )
    include_Outh: Optional[bool] = Field(
        default=True, description="Whether OAuth should be ingested."
    )
    
	
# config flags to emit telemetry for
config_options_to_report = [
    "include_views",
    "include_tables",
    "include_projections",
    "include_models",
    "include_Outh"
   
]

# flags to emit telemetry for
profiling_flags_to_report = [
    "turn_off_expensive_profiling_metrics",
    "profile_table_level_only",
    "include_field_null_count",
    "include_field_min_value",
    "include_field_max_value",
    "include_field_mean_value",
    "include_field_median_value",
    "include_field_stddev_value",
    "include_field_quantiles",
    "include_field_distinct_value_frequencies",
    "include_field_histogram",
    "include_field_sample_values",
    "query_combiner_enabled",
]


class SchemaKeyHelper(SchemaKey):
    numberOfProjection: Optional[str]    
    udxsFunctions : Optional[str] = None
    UDXsLanguage : Optional[str] = None
    
class DatabaseKeyHelper(DatabaseKey):
    clusterType : Optional[str] =  None
    clusterSize : Optional[str] = None
    subClusters : Optional[str] = None
    communalStoragePath : Optional[str] = None
    

