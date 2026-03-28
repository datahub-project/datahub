import dataclasses
import fnmatch
import glob as glob_module
import json
import logging
import re
from typing import Any, Dict, List, Literal, Optional, Tuple, cast
from urllib.parse import urlparse

import requests
from packaging import version
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from datahub.configuration.common import ConfigModel
from datahub.configuration.git import GitReference
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.dbt.dbt_common import (
    DBT_EXPOSURE_MATURITY,
    DBT_EXPOSURE_TYPES,
    DBTColumn,
    DBTCommonConfig,
    DBTExposure,
    DBTModelPerformance,
    DBTNode,
    DBTSourceBase,
    DBTSourceReport,
    convert_semantic_model_fields_to_columns,
    parse_dbt_timestamp,
)
from datahub.ingestion.source.dbt.dbt_tests import (
    DBTFreshnessInfo,
    DBTTest,
    DBTTestResult,
    parse_freshness_criteria,
)
from datahub.ingestion.source.gcs.gcs_utils import is_gcs_uri

logger = logging.getLogger(__name__)

_GLOB_CHARACTERS = frozenset("*?[]")

GCS_ENDPOINT_URL = "https://storage.googleapis.com"


class GCSConnectionConfig(ConfigModel):
    """GCS connection using HMAC keys, accessed via the S3-compatible XML API."""

    hmac_access_id: str = Field(
        description="GCS HMAC access ID. See https://cloud.google.com/storage/docs/authentication/hmackeys",
    )
    hmac_access_secret: SecretStr = Field(
        description="GCS HMAC access secret.",
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Override the GCS S3-compatible endpoint URL. "
        "Defaults to https://storage.googleapis.com. Useful for testing with local S3-compatible servers.",
    )

    def get_s3_compatible_connection(self) -> AwsConnectionConfig:
        return AwsConnectionConfig(
            aws_endpoint_url=self.endpoint_url or GCS_ENDPOINT_URL,
            aws_access_key_id=self.hmac_access_id,
            aws_secret_access_key=self.hmac_access_secret,
            aws_region="auto",
        )


def _has_glob_characters(path: str) -> bool:
    return any(c in path for c in _GLOB_CHARACTERS)


@dataclasses.dataclass
class DBTCoreReport(DBTSourceReport):
    catalog_info: Optional[dict] = None
    manifest_info: Optional[dict] = None
    run_results_paths_expanded: Optional[List[str]] = None


class DBTCoreConfig(DBTCommonConfig):
    manifest_path: str = Field(
        description="Path to dbt manifest JSON. See https://docs.getdbt.com/reference/artifacts/manifest-json. "
        "This can be a local file or a URI."
    )
    catalog_path: Optional[str] = Field(
        None,
        description="Path to dbt catalog JSON. See https://docs.getdbt.com/reference/artifacts/catalog-json. "
        "This file is optional, but highly recommended. Without it, some metadata like column info will be incomplete or missing. "
        "This can be a local file or a URI.",
    )
    sources_path: Optional[str] = Field(
        default=None,
        description="Path to dbt sources JSON. See https://docs.getdbt.com/reference/artifacts/sources-json. "
        "If not specified, last-modified fields will not be populated. "
        "This can be a local file or a URI.",
    )
    run_results_paths: List[str] = Field(
        default=[],
        description="Path to output of dbt test run as run_results files in JSON format. "
        "If not specified, test execution results and model performance metadata will not be populated in DataHub. "
        "If invoking dbt multiple times, you can provide paths to multiple run result files. "
        "Glob patterns are supported for S3, GCS, and local paths "
        "(e.g. 's3://bucket/results/*/run_results.json', 'gs://bucket/results/*/run_results.json', "
        "or '/path/to/results/*/run_results.json'). "
        "See https://docs.getdbt.com/reference/artifacts/run-results-json.",
    )

    only_include_if_in_catalog: bool = Field(
        default=False,
        description="[experimental] If true, only include nodes that are also present in the catalog file. "
        "This is useful if you only want to include models that have been built by the associated run.",
    )

    # Because we now also collect model performance metadata, the "test_results" field was renamed to "run_results".
    _convert_test_results_path = pydantic_renamed_field(
        "test_results_path", "run_results_paths", transform=lambda x: [x] if x else []
    )
    _convert_run_result_path_singular = pydantic_renamed_field(
        "run_results_path", "run_results_paths", transform=lambda x: [x] if x else []
    )

    aws_connection: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="When fetching manifest files from s3, configuration for aws connection details",
    )

    gcs_connection: Optional[GCSConnectionConfig] = Field(
        default=None,
        description="When fetching manifest files from gs://, configuration for GCS connection using HMAC keys. "
        "See https://cloud.google.com/storage/docs/authentication/hmackeys",
    )

    git_info: Optional[GitReference] = Field(
        None,
        description="Reference to your git location to enable easy navigation from DataHub to your dbt files.",
    )

    _github_info_deprecated = pydantic_renamed_field("github_info", "git_info")

    @model_validator(mode="after")
    def cloud_connection_needed_if_cloud_uris_present(self) -> "DBTCoreConfig":
        uris = [
            getattr(self, f, None)
            for f in [
                "manifest_path",
                "catalog_path",
                "sources_path",
            ]
        ] + (self.run_results_paths or [])
        s3_uris = [uri for uri in uris if is_s3_uri(uri or "")]
        if s3_uris and self.aws_connection is None:
            raise ValueError(
                f"Please provide aws_connection configuration, since s3 uris have been provided {s3_uris}"
            )

        gcs_uris = [uri for uri in uris if is_gcs_uri(uri or "")]
        if gcs_uris and self.gcs_connection is None:
            raise ValueError(
                f"Please provide gcs_connection configuration, since gs:// uris have been provided {gcs_uris}"
            )
        return self


def get_columns(
    dbt_name: str,
    catalog_node: Optional[dict],
    manifest_node: dict,
    tag_prefix: str,
) -> List[DBTColumn]:
    manifest_columns = manifest_node.get("columns", {})
    manifest_columns_lower = {k.lower(): v for k, v in manifest_columns.items()}

    if catalog_node is not None:
        logger.debug(f"Loading schema info for {dbt_name}")
        catalog_columns = catalog_node["columns"]
    elif manifest_columns:
        # If the end user ran `dbt compile` instead of `dbt docs generate`, then the catalog
        # file will not have any column information. In this case, we will fall back to using
        # information from the manifest file.
        logger.debug(f"Inferring schema info for {dbt_name} from manifest")
        catalog_columns = {
            k: {"name": col["name"], "type": col["data_type"] or "", "index": i}
            for i, (k, col) in enumerate(manifest_columns.items())
        }
    else:
        logger.debug(f"Missing schema info for {dbt_name}")
        return []

    columns = []
    for key, catalog_column in catalog_columns.items():
        manifest_column = manifest_columns.get(
            key, manifest_columns_lower.get(key.lower(), {})
        )

        meta = manifest_column.get("meta", {})

        tags = manifest_column.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]

        dbtCol = DBTColumn(
            name=catalog_column["name"],
            comment=catalog_column.get("comment", ""),
            description=manifest_column.get("description", ""),
            data_type=catalog_column["type"],
            index=catalog_column["index"],
            meta=meta,
            tags=tags,
        )
        columns.append(dbtCol)
    return columns


def _extract_catalog_stats(
    catalog_node: Optional[Dict[str, Any]],
    node_name: Optional[str] = None,
) -> Tuple[Optional[int], Optional[int]]:
    """Extract row_count and size_in_bytes from catalog node stats.

    Returns:
        Tuple of (row_count, size_in_bytes), each can be None if not available.
    """
    if catalog_node is None:
        return None, None

    catalog_stats = catalog_node.get("stats", {})
    row_count: Optional[int] = None
    size_in_bytes: Optional[int] = None

    # Extract row count (num_rows)
    num_rows_stat = catalog_stats.get("num_rows", {})
    if num_rows_stat.get("include", False) and num_rows_stat.get("value") is not None:
        try:
            row_count = int(num_rows_stat["value"])
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse num_rows stat for {node_name}: {e}")

    # Extract size in bytes (num_bytes)
    num_bytes_stat = catalog_stats.get("num_bytes", {})
    if num_bytes_stat.get("include", False) and num_bytes_stat.get("value") is not None:
        try:
            size_in_bytes = int(num_bytes_stat["value"])
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse num_bytes stat for {node_name}: {e}")

    return row_count, size_in_bytes


def extract_dbt_entities(
    all_manifest_entities: Dict[str, Dict[str, Any]],
    all_catalog_entities: Optional[Dict[str, Dict[str, Any]]],
    sources_results: List[Dict[str, Any]],
    manifest_adapter: str,
    use_identifiers: bool,
    tag_prefix: str,
    only_include_if_in_catalog: bool,
    include_database_name: bool,
    report: DBTSourceReport,
    sources_invocation_id: Optional[str] = None,
) -> List[DBTNode]:
    sources_by_id = {x["unique_id"]: x for x in sources_results}

    dbt_entities = []
    for key, manifest_node in all_manifest_entities.items():
        name = manifest_node["name"]

        if use_identifiers and manifest_node.get("identifier"):
            name = manifest_node["identifier"]

        if (
            manifest_node.get("alias") is not None
            and manifest_node.get("resource_type")
            != "test"  # tests have non-human-friendly aliases, so we don't want to use it for tests
        ):
            name = manifest_node["alias"]

        materialization = None
        if "materialized" in manifest_node.get("config", {}):
            # It's a model
            materialization = manifest_node["config"]["materialized"]

        upstream_nodes = []
        if "depends_on" in manifest_node and "nodes" in manifest_node["depends_on"]:
            upstream_nodes = manifest_node["depends_on"]["nodes"]

        catalog_node = (
            all_catalog_entities.get(key) if all_catalog_entities is not None else None
        )
        missing_from_catalog = catalog_node is None
        catalog_type = None

        if catalog_node is None:
            if materialization in {"test", "ephemeral", "semantic_view"}:
                # Test, ephemeral, and semantic_view nodes will never show up in the catalog.
                missing_from_catalog = False
            else:
                if all_catalog_entities is not None and not only_include_if_in_catalog:
                    # If the catalog file is missing, we have already generated a general message.
                    report.warning(
                        title="Node missing from catalog",
                        message="Found a node in the manifest file but not in the catalog. "
                        "This usually means the catalog file was not generated by `dbt docs generate` and so is incomplete. "
                        "Some metadata, particularly schema information, will be impacted.",
                        context=key,
                    )
        else:
            catalog_type = catalog_node["metadata"]["type"]

        # Extract stats from catalog (e.g., num_rows, num_bytes from BigQuery/Snowflake)
        row_count, size_in_bytes = _extract_catalog_stats(catalog_node, node_name=key)

        # initialize comment to "" for consistency with descriptions
        # (since dbt null/undefined descriptions as "")
        comment = ""
        if catalog_node is not None and catalog_node.get("metadata", {}).get("comment"):
            comment = catalog_node["metadata"]["comment"]

        query_tag_props = manifest_node.get("query_tag", {})

        meta = manifest_node.get("meta", {})

        owner = meta.get("owner")
        if owner is None:
            owner = (manifest_node.get("config", {}).get("meta") or {}).get("owner")

        if not meta:
            # On older versions of dbt, the meta field was nested under config
            # for some node types.
            meta = manifest_node.get("config", {}).get("meta") or {}

        tags = manifest_node.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]

        source_result = sources_by_id.get(key, {})
        max_loaded_at_str = source_result.get("max_loaded_at")
        max_loaded_at = None
        if max_loaded_at_str:
            max_loaded_at = parse_dbt_timestamp(max_loaded_at_str)

        freshness_info = None
        if source_result and source_result.get("status"):
            snapshotted_at_str = source_result.get("snapshotted_at")
            snapshotted_at = (
                parse_dbt_timestamp(snapshotted_at_str) if snapshotted_at_str else None
            )
            criteria = source_result.get("criteria", {})

            if max_loaded_at and snapshotted_at:
                freshness_info = DBTFreshnessInfo(
                    invocation_id=sources_invocation_id or "unknown",
                    status=source_result.get("status", ""),
                    max_loaded_at=max_loaded_at,
                    snapshotted_at=snapshotted_at,
                    max_loaded_at_time_ago_in_s=source_result.get(
                        "max_loaded_at_time_ago_in_s", 0.0
                    ),
                    warn_after=parse_freshness_criteria(criteria.get("warn_after")),
                    error_after=parse_freshness_criteria(criteria.get("error_after")),
                )

        test_info = None
        if manifest_node.get("resource_type") == "test":
            test_metadata = manifest_node.get("test_metadata", {})
            kw_args = test_metadata.get("kwargs", {})

            qualified_test_name = (
                (test_metadata.get("namespace") or "")
                + "."
                + (test_metadata.get("name") or "")
            )
            qualified_test_name = (
                qualified_test_name[1:]
                if qualified_test_name.startswith(".")
                else qualified_test_name
            )
            test_info = DBTTest(
                qualified_test_name=qualified_test_name,
                column_name=kw_args.get("column_name"),
                kw_args=kw_args,
            )

        dbtNode = DBTNode(
            dbt_name=key,
            dbt_adapter=manifest_adapter,
            dbt_package_name=manifest_node.get("package_name"),
            database=manifest_node["database"] if include_database_name else None,
            schema=manifest_node["schema"],
            name=name,
            alias=manifest_node.get("alias"),
            dbt_file_path=manifest_node["original_file_path"],
            node_type=manifest_node["resource_type"],
            max_loaded_at=max_loaded_at,
            comment=comment,
            description=manifest_node.get("description", ""),
            raw_code=manifest_node.get(
                "raw_code", manifest_node.get("raw_sql")
            ),  # Backward compatibility dbt <=v1.2
            language=manifest_node.get(
                "language", "sql"
            ),  # Backward compatibility dbt <=v1.2
            upstream_nodes=upstream_nodes,
            materialization=materialization,
            catalog_type=catalog_type,
            missing_from_catalog=missing_from_catalog,
            meta=meta,
            query_tag=query_tag_props,
            tags=tags,
            owner=owner,
            compiled_code=manifest_node.get(
                "compiled_code", manifest_node.get("compiled_sql")
            ),  # Backward compatibility dbt <=v1.2
            test_info=test_info,
            freshness_info=freshness_info,
            row_count=row_count,
            size_in_bytes=size_in_bytes,
        )

        # Load columns from catalog, and override some properties from manifest.
        if dbtNode.materialization not in [
            "ephemeral",
            "test",
            "semantic_view",  # semantic views have custom column handling
        ]:
            dbtNode.columns = get_columns(
                dbtNode.dbt_name,
                catalog_node,
                manifest_node,
                tag_prefix,
            )

        else:
            dbtNode.columns = []

        dbt_entities.append(dbtNode)

    return dbt_entities


def extract_dbt_exposures(
    manifest_exposures: Dict[str, Dict[str, Any]],
    tag_prefix: str,
) -> List[DBTExposure]:
    """Extract dbt exposures from the manifest.json exposures section."""
    exposures = []
    for key, exposure_node in manifest_exposures.items():
        owner = exposure_node.get("owner", {})
        depends_on = exposure_node.get("depends_on", {})
        # depends_on can have "nodes" and "macros" keys
        depends_on_nodes = (
            depends_on.get("nodes", []) if isinstance(depends_on, dict) else []
        )

        tags = exposure_node.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]

        raw_type = exposure_node.get("type", "dashboard")
        exposure_type: Literal[
            "dashboard", "notebook", "ml", "application", "analysis"
        ] = cast(
            Literal["dashboard", "notebook", "ml", "application", "analysis"],
            raw_type if raw_type in DBT_EXPOSURE_TYPES else "dashboard",
        )
        raw_maturity = exposure_node.get("maturity")
        maturity = raw_maturity if raw_maturity in DBT_EXPOSURE_MATURITY else None

        exposures.append(
            DBTExposure(
                name=exposure_node["name"],
                unique_id=key,
                type=exposure_type,
                owner_name=owner.get("name") if isinstance(owner, dict) else None,
                owner_email=owner.get("email") if isinstance(owner, dict) else None,
                description=exposure_node.get("description"),
                url=exposure_node.get("url"),
                maturity=maturity,
                depends_on=depends_on_nodes,
                tags=tags,
                meta=exposure_node.get("meta", {}),
                dbt_package_name=exposure_node.get("package_name"),
                dbt_file_path=exposure_node.get("original_file_path"),
            )
        )
    return exposures


def _resolve_database_schema(
    node_relation: Dict[str, Any],
    depends_on: Dict[str, Any],
    manifest_nodes: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve database/schema from node_relation or upstream dependencies."""
    database = node_relation.get("database")
    schema = node_relation.get("schema")

    if database and schema:
        return database, schema

    depends_on_nodes = (
        depends_on.get("nodes", []) if isinstance(depends_on, dict) else []
    )
    for ref_node_id in depends_on_nodes:
        if ref_node_id in manifest_nodes:
            ref_node = manifest_nodes[ref_node_id]
            return (
                database or ref_node.get("database"),
                schema or ref_node.get("schema"),
            )

    return database, schema


def extract_semantic_models(
    manifest_semantic_models: Dict[str, Dict[str, Any]],
    manifest_nodes: Dict[str, Dict[str, Any]],
    manifest_adapter: Optional[str],
    tag_prefix: str,
) -> List[DBTNode]:
    """Extract dbt semantic models (dbt 1.6+) from manifest.json."""
    semantic_model_nodes: List[DBTNode] = []

    for key, sm_node in manifest_semantic_models.items():
        name = sm_node.get("name", "")
        description = sm_node.get("description", "")

        node_relation = sm_node.get("node_relation", {})
        depends_on = sm_node.get("depends_on", {})
        database, schema = _resolve_database_schema(
            node_relation, depends_on, manifest_nodes
        )
        alias = node_relation.get("alias")

        entities = sm_node.get("entities", [])
        dimensions = sm_node.get("dimensions", [])
        measures = sm_node.get("measures", [])

        columns = convert_semantic_model_fields_to_columns(
            entities=entities,
            dimensions=dimensions,
            measures=measures,
        )

        tags = sm_node.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]

        upstream_nodes = (
            depends_on.get("nodes", []) if isinstance(depends_on, dict) else []
        )

        semantic_model_nodes.append(
            DBTNode(
                dbt_name=key,
                dbt_adapter=manifest_adapter,
                dbt_package_name=sm_node.get("package_name"),
                database=database,
                schema=schema,
                name=name,
                alias=alias,
                dbt_file_path=sm_node.get("original_file_path"),
                node_type="semantic_model",
                max_loaded_at=None,
                comment="",
                description=description,
                upstream_nodes=upstream_nodes,
                materialization=None,
                catalog_type=None,
                missing_from_catalog=False,
                meta=sm_node.get("meta", {}),
                query_tag={},
                tags=tags,
                owner=None,
                language="yaml",
                columns=columns,
                compiled_code=None,
                raw_code=None,
            )
        )

    return semantic_model_nodes


class DBTRunTiming(BaseModel):
    name: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    # TODO parse these into datetime objects


class DBTRunResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str
    timing: List[DBTRunTiming] = []
    unique_id: str
    failures: Optional[int] = None
    message: Optional[str] = None

    @property
    def timing_map(self) -> Dict[str, DBTRunTiming]:
        return {x.name: x for x in self.timing if x.name}

    def has_success_status(self) -> bool:
        return self.status in ("pass", "success")


class DBTRunMetadata(BaseModel):
    dbt_schema_version: str
    dbt_version: str
    generated_at: str
    invocation_id: str


def _parse_test_result(
    dbt_metadata: DBTRunMetadata,
    run_result: DBTRunResult,
) -> Optional[DBTTestResult]:
    if not run_result.has_success_status():
        native_results = {"message": run_result.message or ""}
        if run_result.failures:
            native_results.update({"failures": str(run_result.failures)})
    else:
        native_results = {}

    execution_timestamp = run_result.timing_map.get("execute")
    if execution_timestamp and execution_timestamp.started_at:
        execution_timestamp_parsed = parse_dbt_timestamp(execution_timestamp.started_at)
    else:
        execution_timestamp_parsed = parse_dbt_timestamp(dbt_metadata.generated_at)

    return DBTTestResult(
        invocation_id=dbt_metadata.invocation_id,
        status=run_result.status,
        native_results=native_results,
        execution_time=execution_timestamp_parsed,
    )


def _parse_model_run(
    dbt_metadata: DBTRunMetadata,
    run_result: DBTRunResult,
) -> Optional[DBTModelPerformance]:
    status = run_result.status
    if status not in {"success", "error"}:
        return None

    execution_timestamp = run_result.timing_map.get("execute")
    if not execution_timestamp:
        return None
    if not execution_timestamp.started_at or not execution_timestamp.completed_at:
        return None

    return DBTModelPerformance(
        run_id=dbt_metadata.invocation_id,
        status=status,
        start_time=parse_dbt_timestamp(execution_timestamp.started_at),
        end_time=parse_dbt_timestamp(execution_timestamp.completed_at),
    )


def load_run_results(
    config: DBTCommonConfig,
    test_results_json: Dict[str, Any],
    all_nodes: List[DBTNode],
) -> List[DBTNode]:
    if test_results_json.get("args", {}).get("which") == "generate":
        logger.warning(
            "The run results file is from a `dbt docs generate` command, "
            "instead of a build/run/test command. Skipping this file."
        )
        return all_nodes

    dbt_metadata = DBTRunMetadata.model_validate(test_results_json.get("metadata", {}))

    all_nodes_map: Dict[str, DBTNode] = {x.dbt_name: x for x in all_nodes}

    results = test_results_json.get("results", [])
    for result in results:
        run_result = DBTRunResult.model_validate(result)
        id = run_result.unique_id

        if id.startswith("test."):
            test_result = _parse_test_result(dbt_metadata, run_result)
            if not test_result:
                continue

            test_node = all_nodes_map.get(id)
            if not test_node:
                logger.debug(f"Failed to find test node {id} in the catalog")
                continue

            assert test_node.test_info is not None
            test_node.test_results.append(test_result)

        else:
            model_performance = _parse_model_run(dbt_metadata, run_result)
            if not model_performance:
                continue

            model_node = all_nodes_map.get(id)
            if not model_node:
                logger.debug(f"Failed to find model node {id} in the catalog")
                continue

            model_node.model_performances.append(model_performance)

    return all_nodes


@platform_name("dbt")
@config_class(DBTCoreConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class DBTCoreSource(DBTSourceBase, TestableSource):
    config: DBTCoreConfig
    report: DBTCoreReport

    def __init__(self, config: DBTCommonConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.report = DBTCoreReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCoreConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            source_config = DBTCoreConfig.parse_obj_allow_extras(config_dict)
            DBTCoreSource.load_file_as_json(
                source_config.manifest_path,
                source_config.aws_connection,
                source_config.gcs_connection,
            )
            if source_config.catalog_path is not None:
                DBTCoreSource.load_file_as_json(
                    source_config.catalog_path,
                    source_config.aws_connection,
                    source_config.gcs_connection,
                )
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @staticmethod
    def load_file_as_json(
        uri: str,
        aws_connection: Optional[AwsConnectionConfig],
        gcs_connection: Optional[GCSConnectionConfig] = None,
    ) -> Dict:
        if re.match("^https?://", uri):
            return json.loads(requests.get(uri).text)
        elif is_s3_uri(uri):
            u = urlparse(uri)
            if not aws_connection:
                raise ValueError(f"AWS connection required for S3 URI: {uri}")
            response = aws_connection.get_s3_client().get_object(
                Bucket=u.netloc, Key=u.path.lstrip("/")
            )
            return json.loads(response["Body"].read().decode("utf-8"))
        elif is_gcs_uri(uri):
            u = urlparse(uri)
            if not gcs_connection:
                raise ValueError(f"GCS connection required for GCS URI: {uri}")
            s3_compat = gcs_connection.get_s3_compatible_connection()
            response = s3_compat.get_s3_client().get_object(
                Bucket=u.netloc, Key=u.path.lstrip("/")
            )
            return json.loads(response["Body"].read().decode("utf-8"))
        else:
            with open(uri) as f:
                return json.load(f)

    @staticmethod
    def _expand_object_store_glob(
        uri: str, connection: AwsConnectionConfig, scheme: str
    ) -> List[str]:
        u = urlparse(uri)
        bucket = u.netloc
        key_pattern = u.path.lstrip("/")

        # Use the longest static prefix to limit object store listing scope
        prefix_parts: List[str] = []
        for part in key_pattern.split("/"):
            if _has_glob_characters(part):
                break
            prefix_parts.append(part)
        prefix = "/".join(prefix_parts)
        if prefix:
            prefix += "/"

        s3_client = connection.get_s3_client()
        paginator = s3_client.get_paginator("list_objects_v2")

        pattern_parts = key_pattern.split("/")
        matched_keys: List[str] = []
        total_scanned = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                total_scanned += 1
                key = obj["Key"]
                key_parts = key.split("/")
                if len(key_parts) == len(pattern_parts) and all(
                    fnmatch.fnmatchcase(k, p)
                    for k, p in zip(key_parts, pattern_parts, strict=True)
                ):
                    matched_keys.append(key)

        logger.info(
            f"{scheme} glob '{uri}': scanned {total_scanned} object(s) under "
            f"prefix '{prefix}', matched {len(matched_keys)}"
        )
        return [f"{scheme}://{bucket}/{key}" for key in sorted(matched_keys)]

    def _expand_run_results_paths(self) -> List[str]:
        expanded_paths: List[str] = []

        for path in self.config.run_results_paths:
            if not _has_glob_characters(path):
                expanded_paths.append(path)
                continue

            if is_s3_uri(path):
                if not self.config.aws_connection:
                    self.report.failure(
                        title="Missing AWS connection for S3 glob",
                        message=f"aws_connection is required for S3 glob pattern: {path}",
                    )
                    continue
                try:
                    s3_paths = self._expand_object_store_glob(
                        path, self.config.aws_connection, "s3"
                    )
                except Exception as e:
                    self.report.failure(
                        title="S3 glob expansion failed",
                        message=f"Failed to expand S3 glob pattern '{path}': {e}",
                    )
                    continue
                if not s3_paths:
                    self.report.warning(
                        title="S3 glob pattern matched no objects",
                        message=f"Pattern '{path}' did not match any S3 objects",
                    )
                else:
                    logger.info(
                        f"S3 glob pattern '{path}' expanded to {len(s3_paths)} file(s)"
                    )
                expanded_paths.extend(s3_paths)
            elif is_gcs_uri(path):
                if not self.config.gcs_connection:
                    self.report.failure(
                        title="Missing GCS connection for GCS glob",
                        message=f"gcs_connection is required for GCS glob pattern: {path}",
                    )
                    continue
                try:
                    gcs_paths = self._expand_object_store_glob(
                        path,
                        self.config.gcs_connection.get_s3_compatible_connection(),
                        "gs",
                    )
                except Exception as e:
                    self.report.failure(
                        title="GCS glob expansion failed",
                        message=f"Failed to expand GCS glob pattern '{path}': {e}",
                    )
                    continue
                if not gcs_paths:
                    self.report.warning(
                        title="GCS glob pattern matched no objects",
                        message=f"Pattern '{path}' did not match any GCS objects",
                    )
                else:
                    logger.info(
                        f"GCS glob pattern '{path}' expanded to {len(gcs_paths)} file(s)"
                    )
                expanded_paths.extend(gcs_paths)
            elif re.match("^https?://", path):
                self.report.warning(
                    title="Glob patterns not supported for HTTP(S) URIs",
                    message=f"Glob patterns are not supported for HTTP(S) URIs: {path}. "
                    "Please provide explicit file paths.",
                )
            else:
                local_paths = sorted(glob_module.glob(path))
                if not local_paths:
                    self.report.warning(
                        title="Local glob pattern matched no files",
                        message=f"Pattern '{path}' did not match any local files",
                    )
                else:
                    logger.info(
                        f"Local glob pattern '{path}' expanded to {len(local_paths)} file(s)"
                    )
                expanded_paths.extend(local_paths)

        return expanded_paths

    def loadManifestAndCatalog(
        self,
    ) -> Tuple[
        List[DBTNode],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
    ]:
        dbt_manifest_json = self.load_file_as_json(
            self.config.manifest_path,
            self.config.aws_connection,
            self.config.gcs_connection,
        )
        dbt_manifest_metadata = dbt_manifest_json["metadata"]
        self.report.manifest_info = dict(
            generated_at=dbt_manifest_metadata.get("generated_at", "unknown"),
            dbt_version=dbt_manifest_metadata.get("dbt_version", "unknown"),
            project_name=dbt_manifest_metadata.get("project_name", "unknown"),
        )

        dbt_catalog_json = None
        dbt_catalog_metadata = None
        if self.config.catalog_path is not None:
            dbt_catalog_json = self.load_file_as_json(
                self.config.catalog_path,
                self.config.aws_connection,
                self.config.gcs_connection,
            )
            dbt_catalog_metadata = dbt_catalog_json.get("metadata", {})
            self.report.catalog_info = dict(
                generated_at=dbt_catalog_metadata.get("generated_at", "unknown"),
                dbt_version=dbt_catalog_metadata.get("dbt_version", "unknown"),
                project_name=dbt_catalog_metadata.get("project_name", "unknown"),
            )
            # Parse and store catalog's generated_at for use in DatasetProfile timestamps
            if generated_at_str := dbt_catalog_metadata.get("generated_at"):
                try:
                    self.report.catalog_generated_at = parse_dbt_timestamp(
                        generated_at_str
                    )
                except Exception:
                    logger.debug(
                        f"Failed to parse catalog generated_at: {generated_at_str}"
                    )
        else:
            self.report.warning(
                title="No catalog file configured",
                message="Some metadata, particularly schema information, will be missing.",
            )

        sources_invocation_id = None
        if self.config.sources_path is not None:
            dbt_sources_json = self.load_file_as_json(
                self.config.sources_path,
                self.config.aws_connection,
                self.config.gcs_connection,
            )
            sources_results = dbt_sources_json["results"]
            sources_invocation_id = dbt_sources_json.get("metadata", {}).get(
                "invocation_id"
            )
        else:
            sources_results = {}

        manifest_schema = dbt_manifest_json["metadata"].get("dbt_schema_version")
        manifest_version = dbt_manifest_json["metadata"].get("dbt_version")
        manifest_adapter = dbt_manifest_json["metadata"].get("adapter_type")

        catalog_schema = None
        catalog_version = None
        if dbt_catalog_metadata is not None:
            catalog_schema = dbt_catalog_metadata.get("dbt_schema_version")
            catalog_version = dbt_catalog_metadata.get("dbt_version")

        manifest_nodes = dbt_manifest_json["nodes"]
        manifest_sources = dbt_manifest_json["sources"]
        manifest_exposures = dbt_manifest_json.get("exposures", {})
        manifest_semantic_models = dbt_manifest_json.get("semantic_models", {})

        all_manifest_entities = {**manifest_nodes, **manifest_sources}

        all_catalog_entities = None
        if dbt_catalog_json is not None:
            catalog_nodes = dbt_catalog_json["nodes"]
            catalog_sources = dbt_catalog_json["sources"]

            all_catalog_entities = {**catalog_nodes, **catalog_sources}

        nodes = extract_dbt_entities(
            all_manifest_entities=all_manifest_entities,
            all_catalog_entities=all_catalog_entities,
            sources_results=sources_results,
            manifest_adapter=manifest_adapter,
            use_identifiers=self.config.use_identifiers,
            tag_prefix=self.config.tag_prefix,
            only_include_if_in_catalog=self.config.only_include_if_in_catalog,
            include_database_name=self.config.include_database_name,
            report=self.report,
            sources_invocation_id=sources_invocation_id,
        )

        # Extract exposures from manifest
        self._exposures = extract_dbt_exposures(
            manifest_exposures=manifest_exposures,
            tag_prefix=self.config.tag_prefix,
        )

        # Extract semantic models from manifest (dbt 1.6+)
        if (
            self.config.entities_enabled.can_emit_semantic_models
            and manifest_semantic_models
        ):
            semantic_model_nodes = extract_semantic_models(
                manifest_semantic_models=manifest_semantic_models,
                manifest_nodes=manifest_nodes,
                manifest_adapter=manifest_adapter,
                tag_prefix=self.config.tag_prefix,
            )
            nodes.extend(semantic_model_nodes)
            self.report.num_semantic_models_emitted = len(semantic_model_nodes)
            if semantic_model_nodes:
                logger.info(
                    f"Extracted {len(semantic_model_nodes)} semantic models from manifest"
                )

        return (
            nodes,
            manifest_schema,
            manifest_version,
            manifest_adapter,
            catalog_schema,
            catalog_version,
        )

    def load_nodes(self) -> Tuple[List[DBTNode], Dict[str, Optional[str]]]:
        (
            all_nodes,
            manifest_schema,
            manifest_version,
            manifest_adapter,
            catalog_schema,
            catalog_version,
        ) = self.loadManifestAndCatalog()

        # If catalog_version is between 1.7.0 and 1.7.2, report a warning.
        try:
            if (
                catalog_version
                and catalog_version.startswith("1.7.")
                and version.parse(catalog_version) < version.parse("1.7.3")
            ):
                self.report.report_warning(
                    title="Dbt Catalog Version",
                    message="Due to a bug in dbt version between 1.7.0 and 1.7.2, you will have incomplete metadata "
                    "source",
                    context=f"Due to a bug in dbt, dbt version {catalog_version} will have incomplete metadata on "
                    f"sources."
                    "Please upgrade to dbt version 1.7.3 or later. "
                    "See https://github.com/dbt-labs/dbt-core/issues/9119 for details on the bug.",
                )
        except Exception as e:
            self.report.info(
                title="dbt Catalog Version",
                message="Failed to determine the catalog version",
                exc=e,
            )

        additional_custom_props = {
            "manifest_schema": manifest_schema,
            "manifest_version": manifest_version,
            "manifest_adapter": manifest_adapter,
            "catalog_schema": catalog_schema,
            "catalog_version": catalog_version,
        }

        expanded_run_results_paths = self._expand_run_results_paths()
        if expanded_run_results_paths:
            self.report.run_results_paths_expanded = expanded_run_results_paths
        for run_results_path in expanded_run_results_paths:
            all_nodes = load_run_results(
                self.config,
                self.load_file_as_json(
                    run_results_path,
                    self.config.aws_connection,
                    self.config.gcs_connection,
                ),
                all_nodes,
            )

        return all_nodes, additional_custom_props

    def _filter_nodes(self, all_nodes: List[DBTNode]) -> List[DBTNode]:
        nodes = super()._filter_nodes(all_nodes)

        if not self.config.only_include_if_in_catalog:
            return nodes

        filtered_nodes = []
        for node in nodes:
            if node.missing_from_catalog:
                # TODO: We need to do some additional testing of this flag to validate that it doesn't
                # drop important things entirely (e.g. sources).
                self.report.nodes_filtered.append(node.dbt_name)
            else:
                filtered_nodes.append(node)

        return filtered_nodes

    def get_external_url(self, node: DBTNode) -> Optional[str]:
        if self.config.git_info and node.dbt_file_path:
            return self.config.git_info.get_url_for_file_path(node.dbt_file_path)
        return None
