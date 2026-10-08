import dataclasses
import logging
import os
from datetime import datetime
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    cast,
)

from packaging import version
from pydantic import BaseModel, ConfigDict, Field, model_validator

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
from datahub.ingestion.source.common.gcs_connection_config import GCSConnectionConfig
from datahub.ingestion.source.dbt.dbt_artifacts import (
    ArtifactReader,
    expand_glob_path,
    is_glob_pattern,
    is_missing_file_error,
    load_file_as_json,
    sibling_artifact_path,
)
from datahub.ingestion.source.dbt.dbt_common import (
    DBT_EXPOSURE_MATURITY,
    DBT_EXPOSURE_TYPES,
    DBT_NODE_TYPE_SEMANTIC_MODEL,
    METRIC_TYPE_SIMPLE,
    DBTColumn,
    DBTCommonConfig,
    DBTExposure,
    DBTMetric,
    DBTMetricInput,
    DBTMetricsParse,
    DBTModelPerformance,
    DBTNode,
    DBTProject,
    DBTSourceBase,
    DBTSourceReport,
    convert_semantic_model_fields_to_columns,
    parse_dbt_timestamp,
    parse_semantic_model,
)
from datahub.ingestion.source.dbt.dbt_tests import (
    DBTFreshnessInfo,
    DBTTest,
    DBTTestResult,
    parse_freshness_criteria,
)
from datahub.ingestion.source.gcs.gcs_utils import is_gcs_uri

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DBTCoreReport(DBTSourceReport):
    catalog_info: Optional[dict] = None
    manifest_info: Optional[dict] = None
    run_results_paths_expanded: Optional[List[str]] = None
    manifests_loaded: int = 0
    manifests_failed: int = 0
    manifest_paths_expanded: Optional[List[str]] = None


class DBTCoreConfig(DBTCommonConfig):
    manifest_path: str = Field(
        description="Path to dbt manifest JSON. See https://docs.getdbt.com/reference/artifacts/manifest-json. "
        "This can be a local file or a URI. "
        "Glob patterns are supported for S3, GCS, and local paths "
        "(e.g. 's3://bucket/dbt-artifacts/*/manifest.json', 'gs://bucket/dbt-artifacts/*/manifest.json', "
        "or '/path/to/dbt-artifacts/*/manifest.json'), in which case every matched manifest is ingested as an "
        "independent dbt project in a single run, and catalog.json, sources.json and run_results files are resolved "
        "from each matched manifest's own directory, and each project's platform_instance is its manifest's project_name.",
    )
    catalog_path: Optional[str] = Field(
        None,
        description="Path to dbt catalog JSON. See https://docs.getdbt.com/reference/artifacts/catalog-json. "
        "This file is optional, but highly recommended. Without it, some metadata like column info will be incomplete or missing. "
        "This can be a local file or a URI. "
        "Rejected when manifest_path is a glob pattern, since one catalog cannot be paired with many manifests; "
        "the catalog is then read from each matched manifest's own directory instead.",
    )
    sources_path: Optional[str] = Field(
        default=None,
        description="Path to dbt sources JSON. See https://docs.getdbt.com/reference/artifacts/sources-json. "
        "If not specified, last-modified fields will not be populated. "
        "This can be a local file or a URI. "
        "Rejected when manifest_path is a glob pattern, since one sources file cannot be paired with many "
        "manifests; it is then read from each matched manifest's own directory instead.",
    )
    run_results_paths: List[str] = Field(
        default=[],
        description="Path to output of dbt test run as run_results files in JSON format. "
        "If not specified, test execution results and model performance metadata will not be populated in DataHub. "
        "If invoking dbt multiple times, you can provide paths to multiple run result files. "
        "Glob patterns are supported for S3, GCS, and local paths "
        "(e.g. 's3://bucket/results/*/run_results.json', 'gs://bucket/results/*/run_results.json', "
        "or '/path/to/results/*/run_results.json'). "
        "When manifest_path is a glob, each matched run_results file is attached to the project whose manifest shares its directory. "
        "See https://docs.getdbt.com/reference/artifacts/run-results-json.",
    )

    only_include_if_in_catalog: bool = Field(
        default=False,
        description="[experimental] If true, only include nodes that are also present in the catalog file. "
        "This is useful if you only want to include models that have been built by the associated run.",
    )

    artifact_read_concurrency: int = Field(
        default=8,
        ge=1,
        description="Number of parallel reads used to fetch dbt artifact files when manifest_path "
        "is a glob pattern. Peak memory grows with concurrency (roughly concurrency x the largest "
        "project's raw artifact bytes), so lower this for estates with very large manifest or "
        "catalog files. Set to 1 to read artifacts sequentially. Has no effect when manifest_path "
        "is a single file.",
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
        description="When fetching manifest files from gs://, GCS connection using HMAC credentials. "
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

    @model_validator(mode="after")
    def single_project_fields_must_not_be_set_with_globbed_manifest(
        self,
    ) -> "DBTCoreConfig":
        if not is_glob_pattern(self.manifest_path):
            return self

        conflicting = [
            name
            for name, value in (
                ("catalog_path", self.catalog_path),
                ("sources_path", self.sources_path),
                ("platform_instance", self.platform_instance),
                ("semantic_model_project_name", self.semantic_model_project_name),
            )
            if value is not None
        ]
        if conflicting:
            raise ValueError(
                f"{' and '.join(conflicting)} cannot be set when manifest_path is a glob "
                f"pattern ({self.manifest_path}), because one value cannot be paired with "
                "many manifests. When manifest_path is a glob, catalog.json and "
                "sources.json are read from each matched manifest's own directory, and "
                "each project's platform_instance (which also names its semantic model) "
                "is its manifest's project_name."
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
        try:
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
                all_catalog_entities.get(key)
                if all_catalog_entities is not None
                else None
            )
            missing_from_catalog = catalog_node is None
            catalog_type = None

            if catalog_node is None:
                if materialization in {"test", "ephemeral", "semantic_view"}:
                    # Test, ephemeral, and semantic_view nodes will never show up in the catalog.
                    missing_from_catalog = False
                else:
                    if (
                        all_catalog_entities is not None
                        and not only_include_if_in_catalog
                    ):
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
            row_count, size_in_bytes = _extract_catalog_stats(
                catalog_node, node_name=key
            )

            # initialize comment to "" for consistency with descriptions
            # (since dbt null/undefined descriptions as "")
            comment = ""
            if catalog_node is not None and catalog_node.get("metadata", {}).get(
                "comment"
            ):
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
                    parse_dbt_timestamp(snapshotted_at_str)
                    if snapshotted_at_str
                    else None
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
                        error_after=parse_freshness_criteria(
                            criteria.get("error_after")
                        ),
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
        except Exception as e:
            file_path = manifest_node.get("original_file_path") or "unknown path"
            report.record_node_failure(
                f"{key} ({file_path})",
                e,
                title="Failed to parse dbt node",
                message="Failed to parse this node from the manifest; it will be dropped from ingestion.",
                kind="extraction",
            )
            continue

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


def _metric_input(value: Any) -> Optional[DBTMetricInput]:
    """Coerce a dbt measure/metric reference into a DBTMetricInput.

    dbt >= 1.7 emits ``{"name": ..., "alias": ...}``; dbt 1.6 sometimes emits a
    bare string. Anything else (including a null) yields None.
    """
    if isinstance(value, str):
        return DBTMetricInput(name=value) if value else None
    if isinstance(value, dict):
        name = value.get("name")
        if isinstance(name, str) and name:
            return DBTMetricInput(name=name, filter=_metric_filter(value.get("filter")))
    return None


def _metric_inputs(values: Any) -> List[DBTMetricInput]:
    if not isinstance(values, list):
        return []
    return [parsed for parsed in (_metric_input(value) for value in values) if parsed]


def _dedupe_metric_inputs(inputs: List[DBTMetricInput]) -> List[DBTMetricInput]:
    seen: Set[str] = set()
    deduped: List[DBTMetricInput] = []
    for item in inputs:
        if item.name in seen:
            continue
        seen.add(item.name)
        deduped.append(item)
    return deduped


def _metric_window(value: Any) -> Optional[str]:
    """Flatten a cumulative metric's window into `"<count> <granularity>"`.

    dbt >= 1.7 emits ``{"count": 7, "granularity": "day"}``; earlier versions
    emitted a bare string.
    """
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        count = value.get("count")
        granularity = value.get("granularity")
        if isinstance(count, int) and isinstance(granularity, str):
            return f"{count} {granularity}"
    return None


def _optional_str_value(value: Any) -> Optional[str]:
    """Keep a non-string manifest value out of the typed model."""
    return value if isinstance(value, str) else None


def _metric_filter(value: Any) -> Optional[str]:
    """Flatten a dbt metric filter into a single SQL predicate.

    dbt >= 1.7 emits ``{"where_filters": [{"where_sql_template": "..."}]}``;
    dbt 1.6 emitted a bare string.
    """
    if isinstance(value, str):
        return value or None
    if isinstance(value, dict):
        where_filters = value.get("where_filters")
        if isinstance(where_filters, list):
            templates = [
                where_filter["where_sql_template"]
                for where_filter in where_filters
                if isinstance(where_filter, dict)
                and isinstance(where_filter.get("where_sql_template"), str)
            ]
            if len(templates) > 1:
                # AND binds tighter than OR, so joining `a OR b` to `c OR d`
                # bare gives `a OR (b AND c) OR d` - a different predicate
                # than dbt's, which applies each where_filter in turn. A lone
                # template needs no grouping, since nothing is joined to it.
                return " AND ".join(f"({template})" for template in templates)
            if templates:
                return templates[0]
    return None


def _parse_metric(key: str, metric_node: Dict[str, Any], tag_prefix: str) -> DBTMetric:
    type_params = metric_node.get("type_params")
    if type_params is None:
        type_params = {}
    elif not isinstance(type_params, dict):
        # Not a parse crash, so the per-entry guard would never engage - and
        # everything a metric is computed from lives in here, so the metric
        # would be published with neither an expression nor an upstream. Raised
        # so it is dropped and reported like any other unreadable entry.
        raise ValueError(
            f"type_params is {type(type_params).__name__}, expected an object"
        )

    # Rendered into a subTypes aspect, so a non-string would fail at
    # serialization; fall back to the same default as an absent type.
    metric_type = _optional_str_value(metric_node.get("type")) or METRIC_TYPE_SIMPLE

    measures = _metric_inputs(type_params.get("input_measures"))
    single_measure = _metric_input(type_params.get("measure"))
    if single_measure:
        measures.append(single_measure)

    input_metrics = _metric_inputs(type_params.get("metrics"))
    # A ratio's numerator/denominator name metrics in modern dbt but named
    # measures in early 1.6. Collect both; the emitter resolves against the
    # known metric names first and falls back to measures.
    numerator = _metric_input(type_params.get("numerator"))
    denominator = _metric_input(type_params.get("denominator"))
    for ratio_input in (numerator, denominator):
        if ratio_input:
            input_metrics.append(ratio_input)

    # dbt 1.9 moved conversion/cumulative inputs into their own blocks.
    for nested_key, measure_keys in (
        ("conversion_type_params", ("base_measure", "conversion_measure")),
        ("cumulative_type_params", ("measure",)),
    ):
        nested = type_params.get(nested_key)
        if not isinstance(nested, dict):
            continue
        for measure_key in measure_keys:
            nested_measure = _metric_input(nested.get(measure_key))
            if nested_measure:
                measures.append(nested_measure)

    depends_on = metric_node.get("depends_on")
    depends_on_nodes = (
        depends_on.get("nodes", []) if isinstance(depends_on, dict) else []
    )

    tags = [tag_prefix + tag for tag in metric_node.get("tags") or []]

    # dbt 1.9 moved these into their own block; earlier versions put them
    # directly on type_params.
    cumulative = type_params.get("cumulative_type_params")
    cumulative = cumulative if isinstance(cumulative, dict) else {}

    return DBTMetric(
        # Coerced like expr and grain_to_date below: a manifest can hold
        # anything, `name` becomes a urn id and `label` becomes metricInfo.name,
        # and a non-string reaching either fails at serialization in the sink,
        # outside every guard this source has. A non-string name is dropped to
        # "" and skipped by the emitter, like an absent one.
        name=_optional_str_value(metric_node.get("name")) or "",
        unique_id=key,
        label=_optional_str_value(metric_node.get("label")),
        description=_optional_str_value(metric_node.get("description")),
        type=metric_type,
        measures=_dedupe_metric_inputs(measures),
        input_metrics=_dedupe_metric_inputs(input_metrics),
        numerator=numerator,
        denominator=denominator,
        # Coerced: the emitter calls .strip() on it, and a manifest can hold
        # anything. Same guard as a measure's `agg`.
        expr=_optional_str_value(type_params.get("expr")),
        filter=_metric_filter(metric_node.get("filter")),
        window=_metric_window(type_params.get("window") or cumulative.get("window")),
        grain_to_date=_optional_str_value(
            type_params.get("grain_to_date") or cumulative.get("grain_to_date")
        ),
        tags=tags,
        depends_on=depends_on_nodes,
    )


def extract_dbt_metrics(
    manifest_metrics: Dict[str, Dict[str, Any]], tag_prefix: str
) -> DBTMetricsParse:
    """Extract dbt metrics from the manifest.json metrics section (dbt 1.6+).

    Unreadable entries are returned rather than reported here: metrics are
    parsed on every run but only used when semanticModel/metric emission is
    on, and a warning about skipping a metric that was never going to be
    emitted is noise for everyone else. The caller reports them at the point
    of use.
    """
    metrics: List[DBTMetric] = []
    unreadable: List[Tuple[str, Exception]] = []
    for key, metric_node in manifest_metrics.items():
        # Per entry: one unreadable metric must not cost the project every
        # other one.
        try:
            parsed = _parse_metric(key, metric_node, tag_prefix)
        except Exception as e:
            # Logged unconditionally, so a dropped metric is never invisible
            # even on a run that does not reach the report.
            logger.warning(f"Could not read dbt metric {key}: {e}", exc_info=True)
            unreadable.append((key, e))
            continue
        metrics.append(parsed)
    return DBTMetricsParse(metrics=metrics, unreadable=unreadable)


def _resolve_database_schema(
    node_relation: Dict[str, Any],
    depends_on: Dict[str, Any],
    manifest_nodes: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve database/schema from node_relation or upstream dependencies."""
    database = node_relation.get("database")
    # dbt writes `schema_name` here, not `schema` - NodeRelation in the
    # manifest schema sets additionalProperties: false, so `schema` never
    # appears in a real manifest and reading only it left this branch dead,
    # silently deferring every semantic model to the depends_on fallback.
    # `schema` is kept as a fallback for hand-written fixtures.
    #
    # This is inert today, and deliberately so: the only caller is
    # extract_semantic_models, and for a semantic model nothing reads the
    # resolved database/schema. get_db_fqn returns the dbt unique id,
    # exists_in_target_platform is False, _is_allowed_materialized_node
    # short-circuits, and get_custom_properties does not carry either. The
    # branch is repaired so that it is correct if any of that changes.
    schema = node_relation.get("schema_name") or node_relation.get("schema")

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
    report: Optional[DBTSourceReport] = None,
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

        parsed = parse_semantic_model(sm_node)
        definition = parsed.definition
        if parsed.discarded:
            logger.warning(
                f"Could not read part of dbt semantic model {key}: "
                f"{'; '.join(parsed.discarded)}"
            )
            if report is not None:
                # Reported even when the first-class entities are off: the
                # dataset would otherwise be emitted with a partial or empty
                # schema and nothing saying why.
                report.warning(
                    title="Could not read part of a dbt semantic model",
                    message="Some entities, dimensions or measures were skipped "
                    "because the manifest did not have the expected shape. The "
                    "emitted schema is incomplete.",
                    context=f"{key}: {'; '.join(parsed.discarded)}",
                )
        columns = convert_semantic_model_fields_to_columns(definition)

        tags = sm_node.get("tags", [])
        tags = [tag_prefix + tag for tag in tags]

        # In dbt, SemanticModel stores meta under config.meta (not top-level).
        # Fall back to top-level meta for forward-compatibility.
        meta = sm_node.get("config", {}).get("meta") or sm_node.get("meta") or {}
        owner = meta.get("owner")

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
                node_type=DBT_NODE_TYPE_SEMANTIC_MODEL,
                max_loaded_at=None,
                comment="",
                description=description,
                upstream_nodes=upstream_nodes,
                materialization=None,
                catalog_type=None,
                missing_from_catalog=False,
                meta=meta,
                query_tag={},
                tags=tags,
                owner=owner,
                language="yaml",
                columns=columns,
                compiled_code=None,
                raw_code=None,
                semantic_model_def=definition,
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
    # A skipped test never executed (e.g. an upstream node failed during
    # `dbt build`), so there is no verdict to report. dbt Cloud drops skipped
    # nodes the same way.
    if run_result.status == "skipped":
        return None
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
    all_nodes_map: Dict[str, DBTNode],
) -> None:
    """Attach one run_results file's test results and model performances to their nodes.

    Takes the dbt_name -> node lookup rather than the node list, because the caller
    loops this over every matched run_results file. Rebuilding the map per file cost
    O(files x total_nodes) over the whole multi-project node union, and all but the
    first build was wasted - the nodes are mutated in place.
    """
    if test_results_json.get("args", {}).get("which") == "generate":
        logger.warning(
            "The run results file is from a `dbt docs generate` command, "
            "instead of a build/run/test command. Skipping this file."
        )
        return

    dbt_metadata = DBTRunMetadata.model_validate(test_results_json.get("metadata", {}))

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


@platform_name("dbt")
@config_class(DBTCoreConfig)
@support_status(SupportStatus.GA)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class DBTCoreSource(DBTSourceBase, TestableSource):
    config: DBTCoreConfig
    report: DBTCoreReport

    def __init__(self, config: DBTCommonConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.report = DBTCoreReport()
        # self.config is declared as DBTCoreConfig on the class, so the Core-only
        # fields type-check here even though the parameter is the base config.
        self._artifacts = ArtifactReader(
            aws_connection=self.config.aws_connection,
            gcs_connection=self.config.gcs_connection,
            concurrency=self.config.artifact_read_concurrency,
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCoreConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            source_config = DBTCoreConfig.parse_obj_allow_extras(config_dict)
            # A globbed manifest_path has to be expanded before anything can read
            # it - the pattern itself is neither a file nor an object key. One
            # matched manifest is enough: this validates credentials and
            # reachability, not every project the glob will pick up.
            expansion_report = DBTCoreReport()
            manifest_paths = expand_glob_path(
                source_config.manifest_path,
                aws_connection=source_config.aws_connection,
                gcs_connection=source_config.gcs_connection,
                report=expansion_report,
            )
            if not manifest_paths:
                # Expansion yields nothing both when the object store refused the
                # request - bad credentials, a missing bucket, a throttled listing -
                # and when it succeeded over a prefix that holds no manifests. Those
                # are different things to go fix, so the recorded failure detail is
                # surfaced rather than flattened into "matched no files", which
                # sends an operator looking in the wrong place.
                expansion_failures = [
                    f"{failure.message}: {'; '.join(failure.context)}"
                    for failure in expansion_report.failures
                ]
                if expansion_failures:
                    raise ValueError(
                        f"Could not expand manifest_path glob "
                        f"{source_config.manifest_path}: "
                        + " | ".join(expansion_failures)
                    )
                raise ValueError(
                    f"manifest_path matched no files: {source_config.manifest_path}"
                )
            load_file_as_json(
                manifest_paths[0],
                source_config.aws_connection,
                source_config.gcs_connection,
            )
            if source_config.catalog_path is not None:
                load_file_as_json(
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

    def _expand_glob_path(self, path: str) -> List[str]:
        return expand_glob_path(
            path,
            aws_connection=self.config.aws_connection,
            gcs_connection=self.config.gcs_connection,
            report=self.report,
        )

    def _expand_run_results_paths(self) -> List[str]:
        expanded_paths: List[str] = []
        for path in self.config.run_results_paths:
            # Config order is preserved: each glob is already sorted internally, and
            # run_results files are appended per node, so the user's declared order
            # (typically successive dbt invocations) is meaningful.
            expanded_paths.extend(self._expand_glob_path(path))
        return expanded_paths

    @staticmethod
    def _group_run_results_by_directory(paths: List[str]) -> Dict[str, List[str]]:
        # os.path.dirname strips the last segment on both separators, so an
        # object-store URI and a Windows path group the same way as
        # sibling_artifact_path resolves them. Config order is preserved
        # within a directory.
        grouped: Dict[str, List[str]] = {}
        for path in paths:
            grouped.setdefault(os.path.dirname(path), []).append(path)
        return grouped

    def load_projects(self) -> Iterator[DBTProject]:
        multi_project = is_glob_pattern(self.config.manifest_path)
        manifest_paths = sorted(self._expand_glob_path(self.config.manifest_path))
        if multi_project:
            self.report.manifest_paths_expanded = manifest_paths
            if not manifest_paths:
                # The manifest is the one mandatory dbt artifact - a missing literal
                # manifest_path already raises - so a pattern that matches none of
                # them is a failure, matching test_connection on the same recipe. As
                # a warning this produced a green run with zero assets, and left mass
                # soft-deletion to the stale-entity handler's generic fail-safe, whose
                # error never names the glob. A failure suppresses soft-deletion here.
                self.report.failure(
                    title="manifest_path glob matched no files",
                    message="The globbed manifest_path matched no manifests, so no "
                    "dbt project could be ingested. Check the pattern and that the "
                    "artifacts it points at exist.",
                    context=self.config.manifest_path,
                )
                return

        run_results_paths = self._expand_run_results_paths()
        if run_results_paths:
            self.report.run_results_paths_expanded = run_results_paths
        run_results_by_dir = self._group_run_results_by_directory(run_results_paths)

        project_paths: List[Tuple[str, Optional[str], Optional[str], List[str]]] = []
        for manifest_path in manifest_paths:
            if multi_project:
                # No existence probe: pass the sibling guess straight through and let
                # _load_project's single load site handle absence. Probing first
                # would read and parse catalog.json/sources.json twice per project -
                # for wide schemas catalog.json is the largest dbt artifact, and at
                # this feature's scale (many projects, often on S3) that doubles the
                # dominant cost of the run.
                catalog_path: Optional[str] = sibling_artifact_path(
                    manifest_path, "catalog.json"
                )
                sources_path: Optional[str] = sibling_artifact_path(
                    manifest_path, "sources.json"
                )
                # A run_results file belongs to the project whose manifest
                # shares its directory, dbt's own target/ layout.
                run_results = run_results_by_dir.pop(os.path.dirname(manifest_path), [])
            else:
                catalog_path = self.config.catalog_path
                sources_path = self.config.sources_path
                run_results = run_results_paths
            project_paths.append(
                (manifest_path, catalog_path, sources_path, run_results)
            )

        if multi_project and run_results_by_dir:
            self.report.warning(
                title="run_results files matched no project",
                message="These run_results files are not in the directory of any "
                "matched manifest, so their results were not attached to any project.",
                context=", ".join(
                    path for paths in run_results_by_dir.values() for path in paths
                ),
            )

        # Overlap the per-project artifact reads (the dominant cost on object
        # stores) while keeping processing order, reporting, and error
        # classification identical to the sequential path.
        prefetched = (
            self._artifacts.maybe_prefetch(
                [
                    [
                        path
                        for path in (manifest, catalog, sources, *runs)
                        if path is not None
                    ]
                    for manifest, catalog, sources, runs in project_paths
                ]
            )
            if multi_project
            else None
        )

        # project_name -> manifest_path, to refuse a second build of one project:
        # the name is the platform instance, so two would share every urn.
        seen_project_names: Dict[str, str] = {}
        for manifest_path, catalog_path, sources_path, run_results in project_paths:
            if prefetched is not None:
                self._artifacts.prefetched = next(prefetched)
            try:
                project = self._load_project(
                    manifest_path,
                    catalog_path,
                    sources_path,
                    run_results,
                    multi_project=multi_project,
                )
                if multi_project:
                    if project.project_name is None:
                        raise ValueError(
                            "manifest has no metadata.project_name, which names this "
                            "project's platform instance; multi-project ingestion needs "
                            "artifacts from dbt 1.6 or newer, which record it"
                        )
                    if project.project_name in seen_project_names:
                        raise ValueError(
                            f"project_name {project.project_name!r} is also claimed by "
                            f"{seen_project_names[project.project_name]}; two builds of "
                            "one project cannot be ingested in one run"
                        )
                    seen_project_names[project.project_name] = manifest_path
            except MemoryError:
                # Per-project isolation exists to contain one project's bad artifacts.
                # Exhausted memory is not contained by it: every remaining project
                # would be fetched and parsed into the same exhausted process, so
                # continuing produces a run that is slower and no more complete.
                raise
            except Exception as e:
                # In single-project mode, a bad manifest fails the run exactly as it
                # always has. In glob mode, one broken project shouldn't take down
                # ingestion of every other matched project.
                if not multi_project:
                    raise
                self.report.manifests_failed += 1
                self.report.failure(
                    title="Failed to load dbt project",
                    message="Failed to load one dbt project matched by the globbed manifest_path; skipping it",
                    context=f"{manifest_path}: {e}",
                    exc=e,
                )
                continue
            finally:
                # A project that failed before consuming every artifact would
                # otherwise pin its leftover bytes (catalog.json is the largest dbt
                # artifact) for the source's lifetime, through the whole emit phase.
                self._artifacts.prefetched = {}

            self.report.manifests_loaded += 1
            yield project

    def _load_project(
        self,
        manifest_path: str,
        catalog_path: Optional[str],
        sources_path: Optional[str],
        run_results_paths: List[str],
        *,
        multi_project: bool,
    ) -> DBTProject:
        """Load one project's manifest/catalog/sources/run_results.

        multi_project distinguishes a single, explicitly-configured project
        (False: a missing catalog_path/sources_path is a misconfiguration and
        fails loudly; the instance comes from the recipe) from a glob match
        (True: a sibling artifact simply not existing is expected and warns;
        the instance is the manifest's project_name).
        """
        dbt_manifest_json = self._artifacts.load_json(manifest_path)
        dbt_manifest_metadata = dbt_manifest_json["metadata"]
        # Read separately from report.manifest_info, whose "unknown" default
        # must never reach a semanticModel or metric urn.
        project_name: Optional[str] = dbt_manifest_metadata.get("project_name")
        if not multi_project:
            # manifest_info/catalog_info are single report-level fields, so in glob
            # mode "last project wins" would misrepresent the whole run as one
            # project's data. manifest_paths_expanded already lists every project.
            self.report.manifest_info = dict(
                generated_at=dbt_manifest_metadata.get("generated_at", "unknown"),
                dbt_version=dbt_manifest_metadata.get("dbt_version", "unknown"),
                project_name=dbt_manifest_metadata.get("project_name", "unknown"),
            )

        dbt_catalog_json, catalog_load_error = self._artifacts.load_optional_json(
            catalog_path, optional=multi_project
        )
        dbt_catalog_metadata = None
        # This project's catalog generated_at, stamped onto each node below
        # (per-project, like artifact_props) rather than kept on the report, since
        # the report has only one slot and a multi-project run has one catalog per
        # project.
        catalog_generated_at: Optional[datetime] = None
        if dbt_catalog_json is not None:
            dbt_catalog_metadata = dbt_catalog_json.get("metadata", {})
            if not multi_project:
                self.report.catalog_info = dict(
                    generated_at=dbt_catalog_metadata.get("generated_at", "unknown"),
                    dbt_version=dbt_catalog_metadata.get("dbt_version", "unknown"),
                    project_name=dbt_catalog_metadata.get("project_name", "unknown"),
                )
            # Parse and store catalog's generated_at for use in DatasetProfile timestamps
            if generated_at_str := dbt_catalog_metadata.get("generated_at"):
                try:
                    catalog_generated_at = parse_dbt_timestamp(generated_at_str)
                except Exception:
                    logger.debug(
                        f"Failed to parse catalog generated_at: {generated_at_str}"
                    )
        elif catalog_path is None:
            self.report.warning(
                title="No catalog file configured",
                message="Some metadata, particularly schema information, will be missing.",
            )
        elif is_missing_file_error(catalog_load_error):
            # catalog_path was a glob-derived sibling guess, and the file is
            # definitely not there - a project that never ran `dbt docs generate`.
            self.report.warning(
                title="No catalog file found for project",
                message="This dbt project has no catalog.json beside its manifest; "
                "some metadata, particularly schema information, will be missing.",
                context=manifest_path,
            )
        else:
            # catalog_path was a glob-derived sibling guess, and the read failed
            # for a reason that does not establish absence - permissions,
            # throttling, a transient network error - so don't claim the file is
            # missing.
            self.report.warning(
                title="Could not read catalog file for project",
                message="Failed to read this project's catalog.json, and the failure "
                "does not indicate the file is absent (e.g. permissions, throttling, "
                "network). Some metadata, particularly schema information, will be "
                "missing.",
                context=f"{manifest_path}: {catalog_load_error}",
            )

        dbt_sources_json, sources_load_error = self._artifacts.load_optional_json(
            sources_path, optional=multi_project
        )
        sources_invocation_id = None
        sources_results: List[Dict[str, Any]] = []
        if dbt_sources_json is not None:
            sources_results = dbt_sources_json["results"]
            sources_invocation_id = dbt_sources_json.get("metadata", {}).get(
                "invocation_id"
            )
        elif sources_path is not None and is_missing_file_error(sources_load_error):
            # sources_path was a glob-derived sibling guess, and the file is
            # definitely not there - see the catalog.json warning above.
            self.report.warning(
                title="No sources file found for project",
                message="This dbt project has no sources.json beside its manifest; "
                "last-modified fields will not be populated.",
                context=manifest_path,
            )
        elif sources_path is not None and sources_load_error is not None:
            self.report.warning(
                title="Could not read sources file for project",
                message="Failed to read this project's sources.json, and the failure "
                "does not indicate the file is absent (e.g. permissions, throttling, "
                "network). Last-modified fields will not be populated.",
                context=f"{manifest_path}: {sources_load_error}",
            )

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
        manifest_metrics = dbt_manifest_json.get("metrics", {})
        manifest_semantic_models = dbt_manifest_json.get("semantic_models", {})

        all_manifest_entities = {**manifest_nodes, **manifest_sources}

        all_catalog_entities = None
        if dbt_catalog_json is not None:
            catalog_nodes = dbt_catalog_json["nodes"]
            catalog_sources = dbt_catalog_json["sources"]

            all_catalog_entities = {**catalog_nodes, **catalog_sources}

        artifact_props: Dict[str, str] = {
            key: value
            for key, value in {
                "manifest_schema": manifest_schema,
                "manifest_version": manifest_version,
                "manifest_adapter": manifest_adapter,
                "catalog_schema": catalog_schema,
                "catalog_version": catalog_version,
            }.items()
            if value is not None
        }

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

        project_exposures = extract_dbt_exposures(
            manifest_exposures=manifest_exposures,
            tag_prefix=self.config.tag_prefix,
        )

        # Extract metrics from manifest (dbt 1.6+). Unconditional: whether they
        # are used is decided later, by the resolved semantic-model gate.
        metrics = extract_dbt_metrics(
            manifest_metrics=manifest_metrics,
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
                report=self.report,
            )
            nodes.extend(semantic_model_nodes)
            self.report.num_semantic_models_emitted += len(semantic_model_nodes)
            if semantic_model_nodes:
                logger.info(
                    f"Extracted {len(semantic_model_nodes)} semantic models from manifest"
                )

        manifest_generated_at = dbt_manifest_metadata.get("generated_at")

        # If catalog_version is between 1.7.0 and 1.7.2, report a warning. This is
        # per-project because a multi-project run can mix dbt versions across projects.
        try:
            if (
                catalog_version
                and catalog_version.startswith("1.7.")
                and version.parse(catalog_version) < version.parse("1.7.3")
            ):
                self.report.warning(
                    title="Dbt Catalog Version",
                    message="Due to a bug in dbt version between 1.7.0 and 1.7.2, you will have incomplete metadata "
                    "source",
                    context=f"Due to a bug in dbt, dbt version {catalog_version} will have incomplete metadata on "
                    f"sources."
                    "Please upgrade to dbt version 1.7.3 or later. "
                    "See https://github.com/dbt-labs/dbt-core/issues/9119 for details on the bug.",
                    log=False,
                )
        except Exception as e:
            self.report.info(
                title="dbt Catalog Version",
                message="Failed to determine the catalog version",
                exc=e,
            )

        if run_results_paths:
            nodes_by_name = {node.dbt_name: node for node in nodes}
            for run_results_path in run_results_paths:
                load_run_results(
                    self.config,
                    self._artifacts.load_json(run_results_path),
                    nodes_by_name,
                )

        return DBTProject(
            nodes=nodes,
            exposures=project_exposures,
            metrics=metrics,
            platform_instance=project_name
            if multi_project
            else self.config.platform_instance,
            project_name=project_name,
            manifest_path=manifest_path,
            artifact_props=artifact_props,
            catalog_generated_at=catalog_generated_at,
            manifest_generated_at=manifest_generated_at,
        )

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
