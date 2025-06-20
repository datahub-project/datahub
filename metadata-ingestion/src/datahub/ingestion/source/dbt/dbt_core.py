import dataclasses
import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import dateutil.parser
import requests
from packaging import version
from pydantic import BaseModel, Field, validator

from datahub.configuration.git import GitReference
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
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
    DBTColumn,
    DBTCommonConfig,
    DBTModelPerformance,
    DBTNode,
    DBTSourceBase,
    DBTSourceReport,
)
from datahub.ingestion.source.dbt.dbt_tests import DBTTest, DBTTestResult

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DBTCoreReport(DBTSourceReport):
    catalog_info: Optional[dict] = None
    manifest_info: Optional[dict] = None


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
        "If not specified, test execution results and model performance metadata will not be populated in DataHub."
        "If invoking dbt multiple times, you can provide paths to multiple run result files. "
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

    git_info: Optional[GitReference] = Field(
        None,
        description="Reference to your git location to enable easy navigation from DataHub to your dbt files.",
    )

    _github_info_deprecated = pydantic_renamed_field("github_info", "git_info")

    @validator("aws_connection", always=True)
    def aws_connection_needed_if_s3_uris_present(
        cls, aws_connection: Optional[AwsConnectionConfig], values: Dict, **kwargs: Any
    ) -> Optional[AwsConnectionConfig]:
        # first check if there are fields that contain s3 uris
        uris = [
            values.get(f)
            for f in [
                "manifest_path",
                "catalog_path",
                "sources_path",
            ]
        ] + values.get("run_results_paths", [])
        s3_uris = [uri for uri in uris if is_s3_uri(uri or "")]

        if s3_uris and aws_connection is None:
            raise ValueError(
                f"Please provide aws_connection configuration, since s3 uris have been provided {s3_uris}"
            )
        return aws_connection


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
            if materialization in {"test", "ephemeral"}:
                # Test and ephemeral nodes will never show up in the catalog.
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

        max_loaded_at_str = sources_by_id.get(key, {}).get("max_loaded_at")
        max_loaded_at = None
        if max_loaded_at_str:
            max_loaded_at = parse_dbt_timestamp(max_loaded_at_str)

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
        )

        # Load columns from catalog, and override some properties from manifest.
        if dbtNode.materialization not in [
            "ephemeral",
            "test",
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


def parse_dbt_timestamp(timestamp: str) -> datetime:
    return dateutil.parser.parse(timestamp)


class DBTRunTiming(BaseModel):
    name: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None

    # TODO parse these into datetime objects


class DBTRunResult(BaseModel):
    class Config:
        extra = "allow"

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

    dbt_metadata = DBTRunMetadata.parse_obj(test_results_json.get("metadata", {}))

    all_nodes_map: Dict[str, DBTNode] = {x.dbt_name: x for x in all_nodes}

    results = test_results_json.get("results", [])
    for result in results:
        run_result = DBTRunResult.parse_obj(result)
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
class DBTCoreSource(DBTSourceBase, TestableSource):
    config: DBTCoreConfig
    report: DBTCoreReport

    def __init__(self, config: DBTCommonConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.report = DBTCoreReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCoreConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            source_config = DBTCoreConfig.parse_obj_allow_extras(config_dict)
            DBTCoreSource.load_file_as_json(
                source_config.manifest_path, source_config.aws_connection
            )
            if source_config.catalog_path is not None:
                DBTCoreSource.load_file_as_json(
                    source_config.catalog_path, source_config.aws_connection
                )
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @staticmethod
    def load_file_as_json(
        uri: str, aws_connection: Optional[AwsConnectionConfig]
    ) -> Dict:
        if re.match("^https?://", uri):
            return json.loads(requests.get(uri).text)
        elif is_s3_uri(uri):
            u = urlparse(uri)
            assert aws_connection
            response = aws_connection.get_s3_client().get_object(
                Bucket=u.netloc, Key=u.path.lstrip("/")
            )
            return json.loads(response["Body"].read().decode("utf-8"))
        else:
            with open(uri) as f:
                return json.load(f)

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
            self.config.manifest_path, self.config.aws_connection
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
                self.config.catalog_path, self.config.aws_connection
            )
            dbt_catalog_metadata = dbt_catalog_json.get("metadata", {})
            self.report.catalog_info = dict(
                generated_at=dbt_catalog_metadata.get("generated_at", "unknown"),
                dbt_version=dbt_catalog_metadata.get("dbt_version", "unknown"),
                project_name=dbt_catalog_metadata.get("project_name", "unknown"),
            )
        else:
            self.report.warning(
                title="No catalog file configured",
                message="Some metadata, particularly schema information, will be missing.",
            )

        if self.config.sources_path is not None:
            dbt_sources_json = self.load_file_as_json(
                self.config.sources_path, self.config.aws_connection
            )
            sources_results = dbt_sources_json["results"]
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

        for run_results_path in self.config.run_results_paths:
            # This will populate the test_results and model_performance fields on each node.
            all_nodes = load_run_results(
                self.config,
                self.load_file_as_json(run_results_path, self.config.aws_connection),
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
