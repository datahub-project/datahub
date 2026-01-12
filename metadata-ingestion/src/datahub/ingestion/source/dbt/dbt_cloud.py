import logging
from copy import deepcopy
from datetime import datetime
from json import JSONDecodeError
from typing import Dict, List, Literal, Optional, Tuple
from urllib.parse import urlparse

import dateutil.parser
import requests
from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
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
from datahub.ingestion.source.dbt.dbt_cloud_models import (
    DBTCloudDeploymentType,
    DBTCloudEnvironment,
    DBTCloudJob,
)
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumn,
    DBTCommonConfig,
    DBTNode,
    DBTSourceBase,
    DBTSourceReport,
)
from datahub.ingestion.source.dbt.dbt_tests import DBTTest, DBTTestResult

logger = logging.getLogger(__name__)


class AutoDiscoveryConfig(ConfigModel):
    """
    Configuration for auto-discovery mode that automatically discovers jobs for a project.
    Ref: DBT Jobs: http://docs.getdbt.com/docs/deploy/jobs
    TODO: The configuration is oraganised this way to allow for future expansion to project discovery at account level.
    """

    enabled: bool = Field(
        default=False,
        description="Enable/disable auto-discovery mode. When enabled, discovers jobs for the specified project. Only production jobs with generate_docs=True are ingested.",
    )

    job_id_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter jobs by job_id when auto-discovering.",
    )


class DBTCloudConfig(DBTCommonConfig):
    access_url: str = Field(
        description="The base URL of the dbt Cloud instance to use. This should be the URL you use to access the dbt Cloud UI. It should include the scheme (http/https) and not include a trailing slash. See the access url for your dbt Cloud region here: https://docs.getdbt.com/docs/cloud/about-cloud/regions-ip-addresses",
        default="https://cloud.getdbt.com",
    )

    metadata_endpoint: str = Field(
        default="https://metadata.cloud.getdbt.com/graphql",
        description="The dbt Cloud metadata API endpoint. If not provided, we will try to infer it from the access_url.",
    )

    token: str = Field(
        description="The API token to use to authenticate with DBT Cloud.",
    )

    account_id: int = Field(
        description="The DBT Cloud account ID to use.",
    )
    project_id: int = Field(
        description="The dbt Cloud project ID to use.",
    )

    job_id: Optional[int] = Field(
        None,
        description="The ID of the job to ingest metadata from. Required in explicit mode (when auto_discovery is disabled).",
    )
    run_id: Optional[int] = Field(
        None,
        description="The ID of the run to ingest metadata from. If not specified, defaults to the latest run. In auto-discovery mode, always uses the latest run for each job.",
    )

    auto_discovery: Optional[AutoDiscoveryConfig] = Field(
        None,
        description="Auto-discovery configuration. When enabled, automatically discovers jobs for the specified project.",
    )

    external_url_mode: Literal["explore", "ide"] = Field(
        default="explore",
        description='Where should the "View in dbt" link point to - either the "Explore" UI or the dbt Cloud IDE',
    )

    @model_validator(mode="before")
    @classmethod
    def set_metadata_endpoint(cls, values: dict) -> dict:
        # In-place update of the input dict would cause state contamination.
        # So a deepcopy is performed first.
        values = deepcopy(values)

        if values.get("access_url") and not values.get("metadata_endpoint"):
            metadata_endpoint = infer_metadata_endpoint(values["access_url"])
            if metadata_endpoint is None:
                raise ValueError(
                    "Unable to infer the metadata endpoint from the access URL. Please provide a metadata endpoint."
                )
            logger.info(f"Inferred metadata endpoint: {metadata_endpoint}")
            values["metadata_endpoint"] = metadata_endpoint
        return values

    @model_validator(mode="after")
    def validate_config(self) -> "DBTCloudConfig":
        is_auto_discovery = self.auto_discovery and self.auto_discovery.enabled
        if not is_auto_discovery and self.job_id is None:
            raise ValueError(
                "job_id is required in explicit mode. "
                "Either provide job_id or enable auto_discovery mode."
            )
        return self


def infer_metadata_endpoint(access_url: str) -> Optional[str]:
    """Infer the dbt metadata endpoint from the access URL.

    See https://docs.getdbt.com/docs/cloud/about-cloud/access-regions-ip-addresses#api-access-urls
    and https://docs.getdbt.com/docs/dbt-cloud-apis/discovery-querying#discovery-api-endpoints
    for more information.

    Args:
        access_url: The dbt Cloud access URL. This is the URL of the dbt Cloud UI.

    Returns:
        The metadata endpoint, or None if it couldn't be inferred.

    Examples:
        # Standard multi-tenant deployments.
        >>> infer_metadata_endpoint("https://cloud.getdbt.com")
        'https://metadata.cloud.getdbt.com/graphql'

        >>> infer_metadata_endpoint("https://au.dbt.com")
        'https://metadata.au.dbt.com/graphql'

        >>> infer_metadata_endpoint("https://emea.dbt.com")
        'https://metadata.emea.dbt.com/graphql'

        # Cell-based deployment.
        >>> infer_metadata_endpoint("https://prefix.us1.dbt.com")
        'https://prefix.metadata.us1.dbt.com/graphql'

        # Test with an "internal" URL.
        >>> infer_metadata_endpoint("http://dbt.corp.internal")
        'http://metadata.dbt.corp.internal/graphql'
    """

    try:
        parsed_uri = urlparse(access_url)
        assert parsed_uri.scheme is not None
        assert parsed_uri.hostname is not None
    except Exception as e:
        logger.debug(f"Unable to parse access URL {access_url}: {e}", exc_info=e)
        return None

    if parsed_uri.hostname.endswith(".getdbt.com") or parsed_uri.hostname in {
        # Two special cases of multi-tenant deployments that use the dbt.com domain
        # instead of getdbt.com.
        "au.dbt.com",
        "emea.dbt.com",
    }:
        return f"{parsed_uri.scheme}://metadata.{parsed_uri.netloc}/graphql"
    elif parsed_uri.hostname.endswith(".dbt.com"):
        # For cell-based deployments.
        # prefix.region.dbt.com -> prefix.metadata.region.dbt.com
        hostname_parts = parsed_uri.hostname.split(".", maxsplit=1)
        return f"{parsed_uri.scheme}://{hostname_parts[0]}.metadata.{hostname_parts[1]}/graphql"
    else:
        # The self-hosted variants also have the metadata. prefix.
        return f"{parsed_uri.scheme}://metadata.{parsed_uri.netloc}/graphql"


_DBT_GRAPHQL_COMMON_FIELDS = """
  runId
  accountId
  projectId
  environmentId
  jobId
  resourceType
  uniqueId
  name
  description
  meta
  dbtVersion
  tags
"""

_DBT_GRAPHQL_NODE_COMMON_FIELDS = """
  database
  schema
  type
  owner
  comment

  columns {
    name
    index
    type
    comment
    description
    tags
    meta
  }

  # We don't currently support this field, but should in the future.
  #stats {
  #  id
  #  label
  #  description
  #  include
  #  value
  #}
"""

_DBT_GRAPHQL_MODEL_SEED_SNAPSHOT_FIELDS = """
  packageName
  alias
  error
  status
  skip
  rawSql
  rawCode
  compiledSql
  compiledCode
"""

_DBT_FIELDS_BY_TYPE = {
    "models": f"""
    {_DBT_GRAPHQL_COMMON_FIELDS}
    {_DBT_GRAPHQL_NODE_COMMON_FIELDS}
    {_DBT_GRAPHQL_MODEL_SEED_SNAPSHOT_FIELDS}
    dependsOn
    materializedType
""",
    "seeds": f"""
    {_DBT_GRAPHQL_COMMON_FIELDS}
    {_DBT_GRAPHQL_NODE_COMMON_FIELDS}
    {_DBT_GRAPHQL_MODEL_SEED_SNAPSHOT_FIELDS}
""",
    "sources": f"""
    {_DBT_GRAPHQL_COMMON_FIELDS}
    {_DBT_GRAPHQL_NODE_COMMON_FIELDS}
    identifier
    sourceName
    sourceDescription
    maxLoadedAt
    snapshottedAt
    state
    freshnessChecked
    loader
""",
    "snapshots": f"""
    {_DBT_GRAPHQL_COMMON_FIELDS}
    {_DBT_GRAPHQL_NODE_COMMON_FIELDS}
    {_DBT_GRAPHQL_MODEL_SEED_SNAPSHOT_FIELDS}
    parentsSources {{
      uniqueId
    }}
    parentsModels {{
      uniqueId
    }}
""",
    "tests": f"""
    {_DBT_GRAPHQL_COMMON_FIELDS}
    state
    columnName
    status
    error
    dependsOn
    fail
    warn
    skip
    rawSql
    rawCode
    compiledSql
    compiledCode
""",
    # Currently unsupported dbt node types:
    # - metrics
    # - exposures
}

_DBT_GRAPHQL_QUERY = """
query DatahubMetadataQuery_{type}($jobId: BigInt!, $runId: BigInt) {{
  job(id: $jobId, runId: $runId) {{
    {type} {{
{fields}
    }}
  }}
}}
"""


@platform_name("dbt")
@config_class(DBTCloudConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class DBTCloudSource(DBTSourceBase, TestableSource):
    config: DBTCloudConfig
    report: DBTSourceReport  # nothing cloud-specific in the report

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCloudConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _is_auto_discovery_enabled(self) -> bool:
        """Check if auto-discovery mode is enabled."""
        return bool(self.config.auto_discovery and self.config.auto_discovery.enabled)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            source_config = DBTCloudConfig.parse_obj_allow_extras(config_dict)
            is_auto_discovery = (
                source_config.auto_discovery and source_config.auto_discovery.enabled
            )

            if is_auto_discovery:
                # Test auto-discovery: verify we can fetch environments
                DBTCloudSource._get_environments_for_project(
                    source_config.access_url,
                    source_config.token,
                    source_config.account_id,
                    source_config.project_id,
                )
            else:
                # Test explicit mode: verify we can query the job
                DBTCloudSource._send_graphql_query(
                    source_config.metadata_endpoint,
                    source_config.token,
                    _DBT_GRAPHQL_QUERY.format(type="tests", fields="jobId"),
                    {"jobId": source_config.job_id, "runId": source_config.run_id},
                )

            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @staticmethod
    def _send_graphql_query(
        metadata_endpoint: str, token: str, query: str, variables: Dict
    ) -> Dict:
        logger.debug(f"Sending GraphQL query to dbt Cloud: {query}")
        response = requests.post(
            metadata_endpoint,
            json={
                "query": query,
                "variables": variables,
            },
            headers={
                "Authorization": f"Bearer {token}",
                "X-dbt-partner-source": "acryldatahub",
            },
        )

        try:
            res = response.json()
            if "errors" in res:
                raise ValueError(
                    f"Unable to fetch metadata from dbt Cloud: {res['errors']}"
                )
            data = res["data"]
        except JSONDecodeError as e:
            response.raise_for_status()
            raise e

        return data

    @staticmethod
    def _get_jobs_for_project(
        access_url: str,
        token: str,
        account_id: int,
        project_id: int,
        environment_id: int,
    ) -> List[DBTCloudJob]:
        """Fetch all jobs for a specific project from the dbt Cloud REST API.

        Args:
            access_url: The dbt Cloud access URL.
            token: The API token for authentication.
            account_id: The dbt Cloud account ID.
            project_id: The dbt Cloud project ID.
            environment_id: The dbt Cloud environment ID.

        Returns:
            List of job dictionaries

        Raises:
            ValueError: If unable to fetch jobs from the API.
        """
        url = f"{access_url.rstrip('/')}/api/v2/accounts/{account_id}/jobs/"

        logger.debug(f"Fetching jobs for account {account_id} from dbt Cloud: {url}")
        response = requests.get(
            url,
            headers={
                "Authorization": f"Token {token}",
                "Content-Type": "application/json",
            },
            params={
                "project_id": project_id,
                "environment_id": environment_id,
            },
        )

        try:
            response.raise_for_status()
            data = response.json()
            # The API returns jobs in a 'data' field
            jobs = [
                DBTCloudJob(id=job["id"], generate_docs=job["generate_docs"])
                for job in data.get("data", [])
            ]

            logger.info(f"Found {len(jobs)} jobs in dbt Cloud account {account_id}")
            return jobs
        except requests.exceptions.RequestException as e:
            logger.debug(f"Unable to fetch jobs from dbt Cloud API: {e}", exc_info=True)
            raise ValueError(
                f"Failed to fetch jobs from dbt Cloud API. Status code: {response.status_code}"
            ) from e
        except JSONDecodeError as e:
            logger.debug(
                f"Invalid JSON response from dbt Cloud API: {e}", exc_info=True
            )
            raise ValueError("Received invalid JSON response from dbt Cloud API") from e

    @staticmethod
    def _get_environments_for_project(
        access_url: str, token: str, account_id: int, project_id: int
    ) -> List[DBTCloudEnvironment]:
        """Fetch environments for a specific project from the dbt Cloud REST API.

        Args:
            access_url: The dbt Cloud access URL.
            token: The API token for authentication.
            account_id: The dbt Cloud account ID.
            project_id: The dbt Cloud project ID.

        Returns:
            List of environment dictionaries.

        Raises:
            ValueError: If unable to fetch environments from the API.
        """
        url = f"{access_url.rstrip('/')}/api/v2/accounts/{account_id}/environments/"

        logger.debug(
            f"Fetching environments for project {project_id} from dbt Cloud: {url}"
        )
        response = requests.get(
            url,
            headers={
                "Authorization": f"Token {token}",
                "Content-Type": "application/json",
            },
            params={"project_id": project_id},
        )

        try:
            response.raise_for_status()
            data = response.json()
            environments = []
            logger.debug(
                f"Found {len(data.get('data', []))} environments for project {project_id}"
            )
            for environment in data.get("data", []):
                deployment_type = environment.get("deployment_type")
                if deployment_type is None:
                    logger.debug(
                        f"Skipping environment {environment['id']} with null deployment_type"
                    )
                    continue
                environments.append(
                    DBTCloudEnvironment(
                        id=environment["id"],
                        deployment_type=DBTCloudDeploymentType(deployment_type),
                    )
                )

            logger.debug(
                f"Processed {len(environments)} environments out of {len(data.get('data', []))} for project {project_id}"
            )
            return environments
        except requests.exceptions.RequestException as e:
            logger.debug(
                f"Unable to fetch environments from dbt Cloud API: {e}", exc_info=True
            )
            raise ValueError(
                f"Failed to fetch environments from dbt Cloud API. Status code: {response.status_code}: {str(e)}"
            ) from e
        except JSONDecodeError as e:
            logger.debug(
                f"Invalid JSON response from dbt Cloud API: {e}", exc_info=True
            )
            raise ValueError("Received invalid JSON response from dbt Cloud API") from e

    def _auto_discover_projects_and_jobs(self) -> List[int]:
        """Auto-discover jobs for the configured project.

        Returns:
            List of job IDs to ingest.
        """
        if not self._is_auto_discovery_enabled():
            return []
        assert self.config.auto_discovery is not None
        logger.info(
            f"Starting auto-discovery for project {self.config.project_id} in account {self.config.account_id}"
        )

        # Fetch and find production environment
        try:
            environments: List[DBTCloudEnvironment] = (
                self._get_environments_for_project(
                    self.config.access_url,
                    self.config.token,
                    self.config.account_id,
                    self.config.project_id,
                )
            )
        except Exception as e:
            logger.error(
                f"Failed to fetch environments for project {self.config.project_id}: {e}"
            )
            raise

        production_env = next(
            (
                env
                for env in environments
                if env.deployment_type == DBTCloudDeploymentType.PRODUCTION
            ),
            None,
        )
        if not production_env:
            raise ValueError(
                f"Could not find production environment for project {self.config.project_id}"
            )

        logger.info(
            f"Found production environment {production_env.id} for project {self.config.project_id}"
        )

        # Fetch and filter jobs
        try:
            all_jobs: List[DBTCloudJob] = self._get_jobs_for_project(
                self.config.access_url,
                self.config.token,
                self.config.account_id,
                self.config.project_id,
                production_env.id,
            )
            self.report.total_jobs_retreived_from_api = len(all_jobs)
        except Exception as e:
            logger.error(
                f"Failed to fetch jobs for project {self.config.project_id}: {e}"
            )
            raise

        # Filter jobs by generate_docs=True and job_id_pattern
        filtered_job_ids: List[int] = []
        for job in all_jobs:
            if job.generate_docs and self.config.auto_discovery.job_id_pattern.allowed(
                str(job.id)
            ):
                filtered_job_ids.append(job.id)
            else:
                self.report.total_jobs_processed_skipped += 1
                logger.debug(
                    f"Skipping job {job.id}: generate_docs={job.generate_docs}, "
                    f"matches_pattern={self.config.auto_discovery.job_id_pattern.allowed(str(job.id))}"
                )
                self.report.warning(
                    title="DBT Cloud Jobs Skipped Processing",
                    message=f"Jobs from account_id: {self.config.account_id}, project_id: {self.config.project_id}, environment_id: {production_env.id} were skipped because it did not match the job_id_pattern or did not generate_docs",
                    context=str(job.id),
                )

        logger.info(
            f"Auto-discovery completed: found {len(filtered_job_ids)} jobs to ingest for project {self.config.project_id}"
        )
        return filtered_job_ids

    def load_nodes(self) -> Tuple[List[DBTNode], Dict[str, Optional[str]]]:
        # TODO: In dbt Cloud, commands are scheduled as part of jobs, where
        # each job can have multiple runs. We currently only fully support
        # jobs that do a full / mostly full build of the project, and will
        # print out warnings if data is missing. In the future, it'd be nice
        # to automatically detect all jobs that are part of a project and combine
        # their metadata together.
        # Additionally, we'd like to model dbt Cloud jobs/runs in DataHub
        # as DataProcesses or DataJobs.

        job_ids_to_ingest: List[int] = []
        run_id: Optional[int] = self.config.run_id
        if self._is_auto_discovery_enabled():
            logger.info("Auto-discovery mode: discovering jobs for configured project")
            job_ids_to_ingest = self._auto_discover_projects_and_jobs()
            if not job_ids_to_ingest:
                logger.warning("No jobs discovered in auto-discovery mode")
                return [], {}
            run_id = None  # Always use latest run in auto-discovery
        else:
            assert self.config.job_id is not None
            logger.info(f"Explicit mode: ingesting single job {self.config.job_id}")
            job_ids_to_ingest = [self.config.job_id]

        # Fetch nodes from all jobs
        raw_nodes = []
        for job_id in job_ids_to_ingest:
            self.report.processed_jobs_list.append(job_id)
            self.report.total_jobs_processed += 1
            for node_type, fields in _DBT_FIELDS_BY_TYPE.items():
                try:
                    logger.info(
                        f"Fetching {node_type} from dbt Cloud for job_id: {job_id}"
                    )
                    data = self._send_graphql_query(
                        metadata_endpoint=self.config.metadata_endpoint,
                        token=self.config.token,
                        query=_DBT_GRAPHQL_QUERY.format(type=node_type, fields=fields),
                        variables={
                            "jobId": job_id,
                            "runId": run_id,
                        },
                    )

                    raw_nodes.extend(data["job"][node_type])
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch {node_type} from job {job_id}: {e}. Continuing with other jobs."
                    )
                    continue

        nodes = [self._parse_into_dbt_node(node) for node in raw_nodes]

        additional_metadata: Dict[str, Optional[str]] = {
            "account_id": str(self.config.account_id),
        }

        return nodes, additional_metadata

    def _extract_code_fields(
        self, node: Dict, materialization: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """Extract raw_code and compiled_code from a node."""
        key = node["uniqueId"]
        resource_type = node["resourceType"]

        if resource_type in {"model", "seed", "snapshot"}:
            status = node["status"]
            if status is None and materialization != "ephemeral":
                self.report.warning(
                    title="Schema information may be incomplete",
                    message="Some nodes are missing the `status` field, which dbt uses to track the status of the node in the target database.",
                    context=key,
                )

            raw_code = node["rawCode"] or node["rawSql"]
            compiled_code = node["compiledCode"] or node["compiledSql"]

            if not compiled_code:
                self.report.warning(
                    title="Missing compiled_code",
                    message=f"compiled_code is missing (materialization={materialization}). "
                    "Column-level lineage will not be available for this model.",
                    context=key,
                )
            return raw_code, compiled_code

        return None, None

    def _extract_test_info(
        self, node: Dict, name: str
    ) -> Tuple[Optional[DBTTest], Optional[DBTTestResult]]:
        """Extract test info and result from a test node."""
        qualified_test_name = name

        for dependency in node["dependsOn"]:
            if dependency.startswith("macro."):
                _, macro = dependency.split(".", 1)
                if macro.startswith("dbt."):
                    if not macro.startswith("dbt.test_"):
                        continue
                    macro = macro[len("dbt.test_") :]

                qualified_test_name = macro
                break

        test_info = DBTTest(
            qualified_test_name=qualified_test_name,
            column_name=node["columnName"],
            kw_args={},  # TODO: dbt Cloud doesn't expose the args.
        )

        test_result = None
        if not node["skip"]:
            test_result = DBTTestResult(
                invocation_id=f"job{node['jobId']}-run{node['runId']}",
                execution_time=datetime.now(),  # TODO: dbt Cloud doesn't expose this.
                status=node["status"],
                native_results={
                    key: str(node[key])
                    for key in {
                        "columnName",
                        "error",
                        "fail",
                        "warn",
                        "skip",
                        "state",
                        "status",
                    }
                },
            )
        return test_info, test_result

    def _parse_into_dbt_node(self, node: Dict) -> DBTNode:
        key = node["uniqueId"]
        resource_type = node["resourceType"]

        name = node["name"]
        if self.config.use_identifiers and node.get("identifier"):
            name = node["identifier"]
        if resource_type != "test" and node.get("alias"):
            name = node["alias"]

        comment = node.get("comment", "")
        # description is table-level (more specific), sourceDescription is schema-level
        table_level_desc = node.get("description")
        schema_level_desc = node.get("sourceDescription")
        description = table_level_desc or schema_level_desc or ""

        if resource_type == "model":
            materialization = node["materializedType"]
        elif resource_type == "snapshot":
            materialization = "snapshot"
        else:
            materialization = None

        if resource_type == "snapshot":
            upstream_nodes = [
                obj["uniqueId"]
                for obj in [
                    *node.get("parentsModels", []),
                    *node.get("parentsSources", []),
                ]
            ]
        else:
            upstream_nodes = node.get("dependsOn", [])

        catalog_type = node.get("type")
        meta = node["meta"]
        # Note: node["owner"] contains the database user, not the meta property.
        # We use meta.owner to match dbt_core.py behavior.
        owner = meta.get("owner")
        tags = [self.config.tag_prefix + tag for tag in node["tags"]]

        raw_code, compiled_code = self._extract_code_fields(node, materialization)

        max_loaded_at = None
        if resource_type == "source":
            max_loaded_at_str = node["maxLoadedAt"]
            if max_loaded_at_str:
                max_loaded_at = dateutil.parser.parse(max_loaded_at_str)
                if max_loaded_at.year <= 1:
                    max_loaded_at = None

        columns: List[DBTColumn] = []
        if "columns" in node and node["columns"] is not None:
            # columns will be empty for ephemeral models
            columns = list(
                sorted(
                    [self._parse_into_dbt_column(column) for column in node["columns"]],
                    key=lambda c: c.index,
                )
            )

        test_info = None
        test_result = None
        if resource_type == "test":
            test_info, test_result = self._extract_test_info(node, name)

        return DBTNode(
            dbt_name=key,
            # TODO: Get the dbt adapter natively.
            dbt_adapter=self.config.target_platform,
            dbt_package_name=node.get("packageName"),
            database=node.get("database"),
            schema=node.get("schema"),
            name=name,
            alias=node.get("alias"),
            dbt_file_path=None,  # TODO: Get this from the dbt API.
            node_type=resource_type,
            max_loaded_at=max_loaded_at,
            comment=comment,
            description=description,
            upstream_nodes=upstream_nodes,
            materialization=materialization,
            catalog_type=catalog_type,
            missing_from_catalog=False,  # This doesn't really apply to dbt Cloud.
            meta=meta,
            query_tag={},  # TODO: Get this from the dbt API.
            tags=tags,
            owner=owner,
            language="sql",  # TODO: dbt Cloud doesn't surface this
            raw_code=raw_code,
            compiled_code=compiled_code,
            columns=columns,
            test_info=test_info,
            test_results=[test_result] if test_result else [],
            model_performances=[],  # TODO: support model performance with dbt Cloud
        )

    def _parse_into_dbt_column(
        self,
        column: Dict,
    ) -> DBTColumn:
        return DBTColumn(
            name=column["name"],
            comment=column.get("comment", ""),
            description=column["description"],
            # For some reason, the index sometimes comes back as None from the dbt Cloud API.
            # In that case, we just assume that the column is at the end of the table by
            # assigning it a very large index.
            index=column["index"] if column["index"] is not None else 10**6,
            data_type=column["type"],
            meta=column["meta"],
            tags=column["tags"],
        )

    def get_external_url(self, node: DBTNode) -> Optional[str]:
        if self.config.external_url_mode == "explore":
            return f"{self.config.access_url}/explore/{self.config.account_id}/projects/{self.config.project_id}/environments/production/details/{node.dbt_name}"
        else:
            return f"{self.config.access_url}/develop/{self.config.account_id}/projects/{self.config.project_id}"
