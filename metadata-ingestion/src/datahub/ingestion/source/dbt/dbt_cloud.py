import logging
from copy import deepcopy
from datetime import datetime
from functools import lru_cache
from json import JSONDecodeError
from typing import Dict, List, Literal, Optional, Tuple
from urllib.parse import urlparse

import dateutil.parser
import requests
from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern
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
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumn,
    DBTCommonConfig,
    DBTNode,
    DBTSourceBase,
    DBTSourceReport,
)
from datahub.ingestion.source.dbt.dbt_tests import DBTTest, DBTTestResult

logger = logging.getLogger(__name__)


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
    project_id: Optional[int] = Field(
        None,
        description="The dbt Cloud project ID to use. Required when job_id is provided. Optional when auto-discovering jobs.",
    )

    job_id: Optional[int] = Field(
        None,
        description="The ID of the job to ingest metadata from. If not provided, all jobs matching the project_id_pattern and job_id_pattern will be ingested.",
    )
    run_id: Optional[int] = Field(
        None,
        description="The ID of the run to ingest metadata from. If not specified, defaults to the latest run. In auto-discovery mode, always uses the latest run for each job.",
    )

    project_id_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter projects by project_id when auto-discovering jobs.",
    )

    job_id_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter jobs by job_id when auto-discovering jobs.",
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
        # If job_id is provided, project_id should also be provided for backward compatibility
        if self.job_id is not None and self.project_id is None:
            raise ValueError(
                "project_id is required when job_id is provided. Please provide project_id or remove job_id to use auto-discovery."
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
    # - snapshots
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

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            source_config = DBTCloudConfig.parse_obj_allow_extras(config_dict)

            if source_config.job_id is not None:
                # We are ingesting a single job and not auto-discovering jobs
                # Test with single job_id (backward compatibility)
                DBTCloudSource._send_graphql_query(
                    metadata_endpoint=source_config.metadata_endpoint,
                    token=source_config.token,
                    query=_DBT_GRAPHQL_QUERY.format(type="tests", fields="jobId"),
                    variables={
                        "jobId": source_config.job_id,
                        "runId": source_config.run_id,
                    },
                )
            else:
                # Test by fetching jobs list (auto-discovery mode)
                jobs = DBTCloudSource._get_all_jobs(
                    access_url=source_config.access_url,
                    token=source_config.token,
                    account_id=source_config.account_id,
                )

                if not jobs:
                    raise ValueError("No jobs found in dbt Cloud account")

                logger.info(f"Successfully fetched {len(jobs)} jobs from dbt Cloud")

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
    @lru_cache(maxsize=1)  # because we only ingest one account per ingestion run
    def _get_all_jobs(access_url: str, token: str, account_id: int) -> List[Dict]:
        """Fetch all jobs from the dbt Cloud REST API.

        Args:
            access_url: The dbt Cloud access URL.
            token: The API token for authentication.
            account_id: The dbt Cloud account ID.

        Returns:
            List of job dictionaries

        Raises:
            ValueError: If unable to fetch jobs from the API.
        """
        url = f"{access_url.rstrip('/')}/api/v2/accounts/{account_id}/jobs/"

        logger.debug(f"Fetching jobs from dbt Cloud: {url}")
        response = requests.get(
            url,
            headers={
                "Authorization": f"Token {token}",
                "Content-Type": "application/json",
            },
        )

        try:
            response.raise_for_status()
            data = response.json()
            # The API returns jobs in a 'data' field
            jobs = data.get("data", [])

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

    def _filter_jobs(self, jobs: List[Dict]) -> List[int]:
        """Filter jobs based on project_id_pattern and job_id_pattern.

        Args:
            jobs: List of job dictionaries from the API.

        Returns:
            Filtered list of jobs that match the patterns.
        """
        filtered_jobs = []
        for job in jobs:
            job_id_str = str(job.get("id", ""))
            project_id_str = str(job.get("project_id", ""))

            if not job_id_str or not project_id_str:
                logger.debug(f"Skipping job with missing id or project_id: {job}")
                continue

            # Check if project and job ids match the pattern

            project_matches = self.config.project_id_pattern.allowed(project_id_str)
            job_matches = self.config.job_id_pattern.allowed(job_id_str)

            if project_matches and job_matches:
                try:
                    filtered_jobs.append(int(job_id_str))
                except ValueError:
                    logger.debug(
                        f"Skipping job with invalid job id: {job_id_str} (not an integer)"
                    )
                    continue
            else:
                if not project_matches:
                    self.report.warning(
                        title="Skipping DBT projects",
                        message="Skipping projects due to unmatched project id pattern",
                        context=project_id_str,
                    )
                if not job_matches:
                    self.report.warning(
                        title="Skipping DBT jobs",
                        message="Skipping jobs due to unmatched job id pattern",
                        context=job_id_str,
                    )

        logger.info(
            f"Filtered {len(jobs)} jobs down to {len(filtered_jobs)} jobs matching patterns"
        )
        return filtered_jobs

    def load_nodes(self) -> Tuple[List[DBTNode], Dict[str, Optional[str]]]:
        # TODO: In dbt Cloud, commands are scheduled as part of jobs, where
        # each job can have multiple runs. We currently only fully support
        # jobs that do a full / mostly full build of the project, and will
        # print out warnings if data is missing. In the future, it'd be nice
        # to automatically detect all jobs that are part of a project and combine
        # their metadata together.
        # Additionally, we'd like to model dbt Cloud jobs/runs in DataHub
        # as DataProcesses or DataJobs.

        jobs_to_ingest: List[int] = []
        # In auto-discovery mode, always use the latest run (None means latest)
        # In single-job mode, use the configured run_id or latest if not specified
        run_id = self.config.run_id if self.config.job_id is not None else None
        if self.config.job_id is not None:
            # Single job_id provided
            logger.info(f"Ingesting single job: {self.config.job_id}")
            jobs_to_ingest.append(self.config.job_id)
        else:
            # Auto-discovery: fetch all jobs and filter them
            logger.info("Auto-discovering jobs from dbt Cloud")
            # Resetting the run_id to None to get the latest run
            run_id = None
            all_jobs = self._get_all_jobs(
                self.config.access_url, self.config.token, self.config.account_id
            )
            filtered_jobs = self._filter_jobs(all_jobs)

            if not filtered_jobs:
                logger.warning(
                    "No jobs found matching the specified patterns. Please check your project_id_pattern and job_id_pattern settings."
                )
                return [], {}

            jobs_to_ingest = filtered_jobs
            logger.info(f"Ingesting {len(jobs_to_ingest)} jobs from dbt Cloud")

        # Process all jobs uniformly
        raw_nodes = []

        for job_id in jobs_to_ingest:
            logger.info(f"Fetching nodes from job {job_id}")

            # Fetch nodes for each node type for this job
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

    def _parse_into_dbt_node(self, node: Dict) -> DBTNode:
        key = node["uniqueId"]

        name = node["name"]
        if self.config.use_identifiers and node.get("identifier"):
            name = node["identifier"]
        if node["resourceType"] != "test" and node.get("alias"):
            name = node["alias"]

        comment = node.get("comment", "")

        # In dbt sources, there are two types of descriptions:
        # - description: table-level description (specific to the source table)
        # - sourceDescription: schema-level description (describes the overall source schema)
        # The table-level description should take precedence since it's more specific.
        description = node["description"] or node.get("sourceDescription", "")

        if node["resourceType"] == "model":
            materialization = node["materializedType"]
        elif node["resourceType"] == "snapshot":
            materialization = "snapshot"
        else:
            materialization = None

        if node["resourceType"] == "snapshot":
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

        # The dbt owner field is set to the db user, and not the meta property.
        # owner = node.get("owner")
        owner = meta.get("owner")

        tags = node["tags"]
        tags = [self.config.tag_prefix + tag for tag in tags]

        if node["resourceType"] in {"model", "seed", "snapshot"}:
            status = node["status"]
            if status is None and materialization != "ephemeral":
                self.report.warning(
                    title="Schema information may be incomplete",
                    message="Some nodes are missing the `status` field, which dbt uses to track the status of the node in the target database.",
                    context=key,
                    log=False,
                )

            # The code fields are new in dbt 1.3, and replace the sql ones.
            raw_code = node["rawCode"] or node["rawSql"]
            compiled_code = node["compiledCode"] or node["compiledSql"]
        else:
            raw_code = None
            compiled_code = None

        max_loaded_at = None
        if node["resourceType"] == "source":
            max_loaded_at_str = node["maxLoadedAt"]
            if max_loaded_at_str:
                max_loaded_at = dateutil.parser.parse(max_loaded_at_str)

                # For missing data, dbt returns 0001-01-01T00:00:00.000Z.
                if max_loaded_at.year <= 1:
                    max_loaded_at = None

        columns = []
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
        if node["resourceType"] == "test":
            qualified_test_name = name

            # The qualified test name should be the test name from the dbt project.
            # It can be simple (e.g. 'unique') or prefixed (e.g. 'dbt_expectations.expect_column_values_to_not_be_null').
            # We attempt to guess the test name based on the macros used.
            for dependency in node["dependsOn"]:
                # An example dependsOn list could be:
                #     ['model.sample_dbt.monthly_billing_with_cust', 'macro.dbt.test_not_null', 'macro.dbt.get_where_subquery']
                # In that case, the test should be `not_null`.

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
            node_type=node["resourceType"],
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
            project_id=node.get("projectId"),
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
        # Use project_id from the node, account_id from config (one account per ingestion)
        if node.project_id is None:
            return None

        if self.config.external_url_mode == "explore":
            return f"{self.config.access_url}/explore/{self.config.account_id}/projects/{node.project_id}/environments/production/details/{node.dbt_name}"
        else:
            return f"{self.config.access_url}/develop/{self.config.account_id}/projects/{node.project_id}"
