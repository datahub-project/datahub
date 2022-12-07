import logging
from datetime import datetime
from json import JSONDecodeError
from typing import Dict, List, Optional, Tuple

import dateutil.parser
import requests
from pydantic import Field

from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumn,
    DBTCommonConfig,
    DBTNode,
    DBTSourceBase,
    DBTTest,
    DBTTestResult,
)

logger = logging.getLogger(__name__)


class DBTCloudConfig(DBTCommonConfig):
    metadata_endpoint: str = Field(
        default="https://metadata.cloud.getdbt.com/graphql",
        description="The dbt Cloud metadata API endpoint.",
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

    job_id: int = Field(
        description="The ID of the job to ingest metadata from.",
    )
    run_id: Optional[int] = Field(
        description="The ID of the run to ingest metadata from. If not specified, we'll default to the latest run.",
    )


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

_DBT_GRAPHQL_MODEL_SEED_FIELDS = """
  alias
  error
  status
  skip
  rawSql
  rawCode
  compiledSql
  compiledCode
"""

_DBT_GRAPHQL_QUERY = f"""
query DatahubMetadataQuery($jobId: Int!, $runId: Int) {{
  models(jobId: $jobId, runId: $runId) {{
    { _DBT_GRAPHQL_COMMON_FIELDS }
    { _DBT_GRAPHQL_NODE_COMMON_FIELDS }
    { _DBT_GRAPHQL_MODEL_SEED_FIELDS }
    dependsOn
    materializedType
  }}

  seeds(jobId: $jobId, runId: $runId) {{
    { _DBT_GRAPHQL_COMMON_FIELDS }
    { _DBT_GRAPHQL_NODE_COMMON_FIELDS }
    { _DBT_GRAPHQL_MODEL_SEED_FIELDS }
  }}

  sources(jobId: $jobId, runId: $runId) {{
    { _DBT_GRAPHQL_COMMON_FIELDS }
    { _DBT_GRAPHQL_NODE_COMMON_FIELDS }
    identifier
    sourceName
    sourceDescription
    maxLoadedAt
    snapshottedAt
    state
    freshnessChecked
    loader
  }}

  tests(jobId: $jobId, runId: $runId) {{
    { _DBT_GRAPHQL_COMMON_FIELDS }
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
  }}

  # Currently unsupported dbt node types:
  # - metrics
  # - snapshots
  # - exposures
}}
"""


@platform_name("dbt")
@config_class(DBTCloudConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.USAGE_STATS, "", supported=False)
class DBTCloudSource(DBTSourceBase):
    """
    This source pulls dbt metadata directly from the dbt Cloud APIs.

    You'll need to have a dbt Cloud job set up to run your dbt project, and "Generate docs on run" should be enabled.

    The token should have the "read metadata" permission.

    To get the required IDs, go to the job details page (this is the one with the "Run History" table), and look at the URL.
    It should look something like this: https://cloud.getdbt.com/next/deploy/107298/projects/175705/jobs/148094.
    In this example, the account ID is 107298, the project ID is 175705, and the job ID is 148094.
    """

    # TODO: add some screenshots to the docs

    config: DBTCloudConfig

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCloudConfig.parse_obj(config_dict)
        return cls(config, ctx, "dbt")

    def load_nodes(self) -> Tuple[List[DBTNode], Dict[str, Optional[str]]]:
        # TODO: In dbt Cloud, commands are scheduled as part of jobs, where
        # each job can have multiple runs. We currently only fully support
        # jobs that do a full / mostly full build of the project, and will
        # print out warnings if data is missing. In the future, it'd be nice
        # to automatically detect all jobs that are part of a project and combine
        # their metadata together.
        # Additionally, we'd like to model dbt Cloud jobs/runs in DataHub
        # as DataProcesses or DataJobs.

        logger.debug("Sending graphql request to the dbt Cloud metadata API")
        response = requests.post(
            self.config.metadata_endpoint,
            json={
                "query": _DBT_GRAPHQL_QUERY,
                "variables": {
                    "jobId": self.config.job_id,
                    "runId": self.config.run_id,
                },
            },
            headers={
                "Authorization": f"Bearer {self.config.token}",
            },
        )

        try:
            res = response.json()
            if "errors" in res:
                raise ValueError(
                    f'Unable to fetch metadata from dbt Cloud: {res["errors"]}'
                )
            data = res["data"]
        except JSONDecodeError as e:
            response.raise_for_status()
            raise e

        raw_nodes = [
            *data["models"],
            *data["seeds"],
            *data["sources"],
            *data["tests"],
        ]

        nodes = [self._parse_into_dbt_node(node) for node in raw_nodes]

        additional_metadata: Dict[str, Optional[str]] = {
            "project_id": str(self.config.project_id),
            "account_id": str(self.config.account_id),
            "job_id": str(self.config.job_id),
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
        description = node["description"]
        if node.get("sourceDescription"):
            description = node["sourceDescription"]

        if node["resourceType"] == "model":
            materialization = node["materializedType"]
        else:
            materialization = None

        upstream_nodes = node.get("dependsOn", [])

        catalog_type = node.get("type")

        meta = node["meta"]

        # The dbt owner field is set to the db user, and not the meta property.
        # owner = node.get("owner")
        owner = meta.get("owner")

        tags = node["tags"]
        tags = [self.config.tag_prefix + tag for tag in tags]

        if node["resourceType"] in {"model", "seed"}:
            status = node["status"]
            if status is None and materialization != "ephemeral":
                self.report.report_warning(
                    key, "node is missing a status, schema metadata will be incomplete"
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
        if "columns" in node:
            # columns will be empty for ephemeral models
            columns = [
                self._parse_into_dbt_column(column)
                for column in sorted(node["columns"], key=lambda c: c["index"])
            ]

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
            meta=meta,
            query_tag={},  # TODO: Get this from the dbt API.
            tags=tags,
            owner=owner,
            language="sql",  # TODO: dbt Cloud doesn't surface this
            raw_code=raw_code,
            compiled_code=compiled_code,
            columns=columns,
            test_info=test_info,
            test_result=test_result,
        )

    def _parse_into_dbt_column(self, column: Dict) -> DBTColumn:
        return DBTColumn(
            name=column["name"],
            comment=column.get("comment", ""),
            description=column["description"],
            index=column["index"],
            data_type=column["type"],
            meta=column["meta"],
            tags=column["tags"],
        )

    def get_external_url(self, node: DBTNode) -> Optional[str]:
        # TODO: Once dbt Cloud supports deep linking to specific files, we can use that.
        return f"https://cloud.getdbt.com/next/accounts/{self.config.account_id}/projects/{self.config.project_id}/develop"

    def get_platform_instance_id(self) -> str:
        """The DBT project identifier is used as platform instance."""

        return f"{self.platform}_{self.config.project_id}"
