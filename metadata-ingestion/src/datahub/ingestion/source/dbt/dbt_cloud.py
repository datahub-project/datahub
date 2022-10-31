import logging
from json import JSONDecodeError
from typing import Dict, Iterable, List, Optional, Tuple

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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumn,
    DBTCommonConfig,
    DBTNode,
    DBTSourceBase,
)

logger = logging.getLogger(__name__)

DBT_METADATA_API_ENDPOINT = "https://metadata.cloud.getdbt.com/graphql"


class DBTCloudConfig(DBTCommonConfig):
    token: str = Field(
        description="The API token to use to authenticate with DBT Cloud.",
    )

    # In the URL https://cloud.getdbt.com/next/deploy/107298/projects/175705/jobs/148094,
    # the job ID would be 148094.
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

    # TODO account id?
    # TODO project id?


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

  # TODO: We currently don't support this field.
  stats {
    id
    label
    description
    include
    value
  }
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

  # TODO: Currently unsupported dbt node types:
  # - metrics
  # - snapshots
  # - exposures
}}
"""


@platform_name("dbt")
@config_class(DBTCommonConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.USAGE_STATS, "", supported=False)
class DBTCloudSource(DBTSourceBase):
    """
    TODO docs
    """

    config: DBTCloudConfig

    @classmethod
    def create(cls, config_dict, ctx):
        config = DBTCloudConfig.parse_obj(config_dict)
        return cls(config, ctx, "dbt")

    # TODO: Add support for test_connection.

    def load_nodes(self) -> Tuple[List[DBTNode], Dict[str, Optional[str]]]:
        # TODO: model dbt cloud runs as datahub DataProcesses or DataJobs
        # TODO: figure out how to deal with jobs that only run part of the job
        # TODO capture model creation failures?

        logger.debug("Sending graphql request to the dbt Cloud metadata API")
        response = requests.post(
            DBT_METADATA_API_ENDPOINT,
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

        return nodes, {}

    def _parse_into_dbt_node(self, node: Dict) -> DBTNode:
        key = node["uniqueId"]

        name = node["name"]
        if self.config.use_identifiers and node.get("identifier"):
            name = node["identifier"]
        if node["resourceType"] != "test" and node.get("alias"):
            name = node["alias"]
        # TODO check sourceName for alternative source schema

        comment = node.get("comment", "")
        description = node["description"]
        if node.get("sourceDescription"):
            description = node["sourceDescription"]

        if node["resourceType"] == "model":
            materialization = node["materializedType"]
            upstream_nodes = node["dependsOn"]
        else:
            materialization = None
            upstream_nodes = []

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
                    key, "node is missing a status, metadata will be incomplete"
                )

            # The code fields are new in dbt 1.3, and replace the sql ones.
            raw_sql = node["rawCode"] or node["rawSql"]
            compiled_sql = node["compiledCode"] or node["compiledSql"]
        else:
            raw_sql = None
            compiled_sql = None

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

        # TODO add project id, env, etc to custom metadata

        if node["resourceType"] == "test":
            breakpoint()

        return DBTNode(
            dbt_name=key,
            # TODO: Get the dbt adapter natively.
            dbt_adapter=self.config.target_platform,
            database=node["database"],
            schema=node["schema"],
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
            raw_sql=raw_sql,
            compiled_sql=compiled_sql,
            manifest_raw=node,
            columns=columns,
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

    def load_tests(
        self, test_nodes: List[DBTNode], all_nodes_map: Dict[str, DBTNode]
    ) -> Iterable[MetadataWorkUnit]:
        # TODO
        return []

    def get_platform_instance_id(self) -> str:
        """The DBT project identifier is used as platform instance."""

        return f"{self.platform}_{self.config.project_id}"
