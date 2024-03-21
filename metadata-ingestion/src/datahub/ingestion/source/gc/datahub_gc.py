import datetime
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable

import click
from pydantic import Field

from datahub.configuration.common import ConfigModel, OperationalError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit


class DataHubGcSourceConfig(ConfigModel):
    cleanup_expired_tokens: bool = Field(
        default=True,
        description="Whether to clean up expired tokens or not",
    )
    truncate_indices: bool = Field(
        default=True,
        description="Whether to truncate elasticsearch indices or not which can be safely truncated",
    )
    truncate_index_older_than_days: int = Field(
        default=30,
        description="Indices older than this number of days will be truncated",
    )


@dataclass
class DataHubGcSourceReport(SourceReport):
    expired_tokens_revoked: int = 0


@platform_name("DataHubGc")
@config_class(DataHubGcSourceConfig)
@support_status(SupportStatus.TESTING)
class DataHubGcSource(Source):
    def __init__(self, ctx: PipelineContext, config: DataHubGcSourceConfig):
        self.ctx = ctx
        self.config = config
        self.report = DataHubGcSourceReport()
        self.graph = ctx.graph
        assert (
            self.graph is not None
        ), "DataHubGc source requires a graph. Please either use datahub-rest sink or set datahub_api"

    @classmethod
    def create(cls, config_dict, ctx):
        config = DataHubGcSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.cleanup_expired_tokens:
            self.revoke_expired_tokens()
        if self.config.truncate_indices:
            self.truncate_indices()
        yield from []

    def truncate_indices(self) -> None:
        self._truncate_timeseries_helper(aspect_name="operation", entity_type="dataset")
        self._truncate_timeseries_helper(
            aspect_name="datasetusage", entity_type="dataset"
        )
        self._truncate_timeseries_helper(aspect_name="chartusage", entity_type="chart")
        self._truncate_timeseries_helper(
            aspect_name="dashboardusage", entity_type="dashboard"
        )

    def _truncate_timeseries_helper(self, aspect_name: str, entity_type: str) -> None:
        self._truncate_timeseries_with_watch_optional(
            aspect_name=aspect_name, entity_type=entity_type, watch=False
        )
        self._truncate_timeseries_with_watch_optional(
            aspect_name=aspect_name, entity_type=entity_type, watch=True
        )

    def _truncate_timeseries_with_watch_optional(
        self, aspect_name: str, entity_type: str, watch: bool
    ) -> None:
        graph = self.graph
        assert graph is not None
        if watch:
            to_delete = 1
            while to_delete > 0:
                response = self.truncate_timeseries_util(
                    aspect=aspect_name,
                    dry_run=watch,
                    days_ago=self.config.truncate_index_older_than_days,
                    entity_type=entity_type,
                )
                val = response.get("value", "")
                if "This was a dry run" not in val or "out of" not in val:
                    return
                prev_to_delete = to_delete
                to_delete, total = re.findall(r"\d+", val)[:2]
                to_delete = int(to_delete)
                if to_delete <= 0:
                    click.echo("Nothing to delete.")
                    return
                click.echo(f"to_delete {to_delete} / {total}")
                if to_delete == prev_to_delete:
                    click.echo("Seems to be stuck. Ending the loop.")
                    break
                elif to_delete < 50000:
                    click.echo("Too small truncation. Not going to watch.")
                    return
                else:
                    time.sleep(60)
        else:
            self.truncate_timeseries_util(
                aspect=aspect_name,
                dry_run=watch,
                days_ago=self.config.truncate_index_older_than_days,
                entity_type=entity_type,
            )

    def x_days_ago_millis(self, days: int) -> int:
        x_days_ago_datetime = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(days=days)
        return int(x_days_ago_datetime.timestamp() * 1000)

    def truncate_timeseries_util(
        self,
        aspect: str,
        days_ago: int,
        dry_run: bool = True,
        entity_type: str = "dataset",
    ) -> Dict:
        graph = self.graph
        assert graph is not None

        gms_url = graph._gms_server
        if not dry_run:
            click.echo(
                f"Going to truncate timeseries for {aspect} for {gms_url} older than {days_ago} days"
            )
        days_ago_millis = self.x_days_ago_millis(days_ago)
        url = f"{gms_url}/operations?action=truncateTimeseriesAspect"
        try:
            response = graph._post_generic(
                url=url,
                payload_dict={
                    "entityType": entity_type,
                    "aspect": aspect,
                    "endTimeMillis": days_ago_millis,
                    "dryRun": dry_run,
                },
            )
            # click.echo(f"Response: {response}")
        except OperationalError:
            response = graph._post_generic(
                url=url,
                payload_dict={
                    "entityType": entity_type,
                    "aspect": aspect,
                    "endTimeMillis": days_ago_millis,
                    "dryRun": dry_run,
                    "forceDeleteByQuery": True,
                },
            )
            # click.echo(f"Response: {response}")
        return response

    def revoke_expired_tokens(self) -> None:
        total = 1
        while total > 0:
            expired_tokens_res = self._get_expired_tokens()
            list_access_tokens = expired_tokens_res.get("listAccessTokens", {})
            tokens = list_access_tokens.get("tokens", [])
            total = list_access_tokens.get("total", 0)
            for token in tokens:
                self.report.expired_tokens_revoked += 1
                token_id = token["id"]
                self._revoke_token(token_id)

    def _revoke_token(self, token_id: str) -> None:
        assert self.graph is not None
        self.graph.execute_graphql(
            query="""mutation revokeAccessToken($tokenId: String!) {
  revokeAccessToken(tokenId: $tokenId)
}
""",
            variables={"tokenId": token_id},
        )

    def _get_expired_tokens(self) -> dict:
        assert self.graph is not None
        return self.graph.execute_graphql(
            query="""query listAccessTokens($input: ListAccessTokenInput!) {
  listAccessTokens(input: $input) {
    start
    count
    total
    tokens {
      urn
      type
      id
      name
      description
      actorUrn
      ownerUrn
      createdAt
      expiresAt
      __typename
    }
    __typename
  }
}
""",
            variables={
                "input": {
                    "start": 0,
                    "count": 10,
                    "filters": [
                        {
                            "field": "expiresAt",
                            "values": [str(int(time.time() * 1000))],
                            "condition": "LESS_THAN",
                        }
                    ],
                }
            },
        )

    def get_report(self) -> SourceReport:
        return self.report
