import itertools
from typing import Any, Dict, List, Optional

from google.api_core.exceptions import Forbidden

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import soft_listing
from datahub.ingestion.agent.sql_gate import INFORMATION_SCHEMA, CatalogScope
from datahub.ingestion.agent.sql_passthrough import (
    PROBE_QUERY_LABEL,
    CatalogRows,
    QueryBudget,
    SqlCatalogPassthrough,
)
from datahub.ingestion.agent.verdicts import ProbeSoftError
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from datahub.ingestion.source.common.gcp_errors import (
    error_info_reason,
    error_info_service,
    gcp_error_code,
    is_service_disabled,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

# One GiB scanned. BigQuery bills by bytes read regardless of how few rows come
# back, so max_results caps the page and this caps the bill -- generous for the
# INFORMATION_SCHEMA reads a probe is for, and a hard stop on anything that
# wanders into a full table scan.
_MAX_BYTES_BILLED = 1024**3


def _projects_retry(timeout: Optional[int]) -> Any:
    # lazy: the bigquery client library is only needed once a probe runs
    from google.cloud.bigquery.retry import DEFAULT_RETRY

    return DEFAULT_RETRY.with_timeout(timeout) if timeout else DEFAULT_RETRY


# ErrorInfo reasons that mean "this credential may not see these projects" --
# an expected answer for one scoped to named projects, which `project_ids`
# exists to serve. A 403 for any other reason is not: `RATE_LIMIT_EXCEEDED`
# on concurrent projects.list calls is transient and ingestion retries it, and
# reporting it as an IAM gap sends the reader to change roles over a blip.
# A legacy 403 carrying no ErrorInfo is treated as a denial, as it was before.
_ACCESS_DENIED_REASONS = frozenset(
    {
        "IAM_PERMISSION_DENIED",
        "USER_PROJECT_DENIED",
        "ACCESS_TOKEN_SCOPE_INSUFFICIENT",
        "ACCOUNT_STATE_INVALID",
    }
)

# bigquery.Client.list_projects pages GET /bigquery/v2/projects, so a large
# org is many calls. Bounded like every other kind-declaring listing.
_DEFAULT_PROJECT_LIMIT = 200


class BigQueryMetadataProbe(SqlCatalogPassthrough):
    """Catalog-query surface for BigQuery.

    BigQuery addresses catalog views as <dataset>.INFORMATION_SCHEMA.<VIEW>, so a
    query must name the dataset; sql_gate understands that shape.
    """

    sql_dialect = "bigquery"

    # A named-relation allowlist rather than a schema-level allow of
    # information_schema, which is what the framework default gives and what every
    # other standard dialect can safely use.
    #
    # BigQuery is the exception because it extends INFORMATION_SCHEMA with JOBS,
    # whose `query` column holds the SQL text of every job in the project --
    # WHERE-clause literals, which are row values -- alongside user_email. A
    # schema-level allow permits it, and BigQuery's own lineage extractor reads it
    # (queries_extractor.py), so it is not hypothetical.
    #
    # Excluding JOBS by name would work today and rot tomorrow: the next
    # text-bearing view Google adds arrives permitted. Naming what is allowed keeps
    # the default deny. The list is what BigQuery ingestion itself reads, minus
    # JOBS, so a probe can reproduce anything ingestion does.
    #
    # sqlglot parses `myds.INFORMATION_SCHEMA.TABLES` with the name slot
    # `INFORMATION_SCHEMA.TABLES`, so the slot must be split to match. Safe
    # here: a BigQuery identifier cannot contain a dot (inside backticks a dot
    # still separates path parts).
    catalog_scope = CatalogScope(
        split_dotted_identifiers=True,
        schemas=frozenset(),
        relations=frozenset(
            f"{INFORMATION_SCHEMA}.{view}"
            for view in (
                "tables",
                "table_options",
                "table_constraints",
                "table_storage",
                "columns",
                "column_field_paths",
                "views",
                "schemata",
                "schemata_options",
                "partitions",
                "key_column_usage",
                "constraint_column_usage",
            )
        ),
    )

    # Both of these are real, server-side and enforced by BigQuery itself --
    # this is the only dialect here where that is true of the time ceiling as
    # well as the cost one. maximum_bytes_billed is the stronger of the two:
    # the job is refused before it runs rather than cancelled partway, so it
    # bounds spend rather than just duration.
    query_budget = QueryBudget(timeout_seconds=30, max_bytes_billed=_MAX_BYTES_BILLED)

    def __init__(self, client: Any, config: Any = None) -> None:
        self._client = client
        self._config = config

    @classmethod
    def for_config(cls, config: BigQueryConnectionConfig) -> "BigQueryMetadataProbe":
        """Reuse the connector's own client builder rather than a second SQLAlchemy
        engine, so credentials resolve the way ingestion resolves them.

        The config travels too: `projects` has to answer for the recipe, not
        for the credential, and a listing wider than what ingestion reads
        would offer containers a run never touches."""
        return cls(config.get_bigquery_client(), config)

    def __exit__(self, *exc: object) -> None:
        self._client.close()

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        """The HTTP status a google-api-core error carries ("HTTP 403")."""
        return gcp_error_code(exc)

    @probe_method(
        kind=DatasetContainerSubTypes.BIGQUERY_PROJECT, row_limit_param="limit"
    )
    def projects(self, limit: int = _DEFAULT_PROJECT_LIMIT) -> List[str]:
        """The projects this recipe would consider -- the string
        `project_id_pattern` is matched against, and the container
        `dataset_pattern` is qualified with.

        Three branches, one per shape ingestion itself takes. With
        `project_ids` set these are exactly those ids and nothing is called:
        ingestion reads those and no others, so listing the whole credential's
        view would offer projects a run never touches. With `project_labels`
        it is Resource Manager's label search, the API ingestion uses for that
        recipe. With neither, every project the credential can see, including
        ones `project_id_pattern` would exclude -- a denied project is
        explained by `probe filter`, not hidden here.
        """
        pinned = list(getattr(self._config, "project_ids", None) or [])
        if pinned:
            self._warn(
                "`project_ids` is set, so these are the recipe's own projects; "
                "the credential may see others, and ingestion would not read them"
            )
            # Sorted before the cut, for the same reason: the framework drops
            # the last item, so the order it drops from must be the final one.
            return sorted(pinned)[:limit]
        labels = list(getattr(self._config, "project_labels", None) or [])
        with soft_listing(self._warn):
            if labels:
                return self._labelled_projects(labels, limit)
            return self._listed_projects(limit)
        return []

    def _labelled_projects(self, labels: List[str], limit: int) -> List[str]:
        """The label search ingestion runs for this recipe shape.

        `project_labels` sends ingestion down a different API -- Resource
        Manager's `search_projects`, not BigQuery's own `projects.list` -- and
        it reads only what comes back. Listing the credential's view here
        instead would answer a question the recipe does not ask, which is the
        same fault as ignoring `project_ids`. It needs
        `resourcemanager.projects.list`, as ingestion does for this recipe.
        """
        # lazy: Resource Manager is only reached on this branch
        from datahub.ingestion.source.common.gcp_project_filter import (
            iter_projects_by_labels,
        )

        try:
            found = itertools.islice(
                iter_projects_by_labels(
                    frozenset(labels), self._config.get_projects_client()
                ),
                limit,
            )
            ids = [project.id for project in found]
        except Forbidden as exc:
            # Through the same classifier as the unlabelled branch: a
            # SERVICE_DISABLED names the API to enable, and a 403 for any
            # other reason -- a quota blip on concurrent calls -- propagates
            # rather than degrading to "no projects exist, change your IAM".
            raise self._denied(exc, needs="`resourcemanager.projects.list`") from exc
        return sorted(ids) if len(ids) < limit else ids

    def _listed_projects(self, limit: int) -> List[str]:
        timeout = self.query_budget.timeout_seconds
        try:
            # Bounded on both sides: the client leaves `timeout` at None, and
            # DEFAULT_RETRY would otherwise keep retrying a read timeout for
            # ten minutes -- on the one command every refusal points to.
            pager = self._client.list_projects(
                max_results=limit,
                timeout=timeout,
                retry=_projects_retry(timeout),
            )
            ids = [p.project_id for p in itertools.islice(pager, limit)]
        except Forbidden as exc:
            raise self._denied(exc) from exc
        # Sorted only when complete. `limit` is already one past the caller's
        # -- the framework asks for the extra item here and drops the last one
        # itself -- so a full page may be truncated, and the item it drops has
        # to be that extra one rather than whichever happens to sort last.
        return sorted(ids) if len(ids) < limit else ids

    @staticmethod
    def _denied(
        exc: Forbidden, needs: str = "permission to list projects"
    ) -> Exception:
        """A 403 the recipe can act on, as a soft error, or the error itself.

        `needs` names the permission this particular call wants: the two
        branches reach different APIs, so they are denied for different
        grants.
        """
        reason = error_info_reason(exc)
        if is_service_disabled(exc):
            service = error_info_service(exc) or "the API this call needs"
            return ProbeSoftError(
                f"listing projects needs {service}, which is not enabled for "
                f"this project; enable it, or set `project_ids` in the recipe "
                f"to name the projects instead"
            )
        if reason is None or reason in _ACCESS_DENIED_REASONS:
            return ProbeSoftError(
                f"this credential may not list projects ({needs}); set "
                f"`project_ids` in the recipe to name the projects instead"
            )
        # A 403 for some other reason -- a quota blip, say -- is not something
        # `project_ids` fixes, and an empty list would read as "none exist".
        return exc

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        # lazy: the bigquery client library is only needed once a probe runs
        from google.cloud.bigquery import QueryJobConfig

        timeout = self.query_budget.timeout_seconds
        # A ceiling is passed only when there IS one. QueryJobConfig stringifies
        # whatever it is handed, so `maximum_bytes_billed=None` does not mean
        # "no ceiling": it stores the string 'None', to_api_repr() ships that to
        # BigQuery quite happily, and the getter then raises
        #   ValueError: invalid literal for int() with base 10: 'None'
        # Latent while both ceilings are declared on this class, but
        # QueryBudget.max_bytes_billed defaults to None, so a subclass or a
        # future provider that leaves it unset submits that string. Absent is
        # expressed by omitting the key.
        ceilings: Dict[str, Any] = {}
        if self.query_budget.max_bytes_billed is not None:
            ceilings["maximum_bytes_billed"] = self.query_budget.max_bytes_billed
        if timeout is not None:
            # Server-side, and this is the whole point of it. The budget
            # already declared timeout_seconds=30, but the only thing applying
            # it was `.result(timeout=...)` below -- which bounds how long the
            # CLIENT waits and neither cancels the job nor stops it billing.
            # A ceiling that reads as present and is not is exactly what
            # QueryBudget's docstring warns against, and it is the same defect
            # the Redshift ceiling had: the statement was issued, nothing
            # raised, and nothing was bounded. job_timeout_ms is BigQuery's
            # own cancel-the-job knob, so the declared 30s is now true.
            ceilings["job_timeout_ms"] = timeout * 1000
        job_config = QueryJobConfig(
            use_query_cache=True,
            # Labels are the strongest attribution of the three dialects that
            # offer any: they reach INFORMATION_SCHEMA.JOBS and the billing
            # export, so probe cost is separable from ingestion cost rather
            # than merely tellable apart in a log. BigQuery rejects a label
            # outside [a-z0-9_-], which is why PROBE_QUERY_LABEL is spelled
            # the way it is.
            labels={"application": PROBE_QUERY_LABEL},
            **ceilings,
        )
        # max_results caps what BigQuery pages back, so a broad catalog query does
        # not stream an entire result set to be thrown away. It does NOT cap the
        # bill -- BigQuery charges for bytes scanned whatever the page size -- which
        # is what job_config above is for.
        # Both ceilings, and they are not redundant: job_timeout_ms stops the
        # job, this stops us waiting on a call that is hung for some other
        # reason (a stalled fetch of an already-finished job's pages).
        iterator = self._client.query(query, job_config=job_config).result(
            max_results=limit, timeout=timeout
        )
        columns = [field.name for field in iterator.schema]
        return CatalogRows(
            columns=columns,
            rows=[
                [row[column] for column in columns]
                for row in itertools.islice(iterator, limit)
            ],
        )
