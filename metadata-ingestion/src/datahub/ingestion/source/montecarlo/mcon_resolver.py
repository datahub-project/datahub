import logging
from dataclasses import dataclass
from typing import Dict, Optional, Protocol

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.montecarlo.client import (
    MonteCarloAuthError,
    ResolvedTable,
)
from datahub.ingestion.source.montecarlo.config import (
    MonteCarloPlatformDetail,
    MonteCarloSourceConfig,
)
from datahub.ingestion.source.montecarlo.constants import (
    CONNECTION_TYPE_TO_PLATFORM,
)
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.utilities.ratelimiter import DailyCallBudgetExceeded

logger = logging.getLogger(__name__)


class TableResolverClient(Protocol):
    """The subset of the Monte Carlo client the resolver depends on."""

    def get_table(self, mcon: str) -> Optional[ResolvedTable]: ...


@dataclass(frozen=True)
class ParsedMcon:
    account_id: str
    resource_id: str
    object_type: str
    object_id: str


def parse_mcon(mcon: str) -> Optional[ParsedMcon]:
    """Parse ``MCON++{account}++{resource}++{object_type}++{object_id}``."""
    if not mcon or not mcon.startswith("MCON++"):
        return None
    parts = mcon.split("++")
    if len(parts) != 5:
        return None
    _, account_id, resource_id, object_type, object_id = parts
    return ParsedMcon(
        account_id=account_id,
        resource_id=resource_id,
        object_type=object_type,
        object_id=object_id,
    )


class MconResolver:
    """Resolves Monte Carlo MCONs into DataHub dataset (and field) URNs.

    Each MCON is resolved via ``getTable`` to obtain the full table path and the
    warehouse connection type, which is then mapped to a DataHub platform. Results
    are cached per MCON to avoid repeated API calls.
    """

    def __init__(
        self,
        config: MonteCarloSourceConfig,
        client: TableResolverClient,
        report: MonteCarloSourceReport,
    ) -> None:
        self.config = config
        self.client = client
        self.report = report
        self._cache: Dict[str, Optional[str]] = {}

    def _platform_detail(
        self, resource_id: str, connection_type: Optional[str]
    ) -> Optional[MonteCarloPlatformDetail]:
        mapped = self.config.connection_to_platform_map.get(resource_id)
        if mapped is not None:
            return mapped
        if not self.config.auto_map_connection_types:
            return None
        platform: Optional[str] = None
        if connection_type and self.config.auto_map_connection_types:
            platform = CONNECTION_TYPE_TO_PLATFORM.get(connection_type.lower())
        platform = platform or self.config.default_platform
        if platform is None:
            return None
        return MonteCarloPlatformDetail.model_construct(
            platform=platform,
            # The warehouse dataset URN's platform instance/env must be the
            # warehouse's, not Monte Carlo's own (self.config.platform_instance is
            # Monte Carlo's — leaking it onto warehouse URNs attaches assertions
            # to datasets that do not exist). Warehouses listed in
            # connection_to_platform_map carry their own instance/env per entry;
            # this fallback only covers auto-mapped / default_platform warehouses.
            #
            # model_construct skips the platform field_validator on purpose: the
            # platform here is either from CONNECTION_TYPE_TO_PLATFORM (a trusted
            # hardcoded map, not user input) or from default_platform (already
            # validated at config load). The registry is the only validation
            # source for *user-supplied* platforms; the connector's own internal
            # mappings must not be rejected for being absent from the registry
            # (e.g. spark has no registered connector). env/platform_instance are
            # likewise already validated/normalized at config load.
            platform_instance=self.config.target_platform_instance,
            env=self.config.target_env
            if self.config.target_env is not None
            else self.config.env,
        )

    def dataset_urn_for_mcon(self, mcon: str) -> Optional[str]:
        if mcon in self._cache:
            return self._cache[mcon]
        urn, cached = self._resolve(mcon)
        # Only cache permanent results (success or "table genuinely gone").
        # A transient exception must not be cached — the next monitor sharing
        # this MCON would inherit the stale None instead of retrying getTable.
        if cached:
            self._cache[mcon] = urn
        return urn

    def _resolve(self, mcon: str) -> "tuple[Optional[str], bool]":
        """Resolve an MCON to a dataset URN.

        Returns ``(urn, cached)`` where ``cached`` indicates whether the
        result is permanent (safe to cache) or transient (must not be
        cached so the next call retries). Permanent results: a successful
        URN, or None because the table genuinely doesn't exist or the
        platform is unmapped. Transient results: None because an
        unexpected exception occurred during the getTable call.
        """
        try:
            resolved = self.client.get_table(mcon)
            parsed = parse_mcon(mcon)
            if resolved is None or parsed is None:
                self.report.report_mcon_resolution_failed()
                self.report.warning(
                    title="Unresolvable Monte Carlo asset",
                    message="Could not resolve MCON to a warehouse table; skipping.",
                    context=mcon,
                )
                return None, True

            detail = self._platform_detail(parsed.resource_id, resolved.connection_type)
            if detail is None:
                self.report.mcons_unmapped_platform.append(mcon)
                if self.config.auto_map_connection_types:
                    hint = (
                        "Add it to connection_to_platform_map, set default_platform, "
                        "or enable auto_map_connection_types."
                    )
                else:
                    hint = (
                        "Add it to connection_to_platform_map, set default_platform, "
                        "or enable auto_map_connection_types to auto-resolve from the "
                        "warehouse connection type."
                    )
                self.report.warning(
                    title="Unmapped Monte Carlo warehouse",
                    message="No platform mapping for this warehouse connection type. "
                    + hint,
                    context=f"{mcon} (connection_type={resolved.connection_type})",
                )
                return None, True

            # Match the casing the warehouse source emits so the assertion attaches
            # to the same dataset entity. Casing is controlled at recipe level only:
            # the per-warehouse convert_urns_to_lowercase override (on
            # connection_to_platform_map entries) wins when set — it is the way
            # to preserve case for a case-preserving Snowflake/Redshift deployment.
            # Without it the top-level convert_urns_to_lowercase flag applies
            # (true forces lowercase everywhere; false/unset preserves case).
            # Monte Carlo's full_table_id uses its own "database:schema.table" form;
            # DataHub dataset URNs use dot-separated "database.schema.table" for
            # warehouse platforms, so the first colon must become a dot for the
            # assertion to attach to the same dataset entity the warehouse source
            # emitted rather than a phantom lookalike. Validate the result has
            # exactly three segments so a malformed id cannot produce a valid-looking
            # URN for a dataset that does not exist.
            table_id = resolved.full_table_id.replace(":", ".", 1)
            if len(table_id.split(".")) != 3:
                self.report.report_mcon_resolution_failed()
                self.report.warning(
                    title="Malformed Monte Carlo table id",
                    message="Monte Carlo full_table_id does not resolve to "
                    "'database.schema.table' (expected 3 dot-separated segments); "
                    "cannot build a matching warehouse dataset URN. Skipping.",
                    context=f"{mcon} (full_table_id={resolved.full_table_id})",
                )
                return None, True
            lowercase = (
                detail.convert_urns_to_lowercase
                if detail.convert_urns_to_lowercase is not None
                else self.config.convert_urns_to_lowercase
            )
            if lowercase:
                table_id = table_id.lower()
            self.report.report_mcon_resolved()
            return make_dataset_urn_with_platform_instance(
                platform=detail.platform,
                name=table_id,
                platform_instance=detail.platform_instance,
                env=detail.env,
            ), True
        except (DailyCallBudgetExceeded, MonteCarloAuthError):
            # Run-level failures (quota exhausted, bad credentials) that fail for
            # every MCON alike — propagate to abort rather than logging one
            # warning per asset and reporting a misleading empty success.
            raise
        except Exception as e:
            # Transient failure (network blip, transient API error): do NOT
            # cache — the next monitor sharing this MCON should retry getTable
            # instead of inheriting the stale None. Record as a build failure so
            # the partial-run guard in source.py trips the soft-delete interlock.
            self.report.report_build_failure()
            self.report.warning(
                title="Error resolving Monte Carlo asset",
                message="Failed to resolve MCON to a dataset URN; skipping.",
                context=mcon,
                exc=e,
            )
            return None, False
