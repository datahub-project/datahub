from typing import Optional

from typing_extensions import assert_never

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.sql.hana.constants import (
    SYS_BIC_SCHEMA,
    HanaSourceType,
)
from datahub.ingestion.source.sql.hana.hana_config import HanaConfig
from datahub.ingestion.source.sql.hana.hana_schema import HanaCalculationView


class HanaIdentifierBuilder:
    """Centralised URN construction for SAP HANA-specific objects.

    :class:`SQLAlchemySource` already handles regular tables and views via
    ``get_identifier``; this helper only covers calculation views and the
    upstream source shapes surfaced by the XML parser.
    """

    PLATFORM = "hana"

    def __init__(self, config: HanaConfig):
        self.config = config

    def _dataset_urn(self, identifier: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.PLATFORM,
            name=identifier,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def calc_view_urn(self, calc_view: HanaCalculationView) -> str:
        """URN for an activated calculation view (``_SYS_BIC.<package>.<name>``)."""
        return self._dataset_urn(calc_view.qualified_identifier)

    def upstream_urn_for_calc_view_source(
        self,
        *,
        source_type: HanaSourceType,
        source_name: str,
        source_path: Optional[str],
    ) -> Optional[str]:
        """Build a dataset URN for an upstream referenced from a calc view.

        Returns ``None`` for missing source names or source-types that
        don't carry enough metadata (e.g. a calculation view without a
        package path) so the caller can skip partial lineage.
        """
        if not source_name:
            return None

        sys_bic = SYS_BIC_SCHEMA.lower()

        if source_type is HanaSourceType.DATA_BASE_TABLE:
            if not source_path:
                return None
            return self._dataset_urn(f"{source_path}.{source_name}".lower())

        if source_type is HanaSourceType.TABLE_FUNCTION:
            # Table-function refs come as ``namespace::function`` (package
            # scoped) or as a bare function name; both live under _SYS_BIC.
            normalised = source_name.replace("::", ".").lstrip("/")
            return self._dataset_urn(f"{sys_bic}.{normalised}".lower())

        if source_type is HanaSourceType.CALCULATION_VIEW:
            if not source_path:
                return None
            return self._dataset_urn(f"{sys_bic}.{source_path}.{source_name}".lower())

        assert_never(source_type)
