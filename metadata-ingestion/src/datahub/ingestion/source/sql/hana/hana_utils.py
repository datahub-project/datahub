"""URN / identifier helpers for the SAP HANA source."""

from typing import Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.sql.hana.hana_config import HanaConfig
from datahub.ingestion.source.sql.hana.hana_schema import HanaCalculationView


class HanaIdentifierBuilder:
    """Centralised URN construction for SAP HANA datasets.

    The :class:`SQLAlchemySource` base class already handles URN construction
    for regular tables and views via its ``get_identifier`` hook, so this
    helper is intentionally narrow: it only generates URNs for objects the
    base class does not know about (calculation views) and for upstream
    references that the calculation-view XML parser surfaces.
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
        source_type: str,
        source_name: str,
        source_path: Optional[str],
    ) -> Optional[str]:
        """Build a dataset URN for an upstream referenced from a calc view.

        SAP HANA records calculation-view inputs in several flavours; we
        translate each into a stable DataHub identifier:

        - ``DATA_BASE_TABLE``: a regular table or view. ``source_path`` carries
          the schema, ``source_name`` carries the object. Identifier:
          ``<schema>.<table>`` (matches the URN shape this connector emits for
          tables through :class:`SQLAlchemySource`).
        - ``CALCULATION_VIEW``: another calc view. ``source_path`` holds the
          package path and ``source_name`` the leaf name. Identifier:
          ``_sys_bic.<package>.<name>``.
        - ``TABLE_FUNCTION``: a SAP HANA table function exposed in
          ``_SYS_BIC``. Identifier: ``_sys_bic.<source>`` with any
          ``::``-prefixed namespace flattened to a dot path.

        Returns ``None`` if we cannot construct a sensible identifier (e.g.
        an unknown source type or an empty source name), so the caller can
        skip emitting partial lineage rather than producing junk URNs.
        """
        if not source_name:
            return None

        if source_type == "DATA_BASE_TABLE":
            if not source_path:
                return None
            identifier = f"{source_path}.{source_name}".lower()
            return self._dataset_urn(identifier)

        if source_type == "TABLE_FUNCTION":
            # SAP HANA returns table-function references either as the bare
            # function name or as a ``namespace::function`` pair. The latter
            # is package-scoped and maps to ``_SYS_BIC.<namespace>.<function>``.
            normalised = source_name.replace("::", ".").lstrip("/")
            identifier = f"_sys_bic.{normalised}".lower()
            return self._dataset_urn(identifier)

        if source_type == "CALCULATION_VIEW":
            if not source_path:
                return None
            identifier = f"_sys_bic.{source_path}.{source_name}".lower()
            return self._dataset_urn(identifier)

        return None
