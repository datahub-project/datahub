"""Dataclasses describing SAP HANA objects we extract directly.

Regular tables and views are handled by :class:`SQLAlchemySource`'s reflection
path and never materialise into Python objects here. The dataclasses below
only model the metadata that the SAP HANA calculation-view extractor needs to
generate work units, where SQLAlchemy reflection cannot reach (calc views are
stored as XML payloads in ``_SYS_REPO.ACTIVE_OBJECT``).
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class HanaCalcViewColumn:
    """A single column of an activated SAP HANA calculation view."""

    name: str
    data_type: str
    nullable: bool
    ordinal_position: int
    length: Optional[int] = None
    scale: Optional[int] = None
    comment: Optional[str] = None

    def get_precise_native_type(self) -> str:
        """Re-assemble the HANA SQL type spelling, e.g. ``DECIMAL(15,2)``.

        ``SYS.VIEW_COLUMNS`` returns ``LENGTH`` and ``SCALE`` for numeric and
        string types; for everything else they are ``NULL``. We mirror the
        SAP HANA documentation's canonical spellings here so that the
        ``nativeDataType`` we emit on schema fields matches what SAP tooling
        displays.
        """
        if self.data_type in ("DECIMAL", "NUMERIC", "SMALLDECIMAL"):
            if self.length is not None and self.scale is not None:
                return f"{self.data_type}({self.length},{self.scale})"
        if (
            self.data_type
            in ("VARCHAR", "NVARCHAR", "ALPHANUM", "SHORTTEXT", "VARBINARY")
            and self.length is not None
        ):
            return f"{self.data_type}({self.length})"
        return self.data_type


@dataclass
class HanaCalculationView:
    """An activated SAP HANA calculation view from ``_SYS_REPO.ACTIVE_OBJECT``.

    HANA stores calc views under a package path (``acme.analytics``) and a
    leaf object name (``SalesOverview``). When SAP HANA activates a calc view
    it materialises a SQL-queryable runtime view in the ``_SYS_BIC`` schema,
    whose name combines the package and the object joined by ``/`` (e.g.
    ``acme.analytics/SalesOverview``). We keep both forms here because
    ``_SYS_BIC`` is what SQL queries against the calc view see, while the
    package/name pair is what surfaces in design-time tooling.
    """

    package_id: str
    name: str
    definition: str
    columns: List[HanaCalcViewColumn] = field(default_factory=list)

    @property
    def runtime_view_name(self) -> str:
        """Name SAP HANA uses for the activated view in ``_SYS_BIC``."""
        return f"{self.package_id}/{self.name}"

    @property
    def qualified_identifier(self) -> str:
        """Dot-separated identifier used to build DataHub URNs.

        Mirrors the runtime location (``_SYS_BIC.<package>.<name>``) but
        replaces the ``/`` separator with ``.`` so the resulting URN body is
        URN-safe. Lower-cased to keep parity with the rest of the connector,
        which lower-cases HANA's uppercase identifiers when emitting URNs.
        """
        return f"_sys_bic.{self.package_id}.{self.name}".lower()
