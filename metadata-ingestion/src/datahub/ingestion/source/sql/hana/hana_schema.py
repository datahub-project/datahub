from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class HanaCalcViewColumn:
    """A single column of an activated calculation view."""

    name: str
    data_type: str
    nullable: bool
    ordinal_position: int
    length: Optional[int] = None
    scale: Optional[int] = None
    comment: Optional[str] = None

    def get_precise_native_type(self) -> str:
        """Re-assemble the HANA SQL type spelling, e.g. ``DECIMAL(15,2)``.

        ``SYS.VIEW_COLUMNS`` exposes ``LENGTH`` and ``SCALE`` only for
        numeric/string types; this method mirrors SAP HANA documentation's
        canonical spellings so emitted ``nativeDataType`` matches SAP tooling.
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
    """An activated calculation view from ``_SYS_REPO.ACTIVE_OBJECT``.

    HANA stores calc views under a package path (``acme.analytics``) plus a
    leaf object name (``SalesOverview``). Activation materialises a runtime
    view in ``_SYS_BIC`` whose name is ``<package>/<name>``. We retain both
    forms: runtime for SQL execution, package+name for design-time tooling.
    """

    package_id: str
    name: str
    definition: str
    columns: List[HanaCalcViewColumn] = field(default_factory=list)

    @property
    def runtime_view_name(self) -> str:
        """Name HANA uses for the activated view in ``_SYS_BIC``."""
        return f"{self.package_id}/{self.name}"

    @property
    def qualified_identifier(self) -> str:
        """Dot-separated URN-safe identifier (``_sys_bic.<package>.<name>``)."""
        return f"_sys_bic.{self.package_id}.{self.name}".lower()
