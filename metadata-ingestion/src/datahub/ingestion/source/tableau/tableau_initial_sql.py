import io
import logging
import zipfile
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Optional
from xml.etree.ElementTree import Element

# defusedxml hardens against XXE / billion-laughs; .tds content is third-party authored.
from defusedxml.ElementTree import fromstring

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InitialSqlConnection:
    """A connection in a Tableau definition that declares Initial SQL."""

    connection_type: str
    database: Optional[str]
    schema: Optional[str]
    server: Optional[str]
    initial_sql: str

    def __post_init__(self) -> None:
        # initial_sql is the whole reason this object exists, so guarantee it is
        # present and normalized here rather than re-checking at every call site.
        stripped = self.initial_sql.strip() if self.initial_sql else ""
        if not stripped:
            raise ValueError("initial_sql must be a non-empty statement")
        # frozen=True forbids normal assignment; normalize via object.__setattr__.
        object.__setattr__(self, "initial_sql", stripped)


def extract_definition_bytes(downloaded: bytes) -> Optional[bytes]:
    """Return the Tableau definition XML bytes (.tds or .twb) from a downloaded
    .tds / .tdsx / .twb / .twbx payload.

    .tdsx and .twbx are zip archives (magic bytes "PK"); a bare .tds/.twb is raw XML.
    A .twbx carries a .twb, a .tdsx carries a .tds; prefer the .twb when both somehow
    appear so workbook callers get the workbook definition.
    """
    if downloaded[:2] == b"PK":
        try:
            with zipfile.ZipFile(io.BytesIO(downloaded)) as archive:
                names = archive.namelist()
                for ext in (".twb", ".tds"):
                    for name in names:
                        if name.endswith(ext):
                            return archive.read(name)
            return None
        except zipfile.BadZipFile as e:
            logger.warning(f"Payload starts with 'PK' but is not a valid zip: {e}")
            return None
    return downloaded


# Backwards-compatible alias: the published-datasource path downloads .tds/.tdsx.
extract_tds_bytes = extract_definition_bytes


def _connections_from_element(element: Element) -> List[InitialSqlConnection]:
    """Collect Initial SQL connections from all `<connection>` descendants of `element`.

    Initial SQL is stored as the `one-time-sql` attribute on each
    `<connection class='...'>` element (nested under `<named-connection>`). The outer
    `<connection class='federated'>` wrapper has no `one-time-sql` and is skipped by the
    empty-value guard. Shared by the `.tds` (whole-document) and `.twb` (per-datasource)
    parsers so the connection-attribute mapping lives in exactly one place.
    """
    connections: List[InitialSqlConnection] = []
    # TDS/TWB <connection> elements are not namespaced.
    for conn in element.iter("connection"):
        initial_sql = conn.get("one-time-sql")
        # Skip missing or whitespace-only Initial SQL (InitialSqlConnection strips
        # and rejects blanks, so guarding here keeps it to "no Initial SQL = skip").
        if not initial_sql or not initial_sql.strip():
            continue
        connections.append(
            InitialSqlConnection(
                connection_type=conn.get("class", ""),
                database=conn.get("dbname"),
                schema=conn.get("schema"),
                server=conn.get("server"),
                initial_sql=initial_sql,
            )
        )
    return connections


def extract_initial_sql_connections(tds_xml: bytes) -> List[InitialSqlConnection]:
    """Parse a Tableau `.tds` definition and return connections that declare Initial SQL.

    A `.tds` describes a single datasource, so every `<connection>` in the document
    belongs to it.
    """
    try:
        root = fromstring(tds_xml)
    except Exception as e:
        logger.warning(f"Failed to parse TDS XML for Initial SQL: {e}")
        return []
    return _connections_from_element(root)


def extract_initial_sql_by_datasource(
    twb_xml: bytes,
) -> Dict[str, List[InitialSqlConnection]]:
    """Map each embedded datasource (by caption) to its Initial SQL connections.

    Only the workbook's TOP-LEVEL ``<datasources>`` block is considered. A ``.twb``
    embeds many *reference* ``<datasource>`` copies inside worksheet/dashboard XML;
    those are not datasource definitions and must be ignored. Empirically the
    top-level ``<datasource caption=...>`` equals the Tableau Metadata API
    embedded-datasource ``name`` (the connector's match key).

    Keys are every uniquely-captioned top-level datasource (value may be an empty
    list when it declares no Initial SQL). A caption shared by more than one
    top-level datasource is ambiguous and omitted, so callers skip it rather than
    mis-attribute. Captionless pseudo-datasources (e.g. ``Parameters``) are omitted.
    """
    result: Dict[str, List[InitialSqlConnection]] = {}
    try:
        root = fromstring(twb_xml)
    except Exception as e:
        logger.warning(f"Failed to parse workbook XML for Initial SQL: {e}")
        return result

    block = root.find("datasources")
    if block is None:
        return result

    datasources = block.findall("datasource")
    caption_counts = Counter(
        ds.get("caption") for ds in datasources if ds.get("caption")
    )
    ambiguous = {c for c, n in caption_counts.items() if n > 1}

    for ds in datasources:
        caption = ds.get("caption")
        if not caption or caption in ambiguous:
            continue
        result[caption] = _connections_from_element(ds)
    return result
