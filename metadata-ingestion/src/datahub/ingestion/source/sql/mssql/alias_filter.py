import logging
import re
from typing import Callable, Iterable, Iterator, List, Optional, Set

from datahub.emitter.mce_builder import dataset_urn_to_key
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.urns.error import InvalidUrnError

logger = logging.getLogger(__name__)

# MSSQL requires 3-part naming: database.schema.table
MSSQL_QUALIFIED_NAME_PARTS = 3


class MSSQLAliasFilter:
    """
    Filters TSQL aliases from stored procedure lineage.

    TSQL allows aliases in UPDATE/DELETE statements that confuse SQL parsers.
    This class implements a 3-step filtering process to remove spurious aliases
    while preserving real table lineage.

    See module docstring for detailed explanation of the TSQL alias problem.
    """

    def __init__(
        self,
        is_discovered_table: Callable[[str], bool],
        platform_instance: Optional[str] = None,
    ):
        """Initialize the alias filter with discovered table checker and optional platform instance."""
        self.is_discovered_table = is_discovered_table
        self.platform_instance = platform_instance

    def _is_qualified_table_urn(self, urn: str) -> bool:
        """
        Check if a table URN represents a fully qualified table name.

        MSSQL uses 3-part naming: database.schema.table.
        Helps identify real tables vs. unqualified aliases (e.g., 'dst' in TSQL UPDATE statements).
        """
        try:
            dataset_urn = DatasetUrn.from_string(urn)
            name = dataset_urn.name

            if self.platform_instance and name.startswith(f"{self.platform_instance}."):
                name = name[len(self.platform_instance) + 1 :]

            # Check if name has at least 3 parts (database.schema.table)
            return len(name.split(".")) >= MSSQL_QUALIFIED_NAME_PARTS
        except Exception:
            return False

    def _filter_upstream_aliases(self, upstream_urns: List[str]) -> List[str]:
        """
        Filter spurious TSQL aliases from upstream lineage using is_discovered_table().

        TSQL syntax like "UPDATE dst FROM table dst" causes the parser to extract
        both 'dst' (alias) and 'table' (real table). Uses is_discovered_table() to identify real tables:
        - Tables in schema_resolver or discovered_datasets: Real tables (keep)
        - Undiscovered tables: Likely aliases (filter)
        """
        if not upstream_urns:
            return []

        verified_real_tables = []

        for urn in upstream_urns:
            try:
                dataset_urn = DatasetUrn.from_string(urn)
                table_name = dataset_urn.name

                # Strip platform_instance prefix from URN to match internal tracking format
                if self.platform_instance and table_name.startswith(
                    f"{self.platform_instance}."
                ):
                    table_name = table_name[len(self.platform_instance) + 1 :]

                if self.is_discovered_table(table_name):
                    verified_real_tables.append(urn)
            except (InvalidUrnError, ValueError) as e:
                # Keep unparseable URNs to avoid data loss; downstream will validate
                logger.warning("Error parsing URN %s: %s", urn, e)
                verified_real_tables.append(urn)

        return verified_real_tables

    def _remap_column_lineage_for_alias(
        self,
        table_urn: str,
        column_name: str,
        aspect: DataJobInputOutputClass,
        procedure_name: Optional[str],
    ) -> List[str]:
        """
        Remap column lineage from filtered alias to real table(s).

        TSQL allows aliases in UPDATE/DELETE (e.g., `UPDATE dst SET col=val FROM real_table dst`).
        Matches by table name since sqlglot may parse incorrect qualifiers.
        Returns list of remapped field URNs.
        """
        remapped_urns: List[str] = []

        try:
            alias_key = dataset_urn_to_key(table_urn)
            if not alias_key:
                logger.warning(
                    "Could not parse alias URN %s for column remapping in %s",
                    table_urn,
                    procedure_name,
                )
                return remapped_urns

            alias_table_name = alias_key.name.split(".")[-1]

            # Find real tables with matching table name
            matching_tables = []
            if aspect.outputDatasets:
                for real_table_urn in aspect.outputDatasets:
                    real_key = dataset_urn_to_key(real_table_urn)
                    if real_key:
                        real_table_name = real_key.name.split(".")[-1]
                        if real_table_name == alias_table_name:
                            matching_tables.append(real_table_urn)

            # Remap based on number of matches
            if len(matching_tables) == 1:
                remapped_urn = (
                    f"urn:li:schemaField:({matching_tables[0]},{column_name})"
                )
                remapped_urns.append(remapped_urn)
            elif len(matching_tables) > 1:
                # Multiple matches - remap to all
                for real_table_urn in matching_tables:
                    remapped_urn = (
                        f"urn:li:schemaField:({real_table_urn},{column_name})"
                    )
                    remapped_urns.append(remapped_urn)
                logger.warning(
                    "Multiple tables match alias %s in %s, "
                    "remapped column %s to all %d matches",
                    alias_table_name,
                    procedure_name,
                    column_name,
                    len(matching_tables),
                )
            else:
                # No table name match - fallback to all real tables
                if aspect.outputDatasets:
                    for real_table_urn in aspect.outputDatasets:
                        remapped_urn = (
                            f"urn:li:schemaField:({real_table_urn},{column_name})"
                        )
                        remapped_urns.append(remapped_urn)
                    logger.warning(
                        "No table name match for alias %s in %s, "
                        "remapped column %s to all %d real tables",
                        alias_table_name,
                        procedure_name,
                        column_name,
                        len(aspect.outputDatasets),
                    )
        except Exception as e:
            logger.warning(
                "Error parsing alias URN %s for column remapping in %s: %s",
                table_urn,
                procedure_name,
                e,
            )

        if not remapped_urns:
            logger.warning(
                "Could not remap column lineage for filtered alias %s.%s in %s - "
                "no real downstream tables available",
                table_urn,
                column_name,
                procedure_name,
            )

        return remapped_urns

    def _filter_downstream_fields(
        self,
        cll_index: int,
        downstream_fields: List[str],
        filtered_downstream_aliases: Optional[Set[str]],
        field_urn_pattern: re.Pattern,
        aspect: DataJobInputOutputClass,
        procedure_name: Optional[str],
    ) -> List[str]:
        """Filter and remap downstream fields in a column lineage entry."""
        filtered_downstreams = []
        for field_urn in downstream_fields:
            match = field_urn_pattern.search(field_urn)
            if match:
                table_urn = match.group(1)
                column_name = match.group(2)

                if (
                    filtered_downstream_aliases
                    and table_urn in filtered_downstream_aliases
                ):
                    remapped_urns = self._remap_column_lineage_for_alias(
                        table_urn, column_name, aspect, procedure_name
                    )
                    filtered_downstreams.extend(remapped_urns)
                elif self._is_qualified_table_urn(table_urn):
                    filtered_downstreams.append(field_urn)
                else:
                    logger.debug(
                        "Filtered unqualified downstream field in column lineage for %s: %s",
                        procedure_name,
                        field_urn,
                    )
            else:
                filtered_downstreams.append(field_urn)
        return filtered_downstreams

    def _filter_column_lineage(
        self,
        aspect: DataJobInputOutputClass,
        procedure_name: Optional[str],
        filtered_downstream_aliases: Optional[Set[str]] = None,
    ) -> None:
        """
        Filter column lineage (fineGrainedLineages) to remove aliases.

        Applies same 2-step filtering as table-level lineage:
        1. Check if table has 3+ parts (_is_qualified_table_urn)
        2. Check if table is real vs alias (_filter_upstream_aliases for upstreams)
        3. Remap column lineages from filtered downstream aliases to real tables

        Modifies aspect.fineGrainedLineages in place.
        """
        if not aspect.fineGrainedLineages:
            return

        filtered_column_lineage = []
        field_urn_pattern = re.compile(r"urn:li:schemaField:\((.*),(.*)\)")

        for cll_index, cll in enumerate(aspect.fineGrainedLineages, 1):
            if cll.upstreams:
                # Step 1: Filter by qualification (3+ parts)
                qualified_upstream_fields = []
                for field_urn in cll.upstreams:
                    match = field_urn_pattern.search(field_urn)
                    if match:
                        table_urn = match.group(1)
                        if self._is_qualified_table_urn(table_urn):
                            qualified_upstream_fields.append(field_urn)
                        else:
                            logger.debug(
                                "Filtered unqualified upstream field in column lineage for %s: %s",
                                procedure_name,
                                field_urn,
                            )
                    else:
                        qualified_upstream_fields.append(field_urn)

                # Step 2: Filter aliases (extract table URNs and check)
                upstream_table_urns = []
                field_to_table_map = {}
                for field_urn in qualified_upstream_fields:
                    match = field_urn_pattern.search(field_urn)
                    if match:
                        table_urn = match.group(1)
                        upstream_table_urns.append(table_urn)
                        field_to_table_map[field_urn] = table_urn

                # Apply alias filtering to table URNs
                real_table_urns = set(
                    self._filter_upstream_aliases(upstream_table_urns)
                )

                # Keep only field URNs whose tables passed the filter
                filtered_upstreams = [
                    field_urn
                    for field_urn in qualified_upstream_fields
                    if field_to_table_map.get(field_urn) in real_table_urns
                    or field_urn not in field_to_table_map
                ]

                # Log filtered aliases
                for field_urn in qualified_upstream_fields:
                    if field_urn not in filtered_upstreams:
                        table_urn = field_to_table_map.get(field_urn, "unknown")
                        logger.debug(
                            "Filtered alias upstream field in column lineage for %s: %s (table: %s)",
                            procedure_name,
                            field_urn,
                            table_urn,
                        )

                cll.upstreams = filtered_upstreams

            if cll.downstreams:
                cll.downstreams = self._filter_downstream_fields(
                    cll_index,
                    cll.downstreams,
                    filtered_downstream_aliases,
                    field_urn_pattern,
                    aspect,
                    procedure_name,
                )

            # Only keep column lineage if it has both upstreams and downstreams
            if cll.upstreams and cll.downstreams:
                filtered_column_lineage.append(cll)

        aspect.fineGrainedLineages = (
            filtered_column_lineage if filtered_column_lineage else None
        )

    def filter_procedure_lineage(
        self,
        mcps: Iterable[MetadataChangeProposalWrapper],
        procedure_name: Optional[str] = None,
    ) -> Iterator[MetadataChangeProposalWrapper]:
        """
        Filter out unqualified table URNs from stored procedure lineage.

        TSQL syntax like "UPDATE dst FROM table dst" causes sqlglot to extract
        both 'dst' (alias) and 'table' (real table). Unqualified aliases create
        invalid URNs that cause DataHub sink to reject the entire aspect.

        Removes URNs with < 3 parts (database.schema.table).
        """
        for mcp in mcps:
            # Only filter dataJobInputOutput aspects
            if mcp.aspect and isinstance(mcp.aspect, DataJobInputOutputClass):
                aspect: DataJobInputOutputClass = mcp.aspect

                # Filter inputs: first unqualified tables, then aliases
                if aspect.inputDatasets:
                    qualified_inputs = [
                        urn
                        for urn in aspect.inputDatasets
                        if self._is_qualified_table_urn(urn)
                    ]
                    aspect.inputDatasets = self._filter_upstream_aliases(
                        qualified_inputs
                    )

                # Track filtered downstream aliases for column lineage remapping
                filtered_downstream_aliases: Set[str] = set()
                if aspect.outputDatasets:
                    original_outputs = aspect.outputDatasets.copy()
                    aspect.outputDatasets = [
                        urn
                        for urn in aspect.outputDatasets
                        if self._is_qualified_table_urn(urn)
                    ]
                    filtered_downstream_aliases = set(original_outputs) - set(
                        aspect.outputDatasets
                    )

                self._filter_column_lineage(
                    aspect,
                    procedure_name,
                    filtered_downstream_aliases,
                )

                # Skip aspect only if BOTH inputs and outputs are empty
                if not aspect.inputDatasets and not aspect.outputDatasets:
                    logger.warning(
                        "Skipping lineage for %s: all tables were filtered",
                        procedure_name,
                    )
                    continue

            yield mcp
