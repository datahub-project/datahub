"""
Semantic Model Builder for text-to-SQL generation.

Builds semantic models from DataHub metadata using a layered approach:
- Layer 1: Base Schema - table names, columns, types, descriptions
- Layer 2: Column Classification - classify columns as dimensions/facts/time_dimensions
- Layer 3: Query History - fetch sample queries
- Layer 4: JOIN Extraction - parse historical SQL for JOIN patterns
- Layer 5: PK Inference - infer primary keys via LLM when not in metadata
- Layer 6: Relationship Inference - determine relationship types via LLM

Ported and adapted from datahub-semantic-agent/src/datahub_semantic_agent/generate.py
"""

import collections
import contextlib
import dataclasses
import functools
import logging
from collections import defaultdict
from typing import DefaultDict, Dict, List, Literal, Optional, TypeAlias

import datahub.metadata.schema_classes as models

with contextlib.suppress(ImportError):
    import acryl_datahub_cloud.metadata.schema_classes as models  # type: ignore[no-redef]

import pydantic
from datahub.metadata.urns import DatasetUrn, QueryUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.main_client import DataHubClient
from datahub.sql_parsing.sqlglot_lineage import JoinInfo
from datahub.utilities.ordered_set import OrderedSet
from loguru import logger

from datahub_integrations.chat.sql_generator.models import (
    BaseTable,
    Dimension,
    Fact,
    LogicalTable,
    PrimaryKey,
    Relationship,
    RelationshipColumn,
    SemanticModel,
    TimeDimension,
)
from datahub_integrations.gen_ai.llm.factory import get_llm_client
from datahub_integrations.gen_ai.model_config import model_config
from datahub_integrations.mcp_integration.tool import ToolWrapper

# Suppress noisy sqlglot logs
logging.getLogger("sqlglot").setLevel(logging.ERROR)

# Constants for query history fetching
MAX_QUERY_URNS_PER_TABLE = 10
MAX_TOKENS_EXAMPLE_SQL = 15000

# GraphQL query for listing queries associated with a dataset
_LIST_QUERIES_GQL = """\
query listQueries($input: ListQueriesInput!) {
  listQueries(input: $input) {
    start
    total
    count
    queries {
      urn
      properties {
        name
        description
        source
        statement {
          value
          language
        }
      }
      subjects {
        dataset {
          urn
        }
        schemaField {
          urn
        }
      }
    }
  }
}
"""

# LLM prompts
_PK_INFERENCE_SYSTEM_PROMPT = """\
You are an assistant that's an expert in data modeling and SQL, \
and are working to understand a customer environment."""

_PK_INFERENCE_USER_PROMPT = """\
Given the following information about a logical table, determine the primary key column(s).
There will typically be one primary key column, but there may be multiple if the table has a composite primary key. There will always be at least one primary key column.
Columns that are foreign keys to other tables are usually not part of the primary key. In other words, we're looking for the columns that uniquely identify each row in the table - the grain of the table.
The primary key column name(s) must be valid column names in the logical table.

This information will be used to ensure that the table is used correctly in SQL queries.

<logical_table>
{logical_table}
</logical_table>

When unsure, pick the most likely candidates. Respond with only the column names, and no other text."""

_RELATIONSHIP_INFERENCE_SYSTEM_PROMPT = """\
You are an assistant that's an expert in data modeling and SQL, \
and are working to understand a customer environment."""

_RELATIONSHIP_INFERENCE_USER_PROMPT = """\
Given the following two tables, your goal is to understand the data modeling relationship between them.
You'll be given a number of example SQL statements that join the two tables.

The extracted relationship information will serve as documentation for how other users should join the two tables.

The left and right table names should correspond to table names in the logical_tables section.
The left/right columns should also reference column names in the respective logical tables.
For many-to-one relationships, the left table should be the many side of the relationship. For one-to-one relationships, the left table should be the first table listed.
Be sure to include any additional usage notes in the description.

<logical_tables>
{tables}
</logical_tables>

<example_sqls>
{example_sqls}
</example_sqls>
"""


@dataclasses.dataclass
class SourcedJoinInfo:
    """
    A JOIN discovered in a query, with source query information.

    Attributes:
        query_urn: URN of the query containing this JOIN.
        query_text: The SQL text of the query.
        join: The parsed JOIN information.
    """

    query_urn: QueryUrn
    query_text: str
    join: JoinInfo


# Type alias for a pair of joined tables
JoinedTables: TypeAlias = frozenset[DatasetUrn]


# =============================================================================
# LLM Tool Definitions (using FastMCP for schema generation)
# =============================================================================


@dataclasses.dataclass
class _RelationshipColumnSpec:
    """Column pair used in a join condition."""

    left_column: str
    right_column: str


def _infer_primary_keys(columns: List[str]) -> None:
    """Returns the inferred primary key column names for a table.

    Args:
        columns: List of column names that form the primary key.
    """
    pass  # Placeholder - LLM provides the response


def _infer_relationship(
    description: str,
    relationship_type: Literal["many_to_one", "one_to_one"],
    join_type: Literal["left_outer", "inner"],
    left_table: str,
    right_table: str,
    relationship_columns: List[_RelationshipColumnSpec],
) -> None:
    """Returns the inferred relationship between two tables.

    Args:
        description: Description of the relationship.
        relationship_type: Type of relationship (many_to_one or one_to_one).
        join_type: Type of SQL join to use (left_outer or inner).
        left_table: Name of the left table in the join.
        right_table: Name of the right table in the join.
        relationship_columns: Column pairs used in the join condition.
    """
    pass  # Placeholder - LLM provides the response


# Generate tool specs using FastMCP (single source of truth from function signatures)
_pk_inference_tool = ToolWrapper.from_function(
    _infer_primary_keys,
    name="infer_primary_keys",
    description=_infer_primary_keys.__doc__ or "",
)

_relationship_inference_tool = ToolWrapper.from_function(
    _infer_relationship,
    name="infer_relationship",
    description=_infer_relationship.__doc__ or "",
)


def _extract_tool_response(response: dict) -> dict:
    """
    Extract the structured response from an LLM tool use response.

    Args:
        response: The full LLM response from converse()

    Returns:
        The input dict from the tool use block

    Raises:
        ValueError: If no tool use block is found
    """
    output = response.get("output", {})
    message = output.get("message", {})
    content = message.get("content", [])

    for block in content:
        if "toolUse" in block:
            return block["toolUse"].get("input", {})

    raise ValueError("No tool use block found in LLM response")


class SemanticModelBuilder:
    """
    Builds semantic models from DataHub metadata.

    Uses a layered approach to progressively enrich the semantic model:
    - Layers 1-2: Schema extraction and column classification (deterministic)
    - Layer 3: Query history fetching
    - Layer 4: JOIN extraction from SQL parsing
    - Layers 5-6: LLM-based inference for PKs and relationships
    """

    def __init__(
        self,
        client: DataHubClient,
        max_tables: int = 30,
        start_urns: Optional[List[DatasetUrn]] = None,
    ) -> None:
        """
        Initialize the semantic model builder.

        Args:
            client: DataHub client for fetching metadata.
            max_tables: Maximum number of tables to include in the model.
            start_urns: Initial table URNs to include in the model.
        """
        self.client = client
        self.max_tables = max_tables

        self._registered_tables: Dict[DatasetUrn, LogicalTable] = {}
        self._joins: DefaultDict[JoinedTables, List[SourcedJoinInfo]] = defaultdict(
            list
        )

        # Priority queue for candidate tables (URN -> popularity score)
        self._candidate_tables: Dict[DatasetUrn, int] = {}

        if start_urns:
            for urn in start_urns:
                self.request_table(urn)

    # =========================================================================
    # Layer 1-2: Schema Extraction and Column Classification
    # =========================================================================

    @functools.cache  # noqa: B019
    def build_logical_table(self, urn: DatasetUrn) -> LogicalTable:
        """
        Build a logical table from DataHub metadata.

        Layers 1-2: Extracts schema and classifies columns as dimensions, facts,
        or time dimensions based on their data types.

        Args:
            urn: The dataset URN to build a logical table for.

        Returns:
            A LogicalTable with classified columns.
        """
        logger.debug("Building logical table for {}", urn)
        dataset: Dataset = self.client.entities.get(urn)

        # Get qualified name, falling back to URN parsing if not available
        qualified_name = dataset.qualified_name
        if qualified_name is None:
            # Fall back to parsing the URN (e.g., for dbt datasets)
            # URN format: urn:li:dataset:(urn:li:dataPlatform:platform,name,env)
            urn_name = urn.name
            qualified_name = urn_name
            platform = urn.get_data_platform_urn().platform_name
            logger.bind(platform=platform, urn=str(urn)).info(
                "No qualified_name found, using URN name: {}",
                qualified_name,
            )

        name_parts = qualified_name.split(".", maxsplit=2)

        # Handle cases where qualified_name may have fewer parts
        if len(name_parts) < 3:
            # Pad with empty strings or use defaults
            while len(name_parts) < 3:
                name_parts.insert(0, "")

        # Extract primary keys from schema
        pks: List[str] = []
        for column in dataset.schema:
            if column._base_schema_field().isPartOfKey:
                pks.append(column.field_path)

        # Classify columns by type
        time_dimensions: List[TimeDimension] = []
        dimensions: List[Dimension] = []
        facts: List[Fact] = []

        for column in dataset.schema:
            parsed_column_type = column.mapped_type.type

            if isinstance(
                parsed_column_type, (models.TimeTypeClass, models.DateTypeClass)
            ):
                time_dimensions.append(
                    TimeDimension(
                        name=column.field_path,
                        expr=column.field_path,
                        data_type=column.native_type,
                        description=column.description,
                    )
                )
            elif isinstance(parsed_column_type, models.NumberTypeClass):
                facts.append(
                    Fact(
                        name=column.field_path,
                        expr=column.field_path,
                        data_type=column.native_type,
                        description=column.description,
                    )
                )
            else:
                # Default to dimension for string, boolean, enum, etc.
                dimensions.append(
                    Dimension(
                        name=column.field_path,
                        expr=column.field_path,
                        data_type=column.native_type,
                        description=column.description,
                        is_enum=isinstance(
                            parsed_column_type,
                            (models.EnumTypeClass, models.BooleanTypeClass),
                        ),
                    )
                )

        return LogicalTable(
            name=dataset.display_name or dataset.urn.name,
            description=dataset.description,
            base_table=BaseTable(
                database=name_parts[0],
                schema_name=name_parts[1],
                table=name_parts[2],
            ),
            primary_key=PrimaryKey(columns=pks) if pks else None,
            dimensions=dimensions if dimensions else None,
            time_dimensions=time_dimensions if time_dimensions else None,
            facts=facts if facts else None,
        )

    # =========================================================================
    # Layer 3: Query History Fetching
    # =========================================================================

    def _get_query_urns(self, urn: DatasetUrn) -> List[str]:
        """
        Fetch query URNs associated with a dataset.

        Layer 3: Retrieves historical queries that reference this table,
        prioritizing queries that involve multiple tables (for JOIN discovery).

        Args:
            urn: The dataset URN to find queries for.

        Returns:
            List of query URNs (up to MAX_QUERY_URNS_PER_TABLE).
        """
        page_size = 100

        try:
            queries = self.client._graph.execute_graphql(
                _LIST_QUERIES_GQL,
                {
                    "input": {
                        "start": 0,
                        "count": page_size,
                        "source": "SYSTEM",
                        "sortInput": {
                            "sortCriterion": {
                                "field": "runsPercentileLast30days",
                                "sortOrder": "DESCENDING",
                            }
                        },
                        "orFilters": [
                            {
                                "and": [
                                    {
                                        "field": "entities",
                                        "values": [str(urn)],
                                    }
                                ]
                            }
                        ],
                    }
                },
            )
        except Exception as e:
            logger.warning("Failed to fetch queries for {}: {}", urn, e)
            return []

        # Filter for queries that involve multiple tables (for JOIN discovery)
        query_urns = []
        for entry in queries.get("listQueries", {}).get("queries", []):
            dataset_urns = [
                subject["dataset"]["urn"]
                for subject in entry.get("subjects", [])
                if subject.get("dataset") and subject.get("schemaField") is None
            ]
            if len(dataset_urns) >= 2:
                query_urns.append(entry["urn"])
            if len(query_urns) >= MAX_QUERY_URNS_PER_TABLE:
                break

        return query_urns

    # =========================================================================
    # Layer 4: JOIN Extraction from SQL Parsing
    # =========================================================================

    def _infer_joins_from_sql(
        self, query_urn: QueryUrn, default_db: str, default_schema: str
    ) -> Optional[List[SourcedJoinInfo]]:
        """
        Extract JOIN information from a query using SQL parsing.

        Layer 4: Parses the SQL statement to identify JOIN patterns.

        Args:
            query_urn: URN of the query to parse.
            default_db: Default database for unqualified table names.
            default_schema: Default schema for unqualified table names.

        Returns:
            List of SourcedJoinInfo, or None if parsing fails.
        """
        try:
            query = self.client._graph.get_entity_semityped(str(query_urn))
        except Exception as e:
            logger.warning("Failed to fetch query {}: {}", query_urn, e)
            return None

        query_properties = query.get("queryProperties")
        data_platform_instance = query.get("dataPlatformInstance")
        if query_properties is None or data_platform_instance is None:
            return None

        try:
            lineage = self.client._graph.parse_sql_lineage(
                sql=query_properties.statement.value,
                platform=data_platform_instance.platform,
                platform_instance=data_platform_instance.instance,
                default_db=default_db,
                default_schema=default_schema,
            )
        except Exception as e:
            logger.warning("Failed to parse SQL for {}: {}", query_urn, e)
            return None

        if lineage.joins is None:
            return None

        return [
            SourcedJoinInfo(
                query_urn=query_urn,
                query_text=query_properties.statement.value,
                join=join,
            )
            for join in lineage.joins
        ]

    def _infer_joins_for_urn(self, start_urn: DatasetUrn) -> List[SourcedJoinInfo]:
        """
        Discover all JOINs involving a dataset from its query history.

        Args:
            start_urn: The dataset URN to find JOINs for.

        Returns:
            List of all JOINs discovered.
        """
        logical_table = self.build_logical_table(start_urn)
        query_urns = self._get_query_urns(DatasetUrn.from_string(str(start_urn)))

        joins: List[SourcedJoinInfo] = []
        for query_urn in query_urns:
            query_joins = self._infer_joins_from_sql(
                QueryUrn.from_string(query_urn),
                default_db=logical_table.base_table.database,
                default_schema=logical_table.base_table.schema_name,
            )
            if query_joins is not None:
                joins.extend(query_joins)

        return joins

    # =========================================================================
    # Layer 5: Primary Key Inference
    # =========================================================================

    def _ensure_primary_keys(self, logical_table: LogicalTable) -> None:
        """
        Infer primary keys using LLM if not present in metadata.

        Layer 5: Uses LLM to determine the most likely primary key columns
        based on column names and types.

        Args:
            logical_table: The table to infer primary keys for (modified in place).
        """
        if logical_table.primary_key is not None:
            return

        logger.debug("Inferring primary keys for {}", logical_table.name)

        try:
            llm_client = get_llm_client(model_config.chat_assistant_ai.model)

            response = llm_client.converse(
                system=[{"text": _PK_INFERENCE_SYSTEM_PROMPT}],
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "text": _PK_INFERENCE_USER_PROMPT.format(
                                    logical_table=logical_table.model_dump_json(
                                        indent=2
                                    )
                                )
                            }
                        ],
                    }
                ],
                toolConfig={"tools": [_pk_inference_tool.to_bedrock_spec()]},
                inferenceConfig={"temperature": 0.3, "maxTokens": 1024},
            )

            result = _extract_tool_response(response)  # type: ignore[arg-type]
            pk_columns = result.get("columns", [])

            if pk_columns:
                logical_table.primary_key = PrimaryKey(columns=pk_columns)
                logger.debug(
                    "Inferred primary keys for {}: {}",
                    logical_table.name,
                    pk_columns,
                )
        except Exception as e:
            logger.warning(
                "Failed to infer primary keys for {}: {}", logical_table.name, e
            )

    # =========================================================================
    # Layer 6: Relationship Inference
    # =========================================================================

    def _infer_relationship_from_joins(
        self, joins: List[SourcedJoinInfo]
    ) -> Optional[Relationship]:
        """
        Infer relationship type and join conditions using LLM.

        Layer 6: Analyzes JOIN patterns from historical queries to determine
        the relationship type (many-to-one, one-to-one) and join conditions.

        Args:
            joins: List of JOINs between the same pair of tables.

        Returns:
            A Relationship object, or None if inference fails.
        """
        if not joins:
            return None

        # Get the tables involved in the joins
        table_urns = OrderedSet(
            table
            for join in joins
            for table in (join.join.left_tables + join.join.right_tables)
        )
        tables = [
            self.build_logical_table(DatasetUrn.from_string(urn)) for urn in table_urns
        ]
        tables_str = pydantic.TypeAdapter(List[LogicalTable]).dump_json(tables).decode()

        # Collect example SQL statements (shortest first, within token budget)
        example_sqls = []
        total_sql_length = 0
        for query_text in sorted(set(join.query_text for join in joins), key=len):
            # Simple character-based estimation instead of tiktoken
            if total_sql_length + len(query_text) > MAX_TOKENS_EXAMPLE_SQL * 4:
                break
            example_sqls.append(query_text)
            total_sql_length += len(query_text)

        example_sqls_str = "\n".join(
            f"<example_sql>{query_text}</example_sql>" for query_text in example_sqls
        )

        try:
            llm_client = get_llm_client(model_config.chat_assistant_ai.model)

            response = llm_client.converse(
                system=[{"text": _RELATIONSHIP_INFERENCE_SYSTEM_PROMPT}],
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "text": _RELATIONSHIP_INFERENCE_USER_PROMPT.format(
                                    tables=tables_str,
                                    example_sqls=example_sqls_str,
                                )
                            }
                        ],
                    }
                ],
                toolConfig={"tools": [_relationship_inference_tool.to_bedrock_spec()]},
                inferenceConfig={"temperature": 0.3, "maxTokens": 2048},
            )

            result = _extract_tool_response(response)  # type: ignore[arg-type]

            return Relationship(
                name=f"{result['left_table']} -> {result['right_table']}: {result['description']}",
                left_table=result["left_table"],
                right_table=result["right_table"],
                relationship_columns=[
                    RelationshipColumn(
                        left_column=col["left_column"],
                        right_column=col["right_column"],
                    )
                    for col in result.get("relationship_columns", [])
                ],
                join_type=result["join_type"],
                relationship_type=result["relationship_type"],
            )
        except Exception as e:
            logger.warning("Failed to infer relationship: {}", e)
            return None

    # =========================================================================
    # Table Priority Queue
    # =========================================================================

    @functools.cache  # noqa: B019
    def _get_popularity(self, urn: DatasetUrn) -> int:
        """
        Get the popularity score for a dataset.

        Args:
            urn: The dataset URN.

        Returns:
            Popularity score (0-100).
        """
        try:
            usage_features = self.client._graph.get_aspect(
                str(urn),
                models.UsageFeaturesClass,  # type: ignore
            )
            if (
                usage_features
                and usage_features.queryCountPercentileLast30Days is not None
            ):
                return min(100, usage_features.queryCountPercentileLast30Days)
        except Exception:
            pass
        return 0

    def request_table(self, start_urn: DatasetUrn, _is_internal: bool = False) -> None:
        """
        Add a table to the candidate queue for processing.

        Args:
            start_urn: The dataset URN to add.
            _is_internal: If True, use popularity score; if False, give max priority.
        """
        if start_urn in self._registered_tables:
            return

        if start_urn in self._candidate_tables:
            logger.debug("{} already in queue, skipping", start_urn)
            return

        logger.debug("Adding {} to queue", start_urn)
        if _is_internal:
            self._candidate_tables[start_urn] = self._get_popularity(start_urn)
        else:
            self._candidate_tables[start_urn] = 100

    def _has_next_candidate(self) -> bool:
        """Check if there are more candidate tables to process."""
        return len(self._candidate_tables) > 0

    def _pop_next_candidate(self) -> DatasetUrn:
        """
        Pop the highest-priority candidate table.

        Priority is based on centrality (number of JOINs) + popularity.
        """
        centrality_scores = collections.Counter[DatasetUrn]()
        for joined_tables, joins in self._joins.items():
            for urn in joined_tables:
                centrality_scores[urn] += len(joins)

        # Composite score = centrality + popularity
        scores = collections.Counter[DatasetUrn]()
        for urn, popularity in self._candidate_tables.items():
            scores[urn] = centrality_scores[urn] + popularity

        next_urn = scores.most_common(1)[0][0]
        del self._candidate_tables[next_urn]
        return next_urn

    # =========================================================================
    # Main Build Method
    # =========================================================================

    def build(self) -> SemanticModel:
        """
        Build the complete semantic model.

        Processes tables in priority order, discovering relationships
        and adding related tables until max_tables is reached.

        Returns:
            The complete SemanticModel.
        """
        while (
            len(self._registered_tables) < self.max_tables
            and self._has_next_candidate()
        ):
            urn = self._pop_next_candidate()

            logger.info("Registering logical table for {}", urn)
            assert urn not in self._registered_tables
            self._registered_tables[urn] = self.build_logical_table(urn)

            # Layer 5: Ensure primary keys
            self._ensure_primary_keys(self._registered_tables[urn])

            # Layer 4: Discover JOINs
            sourced_joins = self._infer_joins_for_urn(urn)

            for sourced_join in sourced_joins:
                join = sourced_join.join
                if len(join.left_tables) != 1 or len(join.right_tables) != 1:
                    continue

                joined_tables: JoinedTables = frozenset(
                    DatasetUrn.from_string(table_urn)
                    for table_urn in join.left_tables + join.right_tables
                )
                self._joins[joined_tables].append(sourced_join)

                # Add related tables to the queue
                for table in joined_tables:
                    self.request_table(table, _is_internal=True)

        if len(self._registered_tables) < self.max_tables:
            logger.debug("No more tables to add, stopping")
        else:
            logger.debug("Hit max tables, stopping")

        return self._get_semantic_model()

    def get_tables(self) -> List[LogicalTable]:
        """Get all registered logical tables."""
        return list(self._registered_tables.values())

    def get_relationships(self) -> List[Relationship]:
        """
        Get all inferred relationships.

        Layer 6: Calls LLM for each pair of joined tables to determine
        relationship type.
        """
        relationships: List[Relationship] = []

        for joined_tables, joins in self._joins.items():
            if not all(urn in self._registered_tables for urn in joined_tables):
                logger.debug(
                    "Skipping relationship between {} because not all tables are registered",
                    joined_tables,
                )
                continue

            relationship = self._infer_relationship_from_joins(joins)
            if relationship is not None:
                relationships.append(relationship)

        return relationships

    def _get_semantic_model(self) -> SemanticModel:
        """
        Construct the final SemanticModel.

        Validates relationships and removes invalid ones.
        """
        tables_list = self.get_tables()
        tables = {table.name: table for table in tables_list}
        relationships = self.get_relationships()

        # Validate relationships
        bad_relationships = []
        for i, relationship in enumerate(relationships):
            if (
                relationship.left_table not in tables
                or relationship.right_table not in tables
            ):
                bad_relationships.append(
                    (
                        i,
                        f"Relationship {relationship.name} references unknown table",
                    )
                )
                continue

            left_table = tables[relationship.left_table]
            right_table = tables[relationship.right_table]

            if left_table.primary_key is None:
                bad_relationships.append(
                    (
                        i,
                        f"Relationship {relationship.name}: {left_table.name} has no primary key",
                    )
                )
                continue

            if right_table.primary_key is None:
                bad_relationships.append(
                    (
                        i,
                        f"Relationship {relationship.name}: {right_table.name} has no primary key",
                    )
                )
                continue

        # Log and remove bad relationships
        if bad_relationships:
            logger.warning("Removing {} invalid relationships", len(bad_relationships))
            for _, reason in bad_relationships:
                logger.debug("  - {}", reason)

        relationships = [
            relationship
            for i, relationship in enumerate(relationships)
            if i not in {bad_idx for bad_idx, _ in bad_relationships}
        ]

        return SemanticModel(
            name="semantic_model",
            description="Auto-generated semantic model from DataHub metadata",
            tables=tables_list,
            relationships=relationships if relationships else None,
        )
