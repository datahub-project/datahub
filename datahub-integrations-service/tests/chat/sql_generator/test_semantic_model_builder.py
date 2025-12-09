"""Unit tests for SemanticModelBuilder."""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.sql_generator.models import (
    BaseTable,
    Dimension,
    LogicalTable,
    PrimaryKey,
    Relationship,
    RelationshipColumn,
)
from datahub_integrations.chat.sql_generator.semantic_model_builder import (
    SemanticModelBuilder,
    SourcedJoinInfo,
    _extract_tool_response,
    _pk_inference_tool,
    _relationship_inference_tool,
)


class TestToolSpecs:
    """Tests for tool specification generation."""

    def test_pk_inference_tool_spec(self) -> None:
        """Test PK inference tool spec structure (generated via FastMCP)."""
        spec = _pk_inference_tool.to_bedrock_spec()
        assert "toolSpec" in spec
        assert spec["toolSpec"]["name"] == "infer_primary_keys"
        assert "inputSchema" in spec["toolSpec"]
        schema = spec["toolSpec"]["inputSchema"]["json"]
        assert "columns" in schema["properties"]

    def test_relationship_inference_tool_spec(self) -> None:
        """Test relationship inference tool spec structure (generated via FastMCP)."""
        spec = _relationship_inference_tool.to_bedrock_spec()
        assert "toolSpec" in spec
        assert spec["toolSpec"]["name"] == "infer_relationship"
        schema = spec["toolSpec"]["inputSchema"]["json"]
        assert "left_table" in schema["properties"]
        assert "right_table" in schema["properties"]
        assert "relationship_type" in schema["properties"]


class TestExtractToolResponse:
    """Tests for _extract_tool_response helper."""

    def test_extracts_tool_use(self) -> None:
        """Test extracting from tool use block."""
        response = {
            "output": {
                "message": {
                    "content": [
                        {"toolUse": {"input": {"columns": ["id", "created_at"]}}}
                    ]
                }
            }
        }
        result = _extract_tool_response(response)
        assert result == {"columns": ["id", "created_at"]}

    def test_raises_on_missing_tool_use(self) -> None:
        """Test raises error when no tool use found."""
        response: dict = {"output": {"message": {"content": []}}}
        with pytest.raises(ValueError, match="No tool use block"):
            _extract_tool_response(response)


class TestSemanticModelBuilder:
    """Tests for SemanticModelBuilder class."""

    def _create_mock_client(self) -> Mock:
        """Create a mock DataHub client."""
        client = Mock()
        client._graph = Mock()
        return client

    def _create_mock_dataset(
        self,
        name: str = "orders",
        qualified_name: str = "prod.sales.orders",
        columns: list | None = None,
    ) -> Mock:
        """Create a mock Dataset entity."""
        dataset = Mock()
        dataset.qualified_name = qualified_name
        dataset.display_name = name
        dataset.description = f"Test {name} table"
        dataset.urn = Mock()
        dataset.urn.name = name

        if columns is None:
            # Default columns
            columns = [
                self._create_mock_column("id", "NUMBER", is_pk=True),
                self._create_mock_column("customer_id", "NUMBER"),
                self._create_mock_column("status", "VARCHAR"),
                self._create_mock_column("total", "DECIMAL"),
                self._create_mock_column("created_at", "TIMESTAMP"),
            ]

        dataset.schema = columns
        return dataset

    def _create_mock_column(
        self,
        name: str,
        native_type: str,
        is_pk: bool = False,
        description: str | None = None,
    ) -> Mock:
        """Create a mock column."""
        import datahub.metadata.schema_classes as models

        column = Mock()
        column.field_path = name
        column.native_type = native_type
        column.description = description or f"Column {name}"

        base_field = Mock()
        base_field.isPartOfKey = is_pk
        column._base_schema_field.return_value = base_field

        # Set mapped type based on native type
        mapped_type = Mock()
        if native_type in ("TIMESTAMP", "DATETIME", "DATE"):
            mapped_type.type = models.TimeTypeClass()
        elif native_type in ("NUMBER", "DECIMAL", "FLOAT", "INTEGER", "INT"):
            mapped_type.type = models.NumberTypeClass()
        else:
            mapped_type.type = models.StringTypeClass()

        column.mapped_type = mapped_type
        return column

    def test_initialization(self) -> None:
        """Test builder initialization."""
        client = self._create_mock_client()
        builder = SemanticModelBuilder(client, max_tables=10)

        assert builder.client == client
        assert builder.max_tables == 10
        assert len(builder._registered_tables) == 0

    def test_initialization_with_start_urns(self) -> None:
        """Test initialization with starting URNs."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
        )
        builder = SemanticModelBuilder(client, start_urns=[urn])

        assert urn in builder._candidate_tables
        assert builder._candidate_tables[urn] == 100  # Max priority for explicit urns

    def test_build_logical_table_classifies_columns(self) -> None:
        """Test that columns are classified correctly by type."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        dataset = self._create_mock_dataset()
        client.entities.get.return_value = dataset

        builder = SemanticModelBuilder(client)
        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
        )

        table = builder.build_logical_table(urn)

        # Verify classification
        assert table.name == "orders"
        assert table.base_table.database == "prod"
        assert table.base_table.schema_name == "sales"
        assert table.base_table.table == "orders"

        # Check primary key
        assert table.primary_key is not None
        assert "id" in table.primary_key.columns

        # Check dimensions (string columns)
        assert table.dimensions is not None
        dim_names = [d.name for d in table.dimensions]
        assert "status" in dim_names  # String type -> dimension

        # customer_id is numeric so it's classified as a fact
        # (the semantic model uses type-based classification)

        # Check time dimensions
        assert table.time_dimensions is not None
        td_names = [td.name for td in table.time_dimensions]
        assert "created_at" in td_names

        # Check facts (numeric columns)
        assert table.facts is not None
        fact_names = [f.name for f in table.facts]
        assert "total" in fact_names

    def test_request_table_adds_to_queue(self) -> None:
        """Test that request_table adds URN to candidate queue."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
        )
        builder.request_table(urn)

        assert urn in builder._candidate_tables
        assert builder._candidate_tables[urn] == 100  # Max priority

    def test_request_table_internal_uses_popularity(self) -> None:
        """Test that internal requests use popularity score (calls _get_popularity)."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock _get_popularity directly to return a known value
        with patch.object(builder, "_get_popularity", return_value=75):
            urn = DatasetUrn.from_string(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
            )
            builder.request_table(urn, _is_internal=True)

            assert urn in builder._candidate_tables
            assert builder._candidate_tables[urn] == 75

    def test_request_table_skips_duplicates(self) -> None:
        """Test that duplicate URNs are not added twice."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
        )
        builder.request_table(urn)
        initial_count = len(builder._candidate_tables)

        builder.request_table(urn)  # Add again
        assert len(builder._candidate_tables) == initial_count

    def test_has_next_candidate(self) -> None:
        """Test checking for next candidate."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        assert not builder._has_next_candidate()

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
        )
        builder.request_table(urn)

        assert builder._has_next_candidate()

    @patch(
        "datahub_integrations.chat.sql_generator.semantic_model_builder.get_llm_client"
    )
    def test_ensure_primary_keys_skips_when_present(self, mock_llm) -> None:
        """Test that PK inference is skipped when PK already exists."""
        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        table = LogicalTable(
            name="orders",
            base_table=BaseTable(database="db", schema_name="schema", table="orders"),
            primary_key=PrimaryKey(columns=["id"]),
        )

        builder._ensure_primary_keys(table)

        # LLM should not be called
        mock_llm.assert_not_called()

    @patch(
        "datahub_integrations.chat.sql_generator.semantic_model_builder.get_llm_client"
    )
    def test_ensure_primary_keys_infers_when_missing(self, mock_llm) -> None:
        """Test that PK inference is called when PK is missing."""
        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock LLM response
        mock_llm_client = Mock()
        mock_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [{"toolUse": {"input": {"columns": ["order_id"]}}}]
                }
            }
        }

        table = LogicalTable(
            name="orders",
            base_table=BaseTable(database="db", schema_name="schema", table="orders"),
            dimensions=[
                Dimension(name="order_id", expr="order_id", data_type="VARCHAR")
            ],
        )

        builder._ensure_primary_keys(table)

        # Verify LLM was called
        mock_llm_client.converse.assert_called_once()

        # Verify PK was set
        assert table.primary_key is not None
        assert "order_id" in table.primary_key.columns

    def test_get_tables_returns_registered(self) -> None:
        """Test that get_tables returns all registered tables."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Directly add tables to bypass build
        urn1 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )
        urn2 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)"
        )

        builder._registered_tables[urn1] = LogicalTable(
            name="orders",
            base_table=BaseTable(database="db", schema_name="s", table="orders"),
        )
        builder._registered_tables[urn2] = LogicalTable(
            name="customers",
            base_table=BaseTable(database="db", schema_name="s", table="customers"),
        )

        tables = builder.get_tables()
        assert len(tables) == 2
        names = [t.name for t in tables]
        assert "orders" in names
        assert "customers" in names

    # =========================================================================
    # Layer 3: Query History Tests
    # =========================================================================

    def test_get_query_urns_returns_multi_table_queries(self) -> None:
        """Test that _get_query_urns filters for multi-table queries."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock GraphQL response with queries
        client._graph.execute_graphql.return_value = {
            "listQueries": {
                "queries": [
                    {
                        "urn": "urn:li:query:query1",
                        "subjects": [
                            {"dataset": {"urn": "urn:li:dataset:(...):table1"}},
                            {"dataset": {"urn": "urn:li:dataset:(...):table2"}},
                        ],
                    },
                    {
                        "urn": "urn:li:query:query2",
                        "subjects": [
                            {"dataset": {"urn": "urn:li:dataset:(...):table1"}},
                        ],
                    },  # Single table - should be skipped
                    {
                        "urn": "urn:li:query:query3",
                        "subjects": [
                            {"dataset": {"urn": "urn:li:dataset:(...):table1"}},
                            {"dataset": {"urn": "urn:li:dataset:(...):table3"}},
                        ],
                    },
                ]
            }
        }

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )
        query_urns = builder._get_query_urns(urn)

        # Should only return multi-table queries
        assert len(query_urns) == 2
        assert "urn:li:query:query1" in query_urns
        assert "urn:li:query:query3" in query_urns

    def test_get_query_urns_handles_graphql_failure(self) -> None:
        """Test that _get_query_urns handles GraphQL failures gracefully."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock GraphQL to raise exception
        client._graph.execute_graphql.side_effect = Exception("GraphQL error")

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )
        query_urns = builder._get_query_urns(urn)

        # Should return empty list on failure
        assert query_urns == []

    # =========================================================================
    # Layer 4: JOIN Extraction Tests
    # =========================================================================

    def test_infer_joins_from_sql_parses_successfully(self) -> None:
        """Test that _infer_joins_from_sql parses JOIN statements."""
        from datahub.metadata.urns import QueryUrn
        from datahub.sql_parsing.sqlglot_lineage import JoinInfo

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock query entity
        client._graph.get_entity_semityped.return_value = {
            "queryProperties": Mock(
                statement=Mock(value="SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
            ),
            "dataPlatformInstance": Mock(platform="snowflake", instance="prod"),
        }

        # Mock SQL parsing
        mock_lineage = Mock()
        mock_join = Mock(spec=JoinInfo)
        mock_join.left_tables = ["urn:li:dataset:(...):t1"]
        mock_join.right_tables = ["urn:li:dataset:(...):t2"]
        mock_lineage.joins = [mock_join]
        client._graph.parse_sql_lineage.return_value = mock_lineage

        query_urn = QueryUrn.from_string("urn:li:query:test_query")
        joins = builder._infer_joins_from_sql(query_urn, "db", "schema")

        assert joins is not None
        assert len(joins) == 1
        assert isinstance(joins[0], SourcedJoinInfo)
        assert joins[0].query_urn == query_urn

    def test_infer_joins_from_sql_handles_no_joins(self) -> None:
        """Test handling queries without JOINs."""
        from datahub.metadata.urns import QueryUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock query with no joins
        client._graph.get_entity_semityped.return_value = {
            "queryProperties": Mock(statement=Mock(value="SELECT * FROM t1")),
            "dataPlatformInstance": Mock(platform="snowflake", instance="prod"),
        }

        mock_lineage = Mock()
        mock_lineage.joins = None
        client._graph.parse_sql_lineage.return_value = mock_lineage

        query_urn = QueryUrn.from_string("urn:li:query:test_query")
        joins = builder._infer_joins_from_sql(query_urn, "db", "schema")

        assert joins is None

    def test_infer_joins_from_sql_handles_parsing_failure(self) -> None:
        """Test handling SQL parsing failures."""
        from datahub.metadata.urns import QueryUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock parsing failure
        client._graph.get_entity_semityped.return_value = {
            "queryProperties": Mock(statement=Mock(value="INVALID SQL")),
            "dataPlatformInstance": Mock(platform="snowflake", instance="prod"),
        }
        client._graph.parse_sql_lineage.side_effect = Exception("Parse error")

        query_urn = QueryUrn.from_string("urn:li:query:test_query")
        joins = builder._infer_joins_from_sql(query_urn, "db", "schema")

        assert joins is None

    # =========================================================================
    # Layer 6: Relationship Inference Tests
    # =========================================================================

    @patch(
        "datahub_integrations.chat.sql_generator.semantic_model_builder.get_llm_client"
    )
    def test_infer_relationship_from_joins_creates_relationship(self, mock_llm) -> None:
        """Test that relationships are inferred from JOIN patterns."""
        from datahub.metadata.urns import QueryUrn
        from datahub.sql_parsing.sqlglot_lineage import JoinInfo

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Setup mock datasets
        dataset1 = self._create_mock_dataset("orders", "prod.sales.orders")
        dataset2 = self._create_mock_dataset("customers", "prod.sales.customers")
        client.entities.get.side_effect = [dataset1, dataset2]

        # Mock LLM response
        mock_llm_client = Mock()
        mock_llm.return_value = mock_llm_client
        mock_llm_client.converse.return_value = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "input": {
                                    "description": "Orders belong to customers",
                                    "relationship_type": "many_to_one",
                                    "join_type": "left_outer",
                                    "left_table": "orders",
                                    "right_table": "customers",
                                    "relationship_columns": [
                                        {
                                            "left_column": "customer_id",
                                            "right_column": "id",
                                        }
                                    ],
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Create mock joins
        mock_join = Mock(spec=JoinInfo)
        mock_join.left_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        ]
        mock_join.right_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)"
        ]

        joins = [
            SourcedJoinInfo(
                query_urn=QueryUrn.from_string("urn:li:query:q1"),
                query_text="SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id",
                join=mock_join,
            )
        ]

        relationship = builder._infer_relationship_from_joins(joins)

        assert relationship is not None
        assert relationship.left_table == "orders"
        assert relationship.right_table == "customers"
        assert relationship.relationship_type == "many_to_one"
        assert len(relationship.relationship_columns) == 1

    def test_infer_relationship_from_joins_returns_none_for_empty(self) -> None:
        """Test that empty joins list returns None."""
        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        relationship = builder._infer_relationship_from_joins([])
        assert relationship is None

    @patch(
        "datahub_integrations.chat.sql_generator.semantic_model_builder.get_llm_client"
    )
    def test_infer_relationship_handles_llm_failure(self, mock_llm) -> None:
        """Test that LLM failures are handled gracefully."""
        from datahub.metadata.urns import QueryUrn
        from datahub.sql_parsing.sqlglot_lineage import JoinInfo

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Setup mock datasets
        dataset1 = self._create_mock_dataset("orders", "prod.sales.orders")
        client.entities.get.return_value = dataset1

        # Mock LLM failure
        mock_llm_client = Mock()
        mock_llm.return_value = mock_llm_client
        mock_llm_client.converse.side_effect = Exception("LLM error")

        # Create mock joins
        mock_join = Mock(spec=JoinInfo)
        mock_join.left_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        ]
        mock_join.right_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)"
        ]

        joins = [
            SourcedJoinInfo(
                query_urn=QueryUrn.from_string("urn:li:query:q1"),
                query_text="SELECT * FROM orders JOIN customers",
                join=mock_join,
            )
        ]

        relationship = builder._infer_relationship_from_joins(joins)
        assert relationship is None

    # =========================================================================
    # Priority Queue Tests
    # =========================================================================

    def test_pop_next_candidate_prioritizes_centrality(self) -> None:
        """Test that _pop_next_candidate uses centrality + popularity."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        urn1 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        )
        urn2 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table2,PROD)"
        )

        # Add with different priorities
        builder._candidate_tables[urn1] = 50
        builder._candidate_tables[urn2] = 30

        # Add joins for urn2 (increases centrality)
        from datahub.metadata.urns import QueryUrn
        from datahub.sql_parsing.sqlglot_lineage import JoinInfo

        mock_join = Mock(spec=JoinInfo)
        builder._joins[frozenset([urn2])] = [
            SourcedJoinInfo(
                query_urn=QueryUrn.from_string("urn:li:query:q1"),
                query_text="SELECT * FROM table2",
                join=mock_join,
            )
        ] * 50  # High centrality

        # urn2 should be selected (centrality 50 + popularity 30 = 80 > urn1's 50)
        next_urn = builder._pop_next_candidate()
        assert next_urn == urn2
        assert urn2 not in builder._candidate_tables

    def test_get_popularity_returns_usage_features(self) -> None:
        """Test that _get_popularity retrieves usage statistics."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock usage features
        usage_features = Mock()
        usage_features.queryCountPercentileLast30Days = 85
        client._graph.get_aspect.return_value = usage_features

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )
        popularity = builder._get_popularity(urn)

        assert popularity == 85

    def test_get_popularity_caps_at_100(self) -> None:
        """Test that popularity score is capped at 100."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock high usage
        usage_features = Mock()
        usage_features.queryCountPercentileLast30Days = 150
        client._graph.get_aspect.return_value = usage_features

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )
        popularity = builder._get_popularity(urn)

        assert popularity == 100

    def test_get_popularity_returns_zero_on_failure(self) -> None:
        """Test that _get_popularity returns 0 when data unavailable."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Mock failure
        client._graph.get_aspect.side_effect = Exception("Not found")

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )
        popularity = builder._get_popularity(urn)

        assert popularity == 0

    # =========================================================================
    # Edge Cases and Error Handling
    # =========================================================================

    def test_build_logical_table_handles_short_qualified_name(self) -> None:
        """Test handling of qualified names with fewer than 3 parts."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Dataset with short qualified name
        dataset = self._create_mock_dataset(
            "orders", qualified_name="sales.orders", columns=[]
        )
        client.entities.get.return_value = dataset

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )
        table = builder.build_logical_table(urn)

        # Should pad with empty strings
        assert table.base_table.database == ""
        assert table.base_table.schema_name == "sales"
        assert table.base_table.table == "orders"

    def test_build_logical_table_caches_results(self) -> None:
        """Test that build_logical_table caches results."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        dataset = self._create_mock_dataset()
        client.entities.get.return_value = dataset

        urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"
        )

        # Call twice
        table1 = builder.build_logical_table(urn)
        table2 = builder.build_logical_table(urn)

        # Should be same object (cached)
        assert table1 is table2
        # Client should only be called once
        assert client.entities.get.call_count == 1

    def test_get_relationships_skips_incomplete_tables(self) -> None:
        """Test that get_relationships skips joins with unregistered tables."""
        from datahub.metadata.urns import DatasetUrn, QueryUrn
        from datahub.sql_parsing.sqlglot_lineage import JoinInfo

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Register only one table
        urn1 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        )
        builder._registered_tables[urn1] = LogicalTable(
            name="table1",
            base_table=BaseTable(database="db", schema_name="s", table="table1"),
        )

        # Add join involving unregistered table
        urn2 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table2,PROD)"
        )
        mock_join = Mock(spec=JoinInfo)
        builder._joins[frozenset([urn1, urn2])] = [
            SourcedJoinInfo(
                query_urn=QueryUrn.from_string("urn:li:query:q1"),
                query_text="SELECT * FROM table1 JOIN table2",
                join=mock_join,
            )
        ]

        relationships = builder.get_relationships()

        # Should skip the relationship
        assert len(relationships) == 0

    def test_get_semantic_model_filters_invalid_relationships(self) -> None:
        """Test that _get_semantic_model removes invalid relationships."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        # Register tables
        urn1 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table1,PROD)"
        )
        urn2 = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,table2,PROD)"
        )

        # Table with PK
        builder._registered_tables[urn1] = LogicalTable(
            name="table1",
            base_table=BaseTable(database="db", schema_name="s", table="table1"),
            primary_key=PrimaryKey(columns=["id"]),
        )

        # Table without PK (will cause relationship to be filtered out)
        builder._registered_tables[urn2] = LogicalTable(
            name="table2",
            base_table=BaseTable(database="db", schema_name="s", table="table2"),
        )

        # Mock get_relationships to return a relationship
        with patch.object(
            builder,
            "get_relationships",
            return_value=[
                Relationship(
                    name="table1 -> table2",
                    left_table="table1",
                    right_table="table2",
                    relationship_columns=[
                        RelationshipColumn(left_column="id", right_column="id")
                    ],
                    join_type="inner",
                    relationship_type="one_to_one",
                )
            ],
        ):
            semantic_model = builder._get_semantic_model()

            # Relationship should be filtered out due to missing PK on table2
            assert (
                semantic_model.relationships is None
                or len(semantic_model.relationships) == 0
            )

    def test_build_stops_at_max_tables(self) -> None:
        """Test that build stops when max_tables is reached."""
        from datahub.metadata.urns import DatasetUrn

        client = self._create_mock_client()
        builder = SemanticModelBuilder(client, max_tables=2)

        # Add 3 candidate tables
        for i in range(3):
            urn = DatasetUrn.from_string(
                f"urn:li:dataset:(urn:li:dataPlatform:snowflake,table{i},PROD)"
            )
            builder.request_table(urn)

        # Mock dataset responses
        def mock_get(urn):
            return self._create_mock_dataset(f"table{urn}", f"db.schema.table{urn}")

        client.entities.get.side_effect = mock_get

        # Mock query fetching to return empty
        client._graph.execute_graphql.return_value = {"listQueries": {"queries": []}}

        semantic_model = builder.build()

        # Should stop at max_tables
        assert len(semantic_model.tables) <= 2

    def test_build_handles_no_candidates(self) -> None:
        """Test build with no candidate tables."""
        client = self._create_mock_client()
        builder = SemanticModelBuilder(client)

        semantic_model = builder.build()

        assert len(semantic_model.tables) == 0
        assert (
            semantic_model.relationships is None
            or len(semantic_model.relationships) == 0
        )
