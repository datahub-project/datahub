"""Tests for domain management tools."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.tools.domains import remove_domains, set_domains


@pytest.fixture
def mock_datahub_client():
    """Create a mock DataHub client."""
    mock_client = MagicMock()
    mock_client._graph = MagicMock()
    mock_client._graph.execute_graphql = MagicMock()
    return mock_client


# Set domain tests


def test_set_domains_single_entity(mock_datahub_client):
    """Test setting domain for a single entity."""
    domain_urn = "urn:li:domain:marketing"
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    ]

    # Mock domain validation response
    mock_datahub_client._graph.execute_graphql.side_effect = [
        # First call: domain validation
        {
            "entity": {
                "urn": domain_urn,
                "type": "DOMAIN",
                "properties": {"name": "Marketing"},
            }
        },
        # Second call: batchSetDomain mutation
        {"batchSetDomain": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = set_domains(domain_urn=domain_urn, entity_urns=entity_urns)

    assert result["success"] is True
    assert "1 entit(ies)" in result["message"]

    # Verify GraphQL was called twice (validation + mutation)
    assert mock_datahub_client._graph.execute_graphql.call_count == 2

    # Verify mutation was called with correct parameters
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["operation_name"] == "batchSetDomain"
    assert mutation_call.kwargs["variables"]["input"]["domainUrn"] == domain_urn
    assert (
        mutation_call.kwargs["variables"]["input"]["resources"][0]["resourceUrn"]
        == entity_urns[0]
    )


def test_set_domains_multiple_entities(mock_datahub_client):
    """Test setting domain for multiple entities."""
    domain_urn = "urn:li:domain:finance"
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.transactions,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.accounts,PROD)",
        "urn:li:dashboard:(urn:li:dataPlatform:looker,revenue,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": domain_urn,
                "type": "DOMAIN",
                "properties": {"name": "Finance"},
            }
        },
        {"batchSetDomain": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = set_domains(domain_urn=domain_urn, entity_urns=entity_urns)

    assert result["success"] is True
    assert "3 entit(ies)" in result["message"]

    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    resources = mutation_call.kwargs["variables"]["input"]["resources"]
    assert len(resources) == 3
    assert resources[0]["resourceUrn"] == entity_urns[0]
    assert resources[1]["resourceUrn"] == entity_urns[1]
    assert resources[2]["resourceUrn"] == entity_urns[2]


def test_set_domains_mixed_entity_types(mock_datahub_client):
    """Test setting domain for mixed entity types (datasets, dashboards, dataflows)."""
    domain_urn = "urn:li:domain:engineering"
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.logs,PROD)",
        "urn:li:dataFlow:(urn:li:dataPlatform:airflow,etl_pipeline,PROD)",
        "urn:li:dashboard:(urn:li:dataPlatform:superset,metrics,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": domain_urn,
                "type": "DOMAIN",
                "properties": {"name": "Engineering"},
            }
        },
        {"batchSetDomain": True},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = set_domains(domain_urn=domain_urn, entity_urns=entity_urns)

    assert result["success"] is True
    assert "3 entit(ies)" in result["message"]


# Remove domain tests


def test_remove_domains_single_entity(mock_datahub_client):
    """Test removing domain from a single entity."""
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old,PROD)"]

    mock_datahub_client._graph.execute_graphql.return_value = {"batchSetDomain": True}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_domains(entity_urns=entity_urns)

    assert result["success"] is True
    assert "removed domain from 1 entit(ies)" in result["message"]

    # Verify mutation was called with domainUrn set to None
    call_args = mock_datahub_client._graph.execute_graphql.call_args
    assert call_args.kwargs["variables"]["input"]["domainUrn"] is None
    assert (
        call_args.kwargs["variables"]["input"]["resources"][0]["resourceUrn"]
        == entity_urns[0]
    )


def test_remove_domains_multiple_entities(mock_datahub_client):
    """Test removing domain from multiple entities."""
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.temp1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.temp2,PROD)",
        "urn:li:dashboard:(urn:li:dataPlatform:looker,old_dashboard,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.return_value = {"batchSetDomain": True}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_domains(entity_urns=entity_urns)

    assert result["success"] is True
    assert "removed domain from 3 entit(ies)" in result["message"]

    call_args = mock_datahub_client._graph.execute_graphql.call_args
    resources = call_args.kwargs["variables"]["input"]["resources"]
    assert len(resources) == 3
    assert call_args.kwargs["variables"]["input"]["domainUrn"] is None


# Validation tests


def test_set_domains_empty_domain_urn(mock_datahub_client):
    """Test that empty domain_urn raises ValueError."""
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="domain_urn cannot be empty"):
            set_domains(domain_urn="", entity_urns=entity_urns)


def test_set_domains_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    domain_urn = "urn:li:domain:test"

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            set_domains(domain_urn=domain_urn, entity_urns=[])


def test_remove_domains_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            remove_domains(entity_urns=[])


def test_set_domains_nonexistent_domain(mock_datahub_client):
    """Test that nonexistent domain URN returns error."""
    domain_urn = "urn:li:domain:nonexistent"
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    # Mock validation returning None (domain doesn't exist)
    mock_datahub_client._graph.execute_graphql.return_value = {"entity": None}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="Domain\ URN\ does\ not\ exist"):
            set_domains(domain_urn=domain_urn, entity_urns=entity_urns)


def test_set_domains_invalid_domain_type(mock_datahub_client):
    """Test that URN with wrong entity type returns error."""
    domain_urn = "urn:li:tag:not-a-domain"
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    # Mock validation returning wrong type
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entity": {"urn": domain_urn, "type": "TAG"}
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="not\ a\ domain\ entity"):
            set_domains(domain_urn=domain_urn, entity_urns=entity_urns)


# Error handling tests


def test_set_domains_mutation_returns_false(mock_datahub_client):
    """Test handling when mutation returns false."""
    domain_urn = "urn:li:domain:test"
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": domain_urn, "type": "DOMAIN"}},
        {"batchSetDomain": False},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed\ to\ set\ domain"):
            set_domains(domain_urn=domain_urn, entity_urns=entity_urns)


def test_remove_domains_mutation_returns_false(mock_datahub_client):
    """Test handling when remove mutation returns false."""
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    mock_datahub_client._graph.execute_graphql.return_value = {"batchSetDomain": False}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Failed\ to\ remove\ domain"):
            remove_domains(entity_urns=entity_urns)


def test_set_domains_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL execution errors."""
    domain_urn = "urn:li:domain:test"
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {"entity": {"urn": domain_urn, "type": "DOMAIN"}},
        Exception("GraphQL error"),
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Error\ setting\ domain"):
            set_domains(domain_urn=domain_urn, entity_urns=entity_urns)


def test_remove_domains_graphql_exception(mock_datahub_client):
    """Test handling of GraphQL execution errors during removal."""
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = Exception("Network error")

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError, match="Error removing domain"):
            remove_domains(entity_urns=entity_urns)


def test_set_domains_validation_exception(mock_datahub_client):
    """Test handling of validation errors."""
    domain_urn = "urn:li:domain:test"
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = Exception(
        "Connection timeout"
    )

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="Failed\ to\ validate\ domain\ URN"):
            set_domains(domain_urn=domain_urn, entity_urns=entity_urns)
