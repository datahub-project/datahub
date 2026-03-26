from unittest import mock


def create_mock_dbt_node(table_name: str) -> mock.Mock:
    """Helper to create a mock DBTNode with a name attribute."""
    node = mock.Mock()
    node.name = table_name
    return node
