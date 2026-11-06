import pytest
from pydantic import ValidationError

from datahub_actions.plugin.action.propagation.docs.propagation_action import (
    ColumnPropagationRelationships,
    DocPropagationConfig,
)


def test_column_propagation_relationships_from_config() -> None:
    config = DocPropagationConfig.model_validate(
        {"column_propagation_relationships": ["downstream", "SIBLING", "Upstream"]}
    )
    assert config.column_propagation_relationships == [
        ColumnPropagationRelationships.DOWNSTREAM,
        ColumnPropagationRelationships.SIBLING,
        ColumnPropagationRelationships.UPSTREAM,
    ]


def test_column_propagation_relationships_rejects_unknown_value() -> None:
    with pytest.raises(ValidationError):
        DocPropagationConfig.model_validate(
            {"column_propagation_relationships": ["parent"]}
        )
