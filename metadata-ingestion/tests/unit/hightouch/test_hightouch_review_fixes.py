from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceConfig,
)
from datahub.ingestion.source.hightouch.hightouch import HightouchSource
from datahub.ingestion.source.hightouch.models import (
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    MetadataChangeProposalClass,
)

_CREATED = datetime(2023, 1, 1)
_SNOWFLAKE_TABLE = "urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)"


def _model(name: str = "customers") -> HightouchModel:
    return HightouchModel(
        id="model_1",
        name=name,
        slug="customers-model",
        workspace_id="ws",
        source_id="source_1",
        query_type="table",
        created_at=_CREATED,
        updated_at=_CREATED,
    )


def _sync(slug: str = "customers-sync") -> HightouchSync:
    return HightouchSync(
        id=f"{slug}-id",
        slug=slug,
        workspace_id="ws",
        model_id="model_1",
        destination_id="dest_1",
        created_at=_CREATED,
        updated_at=_CREATED,
    )


def _snowflake_source() -> HightouchSourceConnection:
    return HightouchSourceConnection(
        id="source_1",
        name="Snowflake",
        slug="snowflake",
        type="snowflake",
        workspace_id="ws",
        created_at=_CREATED,
        updated_at=_CREATED,
    )


def _source(mock_client: MagicMock, **config: object) -> HightouchSource:
    with patch(
        "datahub.ingestion.source.hightouch.hightouch.HightouchAPIClient",
        return_value=mock_client,
    ):
        return HightouchSource(
            HightouchSourceConfig(
                api_config=HightouchAPIConfig(api_key="test"),
                env="PROD",
                **config,
            ),
            PipelineContext(run_id="test"),
        )


@pytest.mark.parametrize(
    "name, database, schema, expected",
    [
        pytest.param("orders", "db", "sch", "db.sch.orders", id="bare-name"),
        pytest.param(
            "sch.orders", "db", "other", "db.sch.orders", id="schema-qualified"
        ),
        pytest.param(
            "db2.sch.orders", "db", "sch", "db2.sch.orders", id="fully-qualified"
        ),
        pytest.param("DB.orders", "db", "", "DB.orders", id="database-qualified"),
        pytest.param("orders", "", "", "orders", id="nothing-configured"),
    ],
)
def test_qualify_adds_only_missing_parts(
    name: str, database: str, schema: str, expected: str
) -> None:
    assert HightouchUrnBuilder._qualify(name, database, schema) == expected


def test_denied_model_inlet_is_source_table_not_model() -> None:
    mock_client = MagicMock()
    mock_client.get_source_by_id.return_value = _snowflake_source()
    source = _source(
        mock_client,
        emit_models_as_datasets=True,
        model_patterns=AllowDenyPattern(deny=["customers"]),
    )

    inlets = source._sync_handler.get_inlet_urns_for_model(_model(), _sync())

    assert [str(urn) for urn in inlets] == [_SNOWFLAKE_TABLE]


def test_pattern_filtered_syncs_and_models_are_reported() -> None:
    mock_client = MagicMock()
    mock_client.get_syncs.return_value = [_sync("kept-sync"), _sync("denied-sync")]
    mock_client.get_models.return_value = [_model("denied-model")]
    mock_client.get_model_by_id.return_value = _model("denied-model")
    mock_client.get_source_by_id.return_value = _snowflake_source()
    mock_client.get_destination_by_id.return_value = None
    mock_client.get_contracts.return_value = []
    source = _source(
        mock_client,
        emit_models_as_datasets=True,
        include_sync_runs=False,
        sync_patterns=AllowDenyPattern(deny=["denied-sync"]),
        model_patterns=AllowDenyPattern(deny=["denied-model"]),
    )

    list(source.get_workunits_internal())

    assert list(source.report.filtered_syncs) == ["denied-sync"]
    assert list(source.report.filtered_models) == ["denied-model"]


def test_destination_lineage_is_a_non_primary_patch() -> None:
    source = _source(MagicMock())
    handler = source._lineage_handler
    destination = "urn:li:dataset:(urn:li:dataPlatform:salesforce,Contact,PROD)"
    handler.accumulate_destination_lineage(destination, [_SNOWFLAKE_TABLE], None)

    workunits = list(handler.emit_all_destination_lineage())

    assert workunits
    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalClass)
        assert wu.metadata.changeType == ChangeTypeClass.PATCH
        assert wu.is_primary_source is False


def test_table_model_lineage_is_not_registered_with_aggregator() -> None:
    # The aggregator's own UpstreamLineage would replace the model's aspect that
    # carries column-level lineage.
    source = _source(MagicMock(), emit_models_as_datasets=True)
    aggregator = MagicMock()
    platform = MagicMock(platform="snowflake", database=None)

    source._lineage_handler.register_model_lineage(
        _model(),
        "urn:li:dataset:(urn:li:dataPlatform:hightouch,customers-model,PROD)",
        _snowflake_source(),
        lambda _source: platform,
        lambda _platform: aggregator,
    )

    aggregator.add_known_lineage_mapping.assert_not_called()
