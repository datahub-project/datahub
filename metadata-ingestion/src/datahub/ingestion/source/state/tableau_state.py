import logging
from typing import Callable, Dict, Iterable, List

import pydantic

from datahub.emitter.mce_builder import (
    chart_urn_to_key,
    dashboard_urn_to_key,
    make_chart_urn,
    make_dashboard_urn,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityCheckpointStateBase,
)
from datahub.utilities.checkpoint_state_util import CheckpointStateUtil

logger = logging.getLogger(__name__)


class TableauCheckpointState(StaleEntityCheckpointStateBase["TableauCheckpointState"]):
    """
    Class for representing the checkpoint state for Tableau sources.
    Stores all datasets, charts and dashboards being ingested and is
    used to remove any stale entities.
    """

    encoded_dataset_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_chart_urns: List[str] = pydantic.Field(default_factory=list)
    encoded_dashboard_urns: List[str] = pydantic.Field(default_factory=list)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        return ["dataset", "chart", "dashboard"]

    @staticmethod
    def _get_dataset_lightweight_repr(dataset_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        return CheckpointStateUtil.get_dataset_lightweight_repr(dataset_urn)

    @staticmethod
    def _get_chart_lightweight_repr(chart_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        SEP = CheckpointStateUtil.get_separator()
        key = chart_urn_to_key(chart_urn)
        assert key is not None
        return f"{key.dashboardTool}{SEP}{key.chartId}"

    @staticmethod
    def _get_dashboard_lightweight_repr(dashboard_urn: str) -> str:
        """Reduces the amount of text in the URNs for smaller state footprint."""
        SEP = CheckpointStateUtil.get_separator()
        key = dashboard_urn_to_key(dashboard_urn)
        assert key is not None
        return f"{key.dashboardTool}{SEP}{key.dashboardId}"

    def _add_dataset_urn(self, dataset_urn: str) -> None:
        self.encoded_dataset_urns.append(
            self._get_dataset_lightweight_repr(dataset_urn)
        )

    def _add_chart_urn(self, chart_urn: str) -> None:
        self.encoded_chart_urns.append(self._get_chart_lightweight_repr(chart_urn))

    def _add_dashboard_urn(self, dashboard_urn: str) -> None:
        self.encoded_dashboard_urns.append(
            self._get_dashboard_lightweight_repr(dashboard_urn)
        )

    def _get_dataset_urns_not_in(
        self, checkpoint: "TableauCheckpointState"
    ) -> Iterable[str]:
        yield from CheckpointStateUtil.get_dataset_urns_not_in(
            self.encoded_dataset_urns, checkpoint.encoded_dataset_urns
        )

    def _get_chart_urns_not_in(
        self, checkpoint: "TableauCheckpointState"
    ) -> Iterable[str]:
        difference = CheckpointStateUtil.get_encoded_urns_not_in(
            self.encoded_chart_urns, checkpoint.encoded_chart_urns
        )
        for encoded_urn in difference:
            platform, name = encoded_urn.split(CheckpointStateUtil.get_separator())
            yield make_chart_urn(platform, name)

    def _get_dashboard_urns_not_in(
        self, checkpoint: "TableauCheckpointState"
    ) -> Iterable[str]:
        difference = CheckpointStateUtil.get_encoded_urns_not_in(
            self.encoded_dashboard_urns, checkpoint.encoded_dashboard_urns
        )
        for encoded_urn in difference:
            platform, name = encoded_urn.split(CheckpointStateUtil.get_separator())
            yield make_dashboard_urn(platform, name)

    def add_checkpoint_urn(self, type: str, urn: str) -> None:
        supported_entities_add_handlers: Dict[str, Callable[[str], None]] = {
            "dataset": self._add_dataset_urn,
            "chart": self._add_chart_urn,
            "dashboard": self._add_dashboard_urn,
        }

        if type not in supported_entities_add_handlers:
            logger.error(f"Can not save Unknown entity {type} to checkpoint.")

        supported_entities_add_handlers[type](urn)

    def get_urns_not_in(
        self, type: str, other_checkpoint_state: "TableauCheckpointState"
    ) -> Iterable[str]:
        assert type in self.get_supported_types()
        if type == "dataset":
            yield from self._get_dataset_urns_not_in(other_checkpoint_state)
        elif type == "chart":
            yield from self._get_chart_urns_not_in(other_checkpoint_state)
        elif type == "dashboard":
            yield from self._get_dashboard_urns_not_in(other_checkpoint_state)

    def get_percent_entities_changed(
        self, old_checkpoint_state: "TableauCheckpointState"
    ) -> float:
        return StaleEntityCheckpointStateBase.compute_percent_entities_changed(
            [
                (self.encoded_dataset_urns, old_checkpoint_state.encoded_dataset_urns),
                (self.encoded_chart_urns, old_checkpoint_state.encoded_chart_urns),
                (
                    self.encoded_dashboard_urns,
                    old_checkpoint_state.encoded_dashboard_urns,
                ),
            ]
        )
