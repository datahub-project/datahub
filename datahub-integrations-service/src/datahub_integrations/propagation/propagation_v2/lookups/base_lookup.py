from abc import ABC, abstractmethod

from datahub_actions.api.action_graph import AcrylDataHubGraph

from datahub_integrations.actions.oss.stats_util import ActionStageReport


class BaseLookup(ABC):
    def __init__(
        self,
        graph: AcrylDataHubGraph,
        report: ActionStageReport,
    ):
        self.graph = graph
        self.report = report

    @abstractmethod
    def resolve(self) -> set[str]:
        """Return the set of resolved entity URNs based on the lookup spec."""
