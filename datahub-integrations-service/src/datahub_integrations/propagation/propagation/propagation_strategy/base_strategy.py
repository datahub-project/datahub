from datahub.configuration.common import ConfigModel
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.plugin.action.stats_util import ActionStageReport

from datahub_integrations.propagation.propagation.propagation_rule_config import (
    EntityLookup,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    PropagationDirective,
    SourceDetails,
)
from datahub_integrations.propagation.propagation.propagator import (
    EntityPropagator,
    PropagationOutput,
)


class BaseStrategyConfig(ConfigModel):
    max_propagation_fanout: int = 1000


class BaseStrategy:
    def __init__(
        self,
        graph: AcrylDataHubGraph,
        config: BaseStrategyConfig,
        stats: ActionStageReport,
    ):
        self.graph = graph
        self.config = config
        self._stats = stats

    def propagate(
        self,
        entity_lookup: EntityLookup,
        propagator: EntityPropagator,
        directive: PropagationDirective,
        context: SourceDetails,
    ) -> PropagationOutput:
        raise NotImplementedError("Subclasses must implement this method")
