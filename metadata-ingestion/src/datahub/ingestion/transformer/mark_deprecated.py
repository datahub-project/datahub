import logging
import time
from typing import List, Optional, cast

from datahub.configuration.common import (
    ConfigurationError,
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import DeprecationClass

logger = logging.getLogger(__name__)


class MarkDeprecatedConfig(TransformerSemanticsConfigModel):
    """Configuration for the MarkDeprecated transformer.

    Fields:
        urns: If non-empty, only these URNs are affected. Empty means all.
        deprecated: Whether to mark as deprecated.
        note: Human-readable deprecation reason.
        actor: Who performed the deprecation.
        replacement: URN of the replacement entity.
        decommission_time: Planned decommission timestamp (epoch millis).
                           Defaults to the current time at pipeline start.
        semantics: OVERWRITE (default) replaces the entire aspect.
                   PATCH merges with the server — preserves existing note,
                   actor, and decommissionTime if they are already set.
    """

    urns: List[str] = []
    deprecated: bool = True
    note: str = ""
    actor: str = "urn:li:corpuser:datahub"
    replacement: Optional[str] = None
    decommission_time: Optional[int] = None


class MarkDeprecated(BaseTransformer, SingleAspectTransformer):
    """Marks assets as deprecated (or un-deprecated).

    If ``urns`` is empty every entity flowing through the pipeline is affected.
    If ``urns`` is populated only matching entities are modified.

    With ``semantics: PATCH``, the transformer merges with the existing
    deprecation on the server — it will not overwrite fields that are already
    populated. This is useful when you want to preserve the original
    deprecation date or note set by a human.
    """

    ctx: PipelineContext
    config: MarkDeprecatedConfig

    def __init__(self, config: MarkDeprecatedConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        # Resolve decommission_time once at pipeline start
        self._decommission_time: int = (
            config.decommission_time
            if config.decommission_time is not None
            else int(time.time() * 1000)
        )
        if self.config.semantics == TransformerSemantics.PATCH and ctx.graph is None:
            raise ConfigurationError(
                "With PATCH semantics, MarkDeprecated requires a datahub_api to "
                "connect to. Consider using the datahub-rest sink or provide a "
                "datahub_api: configuration on your ingestion recipe."
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MarkDeprecated":
        config = MarkDeprecatedConfig.model_validate(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return ["dataset", "chart", "dashboard", "dataFlow", "dataJob", "container"]

    def aspect_name(self) -> str:
        return "deprecation"

    def _build_aspect(self) -> DeprecationClass:
        """Build a fresh DeprecationClass from config values."""
        return DeprecationClass(
            deprecated=self.config.deprecated,
            note=self.config.note,
            actor=self.config.actor,
            decommissionTime=self._decommission_time,
            replacement=self.config.replacement,
        )

    def _merge_with_server(
        self, entity_urn: str, new_aspect: DeprecationClass
    ) -> DeprecationClass:
        """Merge config values with existing server state.

        Existing non-empty server values take precedence — the transformer
        only fills in what is missing or explicitly changed (deprecated flag
        is always applied).
        """
        assert self.ctx.graph
        server_aspect = self.ctx.graph.get_aspect(entity_urn, DeprecationClass)

        if server_aspect is None:
            return new_aspect

        # The deprecated flag is always driven by config
        server_aspect.deprecated = new_aspect.deprecated

        # For other fields: keep existing server value if set, otherwise use config
        if not server_aspect.note and new_aspect.note:
            server_aspect.note = new_aspect.note
        if not server_aspect.actor and new_aspect.actor:
            server_aspect.actor = new_aspect.actor
        if server_aspect.decommissionTime is None and new_aspect.decommissionTime:
            server_aspect.decommissionTime = new_aspect.decommissionTime
        if server_aspect.replacement is None and new_aspect.replacement:
            server_aspect.replacement = new_aspect.replacement

        return server_aspect

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        # If urns filter is set and this entity is not in it, pass through unchanged
        if self.config.urns and entity_urn not in self.config.urns:
            return aspect

        new_aspect = self._build_aspect()

        if self.config.semantics == TransformerSemantics.PATCH:
            return cast(
                Optional[Aspect],
                self._merge_with_server(entity_urn, new_aspect),
            )

        # OVERWRITE: stamp config values, ignoring what was there before
        return cast(Optional[Aspect], new_aspect)
