import re
from collections import defaultdict
from typing import Dict, List, Optional, cast

from datahub.configuration.common import (
    TransformerSemanticsConfigModel,
)
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
)
from datahub.utilities.urns.urn import guess_entity_type


class SetBrowsePathTransformerConfig(TransformerSemanticsConfigModel):
    path: List[str]


class SetBrowsePathTransformer(BaseTransformer, SingleAspectTransformer):
    ctx: PipelineContext
    config: SetBrowsePathTransformerConfig

    def __init__(self, config: SetBrowsePathTransformerConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    def aspect_name(self) -> str:
        return "browsePathsV2"

    def entity_types(self) -> List[str]:
        # this is arbitrary list, might be expanded if it makes sense
        return ["dataset", "dataJob", "dataFlow", "chart", "dashboard", "container"]

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SetBrowsePathTransformer":
        config = SetBrowsePathTransformerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _build_model(existing_browse_paths: BrowsePathsV2Class) -> Dict[str, List[str]]:
        template_vars: Dict[str, List[str]] = {}
        model: Dict[str, List[str]] = defaultdict(list)
        for entry in existing_browse_paths.path or []:
            if entry.urn:
                entity_type = guess_entity_type(entry.urn)
                model[entity_type].append(entry.urn)

        for entity_type, urns in model.items():
            template_vars[f"{entity_type}[*]"] = urns
            for i, urn in enumerate(urns):
                template_vars[f"{entity_type}[{i}]"] = [urn]

        return template_vars

    @classmethod
    def _expand_nodes(cls, templates, template_vars) -> BrowsePathsV2Class:
        expanded_nodes: List[str] = []
        for node in templates:
            resolved_nodes = cls._resolve_template_to_nodes(node, template_vars)
            expanded_nodes.extend(resolved_nodes)

        processed_entries: List[BrowsePathEntryClass] = []
        for node in expanded_nodes:
            if not node or node.isspace():
                continue
            processed_entries.append(
                BrowsePathEntryClass(
                    id=node, urn=node if node.startswith("urn:") else None
                )
            )
        return BrowsePathsV2Class(path=processed_entries)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        assert aspect is None or isinstance(aspect, BrowsePathsV2Class)
        template_vars: Dict[str, List[str]] = self._build_model(aspect)
        new_browse_paths: BrowsePathsV2Class = self._expand_nodes(
            self.config.path, template_vars
        )
        if aspect is not None and not self.config.replace_existing:
            for node in aspect.path:
                new_browse_paths.path.append(node)

        return cast(Aspect, new_browse_paths)

    @staticmethod
    def _resolve_template_to_nodes(
        template_str: str, template_vars: Dict[str, List[str]]
    ) -> List[str]:
        # This mechanism can be made simpler (match against known variables only) or more complex (e.g. by using a
        # proper templating engine, like jinja).
        var_pattern = re.findall(r"\$([a-zA-Z_][a-zA-Z0-9\[\]*]*)", template_str)
        expansions = [template_vars.get(var, [""]) for var in var_pattern]

        if not var_pattern:
            return [template_str]

        max_expansion = max(len(exp) for exp in expansions) if expansions else 1

        if max_expansion <= 1:
            result = template_str
            for var in var_pattern:
                values = template_vars.get(var, [""])
                value = values[0] if values else ""
                result = result.replace(f"${var}", value)
            return [result] if result else []

        results = []
        for var in var_pattern:
            if var in template_vars and len(template_vars[var]) > 1:
                for value in template_vars[var]:
                    result = template_str
                    result = result.replace(f"${var}", value)
                    for other_var in var_pattern:
                        if other_var != var:
                            other_values = template_vars.get(other_var, [""])
                            other_value = other_values[0] if other_values else ""
                            result = result.replace(f"${other_var}", other_value)
                    results.append(result)
                return [r for r in results if r]

        return []
