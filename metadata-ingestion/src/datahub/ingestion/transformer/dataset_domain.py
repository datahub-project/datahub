import logging
from enum import auto
from typing import Callable, Dict, List, Optional, Sequence, Union, cast

from datahub.configuration._config_enum import ConfigEnum
from datahub.configuration.common import (
    ConfigurationError,
    KeyValuePattern,
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import DatasetDomainTransformer
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DomainsClass,
    MetadataChangeProposalClass,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


class TransformerOnConflict(ConfigEnum):
    """Describes the behavior of the transformer when writing an aspect that already exists."""

    DO_UPDATE = auto()  # On conflict, apply the new aspect
    DO_NOTHING = auto()  # On conflict, do not apply the new aspect


class AddDatasetDomainSemanticsConfig(TransformerSemanticsConfigModel):
    get_domains_to_add: Union[
        Callable[[str], DomainsClass],
        Callable[[str], DomainsClass],
    ]

    _resolve_domain_fn = pydantic_resolve_key("get_domains_to_add")

    is_container: bool = False
    on_conflict: TransformerOnConflict = TransformerOnConflict.DO_UPDATE


class SimpleDatasetDomainSemanticsConfig(TransformerSemanticsConfigModel):
    domains: List[str]
    on_conflict: TransformerOnConflict = TransformerOnConflict.DO_UPDATE


class PatternDatasetDomainSemanticsConfig(TransformerSemanticsConfigModel):
    domain_pattern: KeyValuePattern = KeyValuePattern.all()
    is_container: bool = False


class AddDatasetDomain(DatasetDomainTransformer):
    """Transformer that adds domains to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetDomainSemanticsConfig

    def __init__(self, config: AddDatasetDomainSemanticsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetDomain":
        config = AddDatasetDomainSemanticsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def raise_ctx_configuration_error(ctx: PipelineContext) -> None:
        if ctx.graph is None:
            raise ConfigurationError(
                "AddDatasetDomain requires a datahub_api to connect to. Consider using the datahub-rest sink or provide a datahub_api: configuration on your ingestion recipe"
            )

    @staticmethod
    def get_domain_class(
        graph: Optional[DataHubGraph], domains: List[str]
    ) -> DomainsClass:
        domain_registry: DomainRegistry = DomainRegistry(
            cached_domains=[k for k in domains], graph=graph
        )
        domain_class = DomainsClass(
            domains=[domain_registry.get_domain_urn(domain) for domain in domains]
        )
        return domain_class

    @staticmethod
    def _merge_with_server_domains(
        graph: Optional[DataHubGraph], urn: str, mce_domain: Optional[DomainsClass]
    ) -> Optional[DomainsClass]:
        if not mce_domain or not mce_domain.domains:
            # nothing to add, no need to consult server
            return None

        assert graph
        server_domain = graph.get_domain(entity_urn=urn)
        if server_domain:
            # compute patch
            # we only include domain who are not present in the server domain list
            domains_to_add: List[str] = []
            for domain in mce_domain.domains:
                if domain not in server_domain.domains:
                    domains_to_add.append(domain)
            # Lets patch
            mce_domain.domains = []
            mce_domain.domains.extend(server_domain.domains)
            mce_domain.domains.extend(domains_to_add)

        return mce_domain

    def handle_end_of_stream(
        self,
    ) -> Sequence[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        domain_mcps: List[MetadataChangeProposalWrapper] = []
        container_domain_mapping: Dict[str, List[str]] = {}

        logger.debug("Generating Domains for containers")

        if not self.config.is_container:
            return domain_mcps

        for entity_urn, domain_to_add in (
            (urn, self.config.get_domains_to_add(urn)) for urn in self.entity_map
        ):
            if not domain_to_add or not domain_to_add.domains:
                continue

            assert self.ctx.graph
            browse_paths = self.ctx.graph.get_aspect(entity_urn, BrowsePathsV2Class)
            if not browse_paths:
                continue

            for path in browse_paths.path:
                container_urn = path.urn

                if not container_urn or not container_urn.startswith(
                    "urn:li:container:"
                ):
                    continue

                if container_urn not in container_domain_mapping:
                    container_domain_mapping[container_urn] = domain_to_add.domains
                else:
                    container_domain_mapping[container_urn] = list(
                        set(
                            container_domain_mapping[container_urn]
                            + domain_to_add.domains
                        )
                    )

        for urn, domains in container_domain_mapping.items():
            domain_mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=DomainsClass(domains=domains),
                )
            )

        return domain_mcps

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_domain_aspect: DomainsClass = cast(DomainsClass, aspect)
        domain_aspect: DomainsClass = DomainsClass(domains=[])
        # Check if we have received existing aspect
        if in_domain_aspect is not None and self.config.replace_existing is False:
            domain_aspect.domains.extend(in_domain_aspect.domains)

        domain_to_add = self.config.get_domains_to_add(entity_urn)

        domain_aspect.domains.extend(domain_to_add.domains)

        final_aspect: Optional[DomainsClass] = domain_aspect
        if domain_aspect.domains:
            if self.config.on_conflict == TransformerOnConflict.DO_NOTHING:
                assert self.ctx.graph
                server_domain = self.ctx.graph.get_domain(entity_urn)
                if server_domain and server_domain.domains:
                    return None
            if self.config.semantics == TransformerSemantics.PATCH:
                final_aspect = AddDatasetDomain._merge_with_server_domains(
                    self.ctx.graph, entity_urn, domain_aspect
                )
        return cast(Optional[Aspect], final_aspect)


class SimpleAddDatasetDomain(AddDatasetDomain):
    """Transformer that adds a specified set of domains to each dataset."""

    def __init__(
        self, config: SimpleDatasetDomainSemanticsConfig, ctx: PipelineContext
    ):
        AddDatasetDomain.raise_ctx_configuration_error(ctx)
        domains = AddDatasetDomain.get_domain_class(ctx.graph, config.domains)
        generic_config = AddDatasetDomainSemanticsConfig(
            get_domains_to_add=lambda _: domains,
            **config.dict(exclude={"domains"}),
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleAddDatasetDomain":
        config = SimpleDatasetDomainSemanticsConfig.parse_obj(config_dict)
        return cls(config, ctx)


class PatternAddDatasetDomain(AddDatasetDomain):
    """Transformer that adds a specified set of domains to each dataset."""

    def __init__(
        self, config: PatternDatasetDomainSemanticsConfig, ctx: PipelineContext
    ):
        AddDatasetDomain.raise_ctx_configuration_error(ctx)

        domain_pattern = config.domain_pattern

        def resolve_domain(domain_urn: str) -> DomainsClass:
            domains = domain_pattern.value(domain_urn)
            return self.get_domain_class(ctx.graph, domains)

        generic_config = AddDatasetDomainSemanticsConfig(
            get_domains_to_add=resolve_domain,
            semantics=config.semantics,
            replace_existing=config.replace_existing,
            is_container=config.is_container,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetDomain":
        config = PatternDatasetDomainSemanticsConfig.parse_obj(config_dict)
        return cls(config, ctx)
