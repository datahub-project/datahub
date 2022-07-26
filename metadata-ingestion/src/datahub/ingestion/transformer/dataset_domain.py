from typing import Callable, List, Optional, Union, cast

from datahub.configuration.common import (
    ConfigurationError,
    KeyValuePattern,
    Semantics,
    SemanticsTransformerConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import DatasetDomainTransformer
from datahub.metadata.schema_classes import DomainsClass


class AddDatasetDomainConfig(SemanticsTransformerConfigModel):
    get_domains_to_add: Union[
        Callable[[str], DomainsClass],
        Callable[[str], DomainsClass],
    ]

    _resolve_domain_fn = pydantic_resolve_key("get_domains_to_add")


class SimpleDatasetDomainConfig(SemanticsTransformerConfigModel):
    domain_urns: List[str]


class PatternDatasetDomainConfig(SemanticsTransformerConfigModel):
    domain_pattern: KeyValuePattern = KeyValuePattern.all()


class AddDatasetDomain(DatasetDomainTransformer):
    """Transformer that adds domains to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetDomainConfig

    def __init__(self, config: AddDatasetDomainConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        if self.config.semantics == Semantics.PATCH and self.ctx.graph is None:
            raise ConfigurationError(
                "With PATCH semantics, AddDatasetDomain requires a datahub_api to connect to. Consider using the datahub-rest sink or provide a datahub_api: configuration on your ingestion recipe"
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetDomain":
        config = AddDatasetDomainConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def get_domains_to_set(
        graph: DataHubGraph, urn: str, mce_domain: Optional[DomainsClass]
    ) -> Optional[DomainsClass]:
        if not mce_domain or not mce_domain.domains:
            # nothing to add, no need to consult server
            return None

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

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:

        in_domain_aspect: DomainsClass = cast(DomainsClass, aspect)
        domain_aspect = DomainsClass(domains=[])
        # Check if we have received existing aspect
        if in_domain_aspect is not None and self.config.replace_existing is False:
            domain_aspect.domains.extend(in_domain_aspect.domains)

        domain_to_add = self.config.get_domains_to_add(entity_urn)

        domain_aspect.domains.extend(domain_to_add.domains)

        if self.config.semantics == Semantics.PATCH:
            assert self.ctx.graph
            domain_aspect = AddDatasetDomain.get_domains_to_set(
                self.ctx.graph, entity_urn, domain_aspect
            )  # type: ignore[assignment]
        # ignore mypy errors as Aspect is not a concrete class
        return domain_aspect  # type: ignore[return-value]


class SimpleAddDatasetDomain(AddDatasetDomain):
    """Transformer that adds a specified set of domains to each dataset."""

    def __init__(self, config: SimpleDatasetDomainConfig, ctx: PipelineContext):
        domain = DomainsClass(domains=config.domain_urns)

        generic_config = AddDatasetDomainConfig(
            get_domains_to_add=lambda _: domain,
            semantics=config.semantics,
            replace_existing=config.replace_existing,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleAddDatasetDomain":
        config = SimpleDatasetDomainConfig.parse_obj(config_dict)
        return cls(config, ctx)


class PatternAddDatasetDomain(AddDatasetDomain):
    """Transformer that adds a specified set of domains to each dataset."""

    def __init__(self, config: PatternDatasetDomainConfig, ctx: PipelineContext):
        domain_pattern = config.domain_pattern

        generic_config = AddDatasetDomainConfig(
            get_domains_to_add=lambda urn: DomainsClass(
                domains=[domain for domain in domain_pattern.value(urn)]
            ),
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetDomain":
        config = PatternDatasetDomainConfig.parse_obj(config_dict)
        return cls(config, ctx)
