from typing import List, Optional, Set, cast

from datahub.configuration.common import ConfigModel, KeyValuePattern
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_domain import AddDatasetDomain
from datahub.ingestion.transformer.dataset_transformer import DatasetDomainTransformer
from datahub.metadata.schema_classes import DomainsClass, GlobalTagsClass


class DatasetTagDomainMapperConfig(ConfigModel):
    domain_mapping: KeyValuePattern = KeyValuePattern.all()


class DatasetTagDomainMapper(DatasetDomainTransformer):
    """A transformer that appends a predefined set of domains to each dataset that includes specific tags defined in the transformer."""

    def __init__(self, config: DatasetTagDomainMapperConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx: PipelineContext = ctx
        self.config: DatasetTagDomainMapperConfig = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "DatasetTagDomainMapper":
        config = DatasetTagDomainMapperConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        domain_mapping = self.config.domain_mapping
        assert self.ctx.graph
        global_tags: Optional[GlobalTagsClass] = self.ctx.graph.get_tags(entity_urn)
        # Check if we have tags received existing aspect
        if global_tags is None:
            return None

        # Initialize or extend the existing domain aspect
        existing_domain_aspect: DomainsClass = cast(DomainsClass, aspect)
        domain_aspect = DomainsClass(
            domains=existing_domain_aspect.domains if existing_domain_aspect else []
        )
        transformer_tags = domain_mapping.rules.keys()
        # Filter tags that match the transformer tags and remove duplicates
        tags_seen = {
            tag_item.tag
            for tag_item in global_tags.tags
            if any(tag_item.tag.endswith(key) for key in transformer_tags)
        }

        if tags_seen:
            domains_to_add = DatasetTagDomainMapper.get_matching_domains(
                domain_mapping, tags_seen
            )
            mapped_domains = AddDatasetDomain.get_domain_class(
                self.ctx.graph, domains_to_add
            )
            domain_aspect.domains.extend(mapped_domains.domains)
            # Try merging with server-side domains
            patch_domain_aspect: Optional[
                DomainsClass
            ] = AddDatasetDomain._merge_with_server_domains(
                self.ctx.graph, entity_urn, domain_aspect
            )
            return cast(Optional[Aspect], patch_domain_aspect)

        return cast(Optional[Aspect], domain_aspect)

    @staticmethod
    def get_matching_domains(
        mapping: KeyValuePattern, tags_seen: Set[str]
    ) -> List[str]:
        domains_to_add = []
        # Iterate over each seen tag
        for tag in tags_seen:
            # Check each key in the domain mapping rules to see if the tag ends with this key
            for key, domains in mapping.rules.items():
                if tag.endswith(key):
                    domains_to_add.extend(domains)
        return domains_to_add
