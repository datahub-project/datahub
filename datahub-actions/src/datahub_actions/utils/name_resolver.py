from abc import abstractmethod
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ContainerPropertiesClass,
    CorpUserEditableInfoClass,
    CorpUserInfoClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    GlossaryTermInfoClass,
    SubTypesClass,
    TagPropertiesClass,
)
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.urn import Urn
from datahub_actions.utils.datahub_util import DATAHUB_SYSTEM_ACTOR_URN


class NameResolver:
    @abstractmethod
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        pass

    @abstractmethod
    def get_specialized_type(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        pass


class DefaultNameResolver(NameResolver):
    """A Default name resolver for Entities"""

    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        default_entity_name = (
            entity_urn.get_entity_id_as_string()
            if str(entity_urn) != DATAHUB_SYSTEM_ACTOR_URN
            else "System"
        )
        return default_entity_name

    def get_specialized_type(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        entity_type = entity_urn.entity_type
        entity_type = {"schemaField": "column"}.get(
            entity_urn.entity_type, entity_urn.entity_type
        )

        if datahub_graph:
            subtypes: Optional[SubTypesClass] = datahub_graph.get_aspect(
                entity_urn=str(entity_urn), aspect_type=SubTypesClass
            )
            subtype = subtypes.typeNames[0] if subtypes and subtypes.typeNames else None
            return f"{subtype or entity_type}"

        return entity_type


class DataFlowNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        dataflow_urn = DataFlowUrn.from_string(str(entity_urn))
        dataflow_name = dataflow_urn.get_flow_id()
        return dataflow_name

    def get_specialized_type(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        """We prefix the type of the dataset with the platform it is part of"""
        dataflow_urn = DataFlowUrn.from_string(str(entity_urn))
        dataflow_orch = dataflow_urn.get_orchestrator_name()
        return f"{dataflow_orch} DAG"


class DataJobNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        datajob_urn = DataJobUrn.from_string(str(entity_urn))
        datajob_name = datajob_urn.get_job_id()
        return datajob_name

    def get_specialized_type(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        """We prefix the type of the datajob with the platform it is part of"""
        datajob_urn = DataJobUrn.from_string(str(entity_urn))
        dataflow_urn = datajob_urn.get_data_flow_urn()
        dataflow_orch = dataflow_urn.get_orchestrator_name()
        return f"{dataflow_orch} Task"


class DatasetNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        dataset_urn = DatasetUrn.from_string(str(entity_urn))
        dataset_name = dataset_urn.name
        if datahub_graph:
            properties: Optional[DatasetPropertiesClass] = datahub_graph.get_aspect(
                entity_urn=str(entity_urn), aspect_type=DatasetPropertiesClass
            )
            if properties and properties.name:
                dataset_name = properties.name

        return f"{dataset_name}"

    def get_specialized_type(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        """We prefix the type of the dataset with the platform it is part of"""
        dataset_urn = DatasetUrn.from_string(str(entity_urn))
        dataset_platform = dataset_urn.get_data_platform_urn().get_entity_id_as_string()
        specialized_type = super().get_specialized_type(entity_urn, datahub_graph)
        return f"{dataset_platform} {specialized_type}"


class SchemaFieldNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        return DatasetUrn.get_simple_field_path_from_v2_field_path(
            entity_urn.entity_ids[1]
        )

    def get_specialized_type(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        return "column"


class ContainerNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        if datahub_graph:
            container_props: Optional[ContainerPropertiesClass] = (
                datahub_graph.get_aspect(
                    entity_urn=str(entity_urn), aspect_type=ContainerPropertiesClass
                )
            )
            if container_props and container_props.name:
                return container_props.name

        # fall-through to default
        return super().get_entity_name(entity_urn, datahub_graph)


class TagNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        if datahub_graph:
            tag_properties: Optional[TagPropertiesClass] = datahub_graph.get_aspect(
                entity_urn=str(entity_urn), aspect_type=TagPropertiesClass
            )
            if tag_properties and tag_properties.name:
                return tag_properties.name

        # fall-through to default
        return super().get_entity_name(entity_urn, datahub_graph)


class GlossaryTermResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        if datahub_graph:
            term_properties: Optional[GlossaryTermInfoClass] = datahub_graph.get_aspect(
                entity_urn=str(entity_urn), aspect_type=GlossaryTermInfoClass
            )
            if term_properties and term_properties.name:
                return term_properties.name

        # fall-through to default
        return super().get_entity_name(entity_urn, datahub_graph)


class CorpUserNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        entity_name = super().get_entity_name(entity_urn, datahub_graph)

        if datahub_graph:
            user_properties: Optional[CorpUserInfoClass] = datahub_graph.get_aspect(
                str(entity_urn), CorpUserInfoClass
            )
            if user_properties and user_properties.displayName:
                entity_name = user_properties.displayName

            editable_properties: Optional[CorpUserEditableInfoClass] = (
                datahub_graph.get_aspect(str(entity_urn), CorpUserEditableInfoClass)
            )
            if editable_properties and editable_properties.displayName:
                entity_name = editable_properties.displayName

        return entity_name


class ChartNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        if datahub_graph:
            chart_properties: Optional[ChartInfoClass] = datahub_graph.get_aspect(
                str(entity_urn), ChartInfoClass
            )
            if chart_properties and chart_properties.title:
                return chart_properties.title

        # fall-through to default
        return super().get_entity_name(entity_urn, datahub_graph)


class DashboardNameResolver(DefaultNameResolver):
    def get_entity_name(
        self, entity_urn: Urn, datahub_graph: Optional[DataHubGraph]
    ) -> str:
        if datahub_graph:
            dashboard_properties: Optional[DashboardInfoClass] = (
                datahub_graph.get_aspect(str(entity_urn), DashboardInfoClass)
            )
            if dashboard_properties and dashboard_properties.title:
                return dashboard_properties.title

        # fall-through to default
        return super().get_entity_name(entity_urn, datahub_graph)


class NameResolverRegistry:
    def __init__(self):
        self.registry = {
            "schemaField": SchemaFieldNameResolver(),
            "dataset": DatasetNameResolver(),
            "glossaryTerm": GlossaryTermResolver(),
            "chart": ChartNameResolver(),
            "dashboard": DashboardNameResolver(),
            "corpuser": CorpUserNameResolver(),
            "container": ContainerNameResolver(),
            "dataJob": DataJobNameResolver(),
            "dataFlow": DataFlowNameResolver(),
        }
        self.default_resolver = DefaultNameResolver()

    def get_resolver(self, entity_urn: Urn) -> NameResolver:
        return self.registry.get(entity_urn.entity_type, self.default_resolver)


_name_resolver_registry = NameResolverRegistry()


def get_entity_name_from_urn(
    entity_urn_str: str, datahub_graph: Optional[DataHubGraph]
) -> str:
    entity_urn = Urn.from_string(entity_urn_str)
    return _name_resolver_registry.get_resolver(entity_urn).get_entity_name(
        entity_urn, datahub_graph
    )


def get_entity_qualifier_from_urn(
    entity_urn_str: str, datahub_graph: Optional[DataHubGraph]
) -> str:
    entity_urn = Urn.from_string(entity_urn_str)
    return _name_resolver_registry.get_resolver(entity_urn).get_specialized_type(
        entity_urn, datahub_graph
    )
