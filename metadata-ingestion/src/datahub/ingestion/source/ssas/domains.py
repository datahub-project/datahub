from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, List, Optional, Union

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn, make_dataplatform_instance_urn
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

# pylint: disable=C0103

STATIC_TYPES = ("TABLE_TYPE", "VIEW", "USER_TABLE")


@dataclass
class Server:
    """
    Server information.
    """

    name: str
    id: str
    version: str
    type: str
    default_compatibility_level: str
    supported_compatibility_levels: str

    @property
    def as_dict(self) -> Dict[str, Any]:
        """
        Get representations as dictionary.

        :return: data in dictionary.
        """
        return asdict(self)


@dataclass
class Catalog:
    """
    Catalog information.
    """

    instance: str
    host_port: str
    name: str
    env: str
    source: str = "ssas"

    @property
    def formatted_name(self) -> str:
        """
        Get formatted catalog name.

        :return: string name.
        """
        return self.name.replace(",", "-")

    @property
    def formatted_instance(self) -> str:
        """
        Get formatted catalog instance.

        :return: string instance.
        """
        return self.instance.replace("\\", "|")

    @property
    def full_type(self) -> str:
        """
        Get catalog type.

        :return: string type.
        """
        return f"({self.source},{self.formatted_name},{self.env})"

    @property
    def orchestrator(self) -> str:
        """
        Get catalog orchestrator.

        :return: string orchestrator.
        """
        return self.source

    @property
    def cluster(self) -> str:
        """
        Get catalog cluster.

        :return: string cluster.
        """
        return f"{self.env}/{self.host_port}/{self.formatted_instance}"


@dataclass
class CubeDependency:
    """
    Cube dependency.
    """

    db: str
    schema: str
    name: str
    env: str
    type: str
    server: str
    source: str
    instance: Optional[str]

    def get_instance(self) -> str:
        """
        Get dependency instance.

        :return: string instance.
        """
        return "default" if self.instance is None else self.instance.lower()


@dataclass
class OLAPLineageStream:
    """
    Lineage representation.
    """

    dependencies: List[CubeDependency]

    @property
    def as_datasets_urn_list(self) -> List[str]:
        """
        Get representation as list of lineage urns.

        :return: list of urns.
        """
        return [
            make_dataset_urn(
                platform=dep.source,
                name=f"{dep.server}.{dep.get_instance()}.{dep.db}.{dep.schema}.{dep.name}",
                env=dep.env,
            )
            for dep in self.dependencies
            if dep.type in STATIC_TYPES
        ]

    @property
    def as_property(self) -> Dict[str, str]:
        """
        Get representation as dictionary.

        :return: dictionary of properties.
        """
        return {
            f"{dep.db}.{dep.schema}.{dep.name}": dep.type for dep in self.dependencies
        }


@dataclass
class Cube:
    """
    Datahub cube.
    """

    instance: str
    host_port: str
    name: str
    env: str
    flow: Catalog
    type: str = "CUBE"
    source: str = "ssas"

    @property
    def full_type(self) -> str:
        """
        Get cube type.

        :return: string type.
        """
        return self.source.upper() + "_" + self.type

    @property
    def formatted_name(self) -> str:
        """
        Get cube formatted name.

        :return: string name.
        """
        return self.name.replace(",", "-")

    @property
    def full_name(self) -> str:
        """
        Get cube full name.

        :return: string name.
        """
        return f"{self.flow.formatted_instance}.{self.flow.formatted_name}.{self.formatted_name}"


@dataclass
class SSASDataSet:
    """
    Datahub dataset.
    """

    entity: Cube
    platform: str = "ssas"
    type: str = "dataset"
    external_url: str = ""
    incoming: List[str] = field(default_factory=list)
    set_properties: Dict[str, str] = field(default_factory=dict)

    @property
    def name(self) -> str:
        """
        Get dataset name.

        :return: string name.
        """
        # TODO: add server to urn
        # return self.entity.formatted_name
        return self.entity.full_name

    def add_property(self, name: str, value: Union[str, float, int]) -> None:
        """
        Add property to dataset.

        :param name: property name.
        :param value: propery value
        """
        self.set_properties[name] = str(value) if value is not None else ""

    @property
    def data_platform(self) -> str:
        """
        Get dataplatform of object.

        :return: string dataplatform.
        """
        return (
            self.platform[self.platform.rindex(":") + 1:]
            if self.platform.startswith("urn:")
            else self.platform
        )

    @property
    def dataplatform_urn(self) -> str:
        """
        Get dataplatform urn of object.

        :return: string dataplatform urn.
        """
        return make_data_platform_urn(self.data_platform)

    @property
    def urn(self) -> str:
        """
        Get urn of object.

        :return: string urn.
        """
        return make_dataset_urn(self.data_platform, self.name, self.entity.env)

    @property
    def as_dataplatform_data(self) -> Dict[str, Any]:
        """
        Get data representation for dataPlatformInstance aspect.

        :return: data in dictionary.
        """

        return dict(
            aspectName="dataPlatformInstance",
            aspect=DataPlatformInstanceClass(
                platform=self.dataplatform_urn,
                instance=make_dataplatform_instance_urn(
                    self.platform,
                    self.entity.instance
                ) if self.entity.instance else None,
            )
        )

    @property
    def as_upstream_lineage_aspect_data(self) -> Dict[str, Any]:
        """
        Get data representation for upstreamLineage aspect.

        :return: data in dictionary.
        """
        return dict(
            aspectName="upstreamLineage",
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(dataset, "VIEW") for dataset in sorted(self.incoming)
                ],
            ),
        )

    @property
    def as_dataset_properties_data(self) -> Dict[str, Any]:
        """
        Get data representation for datasetProperties aspect.

        :return: data in dictionary.
        """
        return dict(
            aspectName="datasetProperties",
            aspect=DatasetPropertiesClass(
                externalUrl=self.external_url,
                customProperties=self.set_properties,
                name=self.entity.formatted_name,
            ),
        )


@dataclass
class Datasource:
    """
    Datasource information.
    """

    name: str
    db_table_name: str
    friendly_name: str
    db_schema_name: str
    table_type: str


@dataclass
class DSConnection:
    """
    Connection information.
    """

    provider: str
    data_source: str
    integrated_security: str
    initial_catalog: str

    @property
    def type(self) -> str:
        """
        Get type of connection.

        :return: string type name.
        """
        if "sql" in self.provider.lower():
            return "mssql"
        return "unknown"

    @classmethod
    def from_tuple(cls, data: List[str]) -> "DSConnection":
        """
        Create instance from tuple.

        :param data: data in tuple.
        :return: DSConnection object.
        """
        return cls(**{key.name: data[i] for i, key in enumerate(fields(cls))})
