"""
Module for domain layer of multidimension ms ssas.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator

from ssas.domains import DSConnection

# pylint: disable=C0103


class XMLAAttribute(BaseModel):
    """
    Class representation of xmla attribute.
    """

    attribute_id: Optional[str] = Field(alias="AttributeID")
    canonic_id: Optional[str] = Field(alias="ID")
    source: Optional[Union[str, List[str]]] = Field(alias="KeyColumns")

    @property
    def id(self) -> Optional[str]:
        """
        Get identifier.

        :return: entity identifier.
        """
        return self.canonic_id or self.attribute_id

    @validator("source", pre=True)
    def validate_source(cls, value):
        """
        Validate datasource value.
        Extract data from xmla structure.

        :return: string datasource or list of datasources.
        """
        if value is None:
            return value
        res = []
        if isinstance(value, list):
            for column in value:
                key_column = column["KeyColumn"]
                if isinstance(key_column, list):
                    for source in key_column:
                        table_id = source["Source"].get("TableID")
                        if table_id:
                            res.append(table_id)
                    continue
                table_id = key_column["Source"].get("TableID")
                if table_id:
                    res.append(table_id)
            return res or None
        key_column = value["KeyColumn"]
        if isinstance(key_column, list):
            for source in key_column:
                table_id = source["Source"].get("TableID")
                if table_id:
                    res.append(table_id)
            return res or None
        return key_column["Source"].get("TableID")


class XMLADimension(BaseModel):
    """
    Class representation of xmla dimension.
    """

    canonic_name: Optional[str] = Field(alias="Name")
    incube_name: Optional[str] = Field(alias="CubeDimensionID")
    canonic_id: Optional[str] = Field(alias="ID")
    dimension_id: Optional[str] = Field(alias="DimensionID")
    attributes: List[XMLAAttribute] = Field(default_factory=list, alias="Attributes")

    @property
    def sources(self) -> List[str]:
        """
        Get list of datasources.

        :return: list of string datasources.
        """
        sources = []
        for attribute in self.attributes:
            current_source = attribute.source
            if current_source:
                if isinstance(current_source, list):
                    sources += current_source
                    continue
                sources.append(current_source)

        return sources

    @validator("attributes", pre=True)
    def validate_attributes(cls, value):
        """
        Validate attributes value.
        Extract data from xmla structure.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        attributes = value["Attribute"]
        if isinstance(attributes, list):
            return attributes
        return [attributes]

    @property
    def name(self) -> Optional[str]:
        """
        Get name.

        :return: entity name.
        """
        return self.canonic_name or self.incube_name

    @property
    def id(self) -> Optional[str]:
        """
        Get identifier.

        :return: entity identifier.
        """
        return self.canonic_id or self.dimension_id

    @property
    def additional_info(self) -> Dict[str, Any]:
        """
        Group of additional information of entity.

        :return: dictionary with information.
        """
        return dict(
            dimension_id=self.dimension_id,
        )


class XMLADimensionsContainer(BaseModel):
    """
    Class representation of xmla dimensions group.
    """

    dimensions: List[XMLADimension] = Field(alias="Dimension")
    current_index: int = 0

    @property
    def sources(self) -> List[str]:
        """
        Get list of datasources.

        :return: list of string datasources.
        """
        sources = []
        for dimension in self.dimensions:
            sources += dimension.sources
        return sources

    @validator("dimensions", pre=True)
    def validate_dimensions(cls, value):
        """
        Validate dimensions value.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        if isinstance(value, list):
            return value
        return [value]

    def __iter__(self) -> "XMLADimensionsContainer":  # type: ignore
        self.current_index = 0
        return self

    def __next__(self) -> XMLADimension:
        if self.current_index < len(self.dimensions):
            next_item = self.dimensions[self.current_index]
            self.current_index += 1
            return next_item
        raise StopIteration


class XMLAMeasure(BaseModel):
    """
    Class representation of xmla measure.
    """

    name: str = Field(alias="Name")
    id: str = Field(alias="ID")
    type: str = Field(alias="DataType")
    source: Optional[Union[str, List[str]]] = Field(alias="Source")

    @validator("source", pre=True)
    def validate_source(cls, value):
        """
        Validate datasources value.
        Extract data from xmla structure.

        :return: string datasource or list of datasources.
        """
        source_data = value["Source"]
        if isinstance(source_data, list):
            res = []
            for source in source_data:
                table_id = source.get("TableID")
                if table_id:
                    res.append(table_id)
            return res
        return source_data.get("TableID")

    @property
    def additional_info(self) -> Dict[str, Any]:
        """
        Group of additional information of entity.

        :return: dictionary with information.
        """
        return dict(
            id=self.id,
            type=self.type,
        )


class XMLAMeasures(BaseModel):
    """
    Class representation of xmla measures group.
    """

    measures: List[XMLAMeasure] = Field(alias="MeasureGroup")
    dimensions: List[XMLADimensionsContainer] = Field(alias="MeasureGroup")
    current_index: int = 0

    @property
    def sources(self) -> List[str]:
        """
        Get list of datasources.

        :return: list of string datasources.
        """
        sources = []
        for measure in self.measures:
            source = measure.source
            if source is None:
                continue
            if isinstance(source, list):
                sources += source
                continue
            sources.append(source)
        for dimension in self.dimensions:
            sources += dimension.sources
        return sources

    @validator("dimensions", pre=True)
    def validate_dimensions(cls, value):
        """
        Validate dimensions value.
        Extract data from xmla structure.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        res = []
        if isinstance(value, dict):
            value = [value]
        for item in value:
            dimensions = item.get("Dimensions")
            if dimensions:
                res.append(dimensions)
        return res

    @validator("measures", pre=True)
    def validate_measures(cls, value):
        """
        Validate measures value.
        Extract measures from xmla stucture.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        res = []
        if isinstance(value, dict):
            value = [value]
        for item in value:
            measures_group = item["Measures"]
            if not isinstance(measures_group, list):
                measures_group = [measures_group]
            for val in measures_group:
                measures = val["Measure"]
                if isinstance(measures, list):
                    res += measures
                    continue
                if isinstance(measures, dict):
                    res += [measures]
        return res

    def __iter__(self) -> "XMLAMeasures":
        self.current_index = 0
        return self

    def __next__(self) -> XMLAMeasure:
        if self.current_index < len(self.measures):
            next_item = self.measures[self.current_index]
            self.current_index += 1
            return next_item
        raise StopIteration


class XMLASchemaBinding(BaseModel):
    """
    Class representation of xmla schemabinding.
    """

    name: str = Field(alias="@name")
    table_name: str = Field(alias="@msprop:DbTableName")
    schema_name: Optional[str] = Field(alias="@msprop:DbSchemaName")
    type: str = Field(alias="@msprop:TableType")


class XMLADataSourceView(BaseModel):
    """
    Class representation of xmla datasourceview.
    """

    name: str = Field(alias="Name")
    id: Optional[str] = Field(alias="ID")
    source_id: Optional[str] = Field(alias="DataSourceID")
    sources: List[XMLASchemaBinding] = Field(alias="Schema")

    @property
    def datasource_id(self) -> Optional[str]:
        """
        Get identifier.

        :return: identifier of source.
        """
        return self.source_id or self.id

    @validator("sources", pre=True)
    def validate_sources(cls, value):
        """
        Validate datasources value.
        Extract data from xmla structure.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        res = value["xs:schema"]["xs:element"]["xs:complexType"]["xs:choice"][
            "xs:element"
        ]
        if isinstance(res, dict):
            return [res]
        return res


class XMLDataSourceViewsContainer(BaseModel):
    """
    Class representation of xmla datasourcesview group.
    """

    datasources: List[XMLADataSourceView] = Field(alias="DataSourceView")
    current_index: int = 0

    @validator("datasources", pre=True)
    def validate_datasources(cls, value):
        """
        Validate datasources value.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        if isinstance(value, list):
            return value
        return [value]

    def __iter__(self) -> "XMLDataSourceViewsContainer":  # type: ignore
        self.current_index = 0
        return self

    def __next__(self) -> XMLADataSourceView:
        if self.current_index < len(self.datasources):
            next_item = self.datasources[self.current_index]
            self.current_index += 1
            return next_item
        raise StopIteration


class XMLADataSource(BaseModel):
    """
    Class representation of xmla datasource.
    """

    name: str = Field(alias="Name")
    id: str = Field(alias="ID")
    connection_string: str = Field(alias="ConnectionString")

    @property
    def connection(self) -> DSConnection:
        """
        Get connection representation.

        :return: connection of datasource.
        """
        return DSConnection.from_tuple(
            [
                item.split("=")[1]
                for item in self.connection_string.replace("\n", "").split(";")
            ]
        )


class XMLADataSourcesContainer(BaseModel):
    """
    Class representation of xmla datasources group.
    """

    datasources: List[XMLADataSource] = Field(alias="DataSource")
    current_index: int = 0

    @validator("datasources", pre=True)
    def validate_datasources(cls, value):
        """
        Validate datasources value.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        if isinstance(value, list):
            return value
        return [value]

    def __iter__(self) -> "XMLADataSourcesContainer":  # type: ignore
        self.current_index = 0
        return self

    def __next__(self) -> XMLADataSource:
        if self.current_index < len(self.datasources):
            next_item = self.datasources[self.current_index]
            self.current_index += 1
            return next_item
        raise StopIteration

    def get_source_by_id(self, id: str) -> Optional[XMLADataSource]:
        """
        Find source by id.

        :param id: source identifier.
        :return: source if id exists else None.
        """
        for source in self.datasources:
            if source.id == id:
                return source
        return None


class XMLACube(BaseModel):
    """
    Class representation of xmla cube.
    """

    name: str = Field(alias="Name")
    id: str = Field(alias="ID")
    created: Optional[str] = Field(alias="CreatedTimestamp")
    last_schema_update: Optional[str] = Field(alias="LastSchemaUpdate")
    last_processed: Optional[str] = Field(alias="LastProcessed")
    dimensions: XMLADimensionsContainer = Field(alias="Dimensions")
    measures: XMLAMeasures = Field(alias="MeasureGroups")

    @property
    def sources_ids(self) -> List[str]:
        """
        Get list of unique sources.

        :return: list with unique entities of string sources ids.
        """
        return list(set(self.measures.sources))

    @property
    def additional_info(self) -> Dict[str, Any]:
        """
        Group of additional information of entity.

        :return: dictionary with information.
        """
        return dict(
            created=self.created,
            last_schema_update=self.last_schema_update,
            last_processed=self.last_processed,
        )


class XMLACubesContainer(BaseModel):
    """
    Class representation of xmla cubes group.
    """

    cubes: List[XMLACube] = Field(alias="Cube")
    current_index: int = 0

    @validator("cubes", pre=True)
    def validate_cubes(cls, value):
        """
        Validate cubes value.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        if isinstance(value, list):
            return value
        return [value]

    def __iter__(self) -> "XMLACubesContainer":  # type: ignore
        self.current_index = 0
        return self

    def __next__(self) -> XMLACube:
        if self.current_index < len(self.cubes):
            next_item = self.cubes[self.current_index]
            self.current_index += 1
            return next_item
        raise StopIteration


@dataclass
class DataSource:
    """
    Datasource representation.
    """

    id: str
    source: str
    server: str
    instance: Optional[str]
    db: str
    schema: str
    name: str
    type: str


class XMLADataBase(BaseModel):
    """
    Class representation of xmla database.
    """

    name: str = Field(alias="Name")
    created: Optional[str] = Field(alias="CreatedTimestamp")
    last_update: Optional[str] = Field(alias="LastUpdate")
    last_schema_update: Optional[str] = Field(alias="LastUpdate")
    last_processed: Optional[str] = Field(alias="LastSchemaUpdate")
    dimensions: XMLADimensionsContainer = Field(
        alias="Dimensions",
        default_factory=lambda: XMLADimensionsContainer(Dimension=[]),
    )
    cubes: XMLACubesContainer = Field(
        alias="Cubes", default_factory=lambda: XMLACubesContainer(Cube=[])
    )
    datasources: XMLADataSourcesContainer = Field(
        alias="DataSources",
        default_factory=lambda: XMLADataSourcesContainer(DataSource=[]),
    )
    datasourceviews: XMLDataSourceViewsContainer = Field(
        alias="DataSourceViews",
        default_factory=lambda: XMLDataSourceViewsContainer(DataSourceView=[]),
    )

    @property
    def sources(self) -> List[DataSource]:
        """
        Get database sources.

        :return: list of sources.
        """
        sources: List[DataSource] = []
        for source_view in self.datasourceviews:
            datasource_id = source_view.datasource_id
            if datasource_id is None:
                continue
            datasource = self.datasources.get_source_by_id(datasource_id)
            if not datasource:
                continue
            connection = datasource.connection
            server = connection.data_source
            instance = None

            try:
                server, instance = connection.data_source.split("\\")
            except ValueError:
                pass
            for source in source_view.sources:
                if not source.schema_name:
                    continue
                sources.append(
                    DataSource(
                        id=source.name,
                        source=connection.type,
                        server=server,
                        instance=instance,
                        db=connection.initial_catalog,
                        schema=source.schema_name,
                        name=source.table_name,
                        type=source.type,
                    )
                )
        return sources

    @property
    def additional_info(self) -> Dict[str, Any]:
        """
        Group of additional information of entity.

        :return: dictionary with information.
        """
        return dict(
            created=self.created,
            last_update=self.last_update,
            last_schema_update=self.last_schema_update,
            last_processed=self.last_processed,
        )


class XMLADataBasesContainer(BaseModel):
    """
    Class representation of xmla databases group.
    """

    databases: List[XMLADataBase] = Field(alias="Database")
    current_index: int = 0

    @validator("databases", pre=True)
    def validate_databases(cls, value):
        """
        Validate databases value.
        Convert to list if it is not list instance.

        :return: list type value.
        """
        if isinstance(value, list):
            return value
        return [value]

    def __iter__(self) -> "XMLADataBasesContainer":  # type: ignore
        self.current_index = 0
        return self

    def __next__(self) -> XMLADataBase:
        if self.current_index < len(self.databases):
            next_item = self.databases[self.current_index]
            self.current_index += 1
            return next_item
        raise StopIteration


class XMLAServer(BaseModel):
    """
    Class representation of xmla server.
    """

    name: str = Field(alias="Name")
    id: str = Field(alias="ID")
    version: str = Field(alias="Version")
    databases: XMLADataBasesContainer = Field(
        alias="Databases",
        default_factory=lambda: XMLADataBasesContainer(Database=[]),
    )

    @property
    def databases_names(self) -> List[str]:
        """
        Get list of databases.

        :return: list of string databases names.
        """
        return [database.name for database in self.databases.databases]
