"""
Module for domain layer of tabular ms ssas.
"""

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from ssas.domains import DSConnection


class XMLACube(BaseModel):
    """Class for represent OLAP cube"""

    name: str = Field(alias="CUBE_NAME")
    created: Optional[str] = Field(alias="CREATED_ON")
    last_schema_update: Optional[str] = Field(alias="LAST_SCHEMA_UPDATE")
    last_processed: Optional[str] = Field(alias="LAST_DATA_UPDATE")
    schema_updated_by: Optional[str] = Field(alias="SCHEMA_UPDATED_BY")
    data_updated_by: Optional[str] = Field(alias="DATA_UPDATED_BY")
    description: Optional[str] = Field(alias="DESCRIPTION")

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
            schema_updated_by=self.schema_updated_by,
            data_updated_by=self.data_updated_by,
            description=self.description,
        )


class XMLADimension(BaseModel):
    """Class for represent dimension in OLAP cube"""

    dimension_name: Optional[str] = Field(alias="DIMENSION_NAME")
    dimension_unique_name: Optional[str] = Field(alias="DIMENSION_UNIQUE_NAME")
    dimension_caption: Optional[str] = Field(alias="DIMENSION_CAPTION")
    dimension_type: Optional[str] = Field(alias="DIMENSION_TYPE")
    default_hierarchy: Optional[str] = Field(alias="DEFAULT_HIERARCHY")
    description: Optional[str] = Field(alias="DESCRIPTION")
    query_definition: Optional[str]

    @property
    def name(self) -> Optional[str]:
        """
        Get name.

        :return: entity name.
        """
        return self.dimension_name or self.dimension_unique_name

    @property
    def additional_info(self) -> Dict[str, Any]:
        """
        Group of additional information of entity.

        :return: dictionary with information.
        """
        return dict(
            dimension_caption=self.dimension_caption,
            dimension_type=self.dimension_type,
            default_hierarchy=self.default_hierarchy,
            description=self.description,
            query_definition=self.query_definition,
        )


class XMLAMeasure(BaseModel):
    """
    Class representation of xmla measure.
    """

    measure_name: str = Field(alias="MEASURE_NAME")
    measure_unique_name: Optional[str] = Field(alias="MEASURE_UNIQUE_NAME")
    measure_caption: Optional[str] = Field(alias="MEASURE_CAPTION")
    description: Optional[str] = Field(alias="DESCRIPTION")
    expression: Optional[str] = Field(alias="EXPRESSION")

    @property
    def name(self) -> Optional[str]:
        """
        Build measure name.

        :return: measure name.
        """

        return self.measure_name or self.measure_unique_name

    @property
    def formatted_name(self) -> Optional[str]:
        """
        Reformat measure name.

        :return: reformatted measure name.
        """

        if self.name is None:
            return None
        return self.name.format(",", "-")

    @property
    def additional_info(self) -> Dict[str, Any]:
        """
        Group of additional information of entity.

        :return: dictionary with information.
        """
        return dict(
            measure_unique_name=self.measure_unique_name,
            id=self.measure_caption,
            description=self.description,
            expression=self.expression,
        )


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
        Get datasource for OLAP cube from connection string .

        :return: datasource name.
        """

        return DSConnection.from_tuple(
            [
                item.split("=")[1]
                for item in self.connection_string.replace("\n", "").split(";")
            ]
        )


class XMLADataBase(BaseModel):
    """
    Class representation of xmla database.
    """

    name: str = Field(alias="CATALOG_NAME")
    description: Optional[str] = Field(alias="DESCRIPTION")
    last_update: str = Field(alias="DATE_MODIFIED")
    compatibility_level: str = Field(alias="COMPATIBILITY_LEVEL")
    database_id: str = Field(alias="DATABASE_ID")

    @property
    def additional_info(self) -> Dict[str, Any]:
        """
        Group of additional information of entity.

        :return: dictionary with information.
        """
        return dict(
            description=self.description,
            last_update=self.last_update,
            last_schema_update=self.compatibility_level,
            last_processed=self.database_id,
        )


class XMLAServer(BaseModel):
    """
    Class representation of xmla server.
    """

    name: str = Field(alias="Name")
    id: str = Field(alias="ID")
    version: str = Field(alias="Version")
