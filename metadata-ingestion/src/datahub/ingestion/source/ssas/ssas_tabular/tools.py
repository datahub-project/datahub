"""
Module contain DVM queries for tabular SSAS
"""
from enum import Enum


class DvmQueries(str, Enum):
    """Container for DVM queries"""

    SELECT_CATALOGS: str = """
        select  [CATALOG_NAME],
                [DESCRIPTION],
                [DATE_MODIFIED],
                [COMPATIBILITY_LEVEL],
                [DATABASE_ID]
        from	$System.DBSCHEMA_CATALOGS"""
    SELECT_CUBES_BY_CATALOG: str = """
        select  [CUBE_NAME],
                [CREATED_ON],
                [LAST_SCHEMA_UPDATE],
                [LAST_DATA_UPDATE],
                [SCHEMA_UPDATED_BY],
                [DATA_UPDATED_BY],
                [DESCRIPTION]
        from $system.mdschema_cubes
        where [CATALOG_NAME] = '{catalog}'"""
    SELECT_DIMENSIONS: str = "select * from $system.mdschema_dimensions"
    SELECT_DIMENSIONS_BY_CUBE: str = """
        select  [DIMENSION_NAME],
                [DIMENSION_UNIQUE_NAME],
                [DIMENSION_CAPTION],
                [DIMENSION_TYPE],
                [DEFAULT_HIERARCHY],
                [DESCRIPTION]
        from $system.mdschema_dimensions
        where [CUBE_NAME] = '{name}'
        """
    SELECT_HIERARCHIES: str = "select * from $system.mdschema_hierarchies"
    SELECT_HIERARCHIE_BY_NAME: str = (
        "select * from $system.mdschema_hierarchies where [HIERARCHY_NAME] = '{name}'"
    )
    SELECT_MEASURES_BY_CUBE: str = """
        select  [MEASURE_NAME],
                [MEASURE_UNIQUE_NAME],
                [MEASURE_CAPTION],
                [DESCRIPTION],
                [EXPRESSION]
        from $system.mdschema_measures
        where   [CUBE_NAME] = '{name}'"""
    SELECT_MEASURES: str = "select * from $system.mdschema_measures"
    SELECT_MEASURE_BY_NAME: str = (
        "select * from $system.mdschema_measures where MEASURE_NAME = '{name}'"
    )
    SELECT_DATA_SOURCES: str = "select * from $system.TMSCHEMA_DATA_SOURCES"
    SELECT_QUERY_DEFINITION = """
        select [QueryDefinition]
        from $system.TMSCHEMA_PARTITIONS
        WHERE [Name] = '{name}'
    """

    SELECT_ANNOTATIONS: str = "select * from $system.TMSCHEMA_ANNOTATIONS"
