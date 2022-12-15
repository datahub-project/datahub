import re
from textwrap import dedent
from typing import Any, Dict, Optional, List, Tuple
from datahub.ingestion.source.vertica.common import VerticaSQLAlchemySource , SQLAlchemyConfigVertica 
import math
import pydantic
from pydantic.class_validators import validator
from sqlalchemy import sql, util
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import TIME, TIMESTAMP, String
from sqlalchemy_vertica.base import VerticaDialect

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)

from datahub.utilities import config_clean



def _get_schema_keys(self, connection, db_name, schema) -> dict:
    try:

        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}

        # Projection count
        projection_count_query = sql.text(dedent("""
            SELECT 
                COUNT(projection_name)  as pc
            from 
                v_catalog.projections 
            WHERE lower(projection_schema) = '%(schema)s'
        """ % {'schema_condition': schema_condition, "schema": schema}))

        projection_count = None
        for each in connection.execute(projection_count_query):
            projection_count = each.pc

        UDL_LANGUAGE = sql.text(dedent("""
            SELECT lib_name , description 
                FROM USER_LIBRARIES
            WHERE lower(schema_name) = '%(schema)s'
        """ % {'schema_condition': schema_condition, "schema": schema}))

        # UDX list
        UDX_functions_qry = sql.text(dedent("""
            SELECT 
                function_name 
            FROM 
                USER_FUNCTIONS
            Where schema_name  = '%(schema)s'
        """ % {'schema': schema, 'schema_condition': schema_condition}))
        udx_list = ""
        for each in connection.execute(UDX_functions_qry):
            udx_list += each.function_name + ", "

        # UDX Language
        user_defined_library = ""
        for data in connection.execute(UDL_LANGUAGE):
            user_defined_library += f"{data['lib_name']} -- {data['description']} |  "

        return {"projection_count": projection_count,
                'udx_list': udx_list , 'Udx_langauge' : user_defined_library}

    except Exception as e:
        print("Exception in _get_schema_keys from vertica ")


def _get_database_keys(self, connection, db_name) -> dict:
    try:

        # Query for CLUSTER TYPE
        cluster_type_qry = sql.text(dedent("""
            SELECT 
            CASE COUNT(*) 
                WHEN 0 
                THEN 'Enterprise' 
                ELSE 'Eon' 
            END AS database_mode 
            FROM v_catalog.shards
        """))

        communal_storage_path = sql.text(dedent("""
            SELECT location_path from storage_locations 
                WHERE sharing_type = 'COMMUNAL'
        """))

        cluster_type = ""
        communical_path = ""
        cluster_type_res = connection.execute(cluster_type_qry)
        for each in cluster_type_res:
            cluster_type = each.database_mode
            if cluster_type.lower() == 'eon':
                for each in connection.execute(communal_storage_path):
                    communical_path += str(each.location_path) + " | "

        SUBCLUSTER_SIZE = sql.text(dedent("""
                        SELECT subclusters.subcluster_name , CAST(sum(disk_space_used_mb // 1024) as varchar(10)) as subclustersize from subclusters  
                        inner join disk_storage using (node_name) 
                        group by subclusters.subcluster_name
                         """))

        subclusters = " "
        for data in connection.execute(SUBCLUSTER_SIZE):
            subclusters += f"{data['subcluster_name']} -- {data['subclustersize']} GB |  "

        cluster__size = sql.text(dedent("""
            select ROUND(SUM(disk_space_used_mb) //1024 ) as cluster_size
            from disk_storage
        """))
        cluster_size = ""
        for each in connection.execute(cluster__size):
            cluster_size = str(each.cluster_size) + " GB"

        return {"cluster_type": cluster_type, "cluster_size": cluster_size, 'Subcluster': subclusters,
                "communinal_storage_path": communical_path}

    except Exception as e:
        print("Exception in _get_database_keys")


def _get_properties_keys(self, connection, db_name, schema, level=None) -> dict:
    try:
        properties_keys = dict()
        if level == "schema":
            properties_keys = _get_schema_keys(self, connection, db_name, schema)
        if level == "database":
            properties_keys = _get_database_keys(self, connection, db_name)
        return properties_keys
    except Exception as e:
        print("Error in finding schema keys in vertica ")



VerticaDialect._get_properties_keys = _get_properties_keys


class VerticaConfig(SQLAlchemyConfigVertica):
    # defaults
    scheme: str = pydantic.Field(default="vertica+vertica_python")

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


@platform_name("Vertica")
@config_class(VerticaConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default, can be disabled via configuration `include_view_lineage` and `include_projection_lineage`")

@capability(SourceCapability.DELETION_DETECTION, "Optionally enabled via `stateful_ingestion.remove_stale_metadata`", supported=True)
class VerticaSource(VerticaSQLAlchemySource):
    def __init__(self, config: VerticaConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx, "vertica_final")
        self.view_lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None
        self.Projection_lineage_map: Optional[Dict[str, List[Tuple[str, str, str]]]] = None

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)