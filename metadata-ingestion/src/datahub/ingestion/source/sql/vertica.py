from functools import cache
import re
import math
from textwrap import dedent
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import json
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


from datahub.ingestion.source.vertica.common import (
    Vertica_BasicSQLAlchemyConfig,
    Vertica_SQLAlchemySource,

)

from datahub.utilities import config_clean


class UUID(String):
    """The SQL UUID type."""

    __visit_name__ = "UUID"



def TIMESTAMP_WITH_TIMEZONE(*args, **kwargs):
    kwargs['timezone'] = True
    return TIMESTAMP(*args, **kwargs)

def TIME_WITH_TIMEZONE(*args, **kwargs):
    kwargs['timezone'] = True
    return TIME(*args, **kwargs)


def get_view_definition(self, connection, view_name, schema=None, **kw):

    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {
            "schema": schema.lower()
        }
    else:
        schema_condition = "1"

    view_def = connection.scalar(
        sql.text(
            dedent(
                """
                SELECT VIEW_DEFINITION
                FROM V_CATALOG.VIEWS
                WHERE table_name='%(view_name)s' AND %(schema_condition)s
                """
                % {"view_name": view_name, "schema_condition": schema_condition}
            )
        )
    )

    return view_def



def get_models_names(self, connection, schema=None, **kw):
        
        if schema is not None:
            schema_condition = "lower(schema_name) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"

        
        get_models_sql = sql.text(dedent("""
            SELECT model_name 
            FROM models
            WHERE lower(schema_name) =  '%(schema)s'
            ORDER BY model_name
        """ % {'schema': schema}))

        c = connection.execute(get_models_sql)
        
        
        return [row[0] for row in c]
def get_projection_names(self, connection, schema=None, **kw):
    if schema is not None:
        schema_condition = "lower(projection_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"
        
        

    get_projection_sql = sql.text(dedent("""
        SELECT projection_name
        from v_catalog.projections
        WHERE lower(projection_schema) =  '%(schema)s'
        ORDER BY projection_name
        """  % {'schema': schema}))
    
    c = connection.execute(get_projection_sql)
    
    return [row[0] for row in c]
        
        
    
    
def get_Oauth_names(self, connection, schema=None, **kw):

    
        get_oauth_sql = sql.text(dedent("""
            SELECT auth_name from v_catalog.client_auth
            WHERE auth_method = 'OAUTH'
        """ % {'schema': schema}))
        print("auth connection", schema)
        c = connection.execute(get_oauth_sql)
        
        return [row[0] for row in c]

def get_oauth_comment(self, connection, model_name, schema=None, **kw):
    
    get_oauth_comments = sql.text(dedent("""
                        SELECT auth_oid ,is_auth_enabled, is_fallthrough_enabled,auth_parameters ,auth_priority ,address_priority from v_catalog.client_auth
                            WHERE auth_method = 'OAUTH'

                            """ ))
    client_id = ""
    client_secret = ""
    for data in connection.execute(get_oauth_comments):
        
        whole_data = str(data['auth_parameters']).split(", ")
        client_id_data = whole_data[0].split("=")
        if client_id_data:
            # client_data.update({client_id_data[0] : client_id_data[1]})
            client_id = client_id_data[1]
            
        client_secret_data = whole_data[1].split("=")
        if client_secret_data:
            # client_data.update({client_secret_data[0] : client_secret_data[1]})
            client_secret = client_secret_data[1]
        
        client_discorvery_url =  whole_data[2].split("=")
        if client_discorvery_url:
            # client_data.update({client_secret_data[0] : client_secret_data[1]})
            discovery_url = client_discorvery_url[1]
            
        client_introspect_url =  whole_data[3].split("=")
        if client_introspect_url:
            # client_data.update({client_secret_data[0] : client_secret_data[1]})
            introspect_url = client_introspect_url[1]
            
        auth_oid = data['auth_oid']
        is_auth_enabled = data['is_auth_enabled']
        auth_priority = data['auth_priority']
        address_priority = data['address_priority']
        is_fallthrough_enabled = data['is_fallthrough_enabled']
       
        
    # print(client_data)
    return {"text": "This Vertica module is still is development Process", "properties": {"discovery_url ": str(discovery_url),
            "client_id  ": str(client_id),"introspect_url ":str(introspect_url), "auth_oid ":str(auth_oid),"client_secret ":str(client_secret),
            "is_auth_enabled":str(is_auth_enabled), "auth_priority ":str(auth_priority),"address_priority ":str(address_priority),"is_fallthrough_enabled":str(is_fallthrough_enabled), }}
    
    
    
      
def get_columns(self, connection, table_name, schema=None, **kw):
    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"

    s = sql.text(dedent("""
        SELECT column_name, data_type, column_default,is_nullable
        FROM v_catalog.columns
        WHERE lower(table_name) = '%(table)s'
        AND %(schema_condition)s
        UNION ALL
        SELECT column_name, data_type, '' as column_default, true as is_nullable
        FROM v_catalog.view_columns
        WHERE lower(table_name) = '%(table)s'
        AND %(schema_condition)s
        UNION ALL
        SELECT projection_column_name, data_type, '' as column_default, true as is_nullable
        FROM v_catalog.projection_columns
        WHERE lower(projection_name) = '%(table)s'
        AND %(schema_condition)s
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

    spk = sql.text(dedent("""
            SELECT column_name
            FROM v_catalog.primary_keys
            WHERE lower(table_name) = '%(table)s'
            AND constraint_type = 'p'
            AND %(schema_condition)s
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

    pk_columns = [x[0] for x in connection.execute(spk)]

    columns = []

    for row in connection.execute(s):

        name = row.column_name
        dtype = row.data_type.lower()
        primary_key = name in pk_columns
        default = row.column_default
        nullable = row.is_nullable

        column_info = self._get_column_info(
            name,
            dtype,
            default,
            nullable,
            schema)

        # primaryKeys = self.get_pk_constraint(connection, table_name,schema)

        column_info.update({'primary_key': primary_key})

        columns.append(column_info)

    return columns


def get_projections_columns(self, connection, projection_name, schema=None, **kw):
    if schema is not None:
        schema_condition = "lower(projection_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"

    s = sql.text(dedent("""
        SELECT projection_name
        FROM v_catalog.projections
        WHERE lower(projection_name) = '%(projection)s'
        AND %(schema_condition)s
        """ % {'projection': projection_name.lower(), 'schema_condition': schema_condition}))

    columns = []
    for row in connection.execute(s):
        columns.append(row)

    return columns


def get_pk_constraint(self, connection, table_name, schema: None, **kw):
    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"

    spk = sql.text(dedent("""
            SELECT column_name
            FROM v_catalog.primary_keys
            WHERE lower(table_name) = '%(table)s'
            AND constraint_type = 'p'
            AND %(schema_condition)s
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

    pk_columns = []

    for row in connection.execute(spk):
        columns = row['column_name']
        pk_columns.append(columns)

    # print(pk_columns)
    return {'constrained_columns': pk_columns , 'name': pk_columns}


def _get_column_info(  # noqa: C901
    self, name, data_type, default, is_nullable , schema=None,
):

    attype: str = re.sub(r"\(.*\)", "", data_type)

    charlen = re.search(r"\(([\d,]+)\)", data_type)
    if charlen:
        charlen = charlen.group(1)  # type: ignore
    args = re.search(r"\((.*)\)", data_type)
    if args and args.group(1):
        args = tuple(re.split(r"\s*,\s*", args.group(1)))  # type: ignore
    else:
        args = ()  # type: ignore
    kwargs: Dict[str, Any] = {}

    if attype == "numeric":
        if charlen:
            prec, scale = charlen.split(",")  # type: ignore
            args = (int(prec), int(scale))  # type: ignore
        else:
            args = ()  # type: ignore
    elif attype == "integer":
        args = ()  # type: ignore
    elif attype in ("timestamptz", "timetz"):
        kwargs["timezone"] = True
        if charlen:
            kwargs["precision"] = int(charlen)  # type: ignore
        args = ()  # type: ignore
    elif attype in ("timestamp", "time"):
        kwargs["timezone"] = False
        if charlen:
            kwargs["precision"] = int(charlen)  # type: ignore
        args = ()  # type: ignore
    elif attype.startswith("interval"):
        field_match = re.match(r"interval (.+)", attype, re.I)
        if charlen:
            kwargs["precision"] = int(charlen)  # type: ignore
        if field_match:
            kwargs["fields"] = field_match.group(1)  # type: ignore
        attype = "interval"
        args = ()  # type: ignore
    elif attype == "date":
        args = ()  # type: ignore
    elif charlen:
        args = (int(charlen),)  # type: ignore

    while True:
        if attype.upper() in self.ischema_names:
            coltype = self.ischema_names[attype.upper()]
            break
        else:
            coltype = None
            break

    self.ischema_names["UUID"] = UUID
    self.ischema_names["TIMESTAMPTZ"] = TIMESTAMP_WITH_TIMEZONE
    self.ischema_names["TIMETZ"] = TIME_WITH_TIMEZONE

    if coltype:
        coltype = coltype(*args, **kwargs)
    else:
        util.warn("Did not recognize type '%s' of column '%s'" % (attype, name))
        coltype = sqltypes.NULLTYPE
    # adjust the default value
    autoincrement = False
    if default is not None:
        match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
        if match is not None:
            if issubclass(coltype._type_affinity, sqltypes.Integer):
                autoincrement = True
            # the default is related to a Sequence
            sch = schema
            if "." not in match.group(2) and sch is not None:
                # unconditionally quote the schema name.  this could
                # later be enhanced to obey quoting rules /
                # "quote schema"
                default = (
                    match.group(1)
                    + ('"%s"' % sch)
                    + "."
                    + match.group(2)
                    + match.group(3)
                )

    column_info = dict(
        name=name,
        type=coltype,
        nullable=is_nullable,
        default=default,
        comment=default,
        autoincrement=autoincrement,

    )

    return column_info


def get_table_comment(self, connection, table_name, schema=None, **kw):

    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"

    sct = sql.text(dedent("""
            SELECT create_time , table_name
            FROM v_catalog.tables
            WHERE lower(table_name) = '%(table)s'
            AND %(schema_condition)s
            UNION ALL
            SELECT create_time , table_name
            FROM V_CATALOG.VIEWS
            WHERE lower(table_name) = '%(table)s'
            AND %(schema_condition)s
            
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

    sts = sql.text(dedent("""
            SELECT ROUND(SUM(used_bytes) / 1024 ) AS table_size
            FROM v_monitor.column_storage
            WHERE lower(anchor_table_name) = '%(table)s'
            
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

    # src = sql.text(dedent("""
    #     SELECT ros_count
    #     FROM v_monitor.projection_storage
    #     WHERE lower(anchor_table_name) = '%(table)s'
    # """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))
    columns = ""
    for column in connection.execute(sct):
        columns = column['create_time']

    # ros_count = ""
    # for data in connection.execute(src):
    #     ros_count = data['ros_count']

    for table_size in connection.execute(sts):
        if table_size[0] is None:
            TableSize = 0
        else:
            TableSize = math.trunc(table_size['table_size'])

    return {"text": "This Vertica module is still is development Process", "properties": {"create_time": str(columns), "Total_Table_Size": str(TableSize) + " KB"}}


def get_projection_comment(self, connection, projection_name, schema=None, **kw):
    if schema is not None:
        schema_condition = "lower(projection_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"

    src = sql.text(dedent("""
            SELECT ros_count 
            FROM v_monitor.projection_storage
            WHERE lower(projection_name) = '%(table)s'

        """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

    sig = sql.text(dedent("""
            SELECT is_segmented 
            FROM v_catalog.projections 
            WHERE lower(projection_name) = '%(table)s'
        """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

    spk = sql.text(dedent("""
            SELECT   partition_key
            FROM v_monitor.partitions
            WHERE lower(projection_name) = '%(table)s'
            LIMIT 1
        """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

    spt = sql.text(dedent("""
            SELECT is_super_projection,is_key_constraint_projection,is_aggregate_projection,has_expressions
            FROM v_catalog.projections
            WHERE lower(projection_name) = '%(table)s'
        """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

    snp = sql.text(dedent("""
            SELECT Count(ros_id) as np
            FROM v_monitor.partitions
            WHERE lower(projection_name) = '%(table)s'
        """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

    ssk = sql.text(dedent("""
            SELECT  segment_expression 
            FROM v_catalog.projections
            WHERE lower(projection_name) = '%(table)s'
        """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

    sps = sql.text(dedent("""
            SELECT ROUND(used_bytes // 1024)   AS used_bytes 
            from v_monitor.projection_storage
            WHERE lower(projection_name) = '%(table)s'
        """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

    depot_pin_policy = sql.text(dedent("""
            SELECT COUNT(*)
            FROM DEPOT_PIN_POLICIES
            WHERE lower(object_name) = '%(table)s'
        """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

    ros_count = ""
    partition_key = ""
    is_segmented = ""
    projection_type = []
    partition_number = ""
    segmentation_key = ""
    projection_size = ""
    cached_projection = ""

    for data in connection.execute(sig):
        is_segmented = data['is_segmented']
        if is_segmented :
            for data in connection.execute(ssk):
                segmentation_key = str(data)

    for data in connection.execute(src):
        ros_count = data['ros_count']

    for data in connection.execute(spk):
        partition_key = data['partition_key']

    for data in connection.execute(spt):
        lst = ["is_super_projection", "is_key_constraint_projection", "is_aggregate_projection", "is_shared"]

        i = 0
        for d in range(len(data)):
            if data[i]:
                projection_type.append(lst[i])
            i += 1

    for data in connection.execute(snp):
        partition_number = data.np

    for data in connection.execute(sps):
        projection_size = data['used_bytes']

    for data in connection.execute(depot_pin_policy):
        if data[0] > 0:
            cached_projection = "True"
        else:
            cached_projection = "False"

    return {"text": "This Vertica module is still is development Process for Projections",
            "properties": {"ROS Count": str(ros_count), "is_segmented": str(is_segmented),
                           "Projection Type": str(projection_type), "Partition Key": str(partition_key),
                           "Number of Partition" : str(partition_number),
                           "Segmentation_key": segmentation_key,
                           "Projection SIze": str(projection_size) + " KB",
                           "Projection Cached": str(cached_projection)}}


def get_model_comment(self, connection, model_name, schema=None, **kw):

    if schema is not None:
        schema_condition = "lower(schema_name) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"

   
    
    model_used_by =  sql.text(dedent("""
            select owner_name from models
            where model_name = '%(model)s'
            
        """ % {'model': model_name }))
    
  
    model_attr_name  = sql.text(dedent("""
            SELECT 
                GET_MODEL_ATTRIBUTE 
                    ( USING PARAMETERS model_name='%(schema)s.%(model)s');
            
        """ % {'model': model_name,'schema': schema}))
    
   
    used_by = ""
    attr_name = []
    attr_details = []
    for data in connection.execute(model_used_by):
        used_by = data['owner_name']
        
    for data in connection.execute(model_attr_name):
       
        attributes = {"attr_name":data[0],"attr_fields":data[1],"#_of_rows":data[2]}
        
        attr_name.append(attributes)
        
    attributes_details = []
    for data in attr_name:
        attr_details_dict = dict()
        attr_names = data['attr_name']
        attr_fields = str(data['attr_fields']).split(',')
        
        get_attr_details = sql.text(dedent("""
                SELECT 
                    GET_MODEL_ATTRIBUTE 
                        ( USING PARAMETERS model_name='%(schema)s.%(model)s', attr_name='%(attr_name)s');
                
            """ % {'model': model_name,'schema': schema, 'attr_name': attr_names}))
        
        
        
        
        value_final = dict()
        attr_details_dict = {"attr_name": attr_names}
        for data in connection.execute(get_attr_details):
           
            
            if len(attr_fields) > 1:
                
                for index, each in enumerate(attr_fields):
                    if each not in value_final:
                        value_final[each] = list()
                    value_final[each].append(data[index])
                
            else:
                if attr_fields[0] not in value_final:
                    value_final[attr_fields[0]] = list()
                value_final[attr_fields[0]].append(data[0])
                
          
         
        attr_details_dict.update(value_final)
        attributes_details.append(attr_details_dict)
        
        
    return {"text": "This Vertica module is still is development Process", "properties": {"used_by": str(used_by),
            "Model Attrributes ": str(attr_name),"Model Specifications":str(attributes_details)}}

def _get_extra_tags(
    self, connection, name, schema=None
) -> Optional[Dict[str, str]]:

    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"
        
    owner_res = None
    if name == "table":
        table_owner_command = sql.text(dedent("""
            SELECT table_name, owner_name
            FROM v_catalog.tables
            WHERE %(schema_condition)s
            """ % {'schema_condition': schema_condition}))

        owner_res = connection.execute(table_owner_command)
        
    elif name == "projection":
        table_owner_command = sql.text(dedent("""
            SELECT projection_name as table_name, owner_name
            FROM v_catalog.projections
            WHERE lower(projection_schema) = '%(schema)s'
            """ % {'schema': schema.lower()}))
        owner_res = connection.execute(table_owner_command)
        
        
    elif name == "view":
        table_owner_command = sql.text(dedent("""
            SELECT table_name, owner_name
            FROM v_catalog.views
            WHERE %(schema_condition)s
            """ % {'schema_condition': schema_condition}))
        owner_res = connection.execute(table_owner_command)

    
    final_tags = dict()
    for each in owner_res:
        final_tags[each['table_name']] = each['owner_name']
    return final_tags


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
                'udx_list': udx_list , 'Udx_langauge' : user_defined_library }

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
        """ ))

        communal_storage_path = sql.text(dedent("""
            SELECT location_path from storage_locations 
                WHERE sharing_type = 'COMMUNAL'
        """ ))

        cluster_type = ""
        communical_path = ""
        communal_data_res = connection.execute(communal_storage_path)
        cluster_type_res = connection.execute(cluster_type_qry)
        for each in cluster_type_res:
            cluster_type = each.database_mode
            if cluster_type.lower() == 'eon':
                for each in communal_data_res:
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
        """ ))
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
        

VerticaDialect.get_view_definition = get_view_definition
VerticaDialect.get_columns = get_columns
VerticaDialect._get_column_info = _get_column_info
VerticaDialect.get_pk_constraint = get_pk_constraint
VerticaDialect._get_extra_tags = _get_extra_tags
VerticaDialect.get_projection_names = get_projection_names
VerticaDialect.get_table_comment = get_table_comment
VerticaDialect.get_projection_comment = get_projection_comment
VerticaDialect._get_properties_keys = _get_properties_keys
VerticaDialect.get_models_names = get_models_names
VerticaDialect.get_model_comment = get_model_comment
VerticaDialect.get_Oauth_names = get_Oauth_names
VerticaDialect.get_oauth_comment = get_oauth_comment



class VerticaConfig(Vertica_BasicSQLAlchemyConfig):
    # defaults
    scheme: str = pydantic.Field(default="vertica+vertica_python")

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


@platform_name("Vertica")
@config_class(VerticaConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
class VerticaSource(Vertica_SQLAlchemySource):
    def __init__(self, config: VerticaConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx, "vertica")

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "VerticaSource":
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    