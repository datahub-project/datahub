from typing import List

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import ChangeTypeClass

from sqlalchemy import create_engine
from sqlalchemy import inspect

#fill in credentials for engine
engine = create_engine("mysql+mysqlconnector://username:password@database_IP/database_name")
insp = inspect(engine)
emitter = DatahubRestEmitter("http://localhost:8080")


# get tables_names
tbl_list = []
tbl_list = insp.get_table_names()


def retrieve_foreign_keys(downstream_string):

    downstream_keys = (insp.get_foreign_keys(downstream_string))
    first_upstream_table_string = downstream_keys[0]["referred_table"]
    print(first_upstream_table_string)
    upstream_table_1 = UpstreamClass(
    dataset=f"urn:li:dataset:(urn:li:dataPlatform:mariadb,platform_instance.{first_upstream_table_string},PROD)",
    type=DatasetLineageTypeClass.TRANSFORMED,
    )  

    upstream_tables: List[UpstreamClass] = [upstream_table_1]

    for key in downstream_keys[1:]:
        upstream_table_string = key["referred_table"] 
        print(upstream_table_string)
        upstream_table_more = UpstreamClass(
	#insert platform instance at platform_instance
        dataset=f"urn:li:dataset:(urn:li:dataPlatform:mariadb,platform_instance.{upstream_table_string},PROD)", 
        type=DatasetLineageTypeClass.TRANSFORMED,
        )
        upstream_tables.append(upstream_table_more)
    return upstream_tables


def send_to_datahub(lineage_list):

    downstream_string = lineage_list[1]
    upstream_tables = retrieve_foreign_keys(downstream_string)
    
    # Construct a lineage object.
    upstream_lineage = UpstreamLineage(upstreams=upstream_tables)

    # Construct a MetadataChangeProposalWrapper object.
    downstream_table_string = lineage_list[1]
    lineage_mcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
	#insert platform instance at platform_instance
        entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:mariadb,platform_instance.{downstream_table_string},PROD)",
        aspectName="upstreamLineage",
        aspect=upstream_lineage,
    )
    print("emitting this relationship: "+str(lineage_list))
    emitter.emit_mcp(lineage_mcp)

def check_foreign_keys(tbl, most_upstream):
    for key in tbl:
        if key["referred_table"] == most_upstream:
            return True
        else: 
            continue
    return False

def looping_lineage(most_upstream):
    for table in tbl_list:
            lineage_list = []
            lineage_list.append(most_upstream)
            tbl = insp.get_foreign_keys(table)
            #check if tables has any foreign key 
            if not tbl:
                continue
            #check if foreign key matches with upstream_table
            elif check_foreign_keys(tbl, most_upstream) == True:
                lineage_list.append(table)
                send_to_datahub(lineage_list) 
                new_upstream = table
                looping_lineage(new_upstream)

        
def start():
    print("started")
    for most_upstream in tbl_list:
        #check if list is empty, this is to find the table that is most upstream, and start iterating recursively to the table that is most downstream.
	#tables that are most upstream return empty, does that do not return empty have a foreign relationship with a table upstream. 
        if not insp.get_foreign_keys(most_upstream):
            looping_lineage(most_upstream)
        else: 
            continue
    print("finished")

# Uncomment below to run code
# start()

