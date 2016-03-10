#
# Copyright 2015 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#

import json
import datetime
import sys, os
import time
from org.slf4j import LoggerFactory
from wherehows.common.writers import FileWriter
from wherehows.common.schemas import DatasetSchemaRecord, DatasetFieldRecord
from wherehows.common import Constant
from HiveExtract import TableInfo
from org.apache.hadoop.hive.ql.tools import LineageInfo
from metadata.etl.dataset.hive import HiveViewDependency

from HiveColumnParser import HiveColumnParser
from AvroColumnParser import AvroColumnParser

class HiveTransform:
  def __init__(self):
    self.logger = LoggerFactory.getLogger('jython script : ' + self.__class__.__name__)

  def transform(self, input, hive_metadata, hive_field_metadata):
    """
    convert from json to csv
    :param input: input json file
    :param hive_metadata: output data file for hive table metadata
    :param hive_field_metadata: output data file for hive field metadata
    :return:
    """
    f_json = open(input)
    all_data = json.load(f_json)
    f_json.close()

    schema_file_writer = FileWriter(hive_metadata)
    field_file_writer = FileWriter(hive_field_metadata)

    lineageInfo = LineageInfo()

    # one db info : 'type', 'database', 'tables'
    # one table info : required : 'name' , 'type', 'serializationFormat' ,'createTime', 'DB_ID', 'TBL_ID', 'SD_ID'
    #                  optional : 'schemaLiteral', 'schemaUrl', 'fieldDelimiter', 'fieldList'
    for one_db_info in all_data:
      i = 0
      for table in one_db_info['tables']:
        i += 1
        schema_json = {}
        prop_json = {}  # set the prop json

        for prop_name in TableInfo.optional_prop:
          if prop_name in table and table[prop_name] is not None:
            prop_json[prop_name] = table[prop_name]

        if TableInfo.view_expended_text in prop_json:
          text = prop_json[TableInfo.view_expended_text].replace('`', '')
          array = HiveViewDependency.getViewDependency(text)
          l = []
          for a in array:
            l.append(a)
          prop_json['view_depends_on'] = l

        # process either schema
        flds = {}
        field_detail_list = []

        if TableInfo.schema_literal in table and table[TableInfo.schema_literal] is not None:
          sort_id = 0
          try:
            schema_data = json.loads(table[TableInfo.schema_literal])
          except ValueError:
            self.logger.error("Schema json error for table : \n" + str(table))
          schema_json = schema_data
          # extract fields to field record
          urn = "hive:///%s/%s" % (one_db_info['database'], table['name'])
          acp = AvroColumnParser(schema_data, urn = urn)
          result = acp.get_column_list_result()
          field_detail_list += result

        elif TableInfo.field_list in table:
          # Convert to avro
          uri = "hive:///%s/%s" % (one_db_info['database'], table['name'])
          hcp = HiveColumnParser(table, urn = uri)
          schema_json = {'fields' : hcp.column_type_dict['fields'], 'type' : 'record', 'name' : table['name'], 'uri' : uri}
          field_detail_list += hcp.column_type_list

        dataset_scehma_record = DatasetSchemaRecord(table['name'], json.dumps(schema_json), json.dumps(prop_json),
                                                    json.dumps(flds),
                                                    "hive:///%s/%s" % (one_db_info['database'], table['name']), 'Hive',
                                                    '', (table[TableInfo.create_time] if table.has_key(
            TableInfo.create_time) else None), (table["lastAlterTime"]) if table.has_key("lastAlterTime") else None)
        schema_file_writer.append(dataset_scehma_record)

        for fields in field_detail_list:
          field_record = DatasetFieldRecord(fields)
          field_file_writer.append(field_record)

      schema_file_writer.flush()
      field_file_writer.flush()
      self.logger.info("%20s contains %6d tables" % (one_db_info['database'], i))

    schema_file_writer.close()
    field_file_writer.close()

  def convert_timestamp(self, time_string):
    return int(time.mktime(time.strptime(time_string, "%Y-%m-%d %H:%M:%S")))


if __name__ == "__main__":
  args = sys.argv[1]
  t = HiveTransform()

  t.transform(args[Constant.HIVE_SCHEMA_JSON_FILE_KEY], args[Constant.HIVE_SCHEMA_CSV_FILE_KEY], args[Constant.HIVE_FIELD_METADATA_KEY])

