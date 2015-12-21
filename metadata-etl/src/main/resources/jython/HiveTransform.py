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
import pprint, datetime
import sys, os
import time
from wherehows.common.writers import FileWriter
from wherehows.common.schemas import DatasetSchemaRecord, DatasetFieldRecord
from wherehows.common import Constant
from HiveExtract import TableInfo
from org.apache.hadoop.hive.ql.tools import LineageInfo
from metadata.etl.dataset.hive import HiveViewDependency

class HiveTransform:
  def transform(self, input, hive_metadata, hive_field_metadata):
    """
    convert from json to csv
    :param input: input json file
    :param hive_metadata: output data file for hive table metadata
    :param hive_field_metadata: output data file for hive field metadata
    :return:
    """
    pp = pprint.PrettyPrinter(indent=1)

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
            print "Schema json error for table : "
            print table
          schema_json = schema_data

          # process each field
          for field in schema_data['fields']:
            field_name = field['name']
            type = field['type']  # could be a list
            default_value = field['default'] if 'default' in field else None
            doc = field['doc'] if 'doc' in field else None

            attributes_json = json.loads(field['attributes_json']) if 'attributes_json' in field else None
            pk = delta = is_nullable = is_indexed = is_partitioned = inside_type = format = data_size = None
            if attributes_json:
              pk = attributes_json['pk'] if 'pk' in attributes_json else None
              delta = attributes_json['delta'] if 'delta' in attributes_json else None
              is_nullable = attributes_json['nullable'] if 'nullable' in attributes_json else None
              inside_type = attributes_json['type'] if 'type' in attributes_json else None
              format = attributes_json['format'] if 'format' in attributes_json else None

            flds[field_name] = {'type': type}
            # String urn, Integer sortId, Integer parentSortId, String parentPath, String fieldName,
            #String dataType, String isNullable, String defaultValue, Integer dataSize, String namespace, String description
            sort_id += 1
            field_detail_list.append(
              ["hive:///%s/%s" % (one_db_info['database'], table['name']), str(sort_id), '0', None, field_name, '',
               type, data_size, None, None, is_nullable, is_indexed, is_partitioned, default_value, None,
               json.dumps(attributes_json)])
        elif TableInfo.field_list in table:
          schema_json = {'type': 'record', 'name': table['name'],
                         'fields': table[TableInfo.field_list]}  # construct a schema for data came from COLUMN_V2
          for field in table[TableInfo.field_list]:
            field_name = field['ColumnName']
            type = field['TypeName']
            # ColumnName, IntegerIndex, TypeName, Comment
            flds[field_name] = {'type': type}
            pk = delta = is_nullable = is_indexed = is_partitioned = inside_type = format = data_size = default_value = None  # TODO ingest
            field_detail_list.append(
              ["hive:///%s/%s" % (one_db_info['database'], table['name']), field['IntegerIndex'], '0', None, field_name,
               '', field['TypeName'], None, None, None, is_nullable, is_indexed, is_partitioned, default_value, None,
               None])

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
      print "%20s contains %6d tables" % (one_db_info['database'], i)

    schema_file_writer.close()
    field_file_writer.close()

  def convert_timestamp(self, time_string):
    return int(time.mktime(time.strptime(time_string, "%Y-%m-%d %H:%M:%S")))


if __name__ == "__main__":
  args = sys.argv[1]
  t = HiveTransform()

  t.transform(args[Constant.HIVE_SCHEMA_JSON_FILE_KEY], args[Constant.HIVE_SCHEMA_CSV_FILE_KEY], args[Constant.HIVE_FIELD_METADATA_KEY])

